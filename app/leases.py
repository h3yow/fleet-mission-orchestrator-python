"""租约管理：领取 / 开始 / 续约 / 进度 / 完成 / 抢占 / 回收 与状态机约束。

安全性质（关键）：
- 每次授予/重授租约 ``fencing_token`` 单调递增，用于拒绝陈旧持有者的写；
- ``complete`` / ``renew`` / ``report_progress`` / ``start`` 必须同时
  匹配当前租约的 robot 与 fencing_token，非持有者一律拒绝；
- 回收（reclaim）/抢占使旧 token 失效；旧持有者之后再上报将被
  ``TASK_NOT_LEASE_HOLDER`` 拒绝，杜绝脑裂双执行；
- 租约过期后（``now > expires_at_ms``）持有者操作按 ``LEASE_EXPIRED`` 拒绝；
- 心跳、完成、续约与回收并发时，全部经事件总线串行化为 WAL 全序，
  同一任务同一时刻至多一个 ACTIVE 租约。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TYPE_CHECKING

from .config import SchedulerConfig
from .errors import DomainError, RejectCode
from .models import (Lease, LeaseState, Robot, RobotState, Task, TaskState,
                     SCHEMA_VERSION)
from .storage import Event, EventType, Storage
from .timeutil import Clock

if TYPE_CHECKING:
    from .events import EventBus

_AT = "_at_ms"


@dataclass
class ClaimResult:
    task: Task
    lease: Lease
    resumed: bool

    @property
    def progress(self) -> dict[str, Any]:
        return self.task.progress


class LeaseManager:
    def __init__(self, storage: Storage, clock: Clock,
                 config: SchedulerConfig) -> None:
        self.storage = storage
        self.clock = clock
        self.config = config
        self.bus: "EventBus | None" = None
        self._leases: dict[str, Lease] = {}
        self._active_by_task: dict[str, str] = {}
        self._terminal_by_task: dict[str, str] = {}
        self._token_seq = 0

    # --------------- 发布辅助 ---------------
    def _publish(self, event_type: str, data: dict[str, Any],
                 at_ms: int | None = None) -> Event:
        if self.bus is not None:
            return self.bus.publish(event_type, data, at_ms)
        at_ms = self.clock.now_ms() if at_ms is None else at_ms
        payload = dict(data)
        payload.setdefault(_AT, at_ms)
        evt = self.storage.append(event_type, at_ms, payload)
        self.apply(evt.event_type, evt.data)
        return evt

    # --------------- 领取 ---------------
    def claim(self, task: Task, robot: Robot) -> ClaimResult:
        if task.state not in (TaskState.QUEUED, TaskState.PREEMPTED):
            raise DomainError(
                RejectCode.TASK_NOT_CLAIMABLE,
                f"任务 {task.task_id} 当前状态 {task.state.value}，"
                "仅 QUEUED/PREEMPTED 可被领取")
        if robot.state != RobotState.REGISTERED:
            raise DomainError(
                RejectCode.ROBOT_UNAVAILABLE,
                f"机器人 {robot.robot_id} 状态 {robot.state.value}，"
                "不可领取任务（需 REGISTERED）")
        if robot.current_task_id is not None:
            raise DomainError(
                RejectCode.ROBOT_UNAVAILABLE,
                f"机器人 {robot.robot_id} 已持有任务 "
                f"{robot.current_task_id}，不得重复领取")
        if not task.required_capabilities <= robot.capabilities:
            missing = sorted(task.required_capabilities - robot.capabilities)
            raise DomainError(
                RejectCode.ROBOT_CAPABILITY_MISMATCH,
                f"机器人 {robot.robot_id} 能力 {sorted(robot.capabilities)} "
                f"不满足任务 {task.task_id} 要求，缺失能力={missing}")

        now = self.clock.now_ms()
        token = self.next_fencing_token()
        attempt = task.attempt + 1
        if attempt > self.config.max_attempts:
            raise DomainError(
                RejectCode.QUEUE_FULL,
                f"任务 {task.task_id} 尝试次数将达 {attempt}，"
                f"超过上限 {self.config.max_attempts}")
        lease_id = f"lease-{task.task_id}-{attempt}"
        resumed = task.state == TaskState.PREEMPTED or bool(task.progress)
        data = {
            "lease_id": lease_id, "task_id": task.task_id,
            "robot_id": robot.robot_id, "fencing_token": token,
            "attempt": attempt, "acquired_ms": now,
            "expires_at_ms": now + self.config.lease_duration_ms,
            "lease_state": LeaseState.ACTIVE.value,
            "resumed": resumed, "v": SCHEMA_VERSION,
        }
        self._publish(EventType.TASK_CLAIMED, data, now)
        return ClaimResult(self._task_after(task.task_id),
                           self.get(lease_id), resumed)

    # --------------- 开始 ---------------
    def start(self, task_id: str, robot_id: str,
              fencing_token: int) -> Task:
        lease = self._require_active(task_id, robot_id, fencing_token)
        task = self._task_after(task_id)
        if task.state != TaskState.LEASED:
            raise DomainError(
                RejectCode.TASK_INVALID_TRANSITION,
                f"任务 {task_id} 状态 {task.state.value}，"
                "仅 LEASED 可 start 为 RUNNING")
        self._publish(EventType.TASK_STARTED, {
            "task_id": task_id, "robot_id": robot_id,
            "fencing_token": fencing_token,
            "lease_id": lease.lease_id})
        return self._task_after(task_id)

    # --------------- 续约 ---------------
    def renew(self, task_id: str, robot_id: str,
              fencing_token: int, duration_ms: int | None = None) -> Lease:
        lease = self._require_active(task_id, robot_id, fencing_token)
        duration = (self.config.lease_duration_ms
                    if duration_ms is None else duration_ms)
        if not isinstance(duration, int) or isinstance(duration, bool) \
                or duration <= 0:
            raise DomainError(
                RejectCode.INVALID_CONFIG,
                f"续约时长必须为正整数，实际={duration!r}")
        if duration > self.config.max_lease_duration_ms:
            raise DomainError(
                RejectCode.INVALID_CONFIG,
                f"续约时长 {duration} 超过上限 "
                f"{self.config.max_lease_duration_ms}")
        now = self.clock.now_ms()
        new_expiry = max(lease.expires_at_ms, now + duration)
        self._publish(EventType.LEASE_RENEWED, {
            "task_id": task_id, "robot_id": robot_id,
            "fencing_token": fencing_token, "lease_id": lease.lease_id,
            "expires_at_ms": new_expiry,
            "lease_state": LeaseState.RENEWED.value}, now)
        return self.get(lease.lease_id)

    # --------------- 进度 ---------------
    def report_progress(self, task_id: str, robot_id: str,
                        fencing_token: int,
                        progress: dict[str, Any]) -> Task:
        lease = self._require_active(task_id, robot_id, fencing_token)
        task = self._task_after(task_id)
        if task.state not in (TaskState.LEASED, TaskState.RUNNING):
            raise DomainError(
                RejectCode.TASK_INVALID_TRANSITION,
                f"任务 {task_id} 状态 {task.state.value}，不可上报进度")
        merged = dict(task.progress)
        merged.update(progress)  # 浅合并：以确定快照落盘
        self._publish(EventType.TASK_PROGRESS, {
            "task_id": task_id, "robot_id": robot_id,
            "fencing_token": fencing_token, "lease_id": lease.lease_id,
            "progress": merged}, self.clock.now_ms())
        return self._task_after(task_id)

    # --------------- 完成 ---------------
    def complete(self, task_id: str, robot_id: str,
                 fencing_token: int, result: dict[str, Any]) -> Task:
        task = self._task_after(task_id)
        # 终态幂等：同一持有者（含匹配的 fencing token）重复上报完成 → 返回首次结果
        if task.state == TaskState.COMPLETED:
            t_lease_id = self._terminal_by_task.get(task_id)
            t_lease = self._leases.get(t_lease_id) if t_lease_id else None
            if (t_lease is not None and t_lease.robot_id == robot_id
                    and t_lease.fencing_token == fencing_token):
                return task
        lease = self._require_active(task_id, robot_id, fencing_token)
        if task.state not in (TaskState.LEASED, TaskState.RUNNING):
            raise DomainError(
                RejectCode.TASK_INVALID_TRANSITION,
                f"任务 {task_id} 状态 {task.state.value}，不可完成")
        self._publish(EventType.TASK_COMPLETED, {
            "task_id": task_id, "robot_id": robot_id,
            "fencing_token": fencing_token, "lease_id": lease.lease_id,
            "result": dict(result), "progress": dict(task.progress)},
            self.clock.now_ms())
        return self._task_after(task_id)

    # --------------- 抢占（租约视角） ---------------
    def preempt(self, task_id: str, by_robot_id: str) -> Lease:
        lease = self._leases.get(self._active_by_task.get(task_id, ""))
        if lease is None:
            raise DomainError(
                RejectCode.TASK_NOT_CLAIMABLE,
                f"任务 {task_id} 无 ACTIVE 租约，无法抢占")
        task = self._task_after(task_id)
        if not task.preemptible:
            raise DomainError(
                RejectCode.TASK_NOT_PREEMPTIBLE,
                f"任务 {task_id} 不可抢占（preemptible=False）")
        if task.state not in (TaskState.LEASED, TaskState.RUNNING):
            raise DomainError(
                RejectCode.TASK_INVALID_TRANSITION,
                f"任务 {task_id} 状态 {task.state.value}，不可抢占")
        now = self.clock.now_ms()
        self._publish(EventType.TASK_PREEMPTED, {
            "task_id": task_id, "robot_id": lease.robot_id,
            "by_robot_id": by_robot_id,
            "fencing_token": lease.fencing_token,
            "lease_id": lease.lease_id,
            "lease_state": LeaseState.PREEMPTED.value,
            "progress": dict(task.progress)}, now)
        return self.get(lease.lease_id)

    # --------------- 回收 ---------------
    def reclaim(self, task_id: str, reason: str) -> Lease:
        task = self._task_after(task_id)
        if task.state in (TaskState.COMPLETED, TaskState.FAILED):
            raise DomainError(
                RejectCode.TASK_INVALID_TRANSITION,
                f"任务 {task_id} 已终态 {task.state.value}，不可回收")
        lease_id = self._active_by_task.get(task_id)
        lease = self._leases.get(lease_id) if lease_id else None
        if lease is None:
            # 无 ACTIVE 租约：幂等安全返回（可能已被并发回收）
            raise DomainError(
                RejectCode.TASK_NOT_CLAIMABLE,
                f"任务 {task_id} 当前无 ACTIVE 租约可回收")
        now = self.clock.now_ms()
        self._publish(EventType.TASK_RECLAIMED, {
            "task_id": task_id, "robot_id": lease.robot_id,
            "fencing_token": lease.fencing_token, "lease_id": lease.lease_id,
            "lease_state": LeaseState.EXPIRED.value,
            "reason": reason, "resumed": True}, now)
        return self.get(lease.lease_id)

    # --------------- 持有者校验（核心安全闸） ---------------
    def _require_active(self, task_id: str, robot_id: str,
                        fencing_token: int) -> Lease:
        lease_id = self._active_by_task.get(task_id)
        lease = self._leases.get(lease_id) if lease_id else None
        if lease is None:
            raise DomainError(
                RejectCode.TASK_NOT_LEASE_HOLDER,
                f"任务 {task_id} 当前不存在 ACTIVE 租约；"
                f"提交者 robot={robot_id} token={fencing_token} 被拒绝")
        if lease.robot_id != robot_id:
            raise DomainError(
                RejectCode.TASK_NOT_LEASE_HOLDER,
                f"机器人 {robot_id} 不是任务 {task_id} 的租约持有者"
                f"（持有者={lease.robot_id}），操作被拒绝")
        if lease.fencing_token != fencing_token:
            raise DomainError(
                RejectCode.LEASE_VERSION_CONFLICT,
                f"租约 fencing token 不匹配：当前="
                f"{lease.fencing_token} 提交={fencing_token}，"
                "判定为陈旧持有者（租约已被回收/重授），操作被拒绝")
        if lease.state != LeaseState.ACTIVE and not (
                lease.state == LeaseState.RENEWED):
            raise DomainError(
                RejectCode.LEASE_EXPIRED,
                f"租约 {lease.lease_id} 状态 {lease.state.value}，"
                "操作被拒绝")
        if self.clock.now_ms() > lease.expires_at_ms:
            raise DomainError(
                RejectCode.LEASE_EXPIRED,
                f"租约 {lease.lease_id} 已于 {lease.expires_at_ms} 过期"
                f"（当前 {self.clock.now_ms()}），请等待回收后重新领取")
        return lease

    # --------------- 令牌 / 查询 ---------------
    def next_fencing_token(self) -> int:
        self._token_seq += 1
        return self._token_seq

    def active_lease(self, task_id: str) -> Lease | None:
        lease_id = self._active_by_task.get(task_id)
        lease = self._leases.get(lease_id) if lease_id else None
        if lease is None:
            return None
        if lease.state not in (LeaseState.ACTIVE, LeaseState.RENEWED):
            return None
        return lease

    def is_expired(self, lease: Lease) -> bool:
        return self.clock.now_ms() > lease.expires_at_ms

    def get(self, lease_id: str) -> Lease:
        lease = self._leases.get(lease_id)
        if lease is None:
            raise DomainError(
                RejectCode.LEASE_EXPIRED, f"租约 {lease_id} 不存在")
        return lease

    def list_leases(self) -> list[Lease]:
        return list(self._leases.values())

    # --------------- 取任务（避免与 TaskManager 循环依赖） ---------------
    _task_lookup: Any = None

    def _task_after(self, task_id: str) -> Task:
        if self._task_lookup is None:
            raise DomainError(
                RejectCode.TASK_NOT_FOUND,
                f"任务 {task_id}（未装配任务查询）")
        return self._task_lookup(task_id)

    # --------------- 事件投影 ---------------
    def apply(self, event_type: str, data: dict) -> None:
        if event_type == EventType.TASK_CLAIMED:
            lease = Lease(
                lease_id=data["lease_id"], task_id=data["task_id"],
                robot_id=data["robot_id"], state=LeaseState.ACTIVE,
                acquired_ms=int(data["acquired_ms"]),
                expires_at_ms=int(data["expires_at_ms"]),
                attempt=int(data["attempt"]),
                fencing_token=int(data["fencing_token"]),
                schema_version=int(data.get("v", SCHEMA_VERSION)))
            self._leases[lease.lease_id] = lease
            self._active_by_task[lease.task_id] = lease.lease_id
            self._terminal_by_task.pop(lease.task_id, None)
            self._token_seq = max(self._token_seq, lease.fencing_token)
            return

        tid = data.get("task_id")
        lease_id = data.get("lease_id") or self._active_by_task.get(tid)
        lease = self._leases.get(lease_id) if lease_id else None
        if lease is None:
            return  # 非租约相关事件或已被投影

        if event_type == EventType.LEASE_RENEWED:
            lease.expires_at_ms = int(data["expires_at_ms"])
            lease.state = LeaseState.ACTIVE
        elif event_type == EventType.TASK_PREEMPTED:
            lease.state = LeaseState.PREEMPTED
            self._active_by_task.pop(tid, None)
        elif event_type == EventType.TASK_COMPLETED:
            lease.state = LeaseState.COMPLETED
            self._active_by_task.pop(tid, None)
            self._terminal_by_task[tid] = lease.lease_id
        elif event_type == EventType.TASK_RECLAIMED:
            lease.state = LeaseState.EXPIRED
            self._active_by_task.pop(tid, None)
