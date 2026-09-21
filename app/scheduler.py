"""调度器：领取（拉模式）、抢占、超时回收与重试。

调度规则：
- ``claim_next``：机器人按能力/可用性从有界队列按优先级
  （高者优先，同级 FIFO，被抢占续跑任务在同级最前）领取；
  队头若因能力不匹配不能领取，则继续尝试后续候选（不阻塞队列），
  全部不匹配返回 None（调用方稍后再拉，即排队/降级）。
- ``request_preempt``：排队中的高优先级任务可让合格空闲机器人
  抢占某个在执行、可抢占且优先级严格更低的任务；被抢占任务回队、
  保留进度；不可抢占 / 无资格 / 非严格更高优先级 → 各自给出可区分原因。
- ``reap_stale_leases``：对心跳超时机器人的租约，以及自身过期的租约
  安全回收，按 ``max_retries`` 重试或转 FAILED。每次判定都落
  ``SCHEDULER_DECISION`` 事件，可解释、可回放。
"""
from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field
from typing import Any, TYPE_CHECKING

from .errors import DomainError, RejectCode
from .models import Lease, LeaseState, Robot, RobotState, Task, TaskState
from .storage import EventType, Storage
from .timeutil import Clock

if TYPE_CHECKING:
    from .leases import ClaimResult, LeaseManager
    from .registry import RobotRegistry
    from .tasks import TaskManager


@dataclass
class Decision:
    """一条可回放的调度判定依据。"""
    action: str
    at_ms: int
    task_id: str | None = None
    robot_id: str | None = None
    reason: str = ""
    detail: dict[str, Any] = field(default_factory=dict)


class Scheduler:
    def __init__(self, storage: Storage, clock: Clock,
                 config: Any, registry: "RobotRegistry",
                 tasks: "TaskManager", leases: "LeaseManager") -> None:
        self.storage = storage
        self.clock = clock
        self.config = config
        self.registry = registry
        self.tasks = tasks
        self.leases = leases
        self.bus: Any = None
        self._decisions: list[Decision] = []

    # --------------- 发布决策（可回放） ---------------
    def _decide(self, action: str, reason: str,
                task_id: str | None = None, robot_id: str | None = None,
                detail: dict[str, Any] | None = None) -> Decision:
        d = Decision(action=action, at_ms=self.clock.now_ms(),
                     task_id=task_id, robot_id=robot_id, reason=reason,
                     detail=detail or {})
        self.bus.publish(EventType.SCHEDULER_DECISION, {
            "decision": json.dumps(asdict(d), ensure_ascii=False)})
        return d

    # --------------- 拉模式领取 ---------------
    def claim_next(self, robot_id: str) -> "ClaimResult | None":
        robot = self.registry.get(robot_id)
        if robot.state == RobotState.DEREGISTERED:
            raise DomainError(
                RejectCode.ROBOT_UNAVAILABLE,
                f"机器人 {robot_id} 已注销，不得领取任务")
        if robot.state == RobotState.DRAINING:
            raise DomainError(
                RejectCode.ROBOT_UNAVAILABLE,
                f"机器人 {robot_id} 处于排空（DRAINING）状态，"
                "不领取新任务（可继续完成手头任务）")
        if robot.state == RobotState.STALE:
            raise DomainError(
                RejectCode.ROBOT_STALE,
                f"机器人 {robot_id} 心跳超时（STALE），"
                "需先恢复心跳（REGISTERED）方可领取")
        if robot.state != RobotState.REGISTERED:
            raise DomainError(
                RejectCode.ROBOT_UNAVAILABLE,
                f"机器人 {robot_id} 状态 {robot.state.value}，不可领取任务")
        if robot.current_task_id is not None:
            raise DomainError(
                RejectCode.ROBOT_UNAVAILABLE,
                f"机器人 {robot_id} 已持有任务 {robot.current_task_id}，"
                "不得重复领取")

        order = self.tasks.queue_peek_order()
        skipped: list[str] = []
        for task_id in order:
            task = self.tasks.try_get(task_id)
            if task is None or task_id not in self.tasks._queued_ids:
                continue
            if not task.required_capabilities <= robot.capabilities:
                skipped.append(task_id)
                continue
            # 命中合格候选：按 ID 取出（惰性删除堆项），授予租约
            if not self.tasks.queue_take(task_id):
                continue  # 已被并发取走
            try:
                res = self.leases.claim(task, robot)
            except DomainError:
                # 极端竞态（如已被其他途径取走）：放回队首由后续轮询处理
                self.tasks.queue_push(task_id, resumed=bool(task.progress))
                raise
            self._decide(
                "CLAIM_GRANTED",
                f"任务 {task_id} 授予机器人 {robot_id} "
                f"token={res.lease.fencing_token} "
                f"attempt={res.lease.attempt} resumed={res.resumed}",
                task_id, robot_id,
                {"fencing_token": res.lease.fencing_token,
                 "attempt": res.lease.attempt, "skipped": skipped})
            return res

        self._decide(
            "CLAIM_NO_MATCH",
            f"机器人 {robot_id} 能力 {sorted(robot.capabilities)} "
            f"在 {len(order)} 个排队任务中无合格候选"
            + (f"，跳过能力不匹配={skipped}" if skipped else ""),
            None, robot_id, {"queued": order, "skipped": skipped})
        return None

    # --------------- 抢占 ---------------
    def request_preempt(self, high_prio_task_id: str,
                        victim_task_id: str,
                        by_robot_id: str) -> Task:
        high = self.tasks.get(high_prio_task_id)
        victim = self.tasks.get(victim_task_id)
        robot = self.registry.get(by_robot_id)

        if high.state not in (TaskState.QUEUED, TaskState.PREEMPTED):
            raise DomainError(
                RejectCode.TASK_INVALID_TRANSITION,
                f"抢占任务 {high_prio_task_id} 状态 {high.state.value}，"
                "必须处于排队中（QUEUED/PREEMPTED）才能发起抢占")
        if victim.state not in (TaskState.LEASED, TaskState.RUNNING):
            raise DomainError(
                RejectCode.PREEMPT_NO_ELIGIBLE_VICTIM,
                f"被抢占任务 {victim_task_id} 状态 {victim.state.value}，"
                "不是在执行任务，无可抢占租约")
        if not victim.preemptible:
            raise DomainError(
                RejectCode.TASK_NOT_PREEMPTIBLE,
                f"被抢占任务 {victim_task_id} 声明 preemptible=False，"
                "不可被任意剥夺")
        if high.priority <= victim.priority:
            raise DomainError(
                RejectCode.PREEMPT_NO_ELIGIBLE_VICTIM,
                f"抢占要求优先级严格更高：发起方优先级 {high.priority} "
                f"<= 受害者 {victim.priority}")
        if robot.state != RobotState.REGISTERED:
            raise DomainError(
                RejectCode.ROBOT_UNAVAILABLE,
                f"发起机器人 {by_robot_id} 状态 {robot.state.value}，"
                "抢占后需由其立即领取，故必须 REGISTERED 且空闲")
        if robot.current_task_id is not None:
            raise DomainError(
                RejectCode.ROBOT_UNAVAILABLE,
                f"发起机器人 {by_robot_id} 正持有任务 "
                f"{robot.current_task_id}，不空闲")
        if not high.required_capabilities <= robot.capabilities:
            missing = sorted(high.required_capabilities - robot.capabilities)
            raise DomainError(
                RejectCode.ROBOT_CAPABILITY_MISMATCH,
                f"发起机器人 {by_robot_id} 不具备抢占任务 "
                f"{high_prio_task_id} 所需能力，缺失={missing}")

        victim_lease = self.leases.active_lease(victim_task_id)
        if victim_lease is None:
            raise DomainError(
                RejectCode.PREEMPT_NO_ELIGIBLE_VICTIM,
                f"任务 {victim_task_id} 无 ACTIVE 租约")

        # 抢占：旧租约失效，受害者任务保留进度、立即回到调度队列（同级最前）
        self.leases.preempt(victim_task_id, by_robot_id)
        self.bus.publish(EventType.TASK_RESCHEDULED, {
            "task_id": victim_task_id, "robot_id": by_robot_id,
            "reason": "preempted", "resumed": True})
        self._decide(
            "PREEMPT_GRANTED",
            f"机器人 {by_robot_id} 以优先级 {high.priority} 抢占 "
            f"{victim_task_id}(优先级 {victim.priority}, "
            f"持有者 {victim_lease.robot_id})；受害者保留进度回队",
            victim_task_id, by_robot_id,
            {"high_prio_task_id": high_prio_task_id,
             "victim_owner": victim_lease.robot_id,
             "victim_token": victim_lease.fencing_token})
        return victim

    # --------------- 超时回收 + 重试 ---------------
    def reap_stale_leases(self) -> list[Decision]:
        decisions: list[Decision] = []
        now = self.clock.now_ms()

        # 1) 心跳超时：机器人 STALE 化（仅对持有任务者回收租约）
        for robot in self.registry.list_robots():
            if robot.state in (RobotState.REGISTERED, RobotState.DRAINING):
                if robot.current_task_id is not None and (
                        now - robot.last_heartbeat_ms
                        > self.config.heartbeat_timeout_ms):
                    self.registry.mark_stale(robot.robot_id)
                    decisions.append(self._decide(
                        "ROBOT_STALE",
                        f"机器人 {robot.robot_id} 心跳超时 "
                        f"(last={robot.last_heartbeat_ms}, now={now})，"
                        "标记 STALE 并回收其任务",
                        robot.current_task_id, robot.robot_id))

        # 2) 回收所有过期/不可用持有者的 ACTIVE 租约
        for lease in list(self.leases.list_leases()):
            if lease.state not in (LeaseState.ACTIVE, LeaseState.RENEWED):
                continue
            task = self.tasks.try_get(lease.task_id)
            if task is None or task.state not in (
                    TaskState.LEASED, TaskState.RUNNING,
                    TaskState.PREEMPTED):
                continue
            reason = self._reap_reason(lease, task, now)
            if reason is None:
                continue
            decisions.extend(self._reclaim_with_retry(lease, task, reason))
        return decisions

    def _reap_reason(self, lease: Lease, task: Task, now: int) -> str | None:
        robot = self.registry.get(lease.robot_id)
        if robot.state in (RobotState.STALE, RobotState.DEREGISTERED):
            return f"holder_{robot.state.value.lower()}"
        if now > lease.expires_at_ms:
            return "lease_expired"
        if task.timeout_ms is not None:
            deadline = lease.acquired_ms + task.timeout_ms
            if now > deadline:
                return "task_timeout"
        return None

    def _reclaim_with_retry(self, lease: Lease, task: Task,
                            reason: str) -> list[Decision]:
        # 资源边界：把“在途（持有中）+ 排队”统一视为系统积压。
        # 回收重试用的任务当前由本机器人持有（不计 queue_size），
        # 故回队前必须保证 queue_size 已达上限时显式拒绝，杜绝无界积压。
        if task.attempt <= task.max_retries:
            if self.tasks.queue_size() >= self.config.max_queue_size:
                raise DomainError(
                    RejectCode.QUEUE_FULL,
                    f"任务 {task.task_id} 因 {reason} 需回收重试，"
                    f"但排队积压已达上限 {self.config.max_queue_size}；"
                    "拒绝重新入队以避免无界占用资源（调用方需扩容/降级）")
        # 回收（旧 token 失效，任务带进度回队）
        self.leases.reclaim(task.task_id, reason)
        if task.attempt <= task.max_retries:
            retry = self._decide(
                "RETRY_ENQUEUED",
                f"任务 {task.task_id} 因 {reason} 回收，"
                f"attempt={task.attempt}<=max_retries={task.max_retries}，"
                "保留进度重新排队",
                task.task_id, lease.robot_id,
                {"reason": reason, "attempt": task.attempt})
            reaped = self._decide(
                "REAP_RECLAIMED",
                f"回收 {task.task_id}（{reason}）并安排重试",
                task.task_id, lease.robot_id, {"reason": reason})
            return [retry, reaped]
        self.tasks.mark_failed(
            task.task_id,
            f"重试耗尽：attempt={task.attempt} > "
            f"max_retries={task.max_retries}，最后原因={reason}")
        return [self._decide(
            "REAP_FAILED",
            f"任务 {task.task_id} 重试耗尽转 FAILED（{reason}）",
            task.task_id, lease.robot_id, {"reason": reason})]

    # --------------- 决策回放 / 查询 ---------------
    def decisions(self) -> list[Decision]:
        return list(self._decisions)

    def apply(self, event_type: str, data: dict[str, Any]) -> None:
        """事件投影：目前只消费 SCHEDULER_DECISION，且不重复执行副作用。"""
        if event_type == EventType.SCHEDULER_DECISION:
            self.apply_decision(data)

    def apply_decision(self, evt_data: dict[str, Any]) -> None:
        """回放 SCHEDULER_DECISION：重建决策日志，不重复执行副作用。"""
        raw = evt_data.get("decision")
        if not isinstance(raw, str):
            raise DomainError(
                RejectCode.CORRUPT_RECORD,
                f"SCHEDULER_DECISION 缺少 decision 字段: {evt_data!r}")
        d = json.loads(raw)
        self._decisions.append(Decision(
            action=d["action"], at_ms=int(d["at_ms"]),
            task_id=d.get("task_id"), robot_id=d.get("robot_id"),
            reason=d.get("reason", ""), detail=dict(d.get("detail", {}))))
