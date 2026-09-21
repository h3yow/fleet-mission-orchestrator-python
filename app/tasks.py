"""任务下发（幂等）、有界优先级队列与抢占标记。

幂等语义（关键）：
- 同一 ``task_id`` 重复下发且内容完全一致 → 返回首次结果
  （``idempotent_hit=True``），绝不重复执行，最终结果也与首次一致；
- 同一 ``task_id`` 携带不同内容（payload / 能力要求 / priority /
  抢占性 / max_retries / timeout_ms 任一不同）→ 明确拒绝
  ``TASK_DUPLICATE_CONFLICT``，错误消息逐一列出差异字段作为判定依据；
- 队列有界：满员时拒绝 ``QUEUE_FULL``，不创建幽灵任务、不无界占用资源。
"""
from __future__ import annotations

import hashlib
import heapq
import json
from typing import Any, Iterable

from .config import SchedulerConfig
from .errors import DomainError, RejectCode
from .models import EnqueueResult, Task, TaskState, SCHEMA_VERSION
from .storage import EventType, Storage
from .timeutil import Clock

_AT = "_at_ms"
# 参与“内容一致性”判定的字段（priority 也在内：重复下发不得变相改参）
_CONTENT_KEYS = ("payload", "required_capabilities", "priority",
                 "preemptible", "max_retries", "timeout_ms")


def fingerprint(payload: dict[str, Any],
                required_capabilities: frozenset[str],
                priority: int, preemptible: bool,
                max_retries: int, timeout_ms: int | None) -> str:
    body = {
        "payload": payload,
        "caps": sorted(required_capabilities),
        "priority": priority,
        "preemptible": preemptible,
        "max_retries": max_retries,
        "timeout_ms": timeout_ms,
    }
    raw = json.dumps(body, sort_keys=True, separators=(",", ":"),
                     ensure_ascii=False)
    return hashlib.sha256(raw.encode("utf-8")).hexdigest()


class TaskManager:
    def __init__(self, storage: Storage, clock: Clock,
                 config: SchedulerConfig) -> None:
        self.storage = storage
        self.clock = clock
        self.config = config
        self._tasks: dict[str, Task] = {}
        # 堆项: (-priority, 抢占续跑组(0)或普通组(1), 入队序号, task_id)
        self._heap: list[tuple[int, int, int, str]] = []
        self._queued_ids: set[str] = set()
        self._enq_counter = 0

    # ---------------- 下发 ----------------
    def submit(self, task_id: str, payload: dict[str, Any],
               required_capabilities: Iterable[str] = (),
               priority: int = 0, preemptible: bool = True,
               max_retries: int = 0,
               timeout_ms: int | None = None) -> EnqueueResult:
        if not isinstance(task_id, str) or not task_id:
            raise DomainError(RejectCode.INVALID_CONFIG, "task_id 不能为空")
        if not isinstance(payload, dict):
            raise DomainError(RejectCode.INVALID_CONFIG,
                              "payload 必须为 JSON 对象")
        caps = frozenset(required_capabilities)
        priority = self.config.validate_priority(priority)
        max_retries = self.config.validate_retries(max_retries)
        timeout_ms = self.config.validate_timeout_ms(timeout_ms)

        existing = self._tasks.get(task_id)
        fp = fingerprint(payload, caps, priority, preemptible,
                         max_retries, timeout_ms)
        if existing is not None:
            if existing.state in (TaskState.COMPLETED, TaskState.FAILED):
                # 终态任务重复下发：仅在内容一致时返回原结果，绝不重跑
                if self._content_matches(existing, payload, caps, priority,
                                         preemptible, max_retries, timeout_ms):
                    return EnqueueResult(existing, idempotent_hit=True)
                raise DomainError(
                    RejectCode.TASK_DUPLICATE_CONFLICT,
                    self._conflict_reason(task_id, existing, payload, caps,
                                          priority, preemptible, max_retries,
                                          timeout_ms))
            if fp != self._fingerprint_of(existing):
                raise DomainError(
                    RejectCode.TASK_DUPLICATE_CONFLICT,
                    self._conflict_reason(task_id, existing, payload, caps,
                                          priority, preemptible, max_retries,
                                          timeout_ms))
            return EnqueueResult(existing, idempotent_hit=True)

        if len(self._queued_ids) >= self.config.max_queue_size:
            raise DomainError(
                RejectCode.QUEUE_FULL,
                f"任务队列已满（上限 {self.config.max_queue_size}），"
                f"任务 {task_id} 被拒绝；调用方可稍后重试")

        now = self.clock.now_ms()
        data = {
            "task_id": task_id, "payload": payload,
            "required_capabilities": sorted(caps), "priority": priority,
            "preemptible": preemptible, "max_retries": max_retries,
            "timeout_ms": timeout_ms, "fingerprint": fp,
            "state": TaskState.QUEUED.value, "v": SCHEMA_VERSION, _AT: now,
        }
        evt = self.storage.append(EventType.TASK_SUBMITTED, now, data)
        self.apply(evt.event_type, evt.data)
        return EnqueueResult(self._tasks[task_id], idempotent_hit=False)

    # ---------------- 队列 ----------------
    def queue_push(self, task_id: str, *, resumed: bool = False) -> None:
        task = self._require(task_id)
        if task_id in self._queued_ids:
            return  # 幂等：已在队中
        if len(self._queued_ids) >= self.config.max_queue_size:
            raise DomainError(
                RejectCode.QUEUE_FULL,
                f"任务队列已满（上限 {self.config.max_queue_size}），"
                f"回收任务 {task_id} 无法重新入队")
        self._enq_counter += 1
        group = 0 if resumed else 1
        heapq.heappush(self._heap,
                       (-task.priority, group, self._enq_counter, task_id))
        self._queued_ids.add(task_id)

    def queue_peek_order(self) -> list[str]:
        """按调度顺序返回队中任务 ID（不弹出）。"""
        return [tid for _, _, _, tid in sorted(self._heap)]

    def queue_pop_id(self) -> str | None:
        while self._heap:
            _, _, _, task_id = heapq.heappop(self._heap)
            if task_id in self._queued_ids:
                self._queued_ids.discard(task_id)
                return task_id
        return None

    def queue_size(self) -> int:
        return len(self._queued_ids)

    # ---------------- 抢占 / 终态 ----------------
    def preempt(self, task_id: str, by_robot_id: str,
                at_ms: int) -> Task:
        task = self._require(task_id)
        if task.state not in (TaskState.LEASED, TaskState.RUNNING):
            raise DomainError(
                RejectCode.TASK_INVALID_TRANSITION,
                f"任务 {task_id} 当前状态 {task.state.value} 不可抢占"
                "（仅 LEASED/RUNNING 可抢占）")
        if not task.preemptible:
            raise DomainError(
                RejectCode.TASK_NOT_PREEMPTIBLE,
                f"任务 {task_id} 声明为不可抢占（preemptible=False），"
                "任何执行者不得被剥夺")
        data = {"task_id": task_id, "by_robot_id": by_robot_id,
                "progress": dict(task.progress), _AT: at_ms}
        evt = self.storage.append(EventType.TASK_PREEMPTED, at_ms, data)
        self.apply(evt.event_type, evt.data)
        return self._tasks[task_id]

    def mark_failed(self, task_id: str, reason: str) -> Task:
        task = self._require(task_id)
        now = self.clock.now_ms()
        data = {"task_id": task_id, "reason": reason, _AT: now}
        evt = self.storage.append(EventType.TASK_FAILED, now, data)
        self.apply(evt.event_type, evt.data)
        return self._tasks[task_id]

    # ---------------- 查询 ----------------
    def get(self, task_id: str) -> Task:
        return self._require(task_id)

    def try_get(self, task_id: str) -> Task | None:
        return self._tasks.get(task_id)

    def list_tasks(self) -> list[Task]:
        return list(self._tasks.values())

    def _require(self, task_id: str) -> Task:
        task = self._tasks.get(task_id)
        if task is None:
            raise DomainError(
                RejectCode.TASK_NOT_FOUND, f"任务 {task_id} 不存在")
        return task

    # ---------------- 一致性辅助 ----------------
    @staticmethod
    def _fingerprint_of(task: Task) -> str:
        return fingerprint(task.payload, task.required_capabilities,
                           task.priority, task.preemptible,
                           task.max_retries, task.timeout_ms)

    @staticmethod
    def _content_matches(task: Task, payload: dict[str, Any],
                         caps: frozenset[str], priority: int,
                         preemptible: bool, max_retries: int,
                         timeout_ms: int | None) -> bool:
        return (task.payload == payload
                and task.required_capabilities == caps
                and task.priority == priority
                and task.preemptible == preemptible
                and task.max_retries == max_retries
                and task.timeout_ms == timeout_ms)

    def _conflict_reason(self, task_id: str, task: Task,
                         payload: dict[str, Any], caps: frozenset[str],
                         priority: int, preemptible: bool,
                         max_retries: int,
                         timeout_ms: int | None) -> str:
        diffs: list[str] = []
        if task.payload != payload:
            diffs.append(f"payload: 首次={task.payload!r} 本次={payload!r}")
        if task.required_capabilities != caps:
            diffs.append("required_capabilities: "
                         f"首次={sorted(task.required_capabilities)} "
                         f"本次={sorted(caps)}")
        if task.priority != priority:
            diffs.append(f"priority: 首次={task.priority} 本次={priority}")
        if task.preemptible != preemptible:
            diffs.append(f"preemptible: 首次={task.preemptible} "
                         f"本次={preemptible}")
        if task.max_retries != max_retries:
            diffs.append(
                f"max_retries: 首次={task.max_retries} 本次={max_retries}")
        if task.timeout_ms != timeout_ms:
            diffs.append(
                f"timeout_ms: 首次={task.timeout_ms} 本次={timeout_ms}")
        return (f"任务 {task_id} 已存在但内容不一致，差异字段："
                + "; ".join(diffs))

    # ---------------- 事件投影 ----------------
    def apply(self, event_type: str, data: dict) -> None:
        if event_type == EventType.TASK_SUBMITTED:
            tid = data["task_id"]
            if tid in self._tasks:
                return  # 重放幂等保护
            unknown = {k: v for k, v in data.items()
                       if k not in {"task_id", "payload",
                                    "required_capabilities", "priority",
                                    "preemptible", "max_retries",
                                    "timeout_ms", "fingerprint", "state",
                                    "v", _AT}}
            task = Task(
                task_id=tid,
                payload=data.get("payload", {}),
                required_capabilities=frozenset(
                    data.get("required_capabilities", ())),
                priority=int(data.get("priority", 0)),
                preemptible=bool(data.get("preemptible", True)),
                max_retries=int(data.get("max_retries", 0)),
                timeout_ms=data.get("timeout_ms"),
                state=TaskState.QUEUED,
                progress={},
                schema_version=int(data.get("v", SCHEMA_VERSION)),
                unknown_fields=unknown)
            self._tasks[tid] = task
            self.queue_push(tid)
            return

        tid = data.get("task_id")
        if tid is None:
            return
        task = self._tasks.get(tid)
        if task is None:
            raise DomainError(
                RejectCode.CORRUPT_RECORD,
                f"事件 {event_type} 引用了不存在的任务 {tid!r}")

        if event_type == EventType.TASK_CLAIMED:
            task.state = TaskState.LEASED
            task.attempt = int(data.get("attempt", task.attempt + 1))
            self._queued_ids.discard(tid)
        elif event_type == EventType.TASK_STARTED:
            task.state = TaskState.RUNNING
        elif event_type == EventType.LEASE_RENEWED:
            pass  # 租约续期不改任务状态
        elif event_type == EventType.TASK_PROGRESS:
            # 命令侧已完成 merge，事件里是确定的最终快照
            task.progress = dict(data.get("progress", {}))
        elif event_type == EventType.TASK_PREEMPTED:
            task.state = TaskState.PREEMPTED
            task.progress = dict(data.get("progress", task.progress))
        elif event_type == EventType.TASK_COMPLETED:
            task.state = TaskState.COMPLETED
            task.result = dict(data.get("result", {}))
            task.progress = dict(data.get("progress", task.progress))
            self._queued_ids.discard(tid)
        elif event_type == EventType.TASK_RECLAIMED:
            # 租约被回收：保留进度，等待重新调度入队
            task.state = TaskState.QUEUED
            self.queue_push(tid, resumed=bool(data.get("resumed", True)))
        elif event_type == EventType.TASK_RESCHEDULED:
            task.state = TaskState.QUEUED
            self.queue_push(tid, resumed=bool(data.get("resumed", False)))
        elif event_type == EventType.TASK_FAILED:
            task.state = TaskState.FAILED
            self._queued_ids.discard(tid)
