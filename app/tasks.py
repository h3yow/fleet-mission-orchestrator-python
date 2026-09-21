"""任务下发（幂等）、队列与抢占。

幂等语义：
- 同一 ``task_id`` 重复下发且内容一致 → 返回首次结果（``idempotent_hit=True``），
  绝不重复执行；
- 同一 ``task_id`` 携带不同内容（payload / 能力要求 / 抢占性 / 重试上限）→
  明确拒绝 ``TASK_DUPLICATE_CONFLICT``；
- 内容指纹不包含 priority（见 submit 的 priority 语义）。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Iterable, Protocol

from .config import SchedulerConfig
from .models import EnqueueResult, Task, TaskState
from .storage import Storage
from .timeutil import Clock


class TaskManager:
    def __init__(self, storage: Storage, clock: Clock,
                 config: SchedulerConfig) -> None:
        self.storage = storage
        self.clock = clock
        self.config = config
        self._tasks: dict[str, Task] = {}
        self._queue: list[str] = []

    # -- 命令 --
    def submit(self, task_id: str, payload: dict[str, Any],
               required_capabilities: Iterable[str] = (),
               priority: int = 0, preemptible: bool = True,
               max_retries: int = 0,
               timeout_ms: int | None = None) -> EnqueueResult:
        raise NotImplementedError

    def queue_push(self, task_id: str) -> None:
        raise NotImplementedError

    def queue_pop(self) -> str | None:
        raise NotImplementedError

    def preempt(self, task_id: str, by_robot_id: str,
                at_ms: int) -> Task:
        raise NotImplementedError

    def mark_failed(self, task_id: str, reason: str) -> Task:
        raise NotImplementedError

    # -- 查询 --
    claimable_states = frozenset({TaskState.PENDING, TaskState.QUEUED,
                                  TaskState.PREEMPTED})

    def get(self, task_id: str) -> Task:
        raise NotImplementedError

    def list_tasks(self) -> list[Task]:
        raise NotImplementedError

    def queue_size(self) -> int:
        raise NotImplementedError

    # -- 事件投影（恢复回放用） --
    def apply(self, event_type: str, data: dict) -> None:
        raise NotImplementedError
