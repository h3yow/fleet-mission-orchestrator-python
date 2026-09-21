"""租约管理：领取 / 续约 / 完成 / 进度 / 回收 / 状态机约束。

安全性质（关键）：
- 每次授予/重授租约 ``fencing_token`` 单调递增，用于拒绝陈旧持有者的写；
- ``complete`` / ``renew`` / ``report_progress`` / ``start`` 必须同时
  匹配当前租约的 robot 与 fencing_token，非持有者一律拒绝；
- 回收（reclaim）使旧 token 失效；旧持有者之后再上报将被
  ``TASK_NOT_LEASE_HOLDER`` 拒绝；
- 心跳、完成上报、续约与回收并发时，以 WAL 事件顺序为准，
  同一任务同一时刻只存在一个 ACTIVE 租约。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from .config import SchedulerConfig
from .models import Lease, Robot, Task
from .storage import Storage
from .timeutil import Clock


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
        self._leases: dict[str, Lease] = {}
        self._active_by_task: dict[str, str] = {}
        self._token_seq = 0

    # -- 命令 --
    def claim(self, task: Task, robot: Robot) -> ClaimResult:
        raise NotImplementedError

    def start(self, task_id: str, robot_id: str,
              fencing_token: int) -> Task:
        raise NotImplementedError

    def renew(self, task_id: str, robot_id: str,
              fencing_token: int) -> Lease:
        raise NotImplementedError

    def report_progress(self, task_id: str, robot_id: str,
                        fencing_token: int,
                        progress: dict[str, Any]) -> Task:
        raise NotImplementedError

    def complete(self, task_id: str, robot_id: str,
                 fencing_token: int, result: dict[str, Any]) -> Task:
        raise NotImplementedError

    def fail_attempt(self, task_id: str, reason: str) -> Task | None:
        raise NotImplementedError

    def reclaim(self, task_id: str, reason: str) -> Lease:
        raise NotImplementedError

    # -- 查询 --
    def active_lease(self, task_id: str) -> Lease | None:
        raise NotImplementedError

    def get(self, lease_id: str) -> Lease:
        raise NotImplementedError

    def list_leases(self) -> list[Lease]:
        raise NotImplementedError

    def next_fencing_token(self) -> int:
        raise NotImplementedError

    # -- 事件投影 --
    def apply(self, event_type: str, data: dict) -> None:
        raise NotImplementedError
