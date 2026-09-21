"""对外门面：聚合注册 / 任务 / 租约 / 调度全部用例，并负责故障恢复。

- 构造时自动从存储重放事件，恢复任务、租约与机器人投影；
- 所有命令在单个进程内串行化（一把内核锁），把并发竞争转化为
  WAL 上确定的全序，杜绝双持有者与半更新。
"""
from __future__ import annotations

import threading
from typing import Any, Iterable

from .config import SchedulerConfig
from .leases import ClaimResult, LeaseManager
from .registry import RobotRegistry
from .scheduler import Decision, Scheduler
from .storage import InMemoryStorage, JsonFileStorage, Storage
from .tasks import TaskManager
from .timeutil import Clock, SystemClock


class FleetService:
    def __init__(self, storage: Storage, clock: Clock | None = None,
                 config: SchedulerConfig | None = None) -> None:
        self.storage = storage
        self.clock = clock or SystemClock()
        self.config = config or SchedulerConfig()
        self._lock = threading.RLock()
        self.registry = RobotRegistry(storage, self.clock)
        self.tasks = TaskManager(storage, self.clock, self.config)
        self.leases = LeaseManager(storage, self.clock, self.config)
        self.scheduler = Scheduler(storage, self.clock, self.config,
                                   self.registry, self.tasks, self.leases)
        self._recover()

    # -- 构造辅助 --
    @classmethod
    def in_memory(cls, clock: Clock | None = None,
                  config: SchedulerConfig | None = None) -> "FleetService":
        raise NotImplementedError

    @classmethod
    def with_json_file(cls, path: str, clock: Clock | None = None,
                       config: SchedulerConfig | None = None) -> "FleetService":
        raise NotImplementedError

    def _recover(self) -> None:
        raise NotImplementedError

    # -- 机器人用例 --
    def register_robot(self, robot_id: str,
                       capabilities: Iterable[str] = ()) -> Any:
        raise NotImplementedError

    def heartbeat(self, robot_id: str) -> Any:
        raise NotImplementedError

    def update_capabilities(self, robot_id: str,
                            capabilities: Iterable[str]) -> Any:
        raise NotImplementedError

    def set_draining(self, robot_id: str, draining: bool) -> Any:
        raise NotImplementedError

    # -- 任务用例 --
    def submit_task(self, task_id: str, payload: dict[str, Any],
                    required_capabilities: Iterable[str] = (),
                    priority: int = 0, preemptible: bool = True,
                    max_retries: int = 0,
                    timeout_ms: int | None = None) -> Any:
        raise NotImplementedError

    # -- 租约/执行用例 --
    def claim_next(self, robot_id: str) -> ClaimResult | None:
        raise NotImplementedError

    def start_task(self, task_id: str, robot_id: str,
                   fencing_token: int) -> Any:
        raise NotImplementedError

    def renew_lease(self, task_id: str, robot_id: str,
                    fencing_token: int) -> Any:
        raise NotImplementedError

    def report_progress(self, task_id: str, robot_id: str,
                        fencing_token: int,
                        progress: dict[str, Any]) -> Any:
        raise NotImplementedError

    def complete_task(self, task_id: str, robot_id: str,
                      fencing_token: int,
                      result: dict[str, Any]) -> Any:
        raise NotImplementedError

    # -- 调度用例 --
    def preempt(self, high_prio_task_id: str, victim_task_id: str,
                by_robot_id: str) -> Any:
        raise NotImplementedError

    def reap_stale_leases(self) -> list[Decision]:
        raise NotImplementedError

    # -- 可观测 --
    def snapshot(self) -> dict[str, Any]:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError
