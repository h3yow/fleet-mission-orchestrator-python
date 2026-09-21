"""对外门面：聚合注册 / 任务 / 租约 / 调度全部用例，并负责故障恢复。

- 构造时自动从存储重放事件，恢复任务、租约、机器人与调度决策；
- 所有命令在单把可重入锁内串行执行，把并发竞争转化为 WAL 上确定全序，
  杜绝双持有者与半更新；
- 查询（snapshot/get）不加锁或只取一致快照，仅用于观测。
"""
from __future__ import annotations

import threading
from typing import Any, Iterable

from .config import SchedulerConfig
from .events import EventBus
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
        self.clock: Clock = clock or SystemClock()
        self.config = config or SchedulerConfig()
        self._lock = threading.RLock()

        self.registry = RobotRegistry(storage, self.clock)
        self.tasks = TaskManager(storage, self.clock, self.config)
        self.leases = LeaseManager(storage, self.clock, self.config)
        self.scheduler = Scheduler(
            storage, self.clock, self.config,
            self.registry, self.tasks, self.leases)

        # 固定扇出顺序：机器人 → 任务 → 租约 → 调度决策
        self.bus = EventBus(storage, self.clock, [
            self.registry, self.tasks, self.leases, self.scheduler])
        self.leases.bus = self.bus
        self.scheduler.bus = self.bus
        self.leases._task_lookup = self.tasks.get

        self._recover()

    # ---------------- 构造辅助 ----------------
    @classmethod
    def in_memory(cls, clock: Clock | None = None,
                  config: SchedulerConfig | None = None) -> "FleetService":
        return cls(InMemoryStorage(), clock=clock, config=config)

    @classmethod
    def with_json_file(cls, path: str, clock: Clock | None = None,
                       config: SchedulerConfig | None = None) -> "FleetService":
        return cls(JsonFileStorage(path), clock=clock, config=config)

    def _recover(self) -> None:
        """重放 WAL：事件总线逐事件投影，恢复全部内存状态。"""
        events = self.bus.replay_all()
        # fencing token 序号在重放 CLAIMED 时由 LeaseManager.apply 维护；
        # 这里确保 token 永不因重启回退（取日志中最大值兜底）。
        max_token = 0
        for evt in events:
            max_token = max(max_token, int(evt.data.get("fencing_token", 0)))
        if max_token > self.leases._token_seq:
            self.leases._token_seq = max_token
        self._replayed = len(events)

    # ---------------- 机器人用例 ----------------
    def register_robot(self, robot_id: str,
                       capabilities: Iterable[str] = ()) -> Any:
        with self._lock:
            return self.registry.register(robot_id, capabilities)

    def heartbeat(self, robot_id: str) -> Any:
        with self._lock:
            return self.registry.heartbeat(robot_id)

    def update_capabilities(self, robot_id: str,
                            capabilities: Iterable[str]) -> Any:
        with self._lock:
            return self.registry.update_capabilities(robot_id, capabilities)

    def set_draining(self, robot_id: str, draining: bool) -> Any:
        with self._lock:
            return self.registry.set_draining(robot_id, draining)

    # ---------------- 任务用例 ----------------
    def submit_task(self, task_id: str, payload: dict[str, Any],
                    required_capabilities: Iterable[str] = (),
                    priority: int = 0, preemptible: bool = True,
                    max_retries: int = 0,
                    timeout_ms: int | None = None) -> Any:
        with self._lock:
            return self.tasks.submit(
                task_id, payload,
                required_capabilities=required_capabilities,
                priority=priority, preemptible=preemptible,
                max_retries=max_retries, timeout_ms=timeout_ms)

    # ---------------- 租约 / 执行用例 ----------------
    def claim_next(self, robot_id: str) -> ClaimResult | None:
        with self._lock:
            return self.scheduler.claim_next(robot_id)

    def start_task(self, task_id: str, robot_id: str,
                   fencing_token: int) -> Any:
        with self._lock:
            return self.leases.start(task_id, robot_id, fencing_token)

    def renew_lease(self, task_id: str, robot_id: str,
                    fencing_token: int,
                    duration_ms: int | None = None) -> Any:
        with self._lock:
            return self.leases.renew(task_id, robot_id, fencing_token,
                                     duration_ms=duration_ms)

    def report_progress(self, task_id: str, robot_id: str,
                        fencing_token: int,
                        progress: dict[str, Any]) -> Any:
        with self._lock:
            return self.leases.report_progress(
                task_id, robot_id, fencing_token, progress)

    def complete_task(self, task_id: str, robot_id: str,
                      fencing_token: int,
                      result: dict[str, Any]) -> Any:
        with self._lock:
            return self.leases.complete(
                task_id, robot_id, fencing_token, result)

    # ---------------- 调度用例 ----------------
    def preempt(self, high_prio_task_id: str, victim_task_id: str,
                by_robot_id: str) -> Any:
        with self._lock:
            return self.scheduler.request_preempt(
                high_prio_task_id, victim_task_id, by_robot_id)

    def reap_stale_leases(self) -> list[Decision]:
        with self._lock:
            return self.scheduler.reap_stale_leases()

    # ---------------- 可观测 ----------------
    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return {
                "replayed_events": getattr(self, "_replayed", 0),
                "queue": self.tasks.queue_peek_order(),
                "queue_size": self.tasks.queue_size(),
                "robots": [
                    {"robot_id": r.robot_id, "state": r.state.value,
                     "capabilities": sorted(r.capabilities),
                     "last_heartbeat_ms": r.last_heartbeat_ms,
                     "current_task_id": r.current_task_id}
                    for r in self.registry.list_robots()],
                "tasks": [
                    {"task_id": t.task_id, "state": t.state.value,
                     "priority": t.priority,
                     "preemptible": t.preemptible,
                     "required_capabilities":
                         sorted(t.required_capabilities),
                     "attempt": t.attempt,
                     "progress": t.progress, "result": t.result}
                    for t in self.tasks.list_tasks()],
                "leases": [
                    {"lease_id": l.lease_id, "task_id": l.task_id,
                     "robot_id": l.robot_id, "state": l.state.value,
                     "fencing_token": l.fencing_token,
                     "attempt": l.attempt,
                     "expires_at_ms": l.expires_at_ms}
                    for l in self.leases.list_leases()],
                "decisions": [
                    {"action": d.action, "at_ms": d.at_ms,
                     "task_id": d.task_id, "robot_id": d.robot_id,
                     "reason": d.reason}
                    for d in self.scheduler.decisions()],
            }

    def close(self) -> None:
        with self._lock:
            self.storage.close()
