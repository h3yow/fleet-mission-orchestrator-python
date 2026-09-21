"""调度器：领取（拉模式）、抢占、超时回收与重试。

调度规则：
- ``claim_next``：从有界队列按优先级（高者优先，同级先入先出）取候选任务，
  与机器人能力、可用性匹配后授予租约；资源不足时返回 None（排队/降级由调用方决定）。
- ``request_preempt``：高优先级任务可抢占低优先级且 preemptible 的在执行任务，
  被抢占任务回到队列最前、保留进度；不可抢占 / 无合格受害者 / 状态非法 →
  分别给出可区分拒绝原因。
- ``reap_stale_leases``：标记心跳超时机器人，回收其 ACTIVE 租约，
  按重试策略重新入队或转 FAILED；决策全部写入 ``SCHEDULER_DECISION`` 事件可回放。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from .config import SchedulerConfig
from .models import Robot, Task
from .storage import Storage
from .timeutil import Clock

# 前向导入：具体管理器在实现阶段引入
if False:  # pragma: no cover
    from .leases import LeaseManager
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
    detail: dict[str, Any] | None = None


@dataclass
class ReapSummary:
    reaped: list[str] = field(default_factory=list)
    retried: list[str] = field(default_factory=list)
    failed: list[str] = field(default_factory=list)


class Scheduler:
    def __init__(self, storage: Storage, clock: Clock,
                 config: SchedulerConfig, registry: "RobotRegistry",
                 tasks: "TaskManager", leases: "LeaseManager") -> None:
        self.storage = storage
        self.clock = clock
        self.config = config
        self.registry = registry
        self.tasks = tasks
        self.leases = leases

    def claim_next(self, robot_id: str) -> "ClaimResult | None":
        raise NotImplementedError

    def request_preempt(self, high_prio_task_id: str,
                        victim_task_id: str,
                        by_robot_id: str) -> Task:
        raise NotImplementedError

    def reap_stale_leases(self) -> list[Decision]:
        raise NotImplementedError

    def decisions(self) -> list[Decision]:
        raise NotImplementedError

    def apply_decision(self, evt_data: dict[str, Any]) -> None:
        raise NotImplementedError
