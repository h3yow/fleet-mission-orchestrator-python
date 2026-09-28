"""编排与调度核心逻辑。

一致性约定：
- 所有变更操作在同一把可重入锁内完成，租约判定与状态迁移是原子的。
- 不变量：mission 处于 ASSIGNED/RUNNING 当且仅当存在对应 lease；
  lease 存在则持有机器人的 current_mission_id 指向该 mission。
- 积压配额不变量：PENDING/PREEMPTED/WAITING_DEPS/BLOCKED 共享同一
  max_backlog。任何让任务新占配额的准入路径（submit/retry/preempt）都在
  变更前过同一道容量守卫；同槽换状态（解阻、阻断传播与恢复）不改变计数。
  唯一例外是失活租约的强制回收，可能瞬时 cap+1，领取后立即自愈。
- 每次变更先写事件日志再写原子快照，重启后状态可完整恢复。
- 时钟可注入，测试用假时钟确定性模拟心跳超时与租约过期。
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
import uuid
from typing import Any, Callable, Iterable

from .config import SchedulerConfig
from .errors import (
    BacklogFull,
    DependencyCycle,
    InvalidStateTransition,
    LeaseExpired,
    LeaseNotHeld,
    MissionNotAssignable,
    MissionNotFound,
    MissionNotPreemptible,
    MissionPayloadConflict,
    PreemptPriorityTooLow,
    PreemptTargetNotRunning,
    RetriesExhausted,
    RobotCapabilityMismatch,
    RobotNotAvailable,
    RobotNotFound,
)
from .models import (
    TERMINAL_STATES,
    Lease,
    Mission,
    MissionStatus,
    Robot,
    RobotStatus,
    can_transition,
)
from .store import Store


def _payload_hash(payload: dict[str, Any]) -> str:
    canonical = json.dumps(payload, sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class OrchestratorService:
    def __init__(
        self,
        store: Store,
        config: SchedulerConfig | None = None,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._store = store
        self._config = config or SchedulerConfig()
        self._clock = clock
        self._lock = threading.RLock()
        self._robots: dict[str, Robot] = {}
        self._missions: dict[str, Mission] = {}
        self._leases: dict[str, Lease] = {}  # key: mission_id
        self._load()

    # ---------- 持久化 ----------

    def _load(self) -> None:
        state = self._store.load()
        self._robots = {
            k: Robot.model_validate(v) for k, v in state["robots"].items()
        }
        self._missions = {
            k: Mission.model_validate(v) for k, v in state["missions"].items()
        }
        self._leases = {
            k: Lease.model_validate(v) for k, v in state["leases"].items()
        }

    def _persist(self, *events: tuple[str, dict[str, Any]]) -> None:
        for kind, record in events:
            self._store.append_event(kind, record)
        self._store.save_snapshot(
            {
                "robots": {k: r.model_dump(mode="json") for k, r in self._robots.items()},
                "missions": {k: m.model_dump(mode="json") for k, m in self._missions.items()},
                "leases": {k: l.model_dump(mode="json") for k, l in self._leases.items()},
            }
        )

    # ---------- 内部工具 ----------

    def _now(self) -> float:
        return self._clock()

    def _is_stale(self, robot: Robot) -> bool:
        if robot.status == RobotStatus.OFFLINE:
            return False  # OFFLINE 是显式状态，不与 STALE 混淆
        return (self._now() - robot.last_heartbeat) > self._config.heartbeat_timeout_s

    def _lease_expired(self, lease: Lease) -> bool:
        return self._now() >= lease.expires_at

    def _get_robot(self, robot_id: str) -> Robot:
        robot = self._robots.get(robot_id)
        if robot is None:
            raise RobotNotFound(f"robot {robot_id!r} not registered")
        return robot

    def _get_mission(self, mission_id: str) -> Mission:
        mission = self._missions.get(mission_id)
        if mission is None:
            raise MissionNotFound(f"mission {mission_id!r} not found")
        return mission

    def _transition(self, mission: Mission, target: MissionStatus) -> None:
        if not can_transition(mission.status, target):
            raise InvalidStateTransition(
                f"mission {mission.mission_id!r}: {mission.status} -> {target} not allowed"
            )
        mission.status = target

    def _release_lease(self, mission: Mission) -> Lease | None:
        lease = self._leases.pop(mission.mission_id, None)
        if lease is not None:
            robot = self._robots.get(lease.robot_id)
            if robot is not None and robot.current_mission_id == mission.mission_id:
                robot.current_mission_id = None
        return lease

    def _require_lease(self, robot_id: str, mission_id: str, lease_id: str) -> Lease:
        lease = self._leases.get(mission_id)
        if lease is None or lease.robot_id != robot_id or lease.lease_id != lease_id:
            raise LeaseNotHeld(
                f"robot {robot_id!r} does not hold lease on mission {mission_id!r}"
            )
        if self._lease_expired(lease):
            raise LeaseExpired(f"lease on mission {mission_id!r} has expired")
        return lease

    def _check_acquire_eligible(self, robot: Robot, mission: Mission) -> None:
        if robot.status == RobotStatus.OFFLINE:
            raise RobotNotAvailable(f"robot {robot.robot_id!r} is offline")
        if self._is_stale(robot):
            raise RobotNotAvailable(f"robot {robot.robot_id!r} heartbeat stale")
        if robot.current_mission_id is not None:
            raise RobotNotAvailable(
                f"robot {robot.robot_id!r} busy with {robot.current_mission_id!r}"
            )
        missing = set(mission.required_capabilities) - set(robot.capabilities)
        if missing:
            raise RobotCapabilityMismatch(
                f"robot {robot.robot_id!r} missing capabilities: {sorted(missing)}"
            )

    def _grant(self, robot: Robot, mission: Mission) -> Lease:
        self._transition(mission, MissionStatus.ASSIGNED)
        mission.attempts += 1
        mission.lease_generation += 1
        now = self._now()
        lease = Lease(
            mission_id=mission.mission_id,
            robot_id=robot.robot_id,
            lease_id=uuid.uuid4().hex,
            acquired_at=now,
            expires_at=now + self._config.lease_duration_s,
            generation=mission.lease_generation,
        )
        self._leases[mission.mission_id] = lease
        robot.current_mission_id = mission.mission_id
        return lease

    # ---------- 依赖编排 ----------

    # 占用同一份积压配额的状态：已接收但未在执行的任务全部计数，
    # 包括等待调度(PENDING/PREEMPTED)、等待前置(WAITING_DEPS)、
    # 被前置拖住不可执行(BLOCKED)。等待池不得绕开上限无界堆积。
    _BACKLOG_STATUSES = frozenset(
        {
            MissionStatus.PENDING,
            MissionStatus.PREEMPTED,
            MissionStatus.WAITING_DEPS,
            MissionStatus.BLOCKED,
        }
    )

    def _backlog_size(self) -> int:
        return sum(
            1 for m in self._missions.values() if m.status in self._BACKLOG_STATUSES
        )

    def _ensure_capacity(self, extra: int = 1, via: str = "submit") -> None:
        """准入守卫：任何让任务占用积压配额的路径共用同一条线。
        调用方必须在任何状态变更之前调用，拒绝时不留下半截记录。"""
        backlog = self._backlog_size()
        if backlog + extra > self._config.max_backlog:
            raise BacklogFull(
                f"backlog {backlog} + {extra} > max_backlog "
                f"{self._config.max_backlog} (via {via}); "
                f"PENDING/PREEMPTED/WAITING_DEPS/BLOCKED share the quota"
            )

    def _would_cycle(self, mission_id: str, deps: list[str]) -> bool:
        """沿已存在任务的 depends_on 边做可达性搜索，能绕回自身即成环。
        尚未下发的前置任务没有任何出边，不可能参与成环。"""
        seen: set[str] = set()
        stack = list(deps)
        while stack:
            node = stack.pop()
            if node == mission_id:
                return True
            if node in seen:
                continue
            seen.add(node)
            dep = self._missions.get(node)
            if dep is not None:
                stack.extend(dep.depends_on)
        return False

    def _first_failed_dep(self, mission: Mission) -> str | None:
        """返回第一个处于失败态（FAILED/CANCELLED/BLOCKED）的前置任务标识。
        未出现的前置不算失败，只是未满足。"""
        for dep_id in mission.depends_on:
            dep = self._missions.get(dep_id)
            if dep is not None and dep.status in (
                MissionStatus.FAILED,
                MissionStatus.CANCELLED,
                MissionStatus.BLOCKED,
            ):
                return dep_id
        return None

    def _deps_satisfied(self, mission: Mission) -> bool:
        return all(
            (dep := self._missions.get(dep_id)) is not None
            and dep.status == MissionStatus.SUCCEEDED
            for dep_id in mission.depends_on
        )

    def _refresh_dependencies(self) -> list[tuple[str, dict[str, Any]]]:
        """依赖状态刷新（在锁内、随触发操作同事务持久化，无半更新）。

        1) 阻断传播与恢复迭代至不动点（阻断可沿依赖链传递）：
           WAITING_DEPS 有失败前置 -> BLOCKED；BLOCKED 无失败前置 -> WAITING_DEPS。
        2) 解除等待：依赖全部成功 -> PENDING。
           WAITING_DEPS/BLOCKED 在提交时已占用积压配额，状态互转不改变
           配额计数，因此批量化解阻不可能把待分配集合撑到 max_backlog 之上。
        """
        events: list[tuple[str, dict[str, Any]]] = []
        ordered = sorted(self._missions.values(), key=lambda m: m.enqueue_seq)
        changed = True
        while changed:
            changed = False
            for m in ordered:
                if m.status == MissionStatus.WAITING_DEPS:
                    blocker = self._first_failed_dep(m)
                    if blocker is not None:
                        self._transition(m, MissionStatus.BLOCKED)
                        m.blocked_by = blocker
                        events.append(("mission_upsert", m.model_dump(mode="json")))
                        changed = True
                elif m.status == MissionStatus.BLOCKED:
                    if self._first_failed_dep(m) is None:
                        m.blocked_by = None
                        self._transition(m, MissionStatus.WAITING_DEPS)
                        events.append(("mission_upsert", m.model_dump(mode="json")))
                        changed = True
        for m in ordered:
            if m.status != MissionStatus.WAITING_DEPS:
                continue
            if not self._deps_satisfied(m):
                continue
            self._transition(m, MissionStatus.PENDING)
            events.append(("mission_upsert", m.model_dump(mode="json")))
        return events

    # ---------- 机器人管理 ----------

    def register_robot(self, robot_id: str, capabilities: Iterable[str]) -> Robot:
        """注册或更新机器人。重复注册为幂等 upsert：能力被更新、心跳刷新。"""
        with self._lock:
            caps = sorted(set(capabilities))
            now = self._now()
            robot = self._robots.get(robot_id)
            if robot is None:
                robot = Robot(
                    robot_id=robot_id,
                    capabilities=caps,
                    status=RobotStatus.ONLINE,
                    last_heartbeat=now,
                    registered_at=now,
                )
                self._robots[robot_id] = robot
            else:
                robot.capabilities = caps
                robot.last_heartbeat = now
                if robot.status != RobotStatus.OFFLINE:
                    robot.status = RobotStatus.ONLINE
            self._persist(("robot_upsert", robot.model_dump(mode="json")))
            return robot

    def deregister_robot(self, robot_id: str) -> Robot:
        with self._lock:
            robot = self._get_robot(robot_id)
            events: list[tuple[str, dict[str, Any]]] = []
            lease = next(
                (l for l in self._leases.values() if l.robot_id == robot_id), None
            )
            if lease is not None:
                mission = self._missions[lease.mission_id]
                self._reclaim(mission, events)
                events.extend(self._refresh_dependencies())
            robot.status = RobotStatus.OFFLINE
            robot.current_mission_id = None
            events.append(("robot_upsert", robot.model_dump(mode="json")))
            self._persist(*events)
            return robot

    def heartbeat(self, robot_id: str) -> Robot:
        with self._lock:
            robot = self._get_robot(robot_id)
            if robot.status == RobotStatus.OFFLINE:
                raise RobotNotAvailable(f"robot {robot_id!r} is offline")
            robot.last_heartbeat = self._now()
            robot.status = RobotStatus.ONLINE
            self._persist(("robot_upsert", robot.model_dump(mode="json")))
            return robot

    # ---------- 任务下发（幂等） ----------

    def submit_mission(
        self,
        mission_id: str,
        required_capabilities: Iterable[str] = (),
        priority: int = 0,
        preemptible: bool = True,
        payload: dict[str, Any] | None = None,
        max_attempts: int | None = None,
        depends_on: Iterable[str] = (),
    ) -> tuple[Mission, bool]:
        """返回 (mission, created)。同一 id 同内容幂等返回首次结果；
        同 id 不同内容明确拒绝。depends_on 参与内容一致性判定，
        且允许引用尚未下发的任务（先声明、后补建）。"""
        with self._lock:
            deps = sorted(set(depends_on))
            content = {
                "required_capabilities": sorted(set(required_capabilities)),
                "priority": int(priority),
                "preemptible": bool(preemptible),
                "payload": payload or {},
                "max_attempts": max_attempts,
                "depends_on": deps,
            }
            digest = _payload_hash(content)
            existing = self._missions.get(mission_id)
            if existing is not None:
                if existing.payload_hash == digest:
                    return existing, False
                raise MissionPayloadConflict(
                    f"mission {mission_id!r} already submitted with different content"
                )
            if self._would_cycle(mission_id, deps):
                raise DependencyCycle(
                    f"mission {mission_id!r} dependencies form a cycle: {deps}"
                )
            # 等待前置 / 被前置拖住的任务同样占用配额，先校验再落记录
            self._ensure_capacity(via="submit")
            next_seq = max((m.enqueue_seq for m in self._missions.values()), default=-1) + 1
            mission = Mission(
                mission_id=mission_id,
                payload_hash=digest,
                required_capabilities=content["required_capabilities"],
                priority=content["priority"],
                preemptible=content["preemptible"],
                max_attempts=max_attempts or self._config.max_attempts_default,
                created_at=self._now(),
                enqueue_seq=next_seq,
                depends_on=deps,
            )
            # 初始状态按依赖求值：全部已成功 -> PENDING；有失败前置 -> BLOCKED；
            # 否则（前置未出现/未完成）-> WAITING_DEPS
            blocker = self._first_failed_dep(mission)
            if blocker is not None:
                mission.status = MissionStatus.BLOCKED
                mission.blocked_by = blocker
            elif not self._deps_satisfied(mission):
                mission.status = MissionStatus.WAITING_DEPS
            self._missions[mission_id] = mission
            self._persist(("mission_upsert", mission.model_dump(mode="json")))
            return mission, True

    # ---------- 领取 / 执行 ----------

    def acquire_mission(self, robot_id: str, mission_id: str | None = None) -> tuple[Mission, Lease]:
        with self._lock:
            robot = self._get_robot(robot_id)
            if mission_id is not None:
                mission = self._get_mission(mission_id)
                if mission.status not in (MissionStatus.PENDING, MissionStatus.PREEMPTED):
                    raise MissionNotAssignable(
                        f"mission {mission_id!r} is {mission.status}, not acquirable"
                    )
                self._check_acquire_eligible(robot, mission)
                candidates = [mission]
            else:
                candidates = [
                    m
                    for m in self._missions.values()
                    if m.status in (MissionStatus.PENDING, MissionStatus.PREEMPTED)
                ]
                # 确定性调度：优先级降序，同优先级按入队序号 FIFO
                candidates.sort(key=lambda m: (-m.priority, m.enqueue_seq))
                chosen = None
                last_err: Exception | None = None
                for m in candidates:
                    try:
                        self._check_acquire_eligible(robot, m)
                        chosen = m
                        break
                    except (RobotNotAvailable, RobotCapabilityMismatch) as e:
                        last_err = e
                        if isinstance(e, RobotNotAvailable):
                            raise
                if chosen is None:
                    if last_err is not None:
                        raise last_err
                    raise MissionNotAssignable("no pending mission available")
                mission = chosen
            lease = self._grant(robot, mission)
            self._persist(
                ("mission_upsert", mission.model_dump(mode="json")),
                ("lease_upsert", lease.model_dump(mode="json")),
                ("robot_upsert", robot.model_dump(mode="json")),
                # 领取不改变配额计数（执行态也不计）；刷新仍需执行，
                # 以处理此前同事务内可能产生的阻断/解阻状态变化
                *self._refresh_dependencies(),
            )
            return mission, lease

    def start_mission(self, robot_id: str, mission_id: str, lease_id: str) -> Mission:
        with self._lock:
            self._require_lease(robot_id, mission_id, lease_id)
            mission = self._get_mission(mission_id)
            self._transition(mission, MissionStatus.RUNNING)
            self._persist(("mission_upsert", mission.model_dump(mode="json")))
            return mission

    def report_progress(
        self, robot_id: str, mission_id: str, lease_id: str, progress: dict[str, Any]
    ) -> Mission:
        with self._lock:
            self._require_lease(robot_id, mission_id, lease_id)
            mission = self._get_mission(mission_id)
            if mission.status != MissionStatus.RUNNING:
                raise InvalidStateTransition(
                    f"progress report requires RUNNING, got {mission.status}"
                )
            mission.progress.update(progress)
            self._persist(("mission_upsert", mission.model_dump(mode="json")))
            return mission

    def complete_mission(
        self, robot_id: str, mission_id: str, lease_id: str,
        final_progress: dict[str, Any] | None = None,
    ) -> Mission:
        with self._lock:
            self._require_lease(robot_id, mission_id, lease_id)
            mission = self._get_mission(mission_id)
            if final_progress:
                mission.progress.update(final_progress)
            self._transition(mission, MissionStatus.SUCCEEDED)
            self._release_lease(mission)
            robot = self._robots[robot_id]
            self._persist(
                ("mission_upsert", mission.model_dump(mode="json")),
                ("lease_delete", {"mission_id": mission_id}),
                ("robot_upsert", robot.model_dump(mode="json")),
                # 成功可能解阻依赖它的任务
                *self._refresh_dependencies(),
            )
            return mission

    def fail_mission(
        self, robot_id: str, mission_id: str, lease_id: str, error: str = ""
    ) -> Mission:
        with self._lock:
            self._require_lease(robot_id, mission_id, lease_id)
            mission = self._get_mission(mission_id)
            mission.progress["last_error"] = error
            self._transition(mission, MissionStatus.FAILED)
            self._release_lease(mission)
            robot = self._robots[robot_id]
            self._persist(
                ("mission_upsert", mission.model_dump(mode="json")),
                ("lease_delete", {"mission_id": mission_id}),
                ("robot_upsert", robot.model_dump(mode="json")),
                # 失败可能阻断依赖它的任务
                *self._refresh_dependencies(),
            )
            return mission

    def renew_lease(self, robot_id: str, mission_id: str, lease_id: str) -> Lease:
        with self._lock:
            lease = self._require_lease(robot_id, mission_id, lease_id)
            if lease.renewals >= self._config.max_lease_renewals:
                raise LeaseExpired(
                    f"lease renewals exhausted ({self._config.max_lease_renewals})"
                )
            lease.renewals += 1
            lease.expires_at = self._now() + self._config.renew_extension_s
            self._persist(("lease_upsert", lease.model_dump(mode="json")))
            return lease

    # ---------- 重试 / 取消 ----------

    def retry_mission(self, mission_id: str) -> Mission:
        with self._lock:
            mission = self._get_mission(mission_id)
            if mission.status != MissionStatus.FAILED:
                raise InvalidStateTransition(
                    f"retry requires FAILED, got {mission.status}"
                )
            if mission.attempts >= mission.max_attempts:
                raise RetriesExhausted(
                    f"mission {mission_id!r} attempts {mission.attempts}"
                    f" >= max_attempts {mission.max_attempts}"
                )
            # FAILED 是终态不占配额，重试回到待分配前必须先拿到槽位；
            # 守卫在任何状态变更之前，拒绝后任务仍停留在 FAILED。
            self._ensure_capacity(via="retry")
            self._transition(mission, MissionStatus.PENDING)
            self._persist(
                ("mission_upsert", mission.model_dump(mode="json")),
                # 前置重试回到等待：被它阻断的任务恢复为等待依赖
                *self._refresh_dependencies(),
            )
            return mission

    def cancel_mission(self, mission_id: str) -> Mission:
        with self._lock:
            mission = self._get_mission(mission_id)
            if mission.status in TERMINAL_STATES:
                raise InvalidStateTransition(
                    f"mission {mission_id!r} already terminal: {mission.status}"
                )
            events: list[tuple[str, dict[str, Any]]] = []
            lease = self._release_lease(mission)
            self._transition(mission, MissionStatus.CANCELLED)
            events.append(("mission_upsert", mission.model_dump(mode="json")))
            if lease is not None:
                events.append(("lease_delete", {"mission_id": mission_id}))
                robot = self._robots.get(lease.robot_id)
                if robot is not None:
                    events.append(("robot_upsert", robot.model_dump(mode="json")))
            # 取消可能阻断依赖它的任务
            events.extend(self._refresh_dependencies())
            self._persist(*events)
            return mission

    # ---------- 抢占 ----------

    def preempt_mission(self, mission_id: str, by_mission_id: str | None = None) -> Mission:
        with self._lock:
            mission = self._get_mission(mission_id)
            if mission.status not in (MissionStatus.ASSIGNED, MissionStatus.RUNNING):
                raise PreemptTargetNotRunning(
                    f"mission {mission_id!r} is {mission.status}, cannot preempt"
                )
            if not mission.preemptible:
                raise MissionNotPreemptible(f"mission {mission_id!r} is not preemptible")
            if by_mission_id is not None:
                challenger = self._get_mission(by_mission_id)
                if challenger.priority <= mission.priority:
                    raise PreemptPriorityTooLow(
                        f"challenger priority {challenger.priority}"
                        f" <= target priority {mission.priority}"
                    )
            # 执行态不占积压配额，被抢占后进入 PREEMPTED 需要一个槽位；
            # 满队时按既有策略 backlog_full 拒绝，任务继续留在执行态。
            self._ensure_capacity(via="preempt")
            events: list[tuple[str, dict[str, Any]]] = []
            lease = self._release_lease(mission)
            self._transition(mission, MissionStatus.PREEMPTED)  # progress 保留
            events.append(("mission_upsert", mission.model_dump(mode="json")))
            if lease is not None:
                events.append(("lease_delete", {"mission_id": mission_id}))
                robot = self._robots.get(lease.robot_id)
                if robot is not None:
                    events.append(("robot_upsert", robot.model_dump(mode="json")))
            self._persist(*events)
            return mission

    # ---------- 超时回收 ----------

    def sweep(self) -> list[str]:
        """回收心跳超时或租约过期的任务。返回被回收的 mission_id 列表。"""
        with self._lock:
            reclaimed: list[str] = []
            events: list[tuple[str, dict[str, Any]]] = []
            for robot in self._robots.values():
                if robot.status == RobotStatus.ONLINE and self._is_stale(robot):
                    robot.status = RobotStatus.STALE
                    events.append(("robot_upsert", robot.model_dump(mode="json")))
            for lease in list(self._leases.values()):
                holder = self._robots.get(lease.robot_id)
                holder_gone = (
                    holder is None
                    or holder.status != RobotStatus.ONLINE
                    or self._is_stale(holder)
                )
                if holder_gone or self._lease_expired(lease):
                    mission = self._missions[lease.mission_id]
                    self._reclaim(mission, events)
                    reclaimed.append(mission.mission_id)
            if reclaimed:
                # 回收导致的状态变化（PENDING/FAILED）影响依赖阻断与容量
                events.extend(self._refresh_dependencies())
            if events:
                self._persist(*events)
            return reclaimed

    def _reclaim(self, mission: Mission, events: list[tuple[str, dict[str, Any]]]) -> None:
        lease = self._release_lease(mission)
        if mission.attempts < mission.max_attempts:
            self._transition(mission, MissionStatus.PENDING)
        else:
            self._transition(mission, MissionStatus.FAILED)
            mission.progress["last_error"] = "attempts exhausted by reclaim"
        events.append(("mission_upsert", mission.model_dump(mode="json")))
        if lease is not None:
            events.append(("lease_delete", {"mission_id": mission.mission_id}))
            robot = self._robots.get(lease.robot_id)
            if robot is not None:
                events.append(("robot_upsert", robot.model_dump(mode="json")))

    # ---------- 配置 ----------

    @property
    def config(self) -> SchedulerConfig:
        return self._config

    def update_config(self, **overrides: float) -> SchedulerConfig:
        with self._lock:
            self._config = self._config.updated(**overrides)
            return self._config

    # ---------- 查询 ----------

    def get_robot(self, robot_id: str) -> Robot:
        return self._get_robot(robot_id)

    def get_mission(self, mission_id: str) -> Mission:
        return self._get_mission(mission_id)

    def get_lease(self, mission_id: str) -> Lease | None:
        return self._leases.get(mission_id)

    def list_missions(self, status: MissionStatus | None = None) -> list[Mission]:
        missions = sorted(self._missions.values(), key=lambda m: m.enqueue_seq)
        if status is not None:
            missions = [m for m in missions if m.status == status]
        return missions

    def dependency_view(self, mission_id: str) -> dict[str, Any]:
        """依赖编排视图：逐个前置任务的状态（含未出现的 MISSING），
        以及本任务当前卡在哪个前置上（blocked_by）。"""
        mission = self._get_mission(mission_id)
        deps = []
        for dep_id in mission.depends_on:
            dep = self._missions.get(dep_id)
            deps.append(
                {
                    "mission_id": dep_id,
                    "status": dep.status if dep is not None else "MISSING",
                    "satisfied": dep is not None
                    and dep.status == MissionStatus.SUCCEEDED,
                }
            )
        return {
            "mission_id": mission.mission_id,
            "status": mission.status,
            "blocked_by": mission.blocked_by,
            "dependencies": deps,
        }

    def list_robots(self) -> list[Robot]:
        return sorted(self._robots.values(), key=lambda r: r.robot_id)
