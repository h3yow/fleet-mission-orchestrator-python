"""编排与调度核心逻辑。

一致性约定：
- 所有变更操作在同一把可重入锁内完成，租约判定与状态迁移是原子的。
- 不变量：mission 处于 ASSIGNED/RUNNING 当且仅当存在对应 lease；
  lease 存在则持有机器人的 current_mission_id 指向该 mission。
- 每次变更先写事件日志再写原子快照，重启后状态可完整恢复。
- 时钟可注入，测试用假时钟确定性模拟心跳超时与租约过期。
"""

from __future__ import annotations

import hashlib
import json
import threading
import time
import uuid
from collections import deque
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
    DEP_MISSING,
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
        self._reconcile_dependencies()

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

    def _reconcile_dependencies(self) -> None:
        """恢复后对账：依据持久化的依赖图重算 WAITING/BLOCKED 状态。

        按依赖深度自底向上收敛（深度有界，无环）。仅修正与推导不一致者，
        绝不把 PENDING/PREEMPTED 等状态改成等待，因此不会凭空多出或吞掉
        可领取任务。老记录无 dependencies 字段 -> 推导为 PENDING 不受影响，
        但初始本就是 PENDING 的记录不会被改动。
        """
        fixed: list[tuple[str, dict[str, Any]]] = []
        # 简单定长迭代：最深依赖链不超过任务总数，逐层收敛
        for _ in range(max(1, len(self._missions))):
            changed = False
            for mission in self._missions.values():
                if mission.status not in (
                    MissionStatus.WAITING,
                    MissionStatus.BLOCKED,
                ):
                    continue
                target, blocker, reason = self._evaluate_dependencies(mission)
                if (
                    mission.status != target
                    or mission.blocked_by != blocker
                    or mission.blocked_reason != reason
                ):
                    mission.status = target  # 恢复对账直改，不经过状态机
                    mission.blocked_by = blocker
                    mission.blocked_reason = reason
                    fixed.append(("mission_upsert", mission.model_dump(mode="json")))
                    changed = True
            if not changed:
                break
        if fixed:
            self._persist(*fixed)

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

    def _normalize_deps(self, dependencies: Iterable[str] | None) -> list[str]:
        """去重、保序（sorted 保证指纹稳定）、丢弃空标识。"""
        if not dependencies:
            return []
        return sorted({d for d in dependencies if d})

    def _dep_status_value(self, dep_id: str) -> str:
        dep = self._missions.get(dep_id)
        return DEP_MISSING if dep is None else dep.status.value

    def _has_cycle(self, mission_id: str, deps: list[str]) -> bool:
        """在“已有依赖图 + 本次新边”上从 mission_id 出发 DFS 着色检测成环。

        已有图在提交校验下保证无环；新成环的环必经过 mission_id。
        尚未下发的前置标识视为出度为 0 的占位节点（先声明、后补建）。
        """
        adjacency: dict[str, list[str]] = {
            mid: list(m.dependencies) for mid, m in self._missions.items()
        }
        adjacency[mission_id] = deps
        white, gray, black = 0, 1, 2
        color: dict[str, int] = {}
        stack = [mission_id]
        while stack:
            node = stack[-1]
            state = color.get(node, white)
            if state == white:
                color[node] = gray
                for nxt in adjacency.get(node, ()):  # 未补建节点无出边
                    nxt_state = color.get(nxt, white)
                    if nxt_state == gray:
                        return True
                    if nxt_state == white:
                        stack.append(nxt)
            else:
                if state == gray:
                    color[node] = black
                stack.pop()
        return False

    def _evaluate_dependencies(
        self, mission: Mission
    ) -> tuple[MissionStatus, str | None, str | None]:
        """依据当前各前置任务状态推导任务应处状态。

        返回 (status, blocked_by, blocked_reason)：
        - 全部前置 SUCCEEDED            -> PENDING（可领取）
        - 存在 FAILED/CANCELLED/BLOCKED -> BLOCKED（归因到拖垮链根因）
        - 其余（未补建/等待/在执行/排队）-> WAITING
        """
        if not mission.dependencies:
            return MissionStatus.PENDING, None, None
        waiting = False
        for dep_id in mission.dependencies:
            dep = self._missions.get(dep_id)
            if dep is None:
                waiting = True
                continue
            if dep.status == MissionStatus.SUCCEEDED:
                continue
            if dep.status == MissionStatus.FAILED:
                return MissionStatus.BLOCKED, dep_id, "dep_failed"
            if dep.status == MissionStatus.CANCELLED:
                return MissionStatus.BLOCKED, dep_id, "dep_cancelled"
            if dep.status == MissionStatus.BLOCKED:
                # 沿阻断链传播到根因；查询视图仍可见每个直接前置的状态
                root = dep.blocked_by or dep_id
                return MissionStatus.BLOCKED, root, dep.blocked_reason or "dep_failed"
            waiting = True
        if waiting:
            return MissionStatus.WAITING, None, None
        return MissionStatus.PENDING, None, None

    def _refresh_dependents(
        self, root_id: str, events: list[tuple[str, dict[str, Any]]]
    ) -> None:
        """root_id 状态变化后，级联重算依赖它的任务（仅 WAITING/BLOCKED 参与）。

        BFS 逐层传播，单调用方已持锁，所有变更累计进同一批 events，
        由外层一次性落盘，杜绝半更新与自相矛盾的中间状态。
        无环图保证收敛；PENDING 及以后状态的任务不受影响。
        """
        reverse: dict[str, list[str]] = {}
        for mid, m in self._missions.items():
            for dep_id in m.dependencies:
                reverse.setdefault(dep_id, []).append(mid)
        queue: deque[str] = deque(reverse.get(root_id, ()))
        while queue:
            mid = queue.popleft()
            mission = self._missions.get(mid)
            if mission is None or mission.status not in (
                MissionStatus.WAITING,
                MissionStatus.BLOCKED,
            ):
                continue
            target, blocker, reason = self._evaluate_dependencies(mission)
            changed = (
                mission.status != target
                or mission.blocked_by != blocker
                or mission.blocked_reason != reason
            )
            if not changed:
                continue
            if mission.status != target:
                self._transition(mission, target)
            mission.blocked_by = blocker
            mission.blocked_reason = reason
            events.append(("mission_upsert", mission.model_dump(mode="json")))
            for child in reverse.get(mid, ()):
                queue.append(child)

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
        dependencies: Iterable[str] | None = None,
    ) -> tuple[Mission, bool]:
        """返回 (mission, created)。同一 id 同内容幂等返回首次结果；
        同 id 不同内容（含依赖声明不同）明确拒绝。

        依赖规则：
        - dependencies 允许引用尚未下发的任务标识（先声明、后补建）；
        - 依赖全部 SUCCEEDED 才进入 PENDING；否则 WAITING；
          前置 FAILED/CANCELLED/被阻断 -> BLOCKED；
        - 自依赖或间接成环在下发时以 dependency_cycle 拒绝，不影响其它任务；
        - 补建一个被等待的任务后，等待它的下游按规则同步重算。
        """
        with self._lock:
            deps = self._normalize_deps(dependencies)
            content = {
                "required_capabilities": sorted(set(required_capabilities)),
                "priority": int(priority),
                "preemptible": bool(preemptible),
                "payload": payload or {},
                "max_attempts": max_attempts,
                "dependencies": deps,
            }
            digest = _payload_hash(content)
            existing = self._missions.get(mission_id)
            if existing is not None:
                if existing.payload_hash == digest:
                    return existing, False
                raise MissionPayloadConflict(
                    f"mission {mission_id!r} already submitted with different content"
                )
            if mission_id in deps or self._has_cycle(mission_id, deps):
                raise DependencyCycle(
                    f"mission {mission_id!r} dependencies form a cycle: {deps}"
                )
            backlog = sum(
                1
                for m in self._missions.values()
                if m.status in (
                    MissionStatus.PENDING,
                    MissionStatus.PREEMPTED,
                    MissionStatus.WAITING,
                    MissionStatus.BLOCKED,
                )
            )
            if backlog >= self._config.max_backlog:
                raise BacklogFull(
                    f"backlog {backlog} >= max_backlog {self._config.max_backlog}"
                )
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
                dependencies=deps,
            )
            initial, blocker, reason = self._evaluate_dependencies(mission)
            mission.status = initial
            mission.blocked_by = blocker
            mission.blocked_reason = reason
            self._missions[mission_id] = mission
            events: list[tuple[str, dict[str, Any]]] = [
                ("mission_upsert", mission.model_dump(mode="json"))
            ]
            # 可能是“先声明、后补建”：补建后解阻/推进等待它的下游
            self._refresh_dependents(mission_id, events)
            self._persist(*events)
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
                # 仅依赖已满足的任务可参与调度：WAITING/BLOCKED 不在候选内
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
            events = [
                ("mission_upsert", mission.model_dump(mode="json")),
                ("lease_delete", {"mission_id": mission_id}),
                ("robot_upsert", robot.model_dump(mode="json")),
            ]
            # 前置成功：级联解阻/推进依赖它的任务（同一批事件原子落盘）
            self._refresh_dependents(mission_id, events)
            self._persist(*events)
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
            events = [
                ("mission_upsert", mission.model_dump(mode="json")),
                ("lease_delete", {"mission_id": mission_id}),
                ("robot_upsert", robot.model_dump(mode="json")),
            ]
            # 前置失败（终态）：依赖方阻断；若之后重试成功会按同规则恢复
            self._refresh_dependents(mission_id, events)
            self._persist(*events)
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
            self._transition(mission, MissionStatus.PENDING)
            events: list[tuple[str, dict[str, Any]]] = [
                ("mission_upsert", mission.model_dump(mode="json"))
            ]
            # 前置重回等待：之前被它阻断的依赖方按同样规则恢复（不再卡在 BLOCKED）
            self._refresh_dependents(mission_id, events)
            self._persist(*events)
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
            # 前置取消：依赖方进入不可执行（dep_cancelled），不可领取/抢占/回收
            self._refresh_dependents(mission_id, events)
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
        # 次数用尽 -> FAILED 终态时，依赖方须级联阻断（回到 PENDING 时刷新为无操作）
        self._refresh_dependents(mission.mission_id, events)

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
        """依赖等待视图：查询任务究竟卡在哪一步。

        - 无依赖且 PENDING/PREEMPTED：wait_kind="schedule"（等待调度，非依赖）
        - WAITING：wait_kind="dependency"，逐个列出前置状态（含未补建=MISSING），
          unsatisfied 列出仍未成功的前置标识
        - BLOCKED：wait_kind="blocked"，给出 blocked_by/blocked_reason
        - 其它（在执行/终态）：wait_kind="none"
        """
        with self._lock:
            mission = self._get_mission(mission_id)
            dep_status = {d: self._dep_status_value(d) for d in mission.dependencies}
            unsatisfied = [
                d
                for d in mission.dependencies
                if dep_status[d] != MissionStatus.SUCCEEDED.value
            ]
            if mission.status == MissionStatus.BLOCKED:
                wait_kind = "blocked"
            elif mission.status == MissionStatus.WAITING:
                wait_kind = "dependency"
            elif mission.status in (MissionStatus.PENDING, MissionStatus.PREEMPTED):
                wait_kind = "schedule"
            else:
                wait_kind = "none"
            return {
                "mission_id": mission_id,
                "status": mission.status.value,
                "wait_kind": wait_kind,
                "dependencies": list(mission.dependencies),
                "dependency_status": dep_status,
                "unsatisfied": unsatisfied,
                "blocked_by": mission.blocked_by,
                "blocked_reason": mission.blocked_reason,
            }

    def list_robots(self) -> list[Robot]:
        return sorted(self._robots.values(), key=lambda r: r.robot_id)
