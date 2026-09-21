"""机器人注册、心跳与能力声明。

确定语义：
- ``register`` 幂等：同 ID 且能力集合相同 → 返回既有记录（idempotent_hit=True）；
  同 ID 但能力不同 → 拒绝 ``ROBOT_ALREADY_REGISTERED``，须走 ``update_capabilities``；
- 已注销机器人可重新注册（开启新生命周期）；
- 心跳更新存活时间；STALE 机器人补发心跳可恢复为 REGISTERED；
  已注销机器人的心跳被拒绝（``ROBOT_UNAVAILABLE``）。

注：事件 data 中统一携带保留字段 ``_at_ms``（事件时间戳），
回放时以它为准，因此重启后心跳时间与故障前完全一致。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

from .errors import DomainError, RejectCode
from .models import Robot, RobotState, SCHEMA_VERSION
from .storage import EventType, Storage
from .timeutil import Clock

_RESERVED_AT = "_at_ms"


def _fset(items: Iterable[str]) -> frozenset[str]:
    if isinstance(items, str):
        raise DomainError(RejectCode.INVALID_CONFIG,
                          "capabilities 必须是字符串集合，不能是单个字符串")
    return frozenset(items)


@dataclass
class RegisterResult:
    robot: Robot
    idempotent_hit: bool


class RobotRegistry:
    def __init__(self, storage: Storage, clock: Clock) -> None:
        self.storage = storage
        self.clock = clock
        self._robots: dict[str, Robot] = {}

    # ---------------- 命令 ----------------
    def register(self, robot_id: str,
                 capabilities: Iterable[str] = ()) -> RegisterResult:
        if not isinstance(robot_id, str) or not robot_id:
            raise DomainError(RejectCode.INVALID_CONFIG, "robot_id 不能为空")
        caps = _fset(capabilities)
        now = self.clock.now_ms()
        existing = self._robots.get(robot_id)

        if existing is not None and existing.state != RobotState.DEREGISTERED:
            if existing.capabilities == caps:
                return RegisterResult(existing, idempotent_hit=True)
            raise DomainError(
                RejectCode.ROBOT_ALREADY_REGISTERED,
                f"机器人 {robot_id} 已注册且能力不同："
                f"已有={sorted(existing.capabilities)} 提交={sorted(caps)}；"
                "更新能力请使用 update_capabilities")

        data = {"robot_id": robot_id, "capabilities": sorted(caps),
                "v": SCHEMA_VERSION, _RESERVED_AT: now}
        evt = self.storage.append(EventType.ROBOT_REGISTERED, now, data)
        self.apply(evt.event_type, evt.data)
        return RegisterResult(self._robots[robot_id], idempotent_hit=False)

    def update_capabilities(self, robot_id: str,
                            capabilities: Iterable[str]) -> Robot:
        robot = self._require(robot_id)
        caps = _fset(capabilities)
        if robot.state == RobotState.DEREGISTERED:
            raise DomainError(
                RejectCode.ROBOT_UNAVAILABLE,
                f"机器人 {robot_id} 已注销，不能更新能力；请重新注册")
        if robot.capabilities == caps:
            return robot
        now = self.clock.now_ms()
        data = {"robot_id": robot_id, "capabilities": sorted(caps),
                _RESERVED_AT: now}
        evt = self.storage.append(
            EventType.ROBOT_CAPABILITIES_UPDATED, now, data)
        self.apply(evt.event_type, evt.data)
        return self._robots[robot_id]

    def heartbeat(self, robot_id: str) -> Robot:
        robot = self._require(robot_id)
        if robot.state == RobotState.DEREGISTERED:
            raise DomainError(
                RejectCode.ROBOT_UNAVAILABLE,
                f"机器人 {robot_id} 已注销，心跳被拒绝")
        now = self.clock.now_ms()
        data = {"robot_id": robot_id, _RESERVED_AT: now}
        evt = self.storage.append(EventType.ROBOT_HEARTBEAT, now, data)
        self.apply(evt.event_type, evt.data)
        return self._robots[robot_id]

    def set_draining(self, robot_id: str, draining: bool) -> Robot:
        robot = self._require(robot_id)
        if robot.state == RobotState.DEREGISTERED:
            raise DomainError(
                RejectCode.ROBOT_UNAVAILABLE,
                f"机器人 {robot_id} 已注销，不能切换 drain")
        target = RobotState.DRAINING if draining else RobotState.REGISTERED
        if robot.state == target:
            return robot
        now = self.clock.now_ms()
        data = {"robot_id": robot_id, "draining": draining, _RESERVED_AT: now}
        evt = self.storage.append(EventType.ROBOT_DRAIN_SET, now, data)
        self.apply(evt.event_type, evt.data)
        return self._robots[robot_id]

    def deregister(self, robot_id: str) -> Robot:
        robot = self._require(robot_id)
        if robot.state == RobotState.DEREGISTERED:
            return robot
        now = self.clock.now_ms()
        data = {"robot_id": robot_id, _RESERVED_AT: now}
        evt = self.storage.append(EventType.ROBOT_DEREGISTERED, now, data)
        self.apply(evt.event_type, evt.data)
        return self._robots[robot_id]

    # ---------------- 调度器使用 ----------------
    def mark_stale(self, robot_id: str) -> Robot:
        """心跳超时标记（纯内存推导，不落事件：重放后由回收器重新判定）。"""
        robot = self._require(robot_id)
        if robot.state in (RobotState.REGISTERED, RobotState.DRAINING):
            robot.state = RobotState.STALE
        return robot

    def is_available(self, robot: Robot,
                     required: frozenset[str]) -> bool:
        if robot.state != RobotState.REGISTERED:
            return False
        if not required <= robot.capabilities:
            return False
        if robot.current_task_id is not None:
            return False
        return True

    # ---------------- 查询 ----------------
    def _require(self, robot_id: str) -> Robot:
        robot = self._robots.get(robot_id)
        if robot is None:
            raise DomainError(
                RejectCode.ROBOT_NOT_FOUND, f"机器人 {robot_id} 不存在")
        return robot

    def get(self, robot_id: str) -> Robot:
        return self._require(robot_id)

    def list_robots(self) -> list[Robot]:
        return list(self._robots.values())

    # ---------------- 事件投影 ----------------
    def apply(self, event_type: str, data: dict) -> None:
        rid = data.get("robot_id")
        at_ms = int(data.get(_RESERVED_AT, 0))
        unknown = {k: v for k, v in data.items()
                   if k not in {"robot_id", "capabilities", "v",
                                "draining", _RESERVED_AT}}

        if event_type == EventType.ROBOT_REGISTERED:
            self._robots[rid] = Robot(
                robot_id=rid,
                capabilities=frozenset(data.get("capabilities", ())),
                state=RobotState.REGISTERED,
                last_heartbeat_ms=at_ms,
                current_task_id=None,
                schema_version=int(data.get("v", SCHEMA_VERSION)),
                unknown_fields=unknown)
            return

        robot = self._robots.get(rid) if rid is not None else None
        if robot is None:
            raise DomainError(
                RejectCode.CORRUPT_RECORD,
                f"事件 {event_type} 引用了未注册的机器人 {rid!r}")

        if event_type == EventType.ROBOT_HEARTBEAT:
            robot.last_heartbeat_ms = at_ms
            if robot.state == RobotState.STALE:
                robot.state = RobotState.REGISTERED
        elif event_type == EventType.ROBOT_CAPABILITIES_UPDATED:
            robot.capabilities = frozenset(data.get("capabilities", ()))
        elif event_type == EventType.ROBOT_DRAIN_SET:
            robot.state = (RobotState.DRAINING
                           if data.get("draining") else RobotState.REGISTERED)
        elif event_type == EventType.ROBOT_DEREGISTERED:
            robot.state = RobotState.DEREGISTERED
            robot.current_task_id = None
        elif event_type == EventType.TASK_CLAIMED:
            # 机器人当前持有的任务（单机器人单任务）
            holder = self._robots.get(data.get("robot_id"))
            if holder is not None:
                holder.current_task_id = data.get("task_id")
        elif event_type in (EventType.TASK_COMPLETED,
                            EventType.TASK_FAILED,
                            EventType.TASK_RECLAIMED,
                            EventType.TASK_PREEMPTED):
            holder = self._robots.get(data.get("robot_id"))
            if holder is not None and holder.current_task_id == data.get(
                    "task_id"):
                holder.current_task_id = None
        robot.unknown_fields.update(unknown)


