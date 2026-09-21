"""机器人注册、心跳与能力声明。

确定语义：
- ``register`` 幂等：同 ID 且能力集合相同 → 返回既有记录（idempotent_hit=True）；
  同 ID 但能力不同 → 拒绝 ``ROBOT_ALREADY_REGISTERED``，须走 ``update_capabilities``；
- 已注销机器人可重新注册（开启新生命周期）；
- 心跳更新存活时间；STALE 机器人补发心跳可恢复为 REGISTERED；
  已注销机器人的心跳被拒绝（``ROBOT_UNAVAILABLE``）。
"""
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable, Protocol

from .models import Robot, RobotState
from .storage import Storage
from .timeutil import Clock


@dataclass
class RegisterResult:
    robot: Robot
    idempotent_hit: bool


class RobotRegistry:
    def __init__(self, storage: Storage, clock: Clock) -> None:
        self.storage = storage
        self.clock = clock
        self._robots: dict[str, Robot] = {}

    # -- 命令 --
    def register(self, robot_id: str,
                 capabilities: Iterable[str] = ()) -> RegisterResult:
        raise NotImplementedError

    def update_capabilities(self, robot_id: str,
                            capabilities: Iterable[str]) -> Robot:
        raise NotImplementedError

    def heartbeat(self, robot_id: str) -> Robot:
        raise NotImplementedError

    def set_draining(self, robot_id: str, draining: bool) -> Robot:
        raise NotImplementedError

    def deregister(self, robot_id: str) -> Robot:
        raise NotImplementedError

    # -- 查询 --
    def get(self, robot_id: str) -> Robot:
        raise NotImplementedError

    def list_robots(self) -> list[Robot]:
        raise NotImplementedError

    def is_available(self, robot: Robot,
                     required: frozenset[str]) -> bool:
        raise NotImplementedError

    # -- 事件投影（恢复回放用） --
    def apply(self, event_type: str, data: dict) -> None:
        raise NotImplementedError

    def mark_stale(self, robot_id: str) -> Robot:
        raise NotImplementedError
