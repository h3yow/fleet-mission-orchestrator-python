"""本地持久化：仅追加的事件日志（WAL）+ 快照。

- 所有状态迁移先序列化为一条事件、原子追加落盘，再更新内存投影；
  崩溃时要么事件完整可见、要么不可见，不产生半更新状态。
- 重启时重放事件即可还原任务、租约与机器人；
- 快照为优化项，事件日志是唯一事实来源（source of truth）。
- 旧版本记录按 ``schema_version`` 迁移；未知字段原样保留在事件里，
  反序列化不报错、不参与判定。
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Protocol


class EventType(str, Enum):
    ROBOT_REGISTERED = "ROBOT_REGISTERED"
    ROBOT_HEARTBEAT = "ROBOT_HEARTBEAT"
    ROBOT_CAPABILITIES_UPDATED = "ROBOT_CAPABILITIES_UPDATED"
    ROBOT_DRAIN_SET = "ROBOT_DRAIN_SET"
    ROBOT_DEREGISTERED = "ROBOT_DEREGISTERED"
    TASK_SUBMITTED = "TASK_SUBMITTED"
    TASK_CLAIMED = "TASK_CLAIMED"
    TASK_STARTED = "TASK_STARTED"
    LEASE_RENEWED = "LEASE_RENEWED"
    TASK_PROGRESS = "TASK_PROGRESS"
    TASK_PREEMPTED = "TASK_PREEMPTED"
    TASK_COMPLETED = "TASK_COMPLETED"
    TASK_RECLAIMED = "TASK_RECLAIMED"
    TASK_RESCHEDULED = "TASK_RESCHEDULED"
    TASK_FAILED = "TASK_FAILED"
    SCHEDULER_DECISION = "SCHEDULER_DECISION"


@dataclass
class Event:
    seq: int
    event_type: EventType
    at_ms: int
    data: dict[str, Any]
    schema_version: int = 1

    def to_dict(self) -> dict[str, Any]:
        raise NotImplementedError

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> "Event":
        raise NotImplementedError


class Storage(Protocol):
    def append(self, event_type: EventType, at_ms: int,
               data: dict[str, Any]) -> Event: ...

    def replay(self) -> list[Event]: ...

    @property
    def next_seq(self) -> int: ...

    def close(self) -> None: ...


@dataclass
class InMemoryStorage:
    """内存存储：单测默认实现，同样遵循仅追加语义。"""
    _events: list[Event] = field(default_factory=list)
    _seq: int = 0

    def append(self, event_type: EventType, at_ms: int,
               data: dict[str, Any]) -> Event:
        raise NotImplementedError

    def replay(self) -> list[Event]:
        raise NotImplementedError

    @property
    def next_seq(self) -> int:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError


class JsonFileStorage:
    """JSONL 事件日志：每行一个事件，fsync 后才算写入成功。"""

    def __init__(self, path: str) -> None:
        self.path = path
        self._fh: Any = None
        self._seq = 0

    def append(self, event_type: EventType, at_ms: int,
               data: dict[str, Any]) -> Event:
        raise NotImplementedError

    def replay(self) -> list[Event]:
        raise NotImplementedError

    @property
    def next_seq(self) -> int:
        raise NotImplementedError

    def close(self) -> None:
        raise NotImplementedError
