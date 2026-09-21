"""本地持久化：仅追加的事件日志（WAL）。

- 所有状态迁移先序列化为一条事件、原子追加落盘，再更新内存投影；
  崩溃时要么事件完整可见、要么不可见，不产生半更新状态。
- 重启时重放事件即可还原任务、租约与机器人；事件日志是唯一事实来源。
- 旧版本记录按 ``schema_version`` 迁移；未知字段原样保留
  （``unknown_keys``），反序列化不报错、不参与任何分配判定。
- 无法解析的损坏行直接 ``CORRUPT_RECORD`` 失败（绝不静默丢弃数据）。
"""
from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass, field
from typing import Any, Protocol

from .errors import DomainError, RejectCode
from .models import SCHEMA_VERSION


class EventType:
    """事件类型常量（字符串）。用普通类而非 Enum，便于容忍未知事件类型。"""
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

    KNOWN: frozenset[str] = frozenset({
        ROBOT_REGISTERED, ROBOT_HEARTBEAT, ROBOT_CAPABILITIES_UPDATED,
        ROBOT_DRAIN_SET, ROBOT_DEREGISTERED, TASK_SUBMITTED, TASK_CLAIMED,
        TASK_STARTED, LEASE_RENEWED, TASK_PROGRESS, TASK_PREEMPTED,
        TASK_COMPLETED, TASK_RECLAIMED, TASK_RESCHEDULED, TASK_FAILED,
        SCHEDULER_DECISION,
    })


@dataclass
class Event:
    seq: int
    event_type: str | None          # None 表示未知的新版本事件
    at_ms: int
    data: dict[str, Any]
    schema_version: int = SCHEMA_VERSION
    raw_type: str | None = None     # 未知事件的原始类型名
    unknown_keys: list[str] = field(default_factory=list)

    def to_dict(self) -> dict[str, Any]:
        out: dict[str, Any] = {
            "seq": self.seq,
            "event_type": self.event_type or self.raw_type,
            "at_ms": self.at_ms,
            "v": self.schema_version,
            "data": self.data,
        }
        return out

    @classmethod
    def from_dict(cls, raw: dict[str, Any], *, line_no: int = 0) -> "Event":
        # 必需字段缺失 → 损坏记录，显式失败
        missing = [k for k in ("seq", "event_type", "at_ms", "data")
                   if k not in raw]
        if missing or not isinstance(raw.get("data"), dict):
            raise DomainError(
                RejectCode.CORRUPT_RECORD,
                f"事件记录缺少必需字段 {missing} 或 data 非对象 "
                f"(line={line_no})")
        known = {"seq", "event_type", "at_ms", "data", "v"}
        unknown_keys = sorted(set(raw) - known)
        etype = raw["event_type"]
        is_known = isinstance(etype, str) and etype in EventType.KNOWN
        return cls(
            seq=int(raw["seq"]),
            event_type=etype if is_known else None,
            at_ms=int(raw["at_ms"]),
            data=raw["data"],
            schema_version=int(raw.get("v", 1)),
            raw_type=None if is_known else str(etype),
            unknown_keys=unknown_keys,
        )


class Storage(Protocol):
    def append(self, event_type: str, at_ms: int,
               data: dict[str, Any]) -> Event: ...

    def replay(self) -> list[Event]: ...

    @property
    def next_seq(self) -> int: ...

    def close(self) -> None: ...


@dataclass
class InMemoryStorage:
    """内存存储：单测默认实现，严格遵循仅追加语义。"""
    _events: list[Event] = field(default_factory=list)
    _seq: int = 0
    _lock: threading.Lock = field(default_factory=threading.Lock)

    def append(self, event_type: str, at_ms: int,
               data: dict[str, Any]) -> Event:
        with self._lock:
            self._seq += 1
            evt = Event(seq=self._seq, event_type=event_type,
                        at_ms=at_ms, data=dict(data))
            self._events.append(evt)
            return evt

    def replay(self) -> list[Event]:
        with self._lock:
            return list(self._events)

    @property
    def next_seq(self) -> int:
        with self._lock:
            return self._seq + 1

    def close(self) -> None:
        pass


class JsonFileStorage:
    """JSONL 事件日志：每行一个事件，flush+fsync 后才算写入成功。"""

    def __init__(self, path: str) -> None:
        self.path = path
        self._lock = threading.Lock()
        self._seq = 0
        # 若已有日志，先校验并确定序号，避免重启后 seq 冲突。
        if os.path.exists(path):
            for evt in self._read_all():
                if evt.seq != self._seq + 1:
                    raise DomainError(
                        RejectCode.CORRUPT_RECORD,
                        f"事件序号不连续: 期望 {self._seq + 1}，"
                        f"实际 {evt.seq}")
                self._seq = evt.seq
        # a+ 保证追加且不截断既有日志
        self._fh = open(path, "a+", encoding="utf-8")

    def _read_all(self) -> list[Event]:
        events: list[Event] = []
        with open(self.path, "r", encoding="utf-8") as fh:
            for line_no, line in enumerate(fh, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    raw = json.loads(line)
                except json.JSONDecodeError as exc:
                    raise DomainError(
                        RejectCode.CORRUPT_RECORD,
                        f"事件日志第 {line_no} 行不是合法 JSON: {exc}") from exc
                if not isinstance(raw, dict):
                    raise DomainError(
                        RejectCode.CORRUPT_RECORD,
                        f"事件日志第 {line_no} 行不是 JSON 对象")
                events.append(Event.from_dict(raw, line_no=line_no))
        return events

    def append(self, event_type: str, at_ms: int,
               data: dict[str, Any]) -> Event:
        with self._lock:
            self._seq += 1
            evt = Event(seq=self._seq, event_type=event_type,
                        at_ms=at_ms, data=dict(data))
            self._fh.write(json.dumps(evt.to_dict(),
                                      ensure_ascii=False) + "\n")
            self._fh.flush()
            os.fsync(self._fh.fileno())
            return evt

    def replay(self) -> list[Event]:
        with self._lock:
            return self._read_all()

    @property
    def next_seq(self) -> int:
        with self._lock:
            return self._seq + 1

    def close(self) -> None:
        with self._lock:
            if self._fh is not None:
                self._fh.flush()
                self._fh.close()
                self._fh = None
