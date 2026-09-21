"""事件总线：持久化 + 固定顺序扇出投影。

所有状态变更只允许经由总线发布：
1. 先把事件原子追加到 WAL（持久化成功才算发生）；
2. 再按固定顺序（registry → tasks → leases）投影到各管理器内存状态。

这样命令路径与重启回放路径走的是同一份 ``apply`` 代码，
杜绝半更新与重放分歧。总线的整段发布在服务级串行锁内执行。
"""
from __future__ import annotations

from typing import Any, Protocol

from .storage import Event, Storage
from .timeutil import Clock

_AT = "_at_ms"


class Projector(Protocol):
    def apply(self, event_type: str, data: dict[str, Any]) -> None: ...


class EventBus:
    def __init__(self, storage: Storage, clock: Clock,
                 projectors: list[Projector]) -> None:
        self.storage = storage
        self.clock = clock
        # 固定顺序：机器人 → 任务 → 租约
        self.projectors = projectors

    def publish(self, event_type: str, data: dict[str, Any],
                at_ms: int | None = None) -> Event:
        at_ms = self.clock.now_ms() if at_ms is None else at_ms
        payload = dict(data)
        payload.setdefault(_AT, at_ms)
        evt = self.storage.append(event_type, at_ms, payload)
        for proj in self.projectors:
            proj.apply(evt.event_type, evt.data)
        return evt

    def replay_all(self) -> list[Event]:
        """重启恢复：按 WAL 全序投影全部事件。"""
        events = self.storage.replay()
        for evt in events:
            # 未知新版本事件：跳过投影但保留在日志中，不报错
            if evt.event_type is None:
                continue
            for proj in self.projectors:
                proj.apply(evt.event_type, evt.data)
        return events
