"""可注入的时钟抽象。

生产使用墙钟毫秒；测试注入 :class:`FakeClock` 手动推进，
以确定性地验证心跳超时、租约过期与回收竞态。
"""
from __future__ import annotations

from typing import Protocol


class Clock(Protocol):
    def now_ms(self) -> int: ...


class SystemClock:
    def now_ms(self) -> int:
        raise NotImplementedError


class FakeClock:
    """测试时钟：手动设置/推进时间。"""

    def __init__(self, start_ms: int = 0) -> None:
        self._now = start_ms

    def now_ms(self) -> int:
        raise NotImplementedError

    def advance(self, delta_ms: int) -> int:
        raise NotImplementedError

    def set(self, ms: int) -> None:
        raise NotImplementedError
