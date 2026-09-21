"""可注入的时钟抽象。

生产使用墙钟毫秒；测试注入 :class:`FakeClock` 手动推进，
以确定性地验证心跳超时、租约过期与回收竞态。
"""
from __future__ import annotations

import time
from typing import Protocol


class Clock(Protocol):
    def now_ms(self) -> int: ...


class SystemClock:
    """单调墙钟毫秒，生产使用。"""

    def now_ms(self) -> int:
        # time.monotonic 只用于进程内超时判定；落盘时间戳另由调用方决定。
        return int(time.monotonic() * 1000)


class FakeClock:
    """测试时钟：手动设置/推进时间，不会自行流逝。"""

    def __init__(self, start_ms: int = 0) -> None:
        self._now = start_ms

    def now_ms(self) -> int:
        return self._now

    def advance(self, delta_ms: int) -> int:
        if delta_ms < 0:
            raise ValueError("delta_ms 不得为负")
        self._now += delta_ms
        return self._now

    def set(self, ms: int) -> None:
        if ms < self._now:
            raise ValueError("测试时钟不可倒流（避免破坏租约全序）")
        self._now = ms
