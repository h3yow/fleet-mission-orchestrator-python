"""调度与超时参数配置。

所有参数有确定的范围校验；非法或超范围时抛出 ConfigOutOfRange，
拒绝原因可区分（携带具体字段与允许范围）。
"""

from __future__ import annotations

from dataclasses import dataclass, field

from .errors import ConfigOutOfRange

# (min, max) 闭区间；None 表示无界方向不允许出现（所有参数都必须有界）
_BOUNDS: dict[str, tuple[float, float]] = {
    "lease_duration_s": (1.0, 3600.0),        # 租约时长
    "heartbeat_timeout_s": (1.0, 3600.0),     # 心跳超时判定
    "max_backlog": (1.0, 100000.0),           # 待分配队列上限（防无界占用）
    "max_attempts_default": (1.0, 100.0),     # 任务默认最大重试次数
    "max_lease_renewals": (0.0, 1000.0),      # 单次租约最大续约次数
    "renew_extension_s": (1.0, 3600.0),       # 每次续约延长时长
}


@dataclass(frozen=True)
class SchedulerConfig:
    lease_duration_s: float = 30.0
    heartbeat_timeout_s: float = 15.0
    max_backlog: int = 1000
    max_attempts_default: int = 3
    max_lease_renewals: int = 10
    renew_extension_s: float = 30.0

    def __post_init__(self) -> None:
        for name, (lo, hi) in _BOUNDS.items():
            value = getattr(self, name)
            if not (lo <= float(value) <= hi):
                raise ConfigOutOfRange(
                    f"config '{name}'={value} out of range [{lo}, {hi}]"
                )
        # 整数语义字段强制取整，避免浮点歧义
        for name in ("max_backlog", "max_attempts_default", "max_lease_renewals"):
            object.__setattr__(self, name, int(getattr(self, name)))

    def updated(self, **overrides: float) -> "SchedulerConfig":
        """以校验后的新配置替换部分字段；非法值整体拒绝，不产生半更新。"""
        current = {name: getattr(self, name) for name in _BOUNDS}
        unknown = set(overrides) - set(_BOUNDS)
        if unknown:
            raise ConfigOutOfRange(f"unknown config keys: {sorted(unknown)}")
        current.update(overrides)
        return SchedulerConfig(**current)  # type: ignore[arg-type]
