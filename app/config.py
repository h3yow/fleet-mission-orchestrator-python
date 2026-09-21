"""调度与超时参数的可配置上限。

关键设计：
- 所有时长参数单位为毫秒（ms），优先级为整数；
- 每个参数既有当前值也有允许上限，调用方不能通过配置绕过资源边界；
- 构造时严格校验，非法或越界配置抛 ``INVALID_CONFIG``，服务拒绝启动；
- ``validate_*`` 方法在任务声明覆盖默认值时做边界校验。
"""
from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class SchedulerConfig:
    heartbeat_timeout_ms: int = 10_000
    lease_duration_ms: int = 30_000
    max_lease_duration_ms: int = 300_000
    max_queue_size: int = 10_000
    max_attempts: int = 5
    max_priority: int = 100
    min_priority: int = -100
    max_task_timeout_ms: int = 3_600_000
    max_retries_cap: int = 10

    def __post_init__(self) -> None:
        raise NotImplementedError

    # -- 任务级参数边界校验（返回标准化后的值） --
    def validate_priority(self, value: int) -> int:
        raise NotImplementedError

    def validate_retries(self, value: int) -> int:
        raise NotImplementedError

    def validate_timeout_ms(self, value: int | None) -> int | None:
        raise NotImplementedError
