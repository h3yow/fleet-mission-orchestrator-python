"""调度与超时参数的可配置上限。

关键设计：
- 所有时长参数单位为毫秒（ms），优先级为整数；
- 每个参数既有当前值也有允许上限，调用方不能通过任务级参数绕过资源边界；
- 构造时严格校验，非法或越界配置抛 ``INVALID_CONFIG``，服务拒绝启动；
- ``validate_*`` 方法在任务声明覆盖默认值时做同样的边界校验。
"""
from __future__ import annotations

from dataclasses import dataclass

from .errors import DomainError, RejectCode


@dataclass(frozen=True)
class SchedulerConfig:
    # 心跳超过该时长未到达 → 机器人判定 STALE
    heartbeat_timeout_ms: int = 10_000
    # 租约默认时长
    lease_duration_ms: int = 30_000
    # 租约时长允许上限（任务/续约不可超过）
    max_lease_duration_ms: int = 300_000
    # 待调度队列长度上限（有界，拒绝无界占用资源）
    max_queue_size: int = 10_000
    # 任务最大尝试次数（首次执行 + 重试）
    max_attempts: int = 5
    max_priority: int = 100
    min_priority: int = -100
    # 任务声明的执行超时上限
    max_task_timeout_ms: int = 3_600_000
    # 任务级 max_retries 的硬上限
    max_retries_cap: int = 10

    def __post_init__(self) -> None:
        problems: list[str] = []

        def positive(name: str, value: int) -> None:
            if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
                problems.append(f"{name} 必须为正整数，实际={value!r}")

        positive("heartbeat_timeout_ms", self.heartbeat_timeout_ms)
        positive("lease_duration_ms", self.lease_duration_ms)
        positive("max_lease_duration_ms", self.max_lease_duration_ms)
        positive("max_task_timeout_ms", self.max_task_timeout_ms)
        if self.lease_duration_ms > self.max_lease_duration_ms:
            problems.append(
                "lease_duration_ms 超过 max_lease_duration_ms: "
                f"{self.lease_duration_ms} > {self.max_lease_duration_ms}")
        if not isinstance(self.max_queue_size, int) or isinstance(
                self.max_queue_size, bool) or self.max_queue_size < 1:
            problems.append(f"max_queue_size 必须 >= 1，实际={self.max_queue_size!r}")
        if not isinstance(self.max_attempts, int) or isinstance(
                self.max_attempts, bool) or self.max_attempts < 1:
            problems.append(f"max_attempts 必须 >= 1，实际={self.max_attempts!r}")
        if not isinstance(self.max_retries_cap, int) or isinstance(
                self.max_retries_cap, bool) or self.max_retries_cap < 0:
            problems.append(f"max_retries_cap 必须 >= 0，实际={self.max_retries_cap!r}")
        if self.min_priority > self.max_priority:
            problems.append(
                f"min_priority({self.min_priority}) 不得大于 "
                f"max_priority({self.max_priority})")

        if problems:
            raise DomainError(
                RejectCode.INVALID_CONFIG,
                "非法调度配置: " + "; ".join(problems))

    # -- 任务级参数边界校验（返回标准化后的值） --
    def validate_priority(self, value: int) -> int:
        if not isinstance(value, int) or isinstance(value, bool):
            raise DomainError(
                RejectCode.INVALID_CONFIG,
                f"priority 必须为整数，实际={value!r}")
        if not self.min_priority <= value <= self.max_priority:
            raise DomainError(
                RejectCode.INVALID_CONFIG,
                f"priority={value} 超出允许范围 "
                f"[{self.min_priority}, {self.max_priority}]")
        return value

    def validate_retries(self, value: int) -> int:
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise DomainError(
                RejectCode.INVALID_CONFIG,
                f"max_retries 必须为非负整数，实际={value!r}")
        if value > self.max_retries_cap:
            raise DomainError(
                RejectCode.INVALID_CONFIG,
                f"max_retries={value} 超过上限 {self.max_retries_cap}")
        return value

    def validate_timeout_ms(self, value: int | None) -> int | None:
        if value is None:
            return None
        if not isinstance(value, int) or isinstance(value, bool) or value <= 0:
            raise DomainError(
                RejectCode.INVALID_CONFIG,
                f"timeout_ms 必须为正整数或 null，实际={value!r}")
        if value > self.max_task_timeout_ms:
            raise DomainError(
                RejectCode.INVALID_CONFIG,
                f"timeout_ms={value} 超过上限 {self.max_task_timeout_ms}")
        return value
