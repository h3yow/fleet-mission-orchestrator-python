"""可区分的拒绝原因。

每个失败路径都携带唯一的 :class:`RejectCode`，调用方可据此区分处理；
异常消息中同时给出判定依据，便于单测打印与排障。
"""
from __future__ import annotations

from enum import Enum


class RejectCode(Enum):
    # ---- 机器人 ----
    ROBOT_ALREADY_REGISTERED = "ROBOT_ALREADY_REGISTERED"
    ROBOT_NOT_FOUND = "ROBOT_NOT_FOUND"
    ROBOT_UNAVAILABLE = "ROBOT_UNAVAILABLE"
    ROBOT_CAPABILITY_MISMATCH = "ROBOT_CAPABILITY_MISMATCH"
    ROBOT_STALE = "ROBOT_STALE"

    # ---- 任务 ----
    TASK_NOT_FOUND = "TASK_NOT_FOUND"
    TASK_DUPLICATE_CONFLICT = "TASK_DUPLICATE_CONFLICT"
    TASK_NOT_CLAIMABLE = "TASK_NOT_CLAIMABLE"
    TASK_NOT_LEASE_HOLDER = "TASK_NOT_LEASE_HOLDER"
    TASK_INVALID_TRANSITION = "TASK_INVALID_TRANSITION"
    TASK_NOT_PREEMPTIBLE = "TASK_NOT_PREEMPTIBLE"
    PREEMPT_NO_ELIGIBLE_VICTIM = "PREEMPT_NO_ELIGIBLE_VICTIM"

    # ---- 租约 ----
    LEASE_EXPIRED = "LEASE_EXPIRED"
    LEASE_VERSION_CONFLICT = "LEASE_VERSION_CONFLICT"

    # ---- 队列 / 资源 ----
    QUEUE_FULL = "QUEUE_FULL"
    NO_ELIGIBLE_ROBOT = "NO_ELIGIBLE_ROBOT"

    # ---- 配置 ----
    INVALID_CONFIG = "INVALID_CONFIG"

    # ---- 持久化 ----
    CORRUPT_RECORD = "CORRUPT_RECORD"


class DomainError(Exception):
    """所有可预期的领域拒绝统一异常。"""

    def __init__(self, code: RejectCode, reason: str):
        self.code = code
        self.reason = reason
        super().__init__(f"[{code.value}] {reason}")
