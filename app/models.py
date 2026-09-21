"""领域模型：任务、机器人、租约及其状态枚举。

记录均带 ``schema_version`` 以支持数据格式演进：
- 反序列化旧记录时按版本补齐新字段的确定缺省值；
- 含未知字段的记录保留未知字段（``unknown_fields``），不静默丢弃，
  且未知字段绝不参与分配判定。
"""
from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any

SCHEMA_VERSION = 1

# 缺省语义常量（新字段对旧记录的稳定缺省值）
DEFAULT_PRIORITY = 0
DEFAULT_PREEMPTIBLE = True
DEFAULT_MAX_RETRIES = 0
DEFAULT_TASK_TIMEOUT_MS: int | None = None
# progress 永远缺省为新的空 dict（见 Task.progress 的 default_factory）


class TaskState(str, Enum):
    PENDING = "PENDING"          # 已下发，等待调度
    QUEUED = "QUEUED"            # 进入有界队列排队
    LEASED = "LEASED"            # 已被某机器人持有租约
    RUNNING = "RUNNING"          # 执行者已开始上报进度
    PREEMPTED = "PREEMPTED"      # 被抢占，保留进度，等待重新分配
    COMPLETED = "COMPLETED"      # 成功完成（终态，结果固化）
    FAILED = "FAILED"            # 重试耗尽后的失败终态
    REJECTED = "REJECTED"        # 下发被明确拒绝（冲突等），不占队列


class RobotState(str, Enum):
    REGISTERED = "REGISTERED"    # 在线
    DRAINING = "DRAINING"        # 暂停领取新任务（可继续持有/完成现有任务）
    STALE = "STALE"              # 心跳超时
    DEREGISTERED = "DEREGISTERED"


class LeaseState(str, Enum):
    ACTIVE = "ACTIVE"
    RENEWED = "RENEWED"
    PREEMPTED = "PREEMPTED"
    COMPLETED = "COMPLETED"
    EXPIRED = "EXPIRED"


@dataclass
class Task:
    task_id: str
    payload: dict[str, Any]
    required_capabilities: frozenset[str] = frozenset()
    priority: int = DEFAULT_PRIORITY
    preemptible: bool = DEFAULT_PREEMPTIBLE
    max_retries: int = DEFAULT_MAX_RETRIES
    timeout_ms: int | None = DEFAULT_TASK_TIMEOUT_MS
    state: TaskState = TaskState.PENDING
    progress: dict[str, Any] = field(default_factory=dict)
    result: dict[str, Any] | None = None
    attempt: int = 0
    created_seq: int = 0
    updated_seq: int = 0
    schema_version: int = SCHEMA_VERSION
    unknown_fields: dict[str, Any] = field(default_factory=dict)


@dataclass
class Robot:
    robot_id: str
    capabilities: frozenset[str] = frozenset()
    state: RobotState = RobotState.REGISTERED
    last_heartbeat_ms: int = 0
    current_task_id: str | None = None
    schema_version: int = SCHEMA_VERSION
    unknown_fields: dict[str, Any] = field(default_factory=dict)


@dataclass
class Lease:
    lease_id: str
    task_id: str
    robot_id: str
    state: LeaseState = LeaseState.ACTIVE
    acquired_ms: int = 0
    expires_at_ms: int = 0
    attempt: int = 0
    fencing_token: int = 0
    schema_version: int = SCHEMA_VERSION
    unknown_fields: dict[str, Any] = field(default_factory=dict)


@dataclass
class EnqueueResult:
    """下发结果：幂等命中 / 新建。"""
    task: Task
    idempotent_hit: bool
