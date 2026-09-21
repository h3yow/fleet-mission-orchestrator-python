"""领域模型与状态机定义。

数据格式演进策略：
- 所有持久化模型使用 extra="allow"：含未知字段的旧/新记录不会报错，
  未知字段被原样保留（不静默丢弃），但不参与调度判定。
- 新增字段一律带默认值，缺省语义确定且稳定。
"""

from __future__ import annotations

import enum
from typing import Any

from pydantic import BaseModel, ConfigDict, Field


class RobotStatus(str, enum.Enum):
    ONLINE = "ONLINE"      # 心跳正常，可领取任务
    STALE = "STALE"        # 心跳超时，租约可被回收
    OFFLINE = "OFFLINE"    # 显式注销


class MissionStatus(str, enum.Enum):
    PENDING = "PENDING"        # 已下发，等待分配
    ASSIGNED = "ASSIGNED"      # 已建立租约，未开始执行
    RUNNING = "RUNNING"        # 执行中
    PREEMPTED = "PREEMPTED"    # 被抢占，保留进度，等待重分配
    SUCCEEDED = "SUCCEEDED"    # 完成（终态）
    FAILED = "FAILED"          # 失败（终态，可重试时回到 PENDING）
    CANCELLED = "CANCELLED"    # 取消（终态）


TERMINAL_STATES = frozenset(
    {MissionStatus.SUCCEEDED, MissionStatus.FAILED, MissionStatus.CANCELLED}
)

# 合法状态跳转表：source -> 允许的 target 集合
MISSION_TRANSITIONS: dict[MissionStatus, frozenset[MissionStatus]] = {
    MissionStatus.PENDING: frozenset(
        {MissionStatus.ASSIGNED, MissionStatus.CANCELLED}
    ),
    MissionStatus.ASSIGNED: frozenset(
        {MissionStatus.RUNNING, MissionStatus.PENDING, MissionStatus.PREEMPTED,
         MissionStatus.FAILED, MissionStatus.CANCELLED}
    ),
    MissionStatus.RUNNING: frozenset(
        {MissionStatus.SUCCEEDED, MissionStatus.FAILED, MissionStatus.PREEMPTED,
         MissionStatus.PENDING, MissionStatus.CANCELLED}
    ),
    MissionStatus.PREEMPTED: frozenset(
        {MissionStatus.ASSIGNED, MissionStatus.CANCELLED}
    ),
    MissionStatus.SUCCEEDED: frozenset(),
    MissionStatus.FAILED: frozenset({MissionStatus.PENDING}),  # 重试
    MissionStatus.CANCELLED: frozenset(),
}


def can_transition(src: MissionStatus, dst: MissionStatus) -> bool:
    return dst in MISSION_TRANSITIONS.get(src, frozenset())


class EvolveModel(BaseModel):
    """持久化模型基类：容忍未知字段，缺省字段有确定默认值。"""

    model_config = ConfigDict(extra="allow", use_enum_values=False)


class Robot(EvolveModel):
    robot_id: str
    capabilities: list[str] = Field(default_factory=list)
    status: RobotStatus = RobotStatus.ONLINE
    last_heartbeat: float = 0.0          # 最近一次心跳的单调时钟读数
    registered_at: float = 0.0
    current_mission_id: str | None = None  # 冗余视图，唯一事实源是 Lease


class Mission(EvolveModel):
    mission_id: str
    payload_hash: str = ""               # 内容指纹，用于幂等下发判定
    required_capabilities: list[str] = Field(default_factory=list)
    priority: int = 0                    # 数值越大优先级越高
    preemptible: bool = True
    status: MissionStatus = MissionStatus.PENDING
    progress: dict[str, Any] = Field(default_factory=dict)  # 可续执行进度
    attempts: int = 0                    # 已执行次数（含被抢占/回收）
    max_attempts: int = 3
    created_at: float = 0.0
    # 调度时序字段：入队序号，保证同优先级 FIFO 确定性
    enqueue_seq: int = 0
    # 租约代数（随任务持久化，回收/重启后仍单调递增，旧凭证可识别）
    lease_generation: int = 0


class Lease(EvolveModel):
    mission_id: str
    robot_id: str
    lease_id: str                        # 每次授予生成新 id，防止旧持有者误操作
    acquired_at: float = 0.0
    expires_at: float = 0.0              # 单调时钟截止点
    generation: int = 0                  # 租约代数，回收/重分配时递增
    renewals: int = 0                    # 已续约次数（受配置上限约束）
