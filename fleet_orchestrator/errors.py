"""错误分类：所有失败路径的拒绝原因必须可区分。

每个 Rejection 携带稳定的 reason 字符串（机器可判定）与 detail（人可读）。
API 层将 reason 映射为 HTTP 状态码，测试可断言具体 reason。
"""

from __future__ import annotations


class Rejection(Exception):
    """业务拒绝基类。reason 为稳定标识符，用于区分失败路径。"""

    reason: str = "rejected"
    http_status: int = 409

    def __init__(self, detail: str = "") -> None:
        super().__init__(detail)
        self.detail = detail

    def to_dict(self) -> dict:
        return {"error": self.reason, "detail": self.detail}


# ---- 机器人相关 ----

class RobotNotFound(Rejection):
    reason = "robot_not_found"
    http_status = 404


class RobotCapabilityMismatch(Rejection):
    """机器人能力不满足任务要求。"""

    reason = "capability_mismatch"
    http_status = 409


class RobotNotAvailable(Rejection):
    """机器人离线/心跳超时/已有租约，不可领取。"""

    reason = "robot_not_available"
    http_status = 409


# ---- 任务下发（幂等）相关 ----

class MissionPayloadConflict(Rejection):
    """同一 mission_id 携带不同内容重复下发。"""

    reason = "mission_payload_conflict"
    http_status = 409


class DependencyCycle(Rejection):
    """依赖声明成环（直接或间接依赖回自身），下发被拒绝。"""

    reason = "dependency_cycle"
    http_status = 409


# ---- 租约相关 ----

class MissionNotFound(Rejection):
    reason = "mission_not_found"
    http_status = 404


class MissionNotAssignable(Rejection):
    """任务当前状态不允许被领取（非 PENDING 或已被持有）。"""

    reason = "mission_not_assignable"
    http_status = 409


class LeaseNotHeld(Rejection):
    """操作者不是当前租约持有者。"""

    reason = "lease_not_held"
    http_status = 409


class LeaseExpired(Rejection):
    """租约已过期，操作被拒绝。"""

    reason = "lease_expired"
    http_status = 409


# ---- 状态机相关 ----

class InvalidStateTransition(Rejection):
    """非法状态跳转。"""

    reason = "invalid_state_transition"
    http_status = 409


# ---- 抢占相关 ----

class MissionNotPreemptible(Rejection):
    """目标任务不可抢占。"""

    reason = "mission_not_preemptible"
    http_status = 409


class PreemptTargetNotRunning(Rejection):
    """目标任务不在可抢占状态。"""

    reason = "preempt_target_not_running"
    http_status = 409


# ---- 配置相关 ----

class ConfigOutOfRange(Rejection):
    reason = "config_out_of_range"
    http_status = 422


# ---- 资源/积压相关 ----

class BacklogFull(Rejection):
    """待分配队列已满，按策略拒绝。"""

    reason = "backlog_full"
    http_status = 429


class RetriesExhausted(Rejection):
    """任务重试次数已用尽，不允许重新入队。"""

    reason = "retries_exhausted"
    http_status = 409


class PreemptPriorityTooLow(Rejection):
    """发起抢占的任务优先级未严格高于目标任务。"""

    reason = "preempt_priority_too_low"
    http_status = 409
