"""HTTP 层：把领域用例暴露为 FastAPI 路由。

拒绝统一映射：
- 404：引用不存在的机器人/任务；
- 400：非法配置 / 非法参数；
- 409：语义冲突（重复下发内容冲突、能力不匹配、非持有者、非法跳转、
  抢占失败、租约过期、队列满等）；
- 503：机器人暂不可用 / STALE（调用方可稍后重试）。
每个错误响应都含可区分 ``code`` 与人类可读 ``reason``（判定依据）。
"""
from __future__ import annotations

from typing import Any

from fastapi import APIRouter
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .errors import DomainError, RejectCode
from .models import Lease, Robot, Task
from .service import FleetService

router = APIRouter()

_service: FleetService | None = None


def set_service(service: FleetService) -> None:
    global _service
    _service = service


def get_service() -> FleetService:
    assert _service is not None, "服务尚未装配：请先调用 set_service()"
    return _service


def domain_error_response(exc: DomainError) -> JSONResponse:
    """供应用层注册的 DomainError → 结构化 HTTP 响应。"""
    return _error_response(exc)


# 拒绝码 → HTTP 状态
_STATUS_MAP = {
    RejectCode.ROBOT_NOT_FOUND: 404,
    RejectCode.TASK_NOT_FOUND: 404,
    RejectCode.INVALID_CONFIG: 400,
    RejectCode.CORRUPT_RECORD: 500,
    RejectCode.ROBOT_UNAVAILABLE: 503,
    RejectCode.ROBOT_STALE: 503,
}
_CONFLICT_CODES = {
    RejectCode.ROBOT_ALREADY_REGISTERED,
    RejectCode.ROBOT_CAPABILITY_MISMATCH,
    RejectCode.TASK_DUPLICATE_CONFLICT,
    RejectCode.TASK_NOT_CLAIMABLE,
    RejectCode.TASK_NOT_LEASE_HOLDER,
    RejectCode.TASK_INVALID_TRANSITION,
    RejectCode.TASK_NOT_PREEMPTIBLE,
    RejectCode.PREEMPT_NO_ELIGIBLE_VICTIM,
    RejectCode.LEASE_EXPIRED,
    RejectCode.LEASE_VERSION_CONFLICT,
    RejectCode.QUEUE_FULL,
    RejectCode.NO_ELIGIBLE_ROBOT,
}


def _error_response(exc: DomainError) -> JSONResponse:
    status = _STATUS_MAP.get(exc.code,
                             409 if exc.code in _CONFLICT_CODES else 400)
    return JSONResponse(
        status_code=status,
        content={"ok": False, "code": exc.code.value, "reason": exc.reason})


# ---------------- 序列化 ----------------
def _robot(r: Robot) -> dict[str, Any]:
    return {"robot_id": r.robot_id, "state": r.state.value,
            "capabilities": sorted(r.capabilities),
            "last_heartbeat_ms": r.last_heartbeat_ms,
            "current_task_id": r.current_task_id,
            "schema_version": r.schema_version}


def _task(t: Task) -> dict[str, Any]:
    return {"task_id": t.task_id, "state": t.state.value,
            "payload": t.payload,
            "required_capabilities": sorted(t.required_capabilities),
            "priority": t.priority, "preemptible": t.preemptible,
            "max_retries": t.max_retries, "timeout_ms": t.timeout_ms,
            "attempt": t.attempt, "progress": t.progress,
            "result": t.result, "schema_version": t.schema_version}


def _lease(l: Lease) -> dict[str, Any]:
    return {"lease_id": l.lease_id, "task_id": l.task_id,
            "robot_id": l.robot_id, "state": l.state.value,
            "fencing_token": l.fencing_token, "attempt": l.attempt,
            "acquired_ms": l.acquired_ms,
            "expires_at_ms": l.expires_at_ms,
            "schema_version": l.schema_version}


# ---------------- 请求模型 ----------------
class RegisterReq(BaseModel):
    robot_id: str
    capabilities: list[str] = Field(default_factory=list)


class HeartbeatReq(BaseModel):
    robot_id: str


class CapabilitiesReq(BaseModel):
    robot_id: str
    capabilities: list[str]


class DrainReq(BaseModel):
    robot_id: str
    draining: bool


class SubmitTaskReq(BaseModel):
    task_id: str
    payload: dict[str, Any] = Field(default_factory=dict)
    required_capabilities: list[str] = Field(default_factory=list)
    priority: int = 0
    preemptible: bool = True
    max_retries: int = 0
    timeout_ms: int | None = None


class ClaimReq(BaseModel):
    robot_id: str


class StartReq(BaseModel):
    task_id: str
    robot_id: str
    fencing_token: int


class RenewReq(BaseModel):
    task_id: str
    robot_id: str
    fencing_token: int
    duration_ms: int | None = None


class ProgressReq(BaseModel):
    task_id: str
    robot_id: str
    fencing_token: int
    progress: dict[str, Any]


class CompleteReq(BaseModel):
    task_id: str
    robot_id: str
    fencing_token: int
    result: dict[str, Any] = Field(default_factory=dict)


class PreemptReq(BaseModel):
    high_prio_task_id: str
    victim_task_id: str
    by_robot_id: str


# ---------------- 路由 ----------------
@router.post("/robots/register")
def register_robot(req: RegisterReq):
    svc = get_service()
    res = svc.register_robot(req.robot_id, req.capabilities)
    return {"ok": True, "idempotent_hit": res.idempotent_hit,
            "robot": _robot(res.robot)}


@router.post("/robots/heartbeat")
def heartbeat(req: HeartbeatReq):
    return {"ok": True, "robot": _robot(get_service().heartbeat(req.robot_id))}


@router.put("/robots/capabilities")
def update_capabilities(req: CapabilitiesReq):
    r = get_service().update_capabilities(req.robot_id, req.capabilities)
    return {"ok": True, "robot": _robot(r)}


@router.put("/robots/drain")
def set_draining(req: DrainReq):
    r = get_service().set_draining(req.robot_id, req.draining)
    return {"ok": True, "robot": _robot(r)}


@router.post("/tasks")
def submit_task(req: SubmitTaskReq):
    res = get_service().submit_task(
        req.task_id, req.payload,
        required_capabilities=req.required_capabilities,
        priority=req.priority, preemptible=req.preemptible,
        max_retries=req.max_retries, timeout_ms=req.timeout_ms)
    return {"ok": True, "idempotent_hit": res.idempotent_hit,
            "task": _task(res.task)}


@router.post("/tasks/claim")
def claim_next(req: ClaimReq):
    res = get_service().claim_next(req.robot_id)
    if res is None:
        # 排队/降级：当前没有合格任务，200 + granted=False（非错误）
        return JSONResponse(status_code=200, content={
            "ok": True, "granted": False,
            "reason": "当前队列中无该机器人可领取的合格任务（排队/降级）"})
    return {"ok": True, "granted": True, "resumed": res.resumed,
            "task": _task(res.task), "lease": _lease(res.lease),
            "progress": res.progress}


@router.post("/tasks/start")
def start_task(req: StartReq):
    t = get_service().start_task(req.task_id, req.robot_id, req.fencing_token)
    return {"ok": True, "task": _task(t)}


@router.post("/tasks/renew")
def renew_lease(req: RenewReq):
    l = get_service().renew_lease(req.task_id, req.robot_id,
                                  req.fencing_token, req.duration_ms)
    return {"ok": True, "lease": _lease(l)}


@router.post("/tasks/progress")
def report_progress(req: ProgressReq):
    t = get_service().report_progress(
        req.task_id, req.robot_id, req.fencing_token, req.progress)
    return {"ok": True, "task": _task(t)}


@router.post("/tasks/complete")
def complete_task(req: CompleteReq):
    t = get_service().complete_task(
        req.task_id, req.robot_id, req.fencing_token, req.result)
    return {"ok": True, "task": _task(t)}


@router.post("/tasks/preempt")
def preempt(req: PreemptReq):
    t = get_service().preempt(
        req.high_prio_task_id, req.victim_task_id, req.by_robot_id)
    return {"ok": True, "victim_task": _task(t)}


@router.post("/scheduler/reap")
def reap():
    decisions = get_service().reap_stale_leases()
    return {"ok": True, "count": len(decisions),
            "decisions": [
                {"action": d.action, "at_ms": d.at_ms,
                 "task_id": d.task_id, "robot_id": d.robot_id,
                 "reason": d.reason, "detail": d.detail}
                for d in decisions]}


@router.get("/snapshot")
def snapshot():
    return {"ok": True, "snapshot": get_service().snapshot()}
