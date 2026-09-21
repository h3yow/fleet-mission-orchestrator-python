"""HTTP 层：把领域用例暴露为 FastAPI 路由。

拒绝统一映射为 ``409``（语义冲突）/``404``/``400``，响应体包含
可区分的 ``code`` 与人类可读 ``reason``（判定依据）。
"""
from __future__ import annotations

from typing import Any, Iterable

from fastapi import APIRouter
from pydantic import BaseModel, Field

from .errors import DomainError, RejectCode
from .service import FleetService

router = APIRouter()

# 由 main.py 在启动时装配（测试可构造自己的服务实例）
_service: FleetService | None = None


def set_service(service: FleetService) -> None:
    raise NotImplementedError


def get_service() -> FleetService:
    raise NotImplementedError


# ---------- 请求模型 ----------
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


# ---------- 路由（骨架，待实现） ----------
@router.post("/robots/register")
def register_robot(req: RegisterReq) -> dict[str, Any]:
    raise NotImplementedError


@router.post("/robots/heartbeat")
def heartbeat(req: HeartbeatReq) -> dict[str, Any]:
    raise NotImplementedError


@router.put("/robots/capabilities")
def update_capabilities(req: CapabilitiesReq) -> dict[str, Any]:
    raise NotImplementedError


@router.put("/robots/drain")
def set_draining(req: DrainReq) -> dict[str, Any]:
    raise NotImplementedError


@router.post("/tasks")
def submit_task(req: SubmitTaskReq) -> dict[str, Any]:
    raise NotImplementedError


@router.post("/tasks/claim")
def claim_next(req: ClaimReq) -> dict[str, Any]:
    raise NotImplementedError


@router.post("/tasks/start")
def start_task(req: StartReq) -> dict[str, Any]:
    raise NotImplementedError


@router.post("/tasks/renew")
def renew_lease(req: RenewReq) -> dict[str, Any]:
    raise NotImplementedError


@router.post("/tasks/progress")
def report_progress(req: ProgressReq) -> dict[str, Any]:
    raise NotImplementedError


@router.post("/tasks/complete")
def complete_task(req: CompleteReq) -> dict[str, Any]:
    raise NotImplementedError


@router.post("/tasks/preempt")
def preempt(req: PreemptReq) -> dict[str, Any]:
    raise NotImplementedError


@router.post("/scheduler/reap")
def reap() -> dict[str, Any]:
    raise NotImplementedError


@router.get("/snapshot")
def snapshot() -> dict[str, Any]:
    raise NotImplementedError
