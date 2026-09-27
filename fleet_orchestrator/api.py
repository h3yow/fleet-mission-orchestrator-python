"""HTTP API 层：将服务层拒绝原因映射为可区分的 HTTP 响应。"""

from __future__ import annotations

import os
from typing import Any

from fastapi import FastAPI
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from .errors import Rejection
from .models import MissionStatus
from .service import OrchestratorService
from .store import Store


class RegisterRobotReq(BaseModel):
    robot_id: str
    capabilities: list[str] = Field(default_factory=list)


class HeartbeatReq(BaseModel):
    robot_id: str


class SubmitMissionReq(BaseModel):
    mission_id: str
    required_capabilities: list[str] = Field(default_factory=list)
    priority: int = 0
    preemptible: bool = True
    payload: dict[str, Any] = Field(default_factory=dict)
    max_attempts: int | None = None
    depends_on: list[str] = Field(default_factory=list)


class AcquireReq(BaseModel):
    robot_id: str
    mission_id: str | None = None


class LeaseOpReq(BaseModel):
    robot_id: str
    lease_id: str


class ProgressReq(LeaseOpReq):
    progress: dict[str, Any] = Field(default_factory=dict)


class CompleteReq(LeaseOpReq):
    final_progress: dict[str, Any] | None = None


class FailReq(LeaseOpReq):
    error: str = ""


class PreemptReq(BaseModel):
    by_mission_id: str | None = None


class ConfigReq(BaseModel):
    overrides: dict[str, float]


def create_app(service: OrchestratorService | None = None) -> FastAPI:
    if service is None:
        data_dir = os.environ.get("ORCHESTRATOR_DATA_DIR", "./orchestrator-data")
        service = OrchestratorService(Store(data_dir))
    app = FastAPI(title="Fleet Mission Orchestrator")
    app.state.service = service

    @app.exception_handler(Rejection)
    async def rejection_handler(_req: Any, exc: Rejection) -> JSONResponse:
        return JSONResponse(status_code=exc.http_status, content=exc.to_dict())

    # ---- 机器人 ----

    @app.post("/robots/register", status_code=200)
    def register_robot(req: RegisterRobotReq) -> Any:
        return service.register_robot(req.robot_id, req.capabilities)

    @app.post("/robots/deregister")
    def deregister_robot(req: HeartbeatReq) -> Any:
        return service.deregister_robot(req.robot_id)

    @app.post("/robots/heartbeat")
    def heartbeat(req: HeartbeatReq) -> Any:
        return service.heartbeat(req.robot_id)

    @app.get("/robots")
    def list_robots() -> Any:
        return service.list_robots()

    # ---- 任务 ----

    @app.post("/missions", status_code=200)
    def submit_mission(req: SubmitMissionReq) -> Any:
        mission, created = service.submit_mission(
            req.mission_id, req.required_capabilities, req.priority,
            req.preemptible, req.payload, req.max_attempts, req.depends_on,
        )
        return {"created": created, "mission": mission}

    @app.post("/missions/acquire")
    def acquire(req: AcquireReq) -> Any:
        mission, lease = service.acquire_mission(req.robot_id, req.mission_id)
        return {"mission": mission, "lease": lease}

    @app.post("/missions/{mission_id}/start")
    def start(mission_id: str, req: LeaseOpReq) -> Any:
        return service.start_mission(req.robot_id, mission_id, req.lease_id)

    @app.post("/missions/{mission_id}/progress")
    def progress(mission_id: str, req: ProgressReq) -> Any:
        return service.report_progress(req.robot_id, mission_id, req.lease_id, req.progress)

    @app.post("/missions/{mission_id}/complete")
    def complete(mission_id: str, req: CompleteReq) -> Any:
        return service.complete_mission(
            req.robot_id, mission_id, req.lease_id, req.final_progress
        )

    @app.post("/missions/{mission_id}/fail")
    def fail(mission_id: str, req: FailReq) -> Any:
        return service.fail_mission(req.robot_id, mission_id, req.lease_id, req.error)

    @app.post("/missions/{mission_id}/renew")
    def renew(mission_id: str, req: LeaseOpReq) -> Any:
        return service.renew_lease(req.robot_id, mission_id, req.lease_id)

    @app.post("/missions/{mission_id}/retry")
    def retry(mission_id: str) -> Any:
        return service.retry_mission(mission_id)

    @app.post("/missions/{mission_id}/cancel")
    def cancel(mission_id: str) -> Any:
        return service.cancel_mission(mission_id)

    @app.post("/missions/{mission_id}/preempt")
    def preempt(mission_id: str, req: PreemptReq) -> Any:
        return service.preempt_mission(mission_id, req.by_mission_id)

    @app.get("/missions")
    def list_missions(status: MissionStatus | None = None) -> Any:
        return service.list_missions(status)

    @app.get("/missions/{mission_id}")
    def get_mission(mission_id: str) -> Any:
        return service.get_mission(mission_id)

    @app.get("/missions/{mission_id}/lease")
    def get_lease(mission_id: str) -> Any:
        lease = service.get_lease(mission_id)
        return {"lease": lease}

    @app.get("/missions/{mission_id}/dependencies")
    def get_dependencies(mission_id: str) -> Any:
        return service.dependency_view(mission_id)

    # ---- 运维 ----

    @app.post("/admin/sweep")
    def sweep() -> Any:
        return {"reclaimed": service.sweep()}

    @app.post("/admin/config")
    def update_config(req: ConfigReq) -> Any:
        return service.update_config(**req.overrides)

    @app.get("/admin/config")
    def get_config() -> Any:
        return service.config

    return app
