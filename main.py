import os

from fastapi import FastAPI

from app.api import domain_error_response, router as api_router, set_service
from app.errors import DomainError
from app.service import FleetService
from app.timeutil import SystemClock


def create_app() -> FastAPI:
    app = FastAPI(title="Fleet Mission Orchestrator",
                  version="0.1.0")

    # 持久化文件可通过环境变量指定；缺省使用内存存储（进程结束即清空）
    wal_path = os.environ.get("FLEET_WAL_PATH")
    if wal_path:
        service = FleetService.with_json_file(wal_path, clock=SystemClock())
    else:
        service = FleetService.in_memory(clock=SystemClock())
    set_service(service)

    app.add_exception_handler(DomainError,
                              lambda request, exc: domain_error_response(exc))
    app.include_router(api_router)

    @app.get("/")
    def read_root():
        return {"service": "fleet-mission-orchestrator",
                "storage": "json-file" if wal_path else "in-memory"}

    return app


app = create_app()
