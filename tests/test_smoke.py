"""骨架冒烟测试：确认所有模块可导入、应用可装配。"""
from fastapi.testclient import TestClient

import main
from app import api, config, errors, leases, models, registry, scheduler, service, storage, tasks  # noqa: F401
from app.timeutil import FakeClock


def test_modules_importable():
    assert models.TaskState.COMPLETED.value == "COMPLETED"
    assert errors.RejectCode.TASK_NOT_FOUND.value == "TASK_NOT_FOUND"
    assert FakeClock is not None


def test_root_endpoint():
    client = TestClient(main.app)
    resp = client.get("/")
    assert resp.status_code == 200
    assert resp.json() == {"Hello": "World"}
