"""API 层：拒绝原因以可区分的 error 字段返回。"""

import pytest
from fastapi.testclient import TestClient

from fleet_orchestrator.api import create_app


@pytest.fixture()
def client(service):
    return TestClient(create_app(service))


def test_full_flow_over_http(client, case_log):
    r = client.post("/robots/register", json={"robot_id": "r1", "capabilities": ["nav"]})
    assert r.status_code == 200
    r = client.post("/missions", json={"mission_id": "m1", "required_capabilities": ["nav"]})
    assert r.json()["created"] is True
    r = client.post("/missions/acquire", json={"robot_id": "r1", "mission_id": "m1"})
    lease = r.json()["lease"]
    r = client.post("/missions/m1/start", json={"robot_id": "r1", "lease_id": lease["lease_id"]})
    assert r.json()["status"] == "RUNNING"
    r = client.post("/missions/m1/complete", json={"robot_id": "r1", "lease_id": lease["lease_id"]})
    case_log("HTTP 全流程", expect="SUCCEEDED", actual=r.json()["status"])
    assert r.json()["status"] == "SUCCEEDED"


def test_rejection_reasons_over_http(client, case_log):
    client.post("/robots/register", json={"robot_id": "r1", "capabilities": []})
    client.post("/missions", json={"mission_id": "m1", "required_capabilities": ["nav"]})
    r = client.post("/missions/acquire", json={"robot_id": "r1", "mission_id": "m1"})
    case_log("HTTP 能力不匹配", expect="409 capability_mismatch",
             actual=f"{r.status_code} {r.json()}")
    assert r.status_code == 409
    assert r.json()["error"] == "capability_mismatch"

    r = client.post("/missions", json={"mission_id": "m1", "payload": {"different": 1}})
    assert r.status_code == 409
    assert r.json()["error"] == "mission_payload_conflict"

    r = client.post("/robots/heartbeat", json={"robot_id": "ghost"})
    assert r.status_code == 404
    assert r.json()["error"] == "robot_not_found"

    r = client.post("/admin/config", json={"overrides": {"lease_duration_s": 0.01}})
    case_log("HTTP 非法配置", expect="422 config_out_of_range",
             actual=f"{r.status_code} {r.json()}")
    assert r.status_code == 422
    assert r.json()["error"] == "config_out_of_range"


def test_dependencies_over_http(client, case_log):
    r = client.post("/missions", json={"mission_id": "b", "depends_on": ["a"]})
    assert r.status_code == 200
    assert r.json()["mission"]["status"] == "WAITING_DEPS"
    r = client.get("/missions/b/dependencies")
    case_log("HTTP 依赖视图", expect="前置 a 显示 MISSING",
             actual=r.json()["dependencies"])
    assert r.json()["dependencies"] == [
        {"mission_id": "a", "status": "MISSING", "satisfied": False}
    ]
    r = client.post("/missions", json={"mission_id": "c", "depends_on": ["c"]})
    case_log("HTTP 成环拒绝", expect="409 dependency_cycle",
             actual=f"{r.status_code} {r.json()}")
    assert r.status_code == 409
    assert r.json()["error"] == "dependency_cycle"
