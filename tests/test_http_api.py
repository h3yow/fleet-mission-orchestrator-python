"""HTTP 集成：拒绝码映射为可区分 HTTP 状态与 body。"""
from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

import main
from app.api import set_service
from app.service import FleetService
from app.timeutil import FakeClock


@pytest.fixture()
def client():
    svc = FleetService.in_memory(clock=FakeClock(0))
    set_service(svc)
    return TestClient(main.app), svc


def test_full_happy_path_over_http(client):
    c, svc = client
    assert c.post("/robots/register",
                  json={"robot_id": "r1", "capabilities": ["nav"]}).json()["ok"]
    r = c.post("/tasks", json={"task_id": "t1", "payload": {"x": 1},
                               "required_capabilities": ["nav"]})
    assert r.status_code == 200
    claim = c.post("/tasks/claim", json={"robot_id": "r1"}).json()
    assert claim["granted"] is True
    token = claim["lease"]["fencing_token"]
    done = c.post("/tasks/complete",
                  json={"task_id": "t1", "robot_id": "r1",
                        "fencing_token": token, "result": {"ok": True}})
    assert done.json()["task"]["state"] == "COMPLETED"


def test_reject_codes_are_distinguishable(client):
    c, _ = client
    c.post("/robots/register", json={"robot_id": "r1", "capabilities": ["nav"]})
    # 重复注册不同能力 -> 409 ROBOT_ALREADY_REGISTERED
    r = c.post("/robots/register",
               json={"robot_id": "r1", "capabilities": ["x"]})
    assert r.status_code == 409 and r.json()["code"] == "ROBOT_ALREADY_REGISTERED"

    c.post("/tasks", json={"task_id": "t1", "payload": {"a": 1},
                           "required_capabilities": ["nav"]})
    # 内容冲突 -> 409 TASK_DUPLICATE_CONFLICT
    r = c.post("/tasks", json={"task_id": "t1", "payload": {"a": 2}})
    assert r.status_code == 409
    assert r.json()["code"] == "TASK_DUPLICATE_CONFLICT"

    # 未知机器人 -> 404
    r = c.post("/robots/heartbeat", json={"robot_id": "zz"})
    assert r.status_code == 404 and r.json()["code"] == "ROBOT_NOT_FOUND"

    # 非法优先级 -> 400 INVALID_CONFIG
    r = c.post("/tasks", json={"task_id": "tb", "payload": {},
                               "priority": 999999})
    assert r.status_code == 400 and r.json()["code"] == "INVALID_CONFIG"

    # 非持有者完成 -> 409
    c.post("/robots/register", json={"robot_id": "r2", "capabilities": ["nav"]})
    claim = c.post("/tasks/claim", json={"robot_id": "r1"}).json()
    r = c.post("/tasks/complete",
               json={"task_id": "t1", "robot_id": "r2",
                     "fencing_token": claim["lease"]["fencing_token"],
                     "result": {}})
    assert r.status_code == 409
    assert r.json()["code"] == "TASK_NOT_LEASE_HOLDER"


def test_claim_returns_degraded_when_no_match(client):
    c, _ = client
    c.post("/robots/register", json={"robot_id": "r1", "capabilities": []})
    c.post("/tasks", json={"task_id": "t1", "payload": {},
                           "required_capabilities": ["nav"]})
    r = c.post("/tasks/claim", json={"robot_id": "r1"})
    assert r.status_code == 200 and r.json()["granted"] is False
