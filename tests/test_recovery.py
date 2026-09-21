"""故障恢复与调度决策回放：重启后状态一致、无幽灵任务/重复分配/半更新。"""
from __future__ import annotations

import json
import os

import pytest

from app.config import SchedulerConfig
from app.models import LeaseState, TaskState
from app.service import FleetService
from app.timeutil import FakeClock


def _cfg():
    return SchedulerConfig(heartbeat_timeout_ms=100,
                           lease_duration_ms=1_000)


def test_restart_state_matches_before_crash(tmp_path):
    path = str(tmp_path / "wal.jsonl")
    clk = FakeClock(5_000)
    svc = FleetService.with_json_file(path, clock=clk, config=_cfg())
    svc.register_robot("r1", ["nav"])
    svc.register_robot("r2", ["nav"])
    svc.submit_task("t1", {"k": "v"}, ["nav"], max_retries=2)
    c = svc.claim_next("r1")
    svc.start_task("t1", "r1", c.lease.fencing_token)
    svc.report_progress("t1", "r1", c.lease.fencing_token, {"p": 50})
    svc.submit_task("t2", {}, ["nav"], priority=2)
    before = svc.snapshot()
    svc.close()

    svc2 = FleetService.with_json_file(path, clock=FakeClock(0), config=_cfg())
    after = svc2.snapshot()

    # 队列（t2 仍排队）、任务进度、持有者、租约完全一致
    assert after["queue"] == ["t2"]
    t1 = next(t for t in after["tasks"] if t["task_id"] == "t1")
    assert t1["state"] == "RUNNING" and t1["progress"] == {"p": 50}
    r1 = next(r for r in after["robots"] if r["robot_id"] == "r1")
    assert r1["current_task_id"] == "t1"
    lease = next(l for l in after["leases"] if l["task_id"] == "t1")
    assert lease["state"] == "ACTIVE"
    # 决策日志也被回放
    assert any(d["action"] == "CLAIM_GRANTED"
               and d["task_id"] == "t1" for d in after["decisions"])


def test_no_duplicate_assignment_after_restart(tmp_path):
    path = str(tmp_path / "wal.jsonl")
    svc = FleetService.with_json_file(path, clock=FakeClock(0), config=_cfg())
    svc.register_robot("r1", ["nav"])
    svc.submit_task("t1", {}, ["nav"])
    c = svc.claim_next("r1")
    svc.complete_task("t1", "r1", c.lease.fencing_token, {"r": 9})
    svc.close()

    svc2 = FleetService.with_json_file(path, clock=FakeClock(0), config=_cfg())
    t = svc2.tasks.get("t1")
    assert t.state is TaskState.COMPLETED and t.result == {"r": 9}
    # 已完成任务不在队列，不会在重启后被再次分配
    assert "t1" not in svc2.tasks.queue_peek_order()
    # fencing token 不回退
    svc2.register_robot("r2", ["nav"])
    svc2.submit_task("t2", {}, ["nav"])
    assert svc2.claim_next("r2").lease.fencing_token == 2


def test_preempted_task_survives_restart_and_stays_queued(tmp_path):
    path = str(tmp_path / "wal.jsonl")
    svc = FleetService.with_json_file(path, clock=FakeClock(0), config=_cfg())
    svc.register_robot("v", ["nav"]); svc.register_robot("h", ["nav"])
    svc.submit_task("low", {}, ["nav"], priority=1)
    cv = svc.claim_next("v")
    svc.start_task("low", "v", cv.lease.fencing_token)
    svc.report_progress("low", "v", cv.lease.fencing_token, {"s": 3})
    svc.submit_task("hi", {}, ["nav"], priority=9)
    svc.preempt("hi", "low", "h")
    svc.close()

    svc2 = FleetService.with_json_file(path, clock=FakeClock(0), config=_cfg())
    # 被抢占任务必须仍在队列（不丢失），且进度保留
    assert set(svc2.tasks.queue_peek_order()) == {"hi", "low"}
    assert svc2.tasks.get("low").progress == {"s": 3}
    assert svc2.leases.active_lease("low") is None


def test_corrupt_wal_is_rejected_not_silently_dropped(tmp_path):
    from app.errors import DomainError, RejectCode
    path = str(tmp_path / "wal.jsonl")
    with open(path, "w") as fh:
        fh.write("{ this is not json\n")
    with pytest.raises(DomainError) as ei:
        FleetService.with_json_file(path, clock=FakeClock(0), config=_cfg())
    assert ei.value.code is RejectCode.CORRUPT_RECORD


def test_unknown_fields_and_future_events_are_tolerated(tmp_path):
    """旧/未来记录：未知顶层字段保留，未知事件跳过，不影响分配。"""
    path = str(tmp_path / "wal.jsonl")
    # 先用正常服务写一条注册，再手工塞未知字段与未来事件
    svc = FleetService.with_json_file(path, clock=FakeClock(0), config=_cfg())
    svc.register_robot("r1", ["nav"])
    svc.close()
    with open(path, "a") as fh:
        # 未知事件类型（新版本写入）
        fh.write(json.dumps({
            "seq": 2, "event_type": "FUTURE_KIND_X", "at_ms": 1,
            "v": 99, "data": {"hello": "future"}}) + "\n")
        # 已知事件但带未知顶层字段
        fh.write(json.dumps({
            "seq": 3, "event_type": "ROBOT_HEARTBEAT", "at_ms": 2,
            "v": 1, "data": {"robot_id": "r1", "_at_ms": 2},
            "mystery_top": 123}) + "\n")
    svc2 = FleetService.with_json_file(path, clock=FakeClock(0), config=_cfg())
    # 机器人仍可正常使用并领取任务（未知记录未干扰分配）
    svc2.submit_task("t1", {}, ["nav"])
    assert svc2.claim_next("r1") is not None
    assert svc2.registry.get("r1").last_heartbeat_ms == 2
