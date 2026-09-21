"""故障恢复与调度决策回放：重启一致性、事件重放、无幽灵任务。"""

import os

from fleet_orchestrator.models import MissionStatus
from fleet_orchestrator.service import OrchestratorService
from fleet_orchestrator.store import Store


def _build_state(store, clock):
    svc = OrchestratorService(store, clock=clock)
    svc.register_robot("r1", ["nav"])
    svc.register_robot("r2", ["lift"])
    svc.submit_mission("m1", ["nav"], priority=5, payload={"g": 1})
    svc.submit_mission("m2", ["lift"])
    m, lease = svc.acquire_mission("r1", "m1")
    svc.start_mission("r1", "m1", lease.lease_id)
    svc.report_progress("r1", "m1", lease.lease_id, {"pct": 40})
    return svc, lease


def test_restart_recovers_exact_state(store, clock, case_log):
    svc, lease = _build_state(store, clock)
    before = {
        "missions": {m.mission_id: (m.status, m.progress) for m in svc.list_missions()},
        "lease": svc.get_lease("m1").lease_id,
        "robot": svc.get_robot("r1").current_mission_id,
    }
    # 模拟进程重启：同一目录重建服务
    svc2 = OrchestratorService(store, clock=clock)
    after = {
        "missions": {m.mission_id: (m.status, m.progress) for m in svc2.list_missions()},
        "lease": svc2.get_lease("m1").lease_id,
        "robot": svc2.get_robot("r1").current_mission_id,
    }
    case_log("重启恢复", expect="故障前后状态一致", actual=f"before={before} after={after}")
    assert before == after
    # 恢复后租约仍有效，持有者可继续完成（无幽灵任务、无重复分配）
    svc2.complete_mission("r1", "m1", lease.lease_id)
    assert svc2.get_mission("m1").status == MissionStatus.SUCCEEDED


def test_replay_matches_snapshot(store, clock, case_log):
    svc, lease = _build_state(store, clock)
    svc.complete_mission("r1", "m1", lease.lease_id)
    svc.submit_mission("m3")
    svc.cancel_mission("m3")
    snapshot = store.load()
    replayed = store.replay()
    case_log("事件重放一致性", expect="replay == snapshot",
             actual=f"equal={snapshot == replayed} events={len(store.read_events())}")
    assert snapshot == replayed


def test_recovery_without_snapshot_falls_back_to_replay(store, clock, case_log):
    svc, lease = _build_state(store, clock)
    os.remove(store.dir / "state.json")  # 快照丢失
    svc2 = OrchestratorService(store, clock=clock)
    m = svc2.get_mission("m1")
    case_log("快照丢失回退重放", expect="m1 仍 RUNNING 且租约在",
             actual=f"status={m.status} lease={svc2.get_lease('m1') is not None}")
    assert m.status == MissionStatus.RUNNING
    assert svc2.get_lease("m1") is not None


def test_no_ghost_mission_after_crash_mid_write(store, clock, case_log):
    """事件日志尾部半行（崩溃截断）不得阻断恢复或产生幽灵记录。"""
    svc, _ = _build_state(store, clock)
    with open(store.dir / "events.jsonl", "a", encoding="utf-8") as f:
        f.write('{"kind": "mission_upsert", "record": {"mission_id": "gho')  # 半行
    replayed = store.replay()
    case_log("半行事件恢复", expect="截断事件被忽略，无 ghost 任务",
             actual=f"missions={sorted(replayed['missions'])}")
    assert "ghost" not in replayed["missions"]
    assert set(replayed["missions"]) == {"m1", "m2"}


def test_reclaimed_lease_not_resurrected_after_restart(store, clock, case_log):
    svc = OrchestratorService(store, clock=clock)
    svc.register_robot("r1", [])
    svc.submit_mission("m1")
    svc.acquire_mission("r1", "m1")
    clock.advance(100)
    svc.sweep()  # 回收 -> PENDING
    svc2 = OrchestratorService(store, clock=clock)
    case_log("回收后重启", expect="m1 PENDING 且无租约残留",
             actual=f"status={svc2.get_mission('m1').status} lease={svc2.get_lease('m1')}")
    assert svc2.get_mission("m1").status == MissionStatus.PENDING
    assert svc2.get_lease("m1") is None
