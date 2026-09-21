"""数据格式演进：未知字段容忍、缺省字段稳定、旧记录不导致错误分配。"""

from fleet_orchestrator.models import Mission, MissionStatus, Robot
from fleet_orchestrator.service import OrchestratorService


def test_unknown_fields_tolerated_and_preserved(case_log):
    rec = {"mission_id": "m1", "future_field": {"nested": 1}, "priority": 3}
    m = Mission.model_validate(rec)
    dumped = m.model_dump()
    case_log("未知字段记录", input=rec, expect="解析不报错且字段保留",
             actual=dumped.get("future_field"))
    assert m.mission_id == "m1"
    assert dumped["future_field"] == {"nested": 1}  # 不静默丢弃
    assert m.status == MissionStatus.PENDING  # 缺省语义确定


def test_old_record_without_new_fields_gets_defaults(case_log):
    old = {"robot_id": "r1"}  # 无 capabilities/status/last_heartbeat
    r = Robot.model_validate(old)
    case_log("旧记录缺省", input=old, expect="确定默认值",
             actual=f"caps={r.capabilities} status={r.status}")
    assert r.capabilities == []
    assert r.status.value == "ONLINE"


def test_store_loads_mixed_generation_records(store, clock, case_log):
    """持久化文件中含旧格式/未知字段记录，服务恢复后不报错、不错误分配。"""
    import json
    state = {
        "robots": {"r1": {"robot_id": "r1", "capabilities": ["nav"],
                          "last_heartbeat": 1000.0, "future_robot_field": "x"}},
        "missions": {
            "m1": {"mission_id": "m1", "required_capabilities": ["nav"],
                   "payload_hash": "h1", "unknown_knob": True},
        },
        "leases": {},
    }
    store.dir.mkdir(parents=True, exist_ok=True)
    with open(store.dir / "state.json", "w") as f:
        json.dump(state, f)
    svc = OrchestratorService(store, clock=clock)
    m, lease = svc.acquire_mission("r1", "m1")
    case_log("混代记录恢复后分配", expect="正常领取且未知字段不参与判定",
             actual=f"status={m.status} holder={lease.robot_id}")
    assert lease.robot_id == "r1"
    # 未知字段在重新持久化后仍保留
    reloaded = store.load()
    assert reloaded["missions"]["m1"].get("unknown_knob") is True
