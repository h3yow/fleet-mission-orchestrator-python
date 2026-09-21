"""任务下发：幂等性、内容冲突拒绝、积压上限。"""

import pytest

from fleet_orchestrator.errors import BacklogFull, MissionPayloadConflict
from fleet_orchestrator.models import MissionStatus
from fleet_orchestrator.service import OrchestratorService


def test_duplicate_submit_same_content_is_idempotent(service, case_log):
    m1, created1 = service.submit_mission("m1", ["nav"], priority=3, payload={"g": 1})
    m2, created2 = service.submit_mission("m1", ["nav"], priority=3, payload={"g": 1})
    case_log("重复下发同内容", input="m1 同内容 x2",
             expect="第二次 created=False 且状态一致",
             actual=f"created=({created1},{created2}) status={m2.status}")
    assert created1 is True
    assert created2 is False
    assert m1.payload_hash == m2.payload_hash
    assert m2.status == MissionStatus.PENDING
    assert len(service.list_missions()) == 1  # 不产生重复任务


def test_duplicate_submit_different_content_rejected(service, case_log):
    service.submit_mission("m1", ["nav"], payload={"g": 1})
    with pytest.raises(MissionPayloadConflict) as exc:
        service.submit_mission("m1", ["nav"], payload={"g": 2})
    case_log("同标识不同内容", input="payload {g:2} vs 已存 {g:1}",
             expect="拒绝 reason=mission_payload_conflict", actual=exc.value.reason)
    assert exc.value.reason == "mission_payload_conflict"


def test_submit_different_capability_set_rejected(service, case_log):
    service.submit_mission("m1", ["nav"])
    with pytest.raises(MissionPayloadConflict) as exc:
        service.submit_mission("m1", ["nav", "lift"])
    case_log("同标识不同能力要求", expect="mission_payload_conflict", actual=exc.value.reason)
    assert exc.value.reason == "mission_payload_conflict"


def test_backlog_full_rejected(store, clock, small_config, case_log):
    svc = OrchestratorService(store, config=small_config(max_backlog=2), clock=clock)
    svc.submit_mission("m1")
    svc.submit_mission("m2")
    with pytest.raises(BacklogFull) as exc:
        svc.submit_mission("m3")
    case_log("积压上限", input="max_backlog=2, 提交第 3 个",
             expect="拒绝 reason=backlog_full", actual=exc.value.reason)
    assert exc.value.reason == "backlog_full"
    assert exc.value.http_status == 429


def test_completed_mission_frees_backlog_slot(store, clock, small_config, case_log):
    svc = OrchestratorService(store, config=small_config(max_backlog=1), clock=clock)
    svc.register_robot("r1", [])
    svc.submit_mission("m1")
    _, lease = svc.acquire_mission("r1", "m1")
    svc.start_mission("r1", "m1", lease.lease_id)
    svc.complete_mission("r1", "m1", lease.lease_id)
    _, created = svc.submit_mission("m2")
    case_log("完成后释放积压槽位", expect="m2 created=True", actual=created)
    assert created is True
