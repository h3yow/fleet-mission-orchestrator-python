"""状态机约束：非法跳转、非持有者上报、状态一致性。"""

import pytest

from fleet_orchestrator.errors import (
    InvalidStateTransition,
    LeaseNotHeld,
    MissionNotAssignable,
)
from fleet_orchestrator.models import MissionStatus


def _assigned(service, robot="r1", mission="m1"):
    service.register_robot(robot, [])
    service.submit_mission(mission)
    return service.acquire_mission(robot, mission)


def test_illegal_transitions_rejected(service, case_log):
    service.register_robot("r1", [])
    service.submit_mission("m1")
    # PENDING 不能直接完成/失败/上报（无租约 -> lease_not_held 优先）
    _, lease = _assigned(service)
    # ASSIGNED 不能直接 SUCCEEDED
    with pytest.raises(InvalidStateTransition) as exc:
        service.complete_mission("r1", "m1", lease.lease_id)
    case_log("ASSIGNED->SUCCEEDED 跳转", expect="invalid_state_transition",
             actual=exc.value.reason)
    assert exc.value.reason == "invalid_state_transition"
    # ASSIGNED 不能上报进度（要求 RUNNING）
    with pytest.raises(InvalidStateTransition) as exc2:
        service.report_progress("r1", "m1", lease.lease_id, {"pct": 1})
    case_log("ASSIGNED 上报进度", expect="invalid_state_transition",
             actual=exc2.value.reason)
    assert exc2.value.reason == "invalid_state_transition"


def test_terminal_state_is_final(service, case_log):
    _, lease = _assigned(service)
    service.start_mission("r1", "m1", lease.lease_id)
    service.complete_mission("r1", "m1", lease.lease_id)
    assert service.get_mission("m1").status == MissionStatus.SUCCEEDED
    with pytest.raises(InvalidStateTransition) as exc:
        service.cancel_mission("m1")
    case_log("终态取消", expect="invalid_state_transition", actual=exc.value.reason)
    assert exc.value.reason == "invalid_state_transition"
    with pytest.raises(MissionNotAssignable) as exc2:
        service.acquire_mission("r1", "m1")
    case_log("终态再领取", expect="mission_not_assignable", actual=exc2.value.reason)
    assert exc2.value.reason == "mission_not_assignable"


def test_non_holder_report_rejected(service, case_log):
    _, lease = _assigned(service, robot="r1")
    service.start_mission("r1", "m1", lease.lease_id)
    service.register_robot("r2", [])
    with pytest.raises(LeaseNotHeld) as exc:
        service.report_progress("r2", "m1", lease.lease_id, {"pct": 5})
    case_log("非持有者上报", input="r2 用 r1 的 lease_id",
             expect="lease_not_held", actual=exc.value.reason)
    assert exc.value.reason == "lease_not_held"
    # 持有者用错误 lease_id 同样拒绝
    with pytest.raises(LeaseNotHeld) as exc2:
        service.report_progress("r1", "m1", "forged-lease", {"pct": 5})
    case_log("伪造租约凭证", expect="lease_not_held", actual=exc2.value.reason)
    assert exc2.value.reason == "lease_not_held"


def test_double_acquire_same_mission_rejected(service, case_log):
    _assigned(service, robot="r1")
    service.register_robot("r2", [])
    with pytest.raises(MissionNotAssignable) as exc:
        service.acquire_mission("r2", "m1")
    case_log("已分配任务再领取", expect="mission_not_assignable", actual=exc.value.reason)
    assert exc.value.reason == "mission_not_assignable"


def test_state_consistency_invariants(service, case_log):
    """任务、租约、机器人状态不得互相矛盾。"""
    _, lease = _assigned(service)
    service.start_mission("r1", "m1", lease.lease_id)
    m = service.get_mission("m1")
    l = service.get_lease("m1")
    r = service.get_robot("r1")
    case_log("运行中不变量", expect="RUNNING <=> 租约存在 <=> robot.current_mission",
             actual=f"status={m.status} lease_holder={l.robot_id} robot_mission={r.current_mission_id}")
    assert m.status == MissionStatus.RUNNING
    assert l is not None and l.robot_id == "r1"
    assert r.current_mission_id == "m1"

    service.fail_mission("r1", "m1", lease.lease_id, "err")
    assert service.get_lease("m1") is None
    assert service.get_robot("r1").current_mission_id is None
    case_log("失败后不变量", expect="租约释放、机器人空闲",
             actual=f"lease={service.get_lease('m1')} robot_mission={service.get_robot('r1').current_mission_id}")


def test_cancel_releases_lease(service, case_log):
    _assigned(service)
    service.cancel_mission("m1")
    m = service.get_mission("m1")
    case_log("取消释放租约", expect="CANCELLED 且无租约",
             actual=f"status={m.status} lease={service.get_lease('m1')}")
    assert m.status == MissionStatus.CANCELLED
    assert service.get_lease("m1") is None
    assert service.get_robot("r1").current_mission_id is None
