"""租约与超时回收：心跳超时、租约过期、续约上限、回收后重分配与重试。"""

import pytest

from fleet_orchestrator.errors import (
    InvalidStateTransition,
    LeaseExpired,
    LeaseNotHeld,
    RetriesExhausted,
)
from fleet_orchestrator.models import MissionStatus, RobotStatus
from fleet_orchestrator.service import OrchestratorService


def _running(service, robot="r1", mission="m1", caps=None):
    service.register_robot(robot, caps or [])
    service.submit_mission(mission, caps or [])
    m, lease = service.acquire_mission(robot, mission)
    service.start_mission(robot, mission, lease.lease_id)
    return m, lease


def test_heartbeat_timeout_reclaims_mission(service, clock, case_log):
    _running(service)
    clock.advance(16)  # > heartbeat_timeout 15s
    reclaimed = service.sweep()
    m = service.get_mission("m1")
    case_log("心跳超时回收", input="advance 16s > 15s",
             expect="m1 回收为 PENDING, robot STALE",
             actual=f"reclaimed={reclaimed} status={m.status} robot={service.get_robot('r1').status}")
    assert reclaimed == ["m1"]
    assert m.status == MissionStatus.PENDING
    assert service.get_lease("m1") is None
    assert service.get_robot("r1").status == RobotStatus.STALE


def test_reclaimed_mission_reassignable_to_another_robot(service, clock, case_log):
    _running(service, robot="r1")
    clock.advance(16)
    service.sweep()
    service.register_robot("r2", [])
    m, lease2 = service.acquire_mission("r2", "m1")
    case_log("回收后重分配", expect="r2 获得新租约, generation 递增",
             actual=f"holder={lease2.robot_id} gen={lease2.generation}")
    assert lease2.robot_id == "r2"
    assert lease2.generation == 2
    # 旧持有者的租约凭证已失效
    with pytest.raises(LeaseNotHeld) as exc:
        service.complete_mission("r1", "m1", "stale-lease-id")
    case_log("旧持有者上报", expect="lease_not_held", actual=exc.value.reason)
    assert exc.value.reason == "lease_not_held"


def test_lease_expiry_without_heartbeat_timeout(service, clock, small_config, store, case_log):
    # 心跳超时 600s，租约 30s：租约先过期
    svc = OrchestratorService(store, config=small_config(
        lease_duration_s=30, heartbeat_timeout_s=600), clock=clock)
    _running(svc)
    clock.advance(31)
    reclaimed = svc.sweep()
    case_log("租约过期回收", input="lease 30s, heartbeat timeout 600s, advance 31s",
             expect="租约过期被回收", actual=reclaimed)
    assert reclaimed == ["m1"]


def test_lease_renewal_extends_and_bounded(service, clock, small_config, store, case_log):
    svc = OrchestratorService(store, config=small_config(
        lease_duration_s=10, heartbeat_timeout_s=600,
        renew_extension_s=10, max_lease_renewals=2), clock=clock)
    _, lease = _running(svc)
    svc.renew_lease("r1", "m1", lease.lease_id)
    svc.renew_lease("r1", "m1", lease.lease_id)
    clock.advance(9)  # 续约后仍在有效期内
    assert svc.sweep() == []
    with pytest.raises(LeaseExpired) as exc:
        svc.renew_lease("r1", "m1", lease.lease_id)
    case_log("续约上限", input="max_lease_renewals=2, 第 3 次续约",
             expect="lease_expired(renewals exhausted)", actual=exc.value.reason)
    assert exc.value.reason == "lease_expired"


def test_operation_after_lease_expiry_rejected(service, clock, small_config, store, case_log):
    svc = OrchestratorService(store, config=small_config(
        lease_duration_s=10, heartbeat_timeout_s=600), clock=clock)
    _, lease = _running(svc)
    clock.advance(11)
    with pytest.raises(LeaseExpired) as exc:
        svc.report_progress("r1", "m1", lease.lease_id, {"pct": 10})
    case_log("租约过期后上报", expect="lease_expired", actual=exc.value.reason)
    assert exc.value.reason == "lease_expired"


def test_reclaim_exhausts_attempts_marks_failed(store, clock, small_config, case_log):
    svc = OrchestratorService(store, config=small_config(
        lease_duration_s=10, heartbeat_timeout_s=5), clock=clock)
    svc.register_robot("r1", [])
    svc.submit_mission("m1", max_attempts=2)
    for attempt in (1, 2):
        svc.heartbeat("r1")  # 机器人恢复在线后再领取
        svc.acquire_mission("r1", "m1")
        clock.advance(6)
        svc.sweep()
    m = svc.get_mission("m1")
    case_log("回收耗尽重试", input="max_attempts=2, 两次超时回收",
             expect="m1 FAILED 终态", actual=f"status={m.status} attempts={m.attempts}")
    assert m.status == MissionStatus.FAILED
    assert m.attempts == 2


def test_retry_failed_mission_and_exhaustion(service, case_log):
    _, lease = _running(service)
    service.fail_mission("r1", "m1", lease.lease_id, "boom")
    m = service.retry_mission("m1")
    case_log("失败后重试", expect="FAILED -> PENDING", actual=m.status)
    assert m.status == MissionStatus.PENDING
    # 重试次数耗尽后拒绝
    service.get_mission("m1").attempts = service.get_mission("m1").max_attempts
    service.get_mission("m1").status = MissionStatus.FAILED
    with pytest.raises(RetriesExhausted) as exc:
        service.retry_mission("m1")
    case_log("重试耗尽", expect="retries_exhausted", actual=exc.value.reason)
    assert exc.value.reason == "retries_exhausted"


def test_retry_non_failed_rejected(service, case_log):
    service.submit_mission("m9")
    with pytest.raises(InvalidStateTransition) as exc:
        service.retry_mission("m9")
    case_log("非 FAILED 重试", expect="invalid_state_transition", actual=exc.value.reason)
    assert exc.value.reason == "invalid_state_transition"
