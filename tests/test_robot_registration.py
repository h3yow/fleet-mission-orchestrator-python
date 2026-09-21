"""机器人注册、心跳与能力声明：重复注册语义、能力/可用性拒绝原因可区分。"""

import pytest

from fleet_orchestrator.errors import (
    RobotCapabilityMismatch,
    RobotNotAvailable,
    RobotNotFound,
)
from fleet_orchestrator.models import RobotStatus


def test_register_and_reregister_updates_capabilities(service, case_log):
    r1 = service.register_robot("r1", ["nav"])
    r2 = service.register_robot("r1", ["nav", "lift"])
    case_log("重复注册", input="r1 caps [nav] -> [nav,lift]",
             expect="幂等 upsert，能力被更新", actual=r2.capabilities)
    assert r2.capabilities == ["lift", "nav"]  # 排序去重，确定语义
    assert len(service.list_robots()) == 1
    assert r2.registered_at == r1.registered_at  # 注册时间保留


def test_capability_mismatch_rejected(service, case_log):
    service.register_robot("r1", ["nav"])
    service.submit_mission("m1", ["nav", "lift"])
    with pytest.raises(RobotCapabilityMismatch) as exc:
        service.acquire_mission("r1", "m1")
    case_log("能力不匹配领取", input="robot caps=[nav], mission requires=[nav,lift]",
             expect="capability_mismatch", actual=exc.value.reason)
    assert exc.value.reason == "capability_mismatch"


def test_unknown_robot_rejected(service, case_log):
    with pytest.raises(RobotNotFound) as exc:
        service.heartbeat("ghost")
    case_log("未注册机器人心跳", expect="robot_not_found", actual=exc.value.reason)
    assert exc.value.reason == "robot_not_found"


def test_stale_robot_cannot_acquire(service, clock, case_log):
    service.register_robot("r1", ["nav"])
    service.submit_mission("m1", ["nav"])
    clock.advance(20)  # > heartbeat_timeout_s=15
    with pytest.raises(RobotNotAvailable) as exc:
        service.acquire_mission("r1", "m1")
    case_log("心跳超时机器人领取", input="advance 20s > timeout 15s",
             expect="robot_not_available", actual=exc.value.reason)
    assert exc.value.reason == "robot_not_available"


def test_offline_robot_cannot_acquire_or_heartbeat(service, case_log):
    service.register_robot("r1", [])
    service.deregister_robot("r1")
    assert service.get_robot("r1").status == RobotStatus.OFFLINE
    service.submit_mission("m1")
    with pytest.raises(RobotNotAvailable) as exc1:
        service.acquire_mission("r1", "m1")
    with pytest.raises(RobotNotAvailable) as exc2:
        service.heartbeat("r1")
    case_log("离线机器人操作", expect="robot_not_available x2",
             actual=f"{exc1.value.reason}, {exc2.value.reason}")
    assert exc1.value.reason == "robot_not_available"
    assert exc2.value.reason == "robot_not_available"


def test_busy_robot_cannot_acquire_second_mission(service, case_log):
    service.register_robot("r1", [])
    service.submit_mission("m1")
    service.submit_mission("m2")
    service.acquire_mission("r1", "m1")
    with pytest.raises(RobotNotAvailable) as exc:
        service.acquire_mission("r1", "m2")
    case_log("持有租约机器人再领取", expect="robot_not_available(busy)",
             actual=exc.value.reason)
    assert exc.value.reason == "robot_not_available"


def test_heartbeat_refreshes_availability(service, clock, case_log):
    service.register_robot("r1", [])
    clock.advance(10)
    service.heartbeat("r1")
    clock.advance(10)  # 距上次心跳仅 10s，未超时
    service.submit_mission("m1")
    mission, _ = service.acquire_mission("r1", "m1")
    case_log("心跳续期后领取", input="heartbeat 间隔 10s < 15s",
             expect="领取成功", actual=mission.status)
    assert mission.mission_id == "m1"
