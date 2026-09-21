"""机器人注册、心跳与能力声明：确定语义与可区分拒绝原因。"""
from __future__ import annotations

import pytest

from app.errors import DomainError, RejectCode
from app.models import RobotState


def test_register_idempotent_same_capabilities(service, logger):
    logger.info("输入 register(robot_id=r1, caps={nav,lift})")
    r1 = service.register_robot("r1", ["nav", "lift"])
    assert r1.idempotent_hit is False
    r1_again = service.register_robot("r1", ["lift", "nav"])  # 集合相同、顺序不同
    logger.info("判定依据：能力集合相等 => 幂等命中 idempotent_hit=%s",
                r1_again.idempotent_hit)
    assert r1_again.idempotent_hit is True
    assert r1_again.robot is r1.robot


def test_register_same_id_different_caps_rejected(service, logger):
    service.register_robot("r1", ["nav"])
    logger.info("输入重复注册 r1 caps={weld}，与首次 {nav} 不一致")
    with pytest.raises(DomainError) as ei:
        service.register_robot("r1", ["weld"])
    logger.info("判定依据：code=%s reason=%s",
                ei.value.code.value, ei.value.reason)
    assert ei.value.code is RejectCode.ROBOT_ALREADY_REGISTERED
    assert "update_capabilities" in ei.value.reason


def test_update_capabilities_deterministic(service):
    service.register_robot("r1", ["nav"])
    r = service.update_capabilities("r1", ["weld", "nav"])
    assert r.capabilities == frozenset({"weld", "nav"})
    # 无变化调用幂等，不产生新事件
    seq_before = service.storage.next_seq
    service.update_capabilities("r1", ["nav", "weld"])
    assert service.storage.next_seq == seq_before


def test_heartbeat_unknown_robot_404(service):
    with pytest.raises(DomainError) as ei:
        service.heartbeat("ghost")
    assert ei.value.code is RejectCode.ROBOT_NOT_FOUND


def test_stale_robot_recovers_with_heartbeat(service, clock):
    service.register_robot("r1")
    service.registry.mark_stale("r1")
    assert service.registry.get("r1").state is RobotState.STALE
    clock.advance(10)
    service.heartbeat("r1")
    assert service.registry.get("r1").state is RobotState.REGISTERED


def test_deregistered_heartbeat_and_reregister(service, logger):
    service.register_robot("r1", ["nav"])
    service.registry.deregister("r1")
    with pytest.raises(DomainError) as ei:
        service.heartbeat("r1")
    assert ei.value.code is RejectCode.ROBOT_UNAVAILABLE
    logger.info("已注销机器人心跳被拒绝 code=%s", ei.value.code.value)
    # 重新注册开启新生命周期，能力可以不同
    r = service.register_robot("r1", ["fly"])
    assert r.idempotent_hit is False
    assert service.registry.get("r1").state is RobotState.REGISTERED


def test_draining_robot_cannot_claim(service):
    service.register_robot("r1", ["nav"])
    service.submit_task("t1", {}, ["nav"])
    service.set_draining("r1", True)
    # 排空中的机器人：原因是 ROBOT_UNAVAILABLE（区别于心跳超时 ROBOT_STALE）
    with pytest.raises(DomainError) as ei:
        service.claim_next("r1")
    assert ei.value.code is RejectCode.ROBOT_UNAVAILABLE
