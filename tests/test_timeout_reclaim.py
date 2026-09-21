"""心跳超时、租约过期回收、进度保留与重试耗尽。"""
from __future__ import annotations

import pytest

from app.errors import DomainError, RejectCode
from app.config import SchedulerConfig
from app.models import RobotState, TaskState


def test_heartbeat_timeout_reclaims_and_reassigns(service, clock, logger):
    service.register_robot("r1", ["nav"])
    service.register_robot("r2", ["nav"])
    service.submit_task("t1", {}, ["nav"], max_retries=2)
    claim = service.claim_next("r1")
    token1 = claim.lease.fencing_token
    service.start_task("t1", "r1", token1)
    service.report_progress("t1", "r1", token1, {"step": 5, "pct": 30})
    logger.info("输入：r1 执行 t1 至进度 %s 后心跳静默", {"step": 5})

    clock.advance(101)  # 超过 heartbeat_timeout_ms=100
    decisions = service.reap_stale_leases()
    actions = [d.action for d in decisions]
    logger.info("回收判定依据：%s", [(d.action, d.reason) for d in decisions])
    assert "ROBOT_STALE" in actions
    assert any(d.action.startswith("REAP") for d in decisions)

    task = service.tasks.get("t1")
    assert task.state is TaskState.QUEUED
    assert task.progress == {"step": 5, "pct": 30}  # 进度保留
    assert service.registry.get("r1").state is RobotState.STALE

    # 旧持有者事后"醒来"完成必须被拒绝，杜绝双执行
    with pytest.raises(DomainError) as ei:
        service.complete_task("t1", "r1", token1, {"ghost": True})
    logger.info("旧持有者完成被拒 code=%s", ei.value.code.value)
    assert ei.value.code is RejectCode.TASK_NOT_LEASE_HOLDER

    # r2 重新领取：token 递增、attempt=2、resumed
    claim2 = service.claim_next("r2")
    assert claim2 is not None
    assert claim2.lease.attempt == 2
    assert claim2.lease.fencing_token > token1
    assert claim2.resumed is True
    assert claim2.progress == {"step": 5, "pct": 30}
    logger.info("重新分配 t1 -> r2 token=%s 续跑进度=%s",
                claim2.lease.fencing_token, claim2.progress)


def test_lease_expiry_even_without_robot_stale(clock, logger):
    # 租约时长(50)短于心跳超时(100)：心跳新鲜，但租约自身过期
    from app.service import FleetService
    service = FleetService.in_memory(
        clock=clock,
        config=SchedulerConfig(heartbeat_timeout_ms=100,
                               lease_duration_ms=50))
    service.register_robot("r1", ["nav"])
    service.submit_task("t1", {}, ["nav"], max_retries=1)
    c = service.claim_next("r1")
    service.heartbeat("r1")  # 心跳新鲜
    clock.advance(51)        # 仅租约过期
    decisions = service.reap_stale_leases()
    reasons = [d.detail.get("reason") for d in decisions]
    logger.info("租约自身过期回收 reasons=%s", reasons)
    assert "lease_expired" in reasons
    assert service.tasks.get("t1").state is TaskState.QUEUED


def test_retry_exhaustion_marks_failed(service, clock):
    service.register_robot("r1", ["nav"])
    service.submit_task("t1", {}, ["nav"], max_retries=0)  # 不重试
    c = service.claim_next("r1")
    service.start_task("t1", "r1", c.lease.fencing_token)
    clock.advance(101)
    service.reap_stale_leases()  # attempt=1 > retries=0 => FAILED
    assert service.tasks.get("t1").state is TaskState.FAILED


def test_reclaim_terminal_task_rejected(service):
    service.register_robot("r1")
    service.submit_task("t1", {})
    c = service.claim_next("r1")
    service.complete_task("t1", "r1", c.lease.fencing_token, {})
    with pytest.raises(DomainError) as ei:
        service.leases.reclaim("t1", "late")
    assert ei.value.code is RejectCode.TASK_INVALID_TRANSITION


def test_renew_after_expiry_rejected(service, clock):
    service.register_robot("r1")
    service.submit_task("t1", {})
    c = service.claim_next("r1")
    clock.advance(1_001)
    with pytest.raises(DomainError) as ei:
        service.renew_lease("t1", "r1", c.lease.fencing_token)
    assert ei.value.code is RejectCode.LEASE_EXPIRED
