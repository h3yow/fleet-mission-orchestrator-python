"""优先级与抢占：进度保留、不可抢占保护、抢占失败原因可区分。"""
from __future__ import annotations

import pytest

from app.errors import DomainError, RejectCode
from app.models import TaskState


def _setup_two(service):
    service.register_robot("victim", ["nav"])
    service.register_robot("hero", ["nav"])
    service.submit_task("low", {}, ["nav"], priority=1)
    c = service.claim_next("victim")
    service.start_task("low", "victim", c.lease.fencing_token)
    service.report_progress("low", "victim", c.lease.fencing_token,
                            {"step": 7})
    return c


def test_preempt_preserves_progress_and_resumes(service, logger):
    c = _setup_two(service)
    service.submit_task("high", {}, ["nav"], priority=9)
    logger.info("输入：high(prio=9) 请求抢占 low(prio=1, 持有者 victim)")
    victim = service.preempt("high", "low", "hero")
    assert victim.state is TaskState.QUEUED
    assert victim.progress == {"step": 7}  # 进度保留
    logger.info("受害者 low 标记=%s 进度保留=%s，旧 token=%s",
                victim.state.value, victim.progress, c.lease.fencing_token)

    # hero 立刻拿到 high
    ch = service.claim_next("hero")
    assert ch.task.task_id == "high"
    # victim 续跑 low：attempt 递增，新 fencing token
    cv = service.claim_next("victim")
    assert cv.task.task_id == "low"
    assert cv.resumed is True and cv.progress == {"step": 7}
    assert cv.lease.fencing_token > c.lease.fencing_token
    # 旧 token 立即失效
    with pytest.raises(DomainError) as ei:
        service.report_progress("low", "victim", c.lease.fencing_token, {})
    assert ei.value.code is RejectCode.LEASE_VERSION_CONFLICT


def test_non_preemptible_task_cannot_be_taken(service):
    service.register_robot("victim")
    service.register_robot("hero")
    service.submit_task("locked", {}, preemptible=False, priority=1)
    c = service.claim_next("victim")
    service.start_task("locked", "victim", c.lease.fencing_token)
    service.submit_task("high", {}, priority=9)
    with pytest.raises(DomainError) as ei:
        service.preempt("high", "locked", "hero")
    assert ei.value.code is RejectCode.TASK_NOT_PREEMPTIBLE


def test_preempt_requires_strictly_higher_priority(service):
    c = _setup_two(service)
    service.submit_task("equal", {}, ["nav"], priority=1)
    with pytest.raises(DomainError) as ei:
        service.preempt("equal", "low", "hero")
    assert ei.value.code is RejectCode.PREEMPT_NO_ELIGIBLE_VICTIM
    assert "严格更高" in ei.value.reason


def test_preempt_hero_must_have_capability(service):
    _setup_two(service)
    service.register_robot("noob")  # 无 nav 能力
    service.submit_task("highnav", {}, ["nav"], priority=9)
    with pytest.raises(DomainError) as ei:
        service.preempt("highnav", "low", "noob")
    assert ei.value.code is RejectCode.ROBOT_CAPABILITY_MISMATCH


def test_preempt_nonexecuting_victim(service):
    service.register_robot("hero")
    service.submit_task("queued", {}, priority=1)   # 仍在队列
    service.submit_task("high", {}, priority=9)
    with pytest.raises(DomainError) as ei:
        service.preempt("high", "queued", "hero")
    assert ei.value.code is RejectCode.PREEMPT_NO_ELIGIBLE_VICTIM


def test_preempt_rejects_busy_hero(service):
    _setup_two(service)
    service.register_robot("busyhero", ["nav"])
    service.submit_task("own", {}, ["nav"], priority=2)
    service.claim_next("busyhero")  # busyhero 已持有 own
    service.submit_task("high", {}, ["nav"], priority=9)
    with pytest.raises(DomainError) as ei:
        service.preempt("high", "low", "busyhero")
    assert ei.value.code is RejectCode.ROBOT_UNAVAILABLE
