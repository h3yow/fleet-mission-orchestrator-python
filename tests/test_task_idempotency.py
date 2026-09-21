"""任务幂等下发、内容冲突与非法状态跳转。"""
from __future__ import annotations

import pytest

from app.errors import DomainError, RejectCode
from app.models import TaskState


def test_duplicate_submit_same_content_no_second_execution(service, logger):
    service.register_robot("r1", ["nav"])
    a = service.submit_task("t1", {"x": 1}, ["nav"], priority=3)
    logger.info("首次下发 t1 payload={'x':1} prio=3 => %s", a.task.state.value)
    claim = service.claim_next("r1")
    token = claim.lease.fencing_token
    service.complete_task("t1", "r1", token, {"sum": 2})

    b = service.submit_task("t1", {"x": 1}, ["nav"], priority=3)
    logger.info("重复下发相同内容 => idempotent_hit=%s 结果固化=%s",
                b.idempotent_hit, b.task.result)
    assert b.idempotent_hit is True
    assert b.task.state is TaskState.COMPLETED
    assert b.task.result == {"sum": 2}


@pytest.mark.parametrize("field,kwargs", [
    ("payload", {"payload": {"x": 2}}),
    ("required_capabilities", {"required_capabilities": ["fly"]}),
    ("priority", {"priority": 9}),
    ("preemptible", {"preemptible": False}),
    ("max_retries", {"max_retries": 4}),
    ("timeout_ms", {"timeout_ms": 12345}),
])
def test_same_id_different_content_rejected(service, logger, field, kwargs):
    base = dict(task_id="t1", payload={"x": 1},
                required_capabilities=["nav"], priority=3)
    service.submit_task(**base)
    changed = dict(task_id="t1", payload={"x": 1},
                   required_capabilities=["nav"], priority=3)
    changed.update(kwargs)
    logger.info("重复下发 t1，差异字段=%s 新值=%r", field, kwargs)
    with pytest.raises(DomainError) as ei:
        service.submit_task(**changed)
    logger.info("判定依据：code=%s reason=%s",
                ei.value.code.value, ei.value.reason)
    assert ei.value.code is RejectCode.TASK_DUPLICATE_CONFLICT
    assert field in ei.value.reason  # 必须指出是哪一个字段不一致


def test_terminal_task_conflict_still_rejected(service):
    service.submit_task("t1", {"x": 1})
    t = service.tasks.get("t1")
    t.state = TaskState.COMPLETED
    t.result = {"r": 1}
    with pytest.raises(DomainError) as ei:
        service.submit_task("t1", {"x": 2})
    assert ei.value.code is RejectCode.TASK_DUPLICATE_CONFLICT


def test_illegal_transition_start_without_claim(service, logger):
    service.register_robot("r1")
    service.submit_task("t1", {})
    logger.info("未领取直接 start(t1, r1)，任务仍 QUEUED")
    with pytest.raises(DomainError) as ei:
        service.start_task("t1", "r1", 1)
    assert ei.value.code in (RejectCode.TASK_NOT_LEASE_HOLDER,
                             RejectCode.TASK_INVALID_TRANSITION)


def test_complete_unknown_task(service):
    service.register_robot("r1")
    with pytest.raises(DomainError) as ei:
        service.complete_task("nope", "r1", 1, {})
    assert ei.value.code is RejectCode.TASK_NOT_FOUND
