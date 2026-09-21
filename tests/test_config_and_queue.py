"""配置上限校验与有界队列（资源不得无界占用）。"""
from __future__ import annotations

import pytest

from app.config import SchedulerConfig
from app.errors import DomainError, RejectCode


@pytest.mark.parametrize("kwargs", [
    {"heartbeat_timeout_ms": 0},
    {"lease_duration_ms": -1},
    {"max_lease_duration_ms": 0},
    {"max_queue_size": 0},
    {"max_attempts": 0},
    {"lease_duration_ms": 999_999, "max_lease_duration_ms": 10},
    {"min_priority": 10, "max_priority": 0},
    {"max_retries_cap": -1},
])
def test_invalid_config_rejected_at_construction(kwargs):
    with pytest.raises(DomainError) as ei:
        SchedulerConfig(**kwargs)
    assert ei.value.code is RejectCode.INVALID_CONFIG


def test_task_param_bounds_enforced(service):
    with pytest.raises(DomainError) as ei:
        service.submit_task("tp", {}, priority=10 ** 9)
    assert ei.value.code is RejectCode.INVALID_CONFIG
    with pytest.raises(DomainError) as ei:
        service.submit_task("tr", {}, max_retries=999_999)
    assert ei.value.code is RejectCode.INVALID_CONFIG
    with pytest.raises(DomainError) as ei:
        service.submit_task("tt", {}, timeout_ms=10 ** 12)
    assert ei.value.code is RejectCode.INVALID_CONFIG


def test_bounded_queue_rejects_when_full():
    cfg = SchedulerConfig(max_queue_size=2)
    svc = FleetService_inmem(cfg)
    svc.submit_task("a", {})
    svc.submit_task("b", {})
    with pytest.raises(DomainError) as ei:
        svc.submit_task("c", {})
    assert ei.value.code is RejectCode.QUEUE_FULL


def test_reclaim_respects_queue_bound_at_capacity():
    """回收重试回队时若队列已满，按 QUEUE_FULL 显式失败而非无界增长。"""
    cfg = SchedulerConfig(max_queue_size=2, heartbeat_timeout_ms=100,
                          lease_duration_ms=1000)
    from app.timeutil import FakeClock
    from app.service import FleetService
    clk = FakeClock(0)
    svc = FleetService.in_memory(clock=clk, config=cfg)
    svc.register_robot("r1", ["nav"])
    svc.submit_task("held", {}, ["nav"], max_retries=1)
    c = svc.claim_next("r1")
    svc.start_task("held", "r1", c.lease.fencing_token)
    # 排队容量被两个其他任务占满（held 另为在途任务）
    svc.submit_task("a", {})
    svc.submit_task("b", {})
    clk.advance(101)
    with pytest.raises(DomainError) as ei:
        svc.reap_stale_leases()
    assert ei.value.code is RejectCode.QUEUE_FULL


# 小工具：避免在模块顶部循环导入
def FleetService_inmem(cfg):
    from app.timeutil import FakeClock
    from app.service import FleetService
    return FleetService.in_memory(clock=FakeClock(0), config=cfg)
