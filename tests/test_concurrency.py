"""并发竞争：同一任务不得被两个执行者同时持有；完成/回收/续约竞态安全。"""
from __future__ import annotations

import threading

import pytest

from app.errors import DomainError, RejectCode
from app.models import LeaseState, TaskState


def test_concurrent_claims_only_one_winner(service, logger):
    for i in range(20):
        service.register_robot(f"r{i}", ["nav"])
    # 只有一个任务
    service.submit_task("only", {}, ["nav"])

    winners: list[str] = []
    errors: list[tuple[str, RejectCode]] = []
    barrier = threading.Barrier(20)

    def worker(rid: str):
        barrier.wait()
        try:
            res = service.claim_next(rid)
            if res is not None:
                winners.append(rid)
        except DomainError as e:
            errors.append((rid, e.code))

    threads = [threading.Thread(target=worker, args=(f"r{i}",))
               for i in range(20)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    logger.info("并发领取输入=20机器人/1任务；成功者=%s 拒绝=%s",
                winners, errors)
    assert winners == ["r0"] or len(winners) == 1
    task = service.tasks.get("only")
    assert task.state in (TaskState.LEASED, TaskState.RUNNING)
    active = [l for l in service.leases.list_leases()
              if l.state in (LeaseState.ACTIVE, LeaseState.RENEWED)
              and l.task_id == "only"]
    assert len(active) <= 1


def test_complete_races_with_reclaim(service, clock, logger):
    service.register_robot("r1", ["nav"])
    service.submit_task("t1", {}, ["nav"], max_retries=3)
    c = service.claim_next("r1")
    token = c.lease.fencing_token
    outcome: dict[str, str] = {}

    def completer():
        try:
            service.complete_task("t1", "r1", token, {"done": 1})
            outcome["completer"] = "completed"
        except DomainError as e:
            outcome["completer"] = e.code.value

    def reaper():
        try:
            service.reap_stale_leases()
            outcome["reaper"] = "reaped"
        except DomainError as e:
            outcome["reaper"] = e.code.value

    # 情形 A：完成先于超时（同一时刻推进时钟前并发，两者均合法序列化）
    t1 = threading.Thread(target=completer)
    t2 = threading.Thread(target=reaper)
    t1.start(); t2.start(); t1.join(); t2.join()
    logger.info("完成/回收竞态结果=%s task=%s",
                outcome, service.tasks.get("t1").state.value)
    # 无论顺序如何：任务要么 COMPLETED 要么在队重试，绝不重复持有
    state = service.tasks.get("t1").state
    assert state in (TaskState.COMPLETED, TaskState.QUEUED)
    active = service.leases.active_lease("t1")
    if state is TaskState.COMPLETED:
        assert active is None


def test_stale_progress_report_after_reclaim_rejected(service, clock):
    service.register_robot("r1", ["nav"])
    service.submit_task("t1", {}, ["nav"], max_retries=2)
    c = service.claim_next("r1")
    service.start_task("t1", "r1", c.lease.fencing_token)
    clock.advance(101)
    service.reap_stale_leases()
    # 旧持有者并发/延迟上报进度：必须被 fencing/无活跃租约拒绝
    with pytest.raises(DomainError) as ei:
        service.report_progress("t1", "r1", c.lease.fencing_token,
                                {"late": True})
    assert ei.value.code is RejectCode.TASK_NOT_LEASE_HOLDER


def test_two_robots_same_token_cannot_both_complete(service, clock):
    service.register_robot("r1", ["nav"])
    service.register_robot("r2", ["nav"])
    service.submit_task("t1", {}, ["nav"], max_retries=2)
    c1 = service.claim_next("r1")
    clock.advance(101)
    service.reap_stale_leases()
    service.registry.heartbeat("r1")
    # r2 拿到新租约（新 token）
    c2 = service.claim_next("r2")
    assert c2.lease.fencing_token != c1.lease.fencing_token
    # r1（已不是持有者）上报 -> 非持有者
    with pytest.raises(DomainError) as ei:
        service.complete_task("t1", "r1", c1.lease.fencing_token, {})
    assert ei.value.code is RejectCode.TASK_NOT_LEASE_HOLDER
    # r2 才是当前持有者，但拿了旧 token -> 版本冲突（fencing）
    with pytest.raises(DomainError) as ei:
        service.complete_task("t1", "r2", c1.lease.fencing_token, {})
    assert ei.value.code is RejectCode.LEASE_VERSION_CONFLICT
    # r2 用正确 token 完成
    done = service.complete_task("t1", "r2", c2.lease.fencing_token, {"ok": 1})
    assert done.state is TaskState.COMPLETED
