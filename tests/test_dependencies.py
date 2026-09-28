"""任务依赖编排：等待依赖 vs 等待调度、阻断传播与恢复、成环拒绝、
先声明后补建、解阻并发竞争、解阻容量约束、重放一致性。"""

import json
import threading

import pytest

from fleet_orchestrator.errors import (
    DependencyCycle,
    MissionNotAssignable,
    MissionPayloadConflict,
    PreemptTargetNotRunning,
    Rejection,
)
from fleet_orchestrator.models import MissionStatus
from fleet_orchestrator.service import OrchestratorService


def _run_to_success(svc, robot_id: str, mission_id: str) -> None:
    _, lease = svc.acquire_mission(robot_id, mission_id)
    svc.start_mission(robot_id, mission_id, lease.lease_id)
    svc.complete_mission(robot_id, mission_id, lease.lease_id)


# ---- 依赖完成前后能否领取 ----

def test_not_acquirable_until_deps_succeed(service, case_log):
    service.register_robot("r1", [])
    service.submit_mission("a")
    service.submit_mission("b", depends_on=["a"])
    m = service.get_mission("b")
    case_log("依赖未完成时领取", input="b depends_on a, a 仍 PENDING",
             expect="b 为 WAITING_DEPS 且不可领取", actual=f"status={m.status}")
    assert m.status == MissionStatus.WAITING_DEPS
    with pytest.raises(MissionNotAssignable) as exc:
        service.acquire_mission("r1", "b")
    case_log("显式领取被依赖卡住的任务", expect="mission_not_assignable",
             actual=exc.value.reason)
    # 自动调度不得选中它：此时唯一可调度的是 a
    m_auto, lease_a = service.acquire_mission("r1")
    case_log("自动调度跳过等待依赖的任务", expect="选中 a 而非 b",
             actual=f"acquired={m_auto.mission_id}")
    assert m_auto.mission_id == "a"

    service.start_mission("r1", "a", lease_a.lease_id)
    service.complete_mission("r1", "a", lease_a.lease_id)
    m = service.get_mission("b")
    case_log("前置完成后", expect="b 自动解阻为 PENDING 且可领取",
             actual=f"status={m.status}")
    assert m.status == MissionStatus.PENDING
    _, lease = service.acquire_mission("r1", "b")
    assert lease.mission_id == "b"


def test_waiting_deps_distinguished_from_pending(service, case_log):
    """等待依赖（WAITING_DEPS）与等待调度（PENDING）可区分。"""
    service.submit_mission("free")
    service.submit_mission("chained", depends_on=["free"])
    pending = {m.mission_id for m in service.list_missions(MissionStatus.PENDING)}
    waiting = {m.mission_id for m in service.list_missions(MissionStatus.WAITING_DEPS)}
    case_log("状态区分", expect="free=PENDING, chained=WAITING_DEPS",
             actual=f"pending={pending} waiting={waiting}")
    assert pending == {"free"}
    assert waiting == {"chained"}


# ---- 先声明、后补建；查询能看出卡在哪个前置上 ----

def test_declare_before_dependency_exists(service, case_log):
    service.register_robot("r1", [])
    service.submit_mission("b", depends_on=["ghost"])
    view = service.dependency_view("b")
    case_log("前置未出现", input="b depends_on ghost(未下发)",
             expect="依赖视图显示 MISSING，状态 WAITING_DEPS",
             actual=f"status={view['status']} deps={view['dependencies']}")
    assert view["status"] == MissionStatus.WAITING_DEPS
    assert view["dependencies"] == [
        {"mission_id": "ghost", "status": "MISSING", "satisfied": False}
    ]
    # 后补建前置并完成，依赖任务按同样规则解阻
    service.submit_mission("ghost")
    _run_to_success(service, "r1", "ghost")
    view = service.dependency_view("b")
    case_log("补建并完成前置", expect="b 解阻为 PENDING，依赖 satisfied",
             actual=f"status={view['status']} deps={view['dependencies']}")
    assert view["status"] == MissionStatus.PENDING
    assert view["dependencies"][0]["satisfied"] is True


# ---- 前置失败/取消后的阻断传播与恢复 ----

def test_dep_failure_blocks_dependent(service, case_log):
    service.register_robot("r1", [])
    service.submit_mission("a", max_attempts=1)
    service.submit_mission("b", depends_on=["a"])
    _, lease = service.acquire_mission("r1", "a")
    service.start_mission("r1", "a", lease.lease_id)
    service.fail_mission("r1", "a", lease.lease_id, error="boom")
    b = service.get_mission("b")
    case_log("前置失败", input="a FAILED(不可再重试)",
             expect="b BLOCKED 且 blocked_by=a，不可领取、不可抢占",
             actual=f"status={b.status} blocked_by={b.blocked_by}")
    assert b.status == MissionStatus.BLOCKED
    assert b.blocked_by == "a"
    with pytest.raises(MissionNotAssignable):
        service.acquire_mission("r1", "b")
    with pytest.raises(PreemptTargetNotRunning):
        service.preempt_mission("b")
    # 回收逻辑不得把 BLOCKED 误当在执行：无租约，sweep 不涉及
    assert service.get_lease("b") is None
    assert service.sweep() == []


def test_dep_cancel_blocks_dependent(service, case_log):
    service.submit_mission("a")
    service.submit_mission("b", depends_on=["a"])
    service.cancel_mission("a")
    b = service.get_mission("b")
    case_log("前置取消", expect="b BLOCKED blocked_by=a",
             actual=f"status={b.status} blocked_by={b.blocked_by}")
    assert b.status == MissionStatus.BLOCKED
    assert b.blocked_by == "a"


def test_transitive_blocking_propagates(service, case_log):
    service.submit_mission("a")
    service.submit_mission("b", depends_on=["a"])
    service.submit_mission("c", depends_on=["b"])
    service.cancel_mission("a")
    b, c = service.get_mission("b"), service.get_mission("c")
    case_log("阻断沿链传播", input="c<-b<-a, a CANCELLED",
             expect="b、c 均 BLOCKED，blocked_by 各自指向直接前置",
             actual=f"b={b.status}/{b.blocked_by} c={c.status}/{c.blocked_by}")
    assert b.status == MissionStatus.BLOCKED and b.blocked_by == "a"
    assert c.status == MissionStatus.BLOCKED and c.blocked_by == "b"


def test_blocked_recovers_after_dep_retry_and_success(service, case_log):
    service.register_robot("r1", [])
    service.submit_mission("a", max_attempts=2)
    service.submit_mission("b", depends_on=["a"])
    _, lease = service.acquire_mission("r1", "a")
    service.start_mission("r1", "a", lease.lease_id)
    service.fail_mission("r1", "a", lease.lease_id, error="boom")
    assert service.get_mission("b").status == MissionStatus.BLOCKED
    service.retry_mission("a")
    b = service.get_mission("b")
    case_log("前置重试回到等待", expect="b 从 BLOCKED 恢复为 WAITING_DEPS",
             actual=f"status={b.status} blocked_by={b.blocked_by}")
    assert b.status == MissionStatus.WAITING_DEPS
    assert b.blocked_by is None
    _run_to_success(service, "r1", "a")
    b = service.get_mission("b")
    case_log("前置最终成功", expect="b 解阻为 PENDING 可领取",
             actual=f"status={b.status}")
    assert b.status == MissionStatus.PENDING
    service.acquire_mission("r1", "b")


# ---- 成环拒绝 ----

def test_self_cycle_rejected(service, case_log):
    with pytest.raises(DependencyCycle) as exc:
        service.submit_mission("x", depends_on=["x"])
    case_log("自依赖", input="x depends_on x", expect="dependency_cycle",
             actual=exc.value.reason)
    assert exc.value.reason == "dependency_cycle"
    assert service.list_missions() == []  # 不留下半截记录


def test_indirect_cycle_rejected_others_unaffected(service, case_log):
    service.submit_mission("a", depends_on=["b"])  # b 未下发，合法
    service.submit_mission("ok")
    with pytest.raises(DependencyCycle) as exc:
        service.submit_mission("b", depends_on=["a"])  # b->a->b 成环
    case_log("间接成环", input="a<-b 且 b<-a", expect="dependency_cycle",
             actual=exc.value.reason)
    assert exc.value.reason == "dependency_cycle"
    ids = {m.mission_id for m in service.list_missions()}
    case_log("其它任务不受连带影响", expect="a、ok 仍在，b 未创建",
             actual=f"missions={sorted(ids)}")
    assert ids == {"a", "ok"}
    # 无环的补建仍然允许：c 依赖 a 合法
    service.submit_mission("c", depends_on=["a"])
    assert service.get_mission("c").status == MissionStatus.WAITING_DEPS


# ---- 幂等：依赖声明参与内容一致性 ----

def test_same_deps_idempotent_different_deps_conflict(service, case_log):
    m1, c1 = service.submit_mission("m", depends_on=["a", "b"])
    m2, c2 = service.submit_mission("m", depends_on=["b", "a"])  # 顺序无关
    case_log("同依赖重复下发", expect="幂等 created=False",
             actual=f"created=({c1},{c2})")
    assert c1 is True and c2 is False
    assert m1.payload_hash == m2.payload_hash
    with pytest.raises(MissionPayloadConflict) as exc:
        service.submit_mission("m", depends_on=["a"])
    case_log("依赖不同", expect="mission_payload_conflict", actual=exc.value.reason)
    assert exc.value.reason == "mission_payload_conflict"


# ---- 解阻与领取并发竞争 ----

def test_concurrent_unblock_and_acquire(service, case_log):
    n = 6
    for i in range(n):
        service.register_robot(f"r{i}", [])
    service.submit_mission("a")
    service.submit_mission("b", depends_on=["a"])
    _, lease = service.acquire_mission("r0", "a")
    service.start_mission("r0", "a", lease.lease_id)

    completed = threading.Event()
    winners: list[str] = []
    barrier = threading.Barrier(n)

    def completer() -> None:
        barrier.wait()
        service.complete_mission("r0", "a", lease.lease_id)
        completed.set()

    def acquirer(rid: str) -> None:
        barrier.wait()
        # 自旋等待解阻；成功必发生在 a 完成之后
        while True:
            try:
                service.acquire_mission(rid, "b")
                assert completed.is_set(), "解阻前不得领取成功"
                winners.append(rid)
                return
            except Rejection as e:
                if e.reason != "mission_not_assignable":
                    raise
                if completed.is_set() and service.get_mission("b").status != MissionStatus.PENDING:
                    return  # 已被别人领走

    threads = [threading.Thread(target=completer)]
    threads += [threading.Thread(target=acquirer, args=(f"r{i}",)) for i in range(1, n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    case_log("解阻与领取并发", input=f"complete(a) ‖ {n - 1} 个 acquire(b)",
             expect="恰好 1 个获胜且租约唯一",
             actual=f"winners={winners}")
    assert len(winners) == 1
    got = service.get_lease("b")
    assert got is not None and got.robot_id == winners[0]


# ---- 解阻过程仍遵守容量上限 ----

def test_unblock_respects_backlog_cap(store, clock, small_config, case_log):
    """等待前置的任务与待分配任务共用同一份配额：配额被等待池占满时
    新下发被拒绝；已容纳的等待任务晋升 PENDING 是配额中性迁移，
    不会把积压撑过上限，也不会被卡死。"""
    from fleet_orchestrator.errors import BacklogFull

    svc = OrchestratorService(store, config=small_config(max_backlog=2), clock=clock)
    svc.register_robot("r1", [])
    svc.submit_mission("dep")                    # PENDING，占用 1
    svc.submit_mission("m1", depends_on=["dep"])  # WAITING_DEPS，占用 1，配额已满
    with pytest.raises(BacklogFull) as exc:
        svc.submit_mission("m2", depends_on=["dep"])
    case_log("等待池占满配额", input="max_backlog=2, dep+m1 已占 2 槽",
             expect="m2 拒绝 backlog_full 且不留记录", actual=exc.value.reason)
    assert exc.value.reason == "backlog_full"
    assert [m.mission_id for m in svc.list_missions()] == ["dep", "m1"]
    # 前置完成：m1 晋升 PENDING 是配额中性迁移，不被上限卡死
    _run_to_success(svc, "r1", "dep")
    s1 = svc.get_mission("m1").status
    case_log("前置完成解阻", expect="m1 晋升 PENDING（配额中性），积压仍 <= 2",
             actual=f"m1={s1}")
    assert s1 == MissionStatus.PENDING
    # dep 已终态退出配额：此时积压仅 m1 占 1 槽，可再容纳一个；
    # 再下一个仍被上限挡住，等待池不会趁机超限
    svc.submit_mission("m3")
    with pytest.raises(BacklogFull):
        svc.submit_mission("m4")
    case_log("配额再度占满", input="m1(PENDING)+m3 占 2 槽",
             expect="m4 拒绝 backlog_full", actual="rejected")
    # 领取释放槽位后才放行新下发
    svc.acquire_mission("r1", "m1")              # 释放一个槽位
    svc.submit_mission("m4")                     # 现在可以入队
    case_log("槽位释放后", expect="m4 入队成功",
             actual=f"missions={sorted(m.mission_id for m in svc.list_missions())}")
    assert svc.get_mission("m4").status == MissionStatus.PENDING


# ---- 重放 / 重启后状态一致 ----

def test_restart_and_replay_preserve_dependency_state(store, clock, case_log):
    svc = OrchestratorService(store, clock=clock)
    svc.register_robot("r1", [])
    svc.submit_mission("done")
    svc.submit_mission("unblocked", depends_on=["done"])
    svc.submit_mission("waiting", depends_on=["todo"])
    svc.submit_mission("todo")
    svc.submit_mission("dead")
    svc.submit_mission("blocked", depends_on=["dead"])
    _run_to_success(svc, "r1", "done")     # unblocked -> PENDING
    svc.cancel_mission("dead")             # blocked -> BLOCKED
    before = {
        m.mission_id: (m.status, tuple(m.depends_on), m.blocked_by)
        for m in svc.list_missions()
    }
    svc2 = OrchestratorService(store, clock=clock)  # 模拟重启
    after = {
        m.mission_id: (m.status, tuple(m.depends_on), m.blocked_by)
        for m in svc2.list_missions()
    }
    case_log("重启后依赖状态", expect="与故障前一致，无凭空可领取",
             actual=f"before={before} after={after}")
    assert before == after
    assert after["unblocked"][0] == MissionStatus.PENDING
    assert after["waiting"][0] == MissionStatus.WAITING_DEPS
    assert after["blocked"][0] == MissionStatus.BLOCKED
    assert after["blocked"][2] == "dead"
    # 事件重放与快照一致
    assert store.replay() == store.load()


# ---- 兼容：老记录缺省无依赖 ----

def test_legacy_record_without_dependency_fields(store, clock, case_log):
    state = {
        "robots": {"r1": {"robot_id": "r1", "capabilities": [],
                          "last_heartbeat": 1000.0}},
        "missions": {"old": {"mission_id": "old", "payload_hash": "h"}},
        "leases": {},
    }
    store.dir.mkdir(parents=True, exist_ok=True)
    with open(store.dir / "state.json", "w") as f:
        json.dump(state, f)
    svc = OrchestratorService(store, clock=clock)
    m = svc.get_mission("old")
    case_log("老记录缺省语义", input="无 depends_on/blocked_by 字段",
             expect="视为无依赖，PENDING 可领取",
             actual=f"deps={m.depends_on} blocked_by={m.blocked_by} status={m.status}")
    assert m.depends_on == []
    assert m.blocked_by is None
    assert m.status == MissionStatus.PENDING
    _, lease = svc.acquire_mission("r1", "old")
    assert lease.mission_id == "old"
