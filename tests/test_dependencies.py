"""任务依赖编排：等待/解阻、阻断传播与恢复、成环拒绝、先声明后补建、
解阻并发竞争、容量上限、幂等一致性、老记录兼容与重放一致。

所有用例通过 case_log 打印输入与判定依据（[CASE] 日志，-s 可见）。
"""

from __future__ import annotations

import threading

import pytest

from fleet_orchestrator.config import SchedulerConfig
from fleet_orchestrator.errors import (
    DependencyCycle,
    MissionNotAssignable,
    MissionPayloadConflict,
    BacklogFull,
    PreemptTargetNotRunning,
)
from fleet_orchestrator.models import MissionStatus
from fleet_orchestrator.service import OrchestratorService
from fleet_orchestrator.store import Store


def _run_ok(service, robot: str, mission: str) -> None:
    """领取 -> 开始 -> 成功 的完整闭环。"""
    _, lease = service.acquire_mission(robot, mission)
    service.start_mission(robot, mission, lease.lease_id)
    service.complete_mission(robot, mission, lease.lease_id)


# ---------- 1. 依赖完成前后能否领取 ----------

def test_dependent_not_acquirable_before_deps_succeed(service, case_log):
    service.register_robot("r1", ["nav", "lift"])
    service.submit_mission("nav", ["nav"])
    service.submit_mission("lift", ["lift"], dependencies=["nav"])
    service.submit_mission("ret", ["nav"], dependencies=["lift"])
    view = service.dependency_view("lift")
    case_log("链式下发", input="nav -> lift -> ret, 均未完成",
             expect="lift/ret 为 WAITING，自动调度只能领到 nav",
             actual=f"lift={service.get_mission('lift').status} "
                    f"ret={service.get_mission('ret').status} wait_kind={view['wait_kind']}")
    assert service.get_mission("lift").status == MissionStatus.WAITING
    assert service.get_mission("ret").status == MissionStatus.WAITING
    assert view["wait_kind"] == "dependency"
    assert view["unsatisfied"] == ["nav"]

    # 自动调度不得提前派出 lift/ret
    picked, _ = service.acquire_mission("r1")
    case_log("自动调度跳过等待任务", input="acquire 不指定 id",
             expect="只能选 nav", actual=picked.mission_id)
    assert picked.mission_id == "nav"

    # 显式领取 WAITING 任务也被明确拒绝
    with pytest.raises(MissionNotAssignable) as exc:
        service.acquire_mission("r1", "lift")
    case_log("显式领取等待中任务", input="acquire lift（前置未完成）",
             expect="mission_not_assignable", actual=exc.value.reason)
    assert exc.value.reason == "mission_not_assignable"


def test_dependent_becomes_acquirable_after_deps_succeed(service, case_log):
    service.register_robot("r1", ["nav", "lift"])
    service.submit_mission("nav", ["nav"])
    service.submit_mission("lift", ["lift"], dependencies=["nav"])
    service.submit_mission("ret", ["nav"], dependencies=["lift"])
    _run_ok(service, "r1", "nav")
    case_log("nav 成功后", input="complete nav",
             expect="lift -> PENDING 可领取, ret 仍 WAITING",
             actual=f"lift={service.get_mission('lift').status} "
                    f"ret={service.get_mission('ret').status}")
    assert service.get_mission("lift").status == MissionStatus.PENDING
    assert service.get_mission("ret").status == MissionStatus.WAITING
    # 此时无依赖任务不存在，自动调度应选到 lift
    picked, lift_lease = service.acquire_mission("r1")
    assert picked.mission_id == "lift"
    service.start_mission("r1", "lift", lift_lease.lease_id)
    service.complete_mission("r1", "lift", lift_lease.lease_id)
    case_log("lift 成功后", input="complete lift",
             expect="ret -> PENDING", actual=service.get_mission("ret").status)
    assert service.get_mission("ret").status == MissionStatus.PENDING


def test_wait_kind_distinguishes_schedule_vs_dependency(service, case_log):
    service.submit_mission("free")
    service.submit_mission("dep", dependencies=["free"])
    free_view = service.dependency_view("free")
    dep_view = service.dependency_view("dep")
    case_log("两种等待可区分", input="free=无依赖PENDING, dep=依赖free",
             expect="free wait_kind=schedule；dep wait_kind=dependency",
             actual=f"free={free_view['wait_kind']} dep={dep_view['wait_kind']}")
    assert free_view["wait_kind"] == "schedule"
    assert dep_view["wait_kind"] == "dependency"


# ---------- 2. 前置失败 / 取消后的阻断传播 ----------

def test_failed_prerequisite_blocks_dependents(service, case_log):
    service.register_robot("r1", [])
    service.submit_mission("a")
    service.submit_mission("b", dependencies=["a"])
    service.submit_mission("c", dependencies=["b"])
    _, lease = service.acquire_mission("r1", "a")
    service.fail_mission("r1", "a", lease.lease_id, "boom")

    b = service.get_mission("b")
    c = service.get_mission("c")
    case_log("前置终态失败", input="a FAILED(不重试), b->a, c->b",
             expect="b/c 均 BLOCKED，归因到根因 a(dep_failed)",
             actual=f"b=({b.status},{b.blocked_by},{b.blocked_reason}) "
                    f"c=({c.status},{c.blocked_by},{c.blocked_reason})")
    assert b.status == MissionStatus.BLOCKED
    assert b.blocked_by == "a" and b.blocked_reason == "dep_failed"
    assert c.status == MissionStatus.BLOCKED
    assert c.blocked_by == "a"  # 阻断链根因传播
    assert service.dependency_view("b")["wait_kind"] == "blocked"

    # 被阻断任务：不可领取、不可抢占、不被 sweep 当作在执行
    with pytest.raises(MissionNotAssignable):
        service.acquire_mission("r1", "b")
    with pytest.raises(PreemptTargetNotRunning):
        service.preempt_mission("b")
    assert service.sweep() == []
    assert service.get_lease("b") is None


def test_cancelled_prerequisite_blocks_dependents(service, case_log):
    service.submit_mission("a")
    service.submit_mission("b", dependencies=["a"])
    service.cancel_mission("a")
    b = service.get_mission("b")
    case_log("前置取消", input="cancel a, b 依赖 a",
             expect="b BLOCKED, dep_cancelled, blocked_by=a",
             actual=f"b=({b.status},{b.blocked_by},{b.blocked_reason})")
    assert b.status == MissionStatus.BLOCKED
    assert b.blocked_by == "a" and b.blocked_reason == "dep_cancelled"


def test_blocked_dependent_recovers_after_retry_and_success(service, case_log):
    service.register_robot("r1", [])
    service.submit_mission("a")
    service.submit_mission("b", dependencies=["a"])
    _, lease = service.acquire_mission("r1", "a")
    service.fail_mission("r1", "a", lease.lease_id)
    assert service.get_mission("b").status == MissionStatus.BLOCKED

    service.retry_mission("a")  # 前置回到等待
    case_log("前置重试回到等待", input="retry a",
             expect="b 从 BLOCKED 恢复为 WAITING（不再卡死）",
             actual=service.get_mission("b").status)
    assert service.get_mission("b").status == MissionStatus.WAITING

    _run_ok(service, "r1", "a")
    view = service.dependency_view("b")
    case_log("前置重试后成功", input="retry a 并 complete",
             expect="b -> PENDING，无阻断归因",
             actual=f"status={service.get_mission('b').status} view={view['wait_kind']} "
                    f"blocked_by={service.get_mission('b').blocked_by}")
    assert service.get_mission("b").status == MissionStatus.PENDING
    assert service.get_mission("b").blocked_by is None
    assert view["wait_kind"] == "schedule"


# ---------- 3. 成环在下发时被拒 ----------

def test_self_dependency_rejected(service, case_log):
    with pytest.raises(DependencyCycle) as exc:
        service.submit_mission("x", dependencies=["x"])
    case_log("自依赖", input="submit x deps=[x]",
             expect="dependency_cycle", actual=exc.value.reason)
    assert exc.value.reason == "dependency_cycle"
    assert service.list_missions() == []


def test_indirect_cycle_rejected_without_affecting_others(service, case_log):
    service.submit_mission("a", dependencies=["b"])
    service.submit_mission("b", dependencies=["c"])
    with pytest.raises(DependencyCycle) as exc:
        service.submit_mission("c", dependencies=["a"])
    remaining = {m.mission_id: m.status for m in service.list_missions()}
    case_log("间接成环", input="a->b, b->c, 再提交 c->a",
             expect="拒绝 dependency_cycle；a/b 保持原样，c 不存在",
             actual=f"reason={exc.value.reason} remaining={remaining}")
    assert exc.value.reason == "dependency_cycle"
    assert set(remaining) == {"a", "b"}
    assert remaining["a"] == MissionStatus.WAITING
    # 补建 c（无环边）后图仍然有效：a/b/c 都 WAITING
    service.submit_mission("c")
    assert service.get_mission("a").status == MissionStatus.WAITING


# ---------- 4. 先声明、后补建 ----------

def test_forward_reference_then_fill_in(service, case_log):
    service.submit_mission("b", dependencies=["a"])  # a 尚未下发
    view = service.dependency_view("b")
    case_log("先声明", input="submit b deps=[a]，a 不存在",
             expect="b WAITING, a 显示 MISSING",
             actual=f"status={service.get_mission('b').status} dep={view['dependency_status']}")
    assert service.get_mission("b").status == MissionStatus.WAITING
    assert view["dependency_status"] == {"a": "MISSING"}
    assert "a" in view["unsatisfied"]

    service.submit_mission("a")  # 后补建：仍未成功，b 继续等待
    assert service.get_mission("b").status == MissionStatus.WAITING
    case_log("后补建未完成", input="submit a（PENDING）",
             expect="b 仍 WAITING，a 状态 PENDING",
             actual=service.dependency_view("b")["dependency_status"])

    service.register_robot("r1", [])
    _run_ok(service, "r1", "a")
    case_log("补建任务完成", input="complete a",
             expect="b -> PENDING", actual=service.get_mission("b").status)
    assert service.get_mission("b").status == MissionStatus.PENDING


# ---------- 5. 解阻与领取并发竞争 ----------

def test_concurrent_unblock_vs_acquire(service, case_log):
    """前置成功(解阻)与多机器人领取解阻任务并发：任务只能被领取一次。"""
    n = 6
    for i in range(n):
        service.register_robot(f"r{i}", [])
    service.submit_mission("a")
    service.submit_mission("b", dependencies=["a"])
    _, lease = service.acquire_mission("r0", "a")
    service.start_mission("r0", "a", lease.lease_id)

    winners: list[str] = []
    rejections: list[str] = []
    barrier = threading.Barrier(n + 1)

    def complete_a() -> None:
        barrier.wait()
        service.complete_mission("r0", "a", lease.lease_id)

    def try_acquire_b(rid: str) -> None:
        barrier.wait()
        try:
            _, got = service.acquire_mission(rid, "b")
            winners.append(got.robot_id)
        except Exception as e:  # noqa: BLE001 - 收集 reason 做判定
            rejections.append(getattr(e, "reason", type(e).__name__))

    threads = [threading.Thread(target=complete_a)]
    threads += [threading.Thread(target=try_acquire_b, args=(f"r{i}",)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    b = service.get_mission("b")
    case_log("解阻‖领取并发", input="1 线程 complete a，6 线程同时 acquire b",
             expect="获胜者 0 或 1 个；若 1 个则 b=ASSIGNED 且租约唯一",
             actual=f"winners={winners} rejections={sorted(set(rejections))} b={b.status}")
    assert len(winners) <= 1
    if winners:
        assert b.status == MissionStatus.ASSIGNED
        lease_b = service.get_lease("b")
        assert lease_b is not None and lease_b.robot_id == winners[0]
        holders = [r.robot_id for r in service.list_robots()
                   if r.current_mission_id == "b"]
        assert holders == winners
        assert len(rejections) == n - len(winners)
    else:
        # 所有领取都发生在解阻提交前：b 必然保持 WAITING 且无租约
        assert b.status == MissionStatus.WAITING
        assert service.get_lease("b") is None


# ---------- 6. 解阻 / 提交 均受同一容量上限 ----------

def test_waiting_and_blocked_count_toward_backlog(store, clock, case_log):
    svc = OrchestratorService(store, config=SchedulerConfig(max_backlog=2), clock=clock)
    svc.submit_mission("w1", dependencies=["ghost1"])  # WAITING 占槽
    svc.submit_mission("w2", dependencies=["ghost2"])  # WAITING 占槽
    with pytest.raises(BacklogFull) as exc:
        svc.submit_mission("plain")  # 无依赖任务同样被排队上限挡住
    case_log("等待任务占积压槽", input="max_backlog=2; 2 个 WAITING 后再提交",
             expect="backlog_full，不无界堆积", actual=exc.value.reason)
    assert exc.value.reason == "backlog_full"
    assert {m.mission_id for m in svc.list_missions()} == {"w1", "w2"}


def test_batch_unblock_respects_capacity_via_shared_cap(store, clock, case_log):
    """一批任务被同一前置解阻时，不能绕开同一套排队上限：
    WAITING/BLOCKED 与 PENDING/PREEMPTED 共用同一个 max_backlog 池，
    解阻只是在池内从 WAITING 变为 PENDING，池总量不变，因此放行批量不受限
    之外不可能超额；这里通过把池子填满来验证解阻不会制造超额可领取集合。"""
    svc = OrchestratorService(store, config=SchedulerConfig(max_backlog=3), clock=clock)
    svc.register_robot("r1", [])
    svc.submit_mission("a")
    svc.submit_mission("b1", dependencies=["a"])  # WAITING
    svc.submit_mission("b2", dependencies=["a"])  # WAITING
    # 池已达上限（a + b1 + b2 = 3），新提交（含无依赖）被拒
    with pytest.raises(BacklogFull):
        svc.submit_mission("extra")
    _run_ok(svc, "r1", "a")  # 一次成功解阻一批
    statuses = {m.mission_id: m.status for m in
                svc.list_missions(MissionStatus.PENDING)}
    case_log("一批解阻", input="a 成功；max_backlog=3",
             expect="b1/b2 均 PENDING；池内总量仍为 3，未凭空多出任务",
             actual=f"pending={statuses} total={len(svc.list_missions())}")
    assert statuses == {"b1": MissionStatus.PENDING, "b2": MissionStatus.PENDING}
    assert len(svc.list_missions()) == 3
    # 解阻腾出一个槽位（a 离开积压池）后，新提交可成功
    _, created = svc.submit_mission("extra")
    assert created is True


# ---------- 7. 重复下发：依赖参与内容一致性 ----------

def test_duplicate_submit_dependency_part_of_identity(service, case_log):
    service.submit_mission("m", dependencies=["a"])
    _, created = service.submit_mission("m", dependencies=["a"])
    case_log("相同依赖重复下发", input="m deps=[a] x2",
             expect="幂等 created=False", actual=created)
    assert created is False

    with pytest.raises(MissionPayloadConflict) as exc:
        service.submit_mission("m", dependencies=["b"])
    case_log("不同依赖重复下发", input="再提交 m deps=[b]",
             expect="mission_payload_conflict", actual=exc.value.reason)
    assert exc.value.reason == "mission_payload_conflict"
    # 拒绝后原记录依赖不被篡改
    assert service.get_mission("m").dependencies == ["a"]


# ---------- 8. 老记录缺省兼容 / 重放与重启一致 ----------

def test_legacy_record_without_dependencies_defaults_no_deps(store, clock, case_log):
    svc = OrchestratorService(store, clock=clock)
    svc.submit_mission("old")
    # 模拟老格式记录：落盘记录里移除依赖编排新字段
    state = store.load()
    rec = state["missions"]["old"]
    for field in ("dependencies", "blocked_by", "blocked_reason"):
        rec.pop(field, None)
    store.save_snapshot(state)
    svc2 = OrchestratorService(store, clock=clock)
    m = svc2.get_mission("old")
    case_log("老记录缺字段", input="mission 记录无 dependencies 等字段",
             expect="dependencies=[]，行为同现状（PENDING 可领取）",
             actual=f"deps={m.dependencies} status={m.status}")
    assert m.dependencies == []
    assert m.status == MissionStatus.PENDING
    svc2.register_robot("r1", [])
    picked, _ = svc2.acquire_mission("r1")
    assert picked.mission_id == "old"


def test_dependency_state_consistent_after_restart_and_replay(store, clock, case_log):
    svc = OrchestratorService(store, clock=clock)
    svc.register_robot("r1", [])
    svc.submit_mission("a")
    svc.submit_mission("b", dependencies=["a"])
    svc.submit_mission("c", dependencies=["a"])
    svc.submit_mission("d", dependencies=["never"])   # 等一个不存在的任务
    _, lease = svc.acquire_mission("r1", "a")
    svc.fail_mission("r1", "a", lease.lease_id)       # b/c 被阻断
    before = {
        mid: (m.status.value, m.blocked_by, m.blocked_reason)
        for mid, m in ((x.mission_id, x) for x in svc.list_missions())
    }
    case_log("故障前状态", input="a FAILED; b,c 依赖 a; d 等 never",
             expect="重启/重放后完全一致，不凭空多出可领取任务",
             actual=before)

    svc2 = OrchestratorService(store, clock=clock)     # 快照恢复
    after = {
        mid: (m.status.value, m.blocked_by, m.blocked_reason)
        for mid, m in ((x.mission_id, x) for x in svc2.list_missions())
    }
    assert after == before
    pending = {m.mission_id for m in svc2.list_missions(MissionStatus.PENDING)}
    assert pending == set()

    replayed = store.replay()                          # 事件重放
    for mid, (status, blocker, reason) in before.items():
        rec = replayed["missions"][mid]
        assert rec["status"] == status
        assert rec.get("blocked_by") == blocker
        assert rec.get("blocked_reason") == reason
    case_log("重放一致", input="折叠 events.jsonl",
             expect="每个任务 status/blocked 与故障前一致",
             actual=f"replay_missions={sorted(replayed['missions'])}")


def test_unblocked_state_not_duplicated_after_restart(store, clock, case_log):
    """已解阻为 PENDING 的任务重启后仍是同一条 PENDING，不会额外冒出来。"""
    svc = OrchestratorService(store, clock=clock)
    svc.register_robot("r1", [])
    svc.submit_mission("a")
    svc.submit_mission("b", dependencies=["a"])
    _run_ok(svc, "r1", "a")
    assert svc.get_mission("b").status == MissionStatus.PENDING
    svc2 = OrchestratorService(store, clock=clock)
    missions = svc2.list_missions()
    b = svc2.get_mission("b")
    case_log("解阻后重启", input="a 成功解阻 b 后重建服务",
             expect="恰好一条 b 且 PENDING，无租约、无重复",
             actual=f"count={len(missions)} b={b.status} lease={svc2.get_lease('b')}")
    assert len(missions) == 2
    assert b.status == MissionStatus.PENDING
    assert svc2.get_lease("b") is None


def test_reclaim_exhaustion_blocks_dependents(store, service, clock, case_log):
    """回收路径次数用尽时进入 FAILED 终态，依赖方同样被阻断，
    不能被回收逻辑漏判而一直卡在等待。"""
    service.register_robot("r1", [])
    service.submit_mission("a", max_attempts=1)
    service.submit_mission("b", dependencies=["a"])
    service.acquire_mission("r1", "a")
    clock.advance(100)  # 心跳超时 + 租约过期
    reclaimed = service.sweep()
    a = service.get_mission("a")
    b = service.get_mission("b")
    case_log("回收用尽", input="a max_attempts=1, 过期 sweep; b 依赖 a",
             expect="a=FAILED 且 reclaimed=[a]；b=BLOCKED(dep_failed, blocked_by=a)",
             actual=f"reclaimed={reclaimed} a={a.status} "
                    f"b=({b.status},{b.blocked_by},{b.blocked_reason})")
    assert reclaimed == ["a"]
    assert a.status == MissionStatus.FAILED
    assert b.status == MissionStatus.BLOCKED
    assert b.blocked_by == "a" and b.blocked_reason == "dep_failed"
    # 重启后阻断结论不变
    svc2 = OrchestratorService(store, clock=clock)
    assert svc2.get_mission("b").status == MissionStatus.BLOCKED
