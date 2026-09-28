"""积压上限统一配额：等待依赖、被前置拖住、重试回待分配、抢占回待分配
都占用同一份 max_backlog 配额，所有准入/放开路径守同一条线。"""

import os
import threading

import pytest

from fleet_orchestrator.errors import (
    BacklogFull,
    RetriesExhausted,
)
from fleet_orchestrator.models import MissionStatus
from fleet_orchestrator.service import OrchestratorService


def _run_to_success(svc, robot_id: str, mission_id: str) -> None:
    _, lease = svc.acquire_mission(robot_id, mission_id)
    svc.start_mission(robot_id, mission_id, lease.lease_id)
    svc.complete_mission(robot_id, mission_id, lease.lease_id)


def _counts(svc) -> dict[str, int]:
    return {
        s.value: len(svc.list_missions(s))
        for s in MissionStatus
        if len(svc.list_missions(s))
    }


# ---- 上限很小时，大量等待前置的任务被挡住 ----

def test_waiting_deps_tasks_share_backlog_quota(store, clock, small_config, case_log):
    cap = 3
    svc = OrchestratorService(store, config=small_config(max_backlog=cap), clock=clock)
    accepted, rejected = [], []
    for i in range(20):
        mid = f"w{i}"
        try:
            svc.submit_mission(mid, depends_on=["ghost"])
            accepted.append(mid)
        except BacklogFull as exc:
            rejected.append((mid, exc.reason))
    case_log(
        "小上限 + 等待未出现前置",
        input=f"max_backlog={cap}, 连续下发 20 个 depends_on=['ghost'] 的任务",
        expect="仅 3 个进入 WAITING_DEPS，其余 backlog_full(429) 拒绝",
        actual=f"accepted={accepted} rejected={len(rejected)} counts={_counts(svc)}",
    )
    assert len(accepted) == cap
    assert all(
        svc.get_mission(m).status == MissionStatus.WAITING_DEPS for m in accepted
    )
    assert len(rejected) == 20 - cap
    assert all(reason == "backlog_full" for _, reason in rejected)
    assert svc._backlog_size() == cap
    # 拒绝不留半截记录、不牵连别的任务
    ids = {m.mission_id for m in svc.list_missions()}
    assert ids == set(accepted)
    assert "w3" not in ids


def test_blocked_dependent_of_cancelled_dep_shares_quota(
    store, clock, small_config, case_log
):
    """依赖一个已经取消、永远不会完成的前置：提交时直接 BLOCKED，同样占配额。"""
    cap = 3
    svc = OrchestratorService(store, config=small_config(max_backlog=cap), clock=clock)
    svc.submit_mission("dead")
    svc.cancel_mission("dead")  # 终态，释放槽位
    accepted, rejected = 0, 0
    for i in range(20):
        try:
            svc.submit_mission(f"b{i}", depends_on=["dead"])
            accepted += 1
        except BacklogFull:
            rejected += 1
    case_log(
        "小上限 + 前置已取消",
        input="dead CANCELLED 后连续下发 20 个 depends_on=['dead']",
        expect="3 个 BLOCKED(占槽)，17 个 backlog_full 拒绝，等待池不增长",
        actual=f"accepted={accepted} rejected={rejected} counts={_counts(svc)}",
    )
    assert accepted == cap
    assert rejected == 17
    assert all(
        svc.get_mission(f"b{i}").status == MissionStatus.BLOCKED
        and svc.get_mission(f"b{i}").blocked_by == "dead"
        for i in range(cap)
    )
    assert svc._backlog_size() == cap


def test_block_propagation_after_cap_full_does_not_grow(
    store, clock, small_config, case_log
):
    """前置失败后被拖住的任务只是 WAITING_DEPS -> BLOCKED 同槽换状态，
    不会新占槽位，也不产生新的可领取任务；此后新提交仍被挡住。"""
    svc = OrchestratorService(store, config=small_config(max_backlog=3), clock=clock)
    svc.register_robot("r1", [])
    svc.submit_mission("a", max_attempts=1)
    svc.submit_mission("b", depends_on=["a"])
    svc.submit_mission("c", depends_on=["b"])  # a+b+c 共 3 槽
    _, lease = svc.acquire_mission("r1", "a")  # a 执行释放槽，但 b/c 仍占 2 槽
    svc.submit_mission("filler")               # 再填满 3 槽
    svc.start_mission("r1", "a", lease.lease_id)
    svc.fail_mission("r1", "a", lease.lease_id, error="boom")
    case_log(
        "满队时前置失败触发阻断传播",
        input="a FAILED(不可重试), b 依赖 a, c 依赖 b, filler 占着槽",
        expect="b、c 同槽换状态为 BLOCKED，计数不变；无新可领取任务",
        actual=f"b={svc.get_mission('b').status} c={svc.get_mission('c').status} "
        f"backlog={svc._backlog_size()} counts={_counts(svc)}",
    )
    assert svc.get_mission("b").status == MissionStatus.BLOCKED
    assert svc.get_mission("c").status == MissionStatus.BLOCKED
    assert svc._backlog_size() == 3
    with pytest.raises(BacklogFull):
        svc.submit_mission("more")
    # filler 仍是唯一可领取的，阻断任务不会混进调度集合
    auto, _ = svc.acquire_mission("r1", "filler")
    assert auto.mission_id == "filler"


# ---- 重试回到待分配守上限 ----

def test_retry_back_to_pending_guarded(store, clock, small_config, case_log):
    cap = 2
    svc = OrchestratorService(store, config=small_config(max_backlog=cap), clock=clock)
    svc.register_robot("r1", [])
    svc.register_robot("r2", [])
    svc.submit_mission("f", max_attempts=2)
    _, lease = svc.acquire_mission("r1", "f")  # f 执行，释放槽位
    svc.submit_mission("p1")
    svc.submit_mission("p2")  # 待分配队列被 p1/p2 填满
    svc.start_mission("r1", "f", lease.lease_id)
    svc.fail_mission("r1", "f", lease.lease_id, error="boom")  # 终态不占槽
    with pytest.raises(BacklogFull) as exc:
        svc.retry_mission("f")
    case_log(
        "失败任务重试回待分配",
        input=f"max_backlog={cap}, p1/p2 占满，f FAILED 尝试 retry",
        expect="backlog_full 拒绝，f 保持 FAILED 无半更新",
        actual=exc.value.reason + f", f={svc.get_mission('f').status}",
    )
    assert svc.get_mission("f").status == MissionStatus.FAILED
    # 腾出一个槽位后重试成功
    svc.acquire_mission("r2", "p1")
    m = svc.retry_mission("f")
    case_log("释放槽位后重试", expect="f 回到 PENDING", actual=f"f={m.status}")
    assert m.status == MissionStatus.PENDING
    assert svc._backlog_size() == cap


def test_retry_attempts_exhausted_reason(store, clock, small_config, case_log):
    """次数用尽时拒绝原因为 retries_exhausted，且 attempts 不增加、状态不变。"""
    svc = OrchestratorService(store, config=small_config(max_backlog=1), clock=clock)
    svc.register_robot("r1", [])
    svc.submit_mission("g", max_attempts=1)
    _, lease = svc.acquire_mission("r1", "g")
    svc.start_mission("r1", "g", lease.lease_id)
    svc.fail_mission("r1", "g", lease.lease_id)
    svc.submit_mission("holder")  # 唯一槽位被占，容量同样不足
    with pytest.raises(RetriesExhausted):
        svc.retry_mission("g")  # 次数用尽优先报可区分的原因
    case_log(
        "重试次数用尽",
        input="g attempts=max_attempts=1（即使容量也不足）",
        expect="retries_exhausted，g 仍 FAILED",
        actual=f"g={svc.get_mission('g').status} attempts={svc.get_mission('g').attempts}",
    )
    assert svc.get_mission("g").status == MissionStatus.FAILED
    assert svc.get_mission("g").attempts == 1


# ---- 抢占回待分配守上限 ----

def test_preempt_guarded_when_backlog_full(store, clock, small_config, case_log):
    cap = 2
    svc = OrchestratorService(store, config=small_config(max_backlog=cap), clock=clock)
    svc.register_robot("r1", [])
    svc.register_robot("r2", [])
    svc.submit_mission("run", priority=1)
    _, lease = svc.acquire_mission("r1", "run")  # run 执行释放槽
    svc.submit_mission("q1")
    svc.submit_mission("q2")  # 填满
    svc.start_mission("r1", "run", lease.lease_id)
    with pytest.raises(BacklogFull) as exc:
        svc.preempt_mission("run")
    case_log(
        "满队时抢占执行中的任务",
        input="run RUNNING, q1/q2 占满待分配",
        expect="backlog_full 拒绝，run 保持 RUNNING、租约仍在",
        actual=exc.value.reason
        + f", run={svc.get_mission('run').status} lease={svc.get_lease('run') is not None}",
    )
    assert svc.get_mission("run").status == MissionStatus.RUNNING
    assert svc.get_lease("run") is not None
    svc.acquire_mission("r2", "q1")  # 腾槽
    m = svc.preempt_mission("run")
    assert m.status == MissionStatus.PREEMPTED
    assert svc._backlog_size() == cap


# ---- 终态释放槽位（含等待/阻断任务） ----

def test_cancel_waiting_or_blocked_frees_slot(store, clock, small_config, case_log):
    svc = OrchestratorService(store, config=small_config(max_backlog=2), clock=clock)
    svc.submit_mission("dead")
    svc.submit_mission("b", depends_on=["dead"])  # dead+b 占 2 槽
    svc.cancel_mission("dead")  # dead 终态释放槽，b 转为 BLOCKED 仍占 1 槽
    case_log(
        "前置取消后被拖住任务仍占槽",
        input="dead 已取消, b 依赖 dead",
        expect="b=BLOCKED 占 1 槽，可再放 1 个",
        actual=f"b={svc.get_mission('b').status} backlog={svc._backlog_size()}",
    )
    svc.submit_mission("w", depends_on=["ghost"])  # 占满 2 槽
    with pytest.raises(BacklogFull):
        svc.submit_mission("x")
    svc.cancel_mission("w")  # 取消等待中的任务同样释放槽位
    m, created = svc.submit_mission("x")
    case_log("取消等待任务后再提交", expect="x created=True PENDING",
             actual=f"created={created} x={m.status}")
    assert created is True
    assert m.status == MissionStatus.PENDING
    # 取消 BLOCKED 任务也释放槽位
    svc.cancel_mission("b")
    m2, c2 = svc.submit_mission("y")
    case_log("取消阻断任务后再提交", expect="y created=True",
             actual=f"created={c2} y={m2.status}")
    assert c2 is True


# ---- 放开与领取并发竞争 ----

def test_concurrent_batch_unblock_and_acquire(store, clock, small_config, case_log):
    """前置完成一次性放开一批任务，多台设备同时抢：
    解阻在触发操作的锁内完成，随后领取各自串行化，
    不出现瞬时超限、重复分配；每个任务恰好一个赢家，租约唯一。"""
    import time as _real_time

    n_wait = 6
    cap = n_wait + 1
    svc = OrchestratorService(store, config=small_config(max_backlog=cap), clock=clock)
    svc.register_robot("r0", [])
    for i in range(1, n_wait + 1):
        svc.register_robot(f"r{i}", [])
    svc.submit_mission("dep")
    for i in range(n_wait):
        svc.submit_mission(f"w{i}", depends_on=["dep"])
    _, lease = svc.acquire_mission("r0", "dep")
    svc.start_mission("r0", "dep", lease.lease_id)
    # 解阻前没有任何可领取任务：等待者全是 WAITING_DEPS
    assert svc.list_missions(MissionStatus.PENDING) == []

    completed = threading.Event()
    winners: list[tuple[str, str]] = []  # (robot, mission)
    winners_lock = threading.Lock()
    barrier = threading.Barrier(n_wait + 1)

    def completer() -> None:
        barrier.wait()
        svc.complete_mission("r0", "dep", lease.lease_id)
        completed.set()

    def acquirer(rid: str) -> None:
        barrier.wait()
        # 解阻前队列为空属正常，必须重试到 completer 完成之后才允许按
        # “空队列 + 本机器人空闲”退出（否则会在解阻窗口前提前离场）。
        # 用真实墙上时间做截止（注入时钟是冻结的），重试间让出 CPU 避免
        # GIL 忙等饿死慢线程。
        deadline = _real_time.monotonic() + 10.0
        while _real_time.monotonic() < deadline:
            try:
                m, _got = svc.acquire_mission(rid)
            except BacklogFull:
                raise AssertionError("领取路径不应触发 backlog_full")
            except Exception as exc:  # noqa: BLE001 - 并发下只允许这些拒绝
                assert getattr(exc, "reason", "") == "mission_not_assignable", exc
                if not completed.is_set():
                    _real_time.sleep(0)
                    continue  # 解阻尚未发生
                robot = svc.get_robot(rid)
                has_acquirable = any(
                    x.status
                    in (MissionStatus.PENDING, MissionStatus.PREEMPTED)
                    for x in svc.list_missions()
                )
                if robot.current_mission_id is None and not has_acquirable:
                    return  # 自己没抢到且已无任务可领
                _real_time.sleep(0)
                continue
            assert completed.is_set(), "解阻前不得领取成功"
            with winners_lock:
                winners.append((rid, m.mission_id))
            return
        raise AssertionError(f"{rid} 超过截止时间仍未收敛")  # pragma: no cover

    threads = [threading.Thread(target=completer)]
    threads += [
        threading.Thread(target=acquirer, args=(f"r{i}",)) for i in range(1, n_wait + 1)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30)
        assert not t.is_alive(), "并发线程未收敛"

    won_missions = [m for _, m in winners]
    won_robots = {r for r, _ in winners}
    case_log(
        "批量解阻 ‖ 多设备领取",
        input=f"complete(dep) 放开 {n_wait} 个任务 ‖ {n_wait} 台设备抢",
        expect=f"恰好 {n_wait} 个赢家、任务/机器人均不重复，租约唯一，计数=0",
        actual=f"winners={sorted(winners)} backlog={svc._backlog_size()}",
    )
    assert len(winners) == n_wait
    assert len(won_missions) == len(set(won_missions)) == n_wait
    assert len(won_robots) == n_wait
    assert set(won_missions) == {f"w{i}" for i in range(n_wait)}
    # 每个被领取的任务有且仅有一个租约，持有者视图一致
    for mid in won_missions:
        lease_now = svc.get_lease(mid)
        assert lease_now is not None
        assert svc.get_robot(lease_now.robot_id).current_mission_id == mid
    assert svc._backlog_size() == 0
    assert len(svc.list_missions(MissionStatus.ASSIGNED)) == n_wait


def test_concurrent_submit_during_unblock(store, clock, small_config, case_log):
    """前置完成放开等待者的同时，其它线程在下发新任务：配额是单一共享资源，
    任何交错下计数都不得超过上限。dep 完成只释放一个槽位，
    因此 20 次下发至多成功 1 次。"""
    cap = 4
    svc = OrchestratorService(store, config=small_config(max_backlog=cap), clock=clock)
    svc.register_robot("r0", [])
    svc.submit_mission("dep")
    svc.submit_mission("w1", depends_on=["dep"])
    svc.submit_mission("w2", depends_on=["dep"])
    svc.submit_mission("q")  # 4 槽占满
    _, lease = svc.acquire_mission("r0", "dep")  # dep 执行，释放 1 槽 -> 3
    svc.start_mission("r0", "dep", lease.lease_id)

    accepted: list[str] = []
    rejected = 0
    counter_lock = threading.Lock()
    barrier = threading.Barrier(2)

    def completer() -> None:
        barrier.wait()
        svc.complete_mission("r0", "dep", lease.lease_id)  # 再释放 1 槽

    def submitter() -> None:
        nonlocal rejected
        barrier.wait()
        for i in range(20):
            mid = f"s{i}"
            try:
                svc.submit_mission(mid)
            except BacklogFull:
                with counter_lock:
                    rejected += 1
            else:
                with counter_lock:
                    accepted.append(mid)
                # 任何成功提交后立即校验不变量（此时计数已含新任务）
                assert svc._backlog_size() <= cap

    t1 = threading.Thread(target=completer)
    t2 = threading.Thread(target=submitter)
    for t in (t1, t2):
        t.start()
    for t in (t1, t2):
        t.join(timeout=30)
        assert not t.is_alive(), "并发线程未收敛"

    backlog = svc._backlog_size()
    case_log(
        "解阻放开 ‖ 新任务下发",
        input="complete(dep) ‖ 20 次 submit（共享 4 槽，仅 dep 终态释放 1 槽）",
        expect="至多 1 次提交成功，最终计数 <= 4，w1/w2/q 全部 PENDING",
        actual=f"accepted={accepted} rejected={rejected} backlog={backlog} "
        f"counts={_counts(svc)}",
    )
    assert backlog <= cap
    assert len(accepted) <= 1
    assert backlog == 3 + len(accepted)  # w1/w2/q + 可能的 1 个新任务
    assert svc.get_mission("w1").status == MissionStatus.PENDING
    assert svc.get_mission("w2").status == MissionStatus.PENDING
    assert svc.get_mission("q").status == MissionStatus.PENDING


# ---- 重启 / 重放后配额口径与依赖状态一致 ----

def test_restart_and_replay_preserve_quota_state(store, clock, small_config, case_log):
    """上限很小的场景下重启：WAITING_DEPS/BLOCKED/PENDING 的占用关系不变，
    不会凭空多出可领取任务；事件重放与快照一致；重启后仍守同一上限。"""
    cap = 4
    svc = OrchestratorService(store, config=small_config(max_backlog=cap), clock=clock)
    svc.register_robot("r1", [])
    svc.submit_mission("done")
    svc.submit_mission("unblocked", depends_on=["done"])
    svc.submit_mission("waiting", depends_on=["ghost"])
    svc.submit_mission("dead")  # 占满 4 槽（done/unblocked/waiting/dead）
    with pytest.raises(BacklogFull):
        svc.submit_mission("blocked", depends_on=["dead"])  # 满队先被挡住
    _run_to_success(svc, "r1", "done")  # done 终态释放 1 槽；unblocked->PENDING
    svc.submit_mission("blocked", depends_on=["dead"])  # 现在恰好 4 槽（满）
    with pytest.raises(BacklogFull):
        svc.submit_mission("after_dead")  # blocked 此刻 WAITING_DEPS 也占槽
    svc.cancel_mission("dead")  # dead 终态；blocked -> BLOCKED 同槽换状态，计数 3
    before = {
        m.mission_id: (m.status, tuple(m.depends_on), m.blocked_by)
        for m in svc.list_missions()
    }
    before_backlog = svc._backlog_size()

    svc2 = OrchestratorService(
        store, config=small_config(max_backlog=cap), clock=clock
    )  # 模拟重启（读快照）
    after = {
        m.mission_id: (m.status, tuple(m.depends_on), m.blocked_by)
        for m in svc2.list_missions()
    }
    case_log(
        "重启后配额状态",
        input=f"max_backlog={cap}; 1 PENDING + 1 WAITING + 1 BLOCKED + 2 终态",
        expect="状态/依赖/阻断来源与故障前一致，计数一致，无凭空可领取任务",
        actual=f"before={before}\nafter ={after} backlog={before_backlog}/{svc2._backlog_size()}",
    )
    assert before == after
    assert before_backlog == 3 and svc2._backlog_size() == 3
    assert after["unblocked"][0] == MissionStatus.PENDING
    assert after["waiting"][0] == MissionStatus.WAITING_DEPS
    assert after["blocked"][0] == MissionStatus.BLOCKED
    assert after["blocked"][2] == "dead"
    # 重启后计数 3（cap=4）：可再放 1 个，第二个必须被挡
    m0, c0 = svc2.submit_mission("slot_filler")
    assert c0 is True and m0.status == MissionStatus.PENDING
    assert svc2._backlog_size() == cap
    with pytest.raises(BacklogFull):
        svc2.submit_mission("after_restart_blocked_full")
    # 取消 BLOCKED 释放槽后才放行
    svc2.cancel_mission("blocked")
    m, created = svc2.submit_mission("after_restart")
    assert created is True and m.status == MissionStatus.PENDING

    # 事件重放折叠结果必须与快照一致
    assert store.replay() == store.load()

    # 从事件日志（无快照）再重建一个实例，状态仍一致
    os.remove(store.dir / "state.json")
    svc3 = OrchestratorService(
        store, config=small_config(max_backlog=cap), clock=clock
    )
    expected_replay = dict(after)
    # blocked 被取消后记录仍在（CANCELLED 终态，保留 blocked_by 历史），只是不再占槽
    expected_replay["blocked"] = (MissionStatus.CANCELLED, ("dead",), "dead")
    expected_replay["slot_filler"] = (MissionStatus.PENDING, (), None)
    expected_replay["after_restart"] = (MissionStatus.PENDING, (), None)
    replay_view = {
        m.mission_id: (m.status, tuple(m.depends_on), m.blocked_by)
        for m in svc3.list_missions()
    }
    case_log(
        "仅事件重放",
        expect="与快照恢复一致；blocked 为 CANCELLED，waiting 仍 WAITING",
        actual=f"view={replay_view}",
    )
    assert replay_view == expected_replay
    assert svc3.get_mission("waiting").status == MissionStatus.WAITING_DEPS
    assert svc3.get_mission("blocked").status == MissionStatus.CANCELLED
    # unblocked/waiting/slot_filler/after_restart 占 4 槽；blocked 已不占槽
    assert svc3._backlog_size() == 4


# ---- 租约回收：唯一允许的瞬时超限，必须立即自愈 ----

def test_reclaim_overshoot_is_transient_and_self_heals(
    store, clock, small_config, case_log
):
    """租约持有者失活时，回收无法拒绝也无处排队（失败态会永久丢失可重试任务），
    是唯一允许越过上限的强制安全路径：回收后可能瞬时 backlog = cap+1，
    但领取一个任务即自愈，且此期间新提交仍被上限挡住。"""
    cap = 2
    svc = OrchestratorService(store, config=small_config(max_backlog=cap), clock=clock)
    svc.register_robot("r1", [])
    svc.submit_mission("run", max_attempts=3)
    _, lease = svc.acquire_mission("r1", "run")
    svc.submit_mission("q1")
    svc.submit_mission("q2")  # 执行 run + 队列 q1/q2
    clock.advance(10_000)      # 心跳与租约全部过期
    reclaimed = svc.sweep()
    case_log(
        "满队时回收过期租约",
        input="run 过期且次数未用尽, q1/q2 占满 cap=2",
        expect="run 强制回 PENDING，瞬时 backlog=3，无任务丢失",
        actual=f"reclaimed={reclaimed} run={svc.get_mission('run').status} "
        f"backlog={svc._backlog_size()}",
    )
    assert reclaimed == ["run"]
    assert svc.get_mission("run").status == MissionStatus.PENDING
    assert svc._backlog_size() == cap + 1  # 唯一允许的瞬时超限
    # 瞬时超限期间新提交仍被挡住，不会继续恶化
    with pytest.raises(BacklogFull):
        svc.submit_mission("x")
    # 领取一个任务立即自愈回 cap
    svc.register_robot("r2", [])
    m, _ = svc.acquire_mission("r2")
    case_log("领走一个任务", expect="计数自愈回 2",
             actual=f"acquired={m.mission_id} backlog={svc._backlog_size()}")
    assert svc._backlog_size() == cap
    with pytest.raises(BacklogFull):
        svc.submit_mission("y")
