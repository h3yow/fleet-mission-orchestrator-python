"""积压上限对依赖等待池的约束：

- 等待前置（WAITING_DEPS）与被前置拖住（BLOCKED）的任务占用同一份
  max_backlog 配额，超限按 backlog_full 拒绝，等待池不能无界堆积；
- 前置完成批量解阻、阻断恢复、失败重试回到待分配，都守同一条线；
- 解阻与多设备领取并发竞争时不超限、不重复分配、状态不自相矛盾；
- 重启/重放后等待与阻断状态一致，不凭空多出可领取任务。
"""

import threading

import pytest

from fleet_orchestrator.errors import BacklogFull, Rejection
from fleet_orchestrator.models import MissionStatus
from fleet_orchestrator.service import OrchestratorService


def _run_to_success(svc, robot_id: str, mission_id: str) -> None:
    _, lease = svc.acquire_mission(robot_id, mission_id)
    svc.start_mission(robot_id, mission_id, lease.lease_id)
    svc.complete_mission(robot_id, mission_id, lease.lease_id)


def _backlog(svc) -> int:
    """与服务等价的配额占用口径：未终态且未持有租约的任务数。"""
    return sum(
        1
        for m in svc.list_missions()
        if m.status
        in (
            MissionStatus.PENDING,
            MissionStatus.PREEMPTED,
            MissionStatus.WAITING_DEPS,
            MissionStatus.BLOCKED,
        )
    )


# ---- 上限很小时，大量"等待前置"的下发被挡住 ----

def test_waiting_on_missing_dep_bounded_by_cap(store, clock, small_config, case_log):
    svc = OrchestratorService(store, config=small_config(max_backlog=3), clock=clock)
    accepted, rejected = [], []
    for i in range(20):
        try:
            svc.submit_mission(f"w{i}", depends_on=["ghost"])  # 前置不存在
            accepted.append(f"w{i}")
        except BacklogFull as e:
            rejected.append((f"w{i}", e.reason))
    case_log("20 个等待缺失前置的下发", input="max_backlog=3, 20x depends_on ghost",
             expect="仅前 3 个被接收，其余 backlog_full",
             actual=f"accepted={len(accepted)} rejected={len(rejected)}")
    assert len(accepted) == 3
    assert len(rejected) == 17
    assert all(reason == "backlog_full" for _, reason in rejected)
    # 任务总数不超限，被拒绝的下发不留半截记录
    missions = svc.list_missions()
    case_log("判定依据", expect="任务数=3 且全部 WAITING_DEPS",
             actual=f"total={len(missions)} statuses={sorted({m.status for m in missions})}")
    assert len(missions) == 3
    assert all(m.status == MissionStatus.WAITING_DEPS for m in missions)
    assert {m.mission_id for m in missions} == set(accepted)
    # 既有任务不被牵连：取消一个释放槽位后，新的下发立即可入队
    svc.cancel_mission(accepted[0])
    svc.submit_mission("w-new", depends_on=["ghost"])
    assert svc.get_mission("w-new").status == MissionStatus.WAITING_DEPS


def test_waiting_on_never_completing_dep_bounded(store, clock, small_config, case_log):
    """依赖一个已取消、永远不会完成的前置：同样被配额挡住。"""
    svc = OrchestratorService(store, config=small_config(max_backlog=2), clock=clock)
    svc.submit_mission("dead")
    svc.cancel_mission("dead")  # 前置已取消，依赖它的任务直接 BLOCKED
    accepted, rejected = 0, 0
    for i in range(10):
        try:
            svc.submit_mission(f"b{i}", depends_on=["dead"])
            accepted += 1
        except BacklogFull:
            rejected += 1
    statuses = sorted({svc.get_mission(f"b{i}").status for i in range(accepted)})
    case_log("依赖已取消前置", input="max_backlog=2, 10x depends_on dead(CANCELLED)",
             expect="仅 2 个被接收并 BLOCKED，其余 backlog_full",
             actual=f"accepted={accepted} rejected={rejected} statuses={statuses}")
    assert accepted == 2 and rejected == 8
    assert statuses == [MissionStatus.BLOCKED]
    assert _backlog(svc) == 2  # 阻断池同样占满配额，不再增长


# ---- 前置失败/取消后，被拖住的任务不再无限增长 ----

def test_blocked_pool_stops_growing_after_dep_failure(
    store, clock, small_config, case_log
):
    svc = OrchestratorService(store, config=small_config(max_backlog=3), clock=clock)
    svc.register_robot("r1", [])
    svc.submit_mission("a", max_attempts=1)
    svc.submit_mission("b1", depends_on=["a"])
    svc.submit_mission("b2", depends_on=["a"])  # 配额 3 已满
    _, lease = svc.acquire_mission("r1", "a")   # a 离队列，配额释放 1
    svc.start_mission("r1", "a", lease.lease_id)
    svc.fail_mission("r1", "a", lease.lease_id, error="boom")  # b1/b2 -> BLOCKED
    b1, b2 = svc.get_mission("b1"), svc.get_mission("b2")
    case_log("前置失败传播阻断", expect="b1/b2 BLOCKED 且仍占配额",
             actual=f"b1={b1.status} b2={b2.status} backlog={_backlog(svc)}")
    assert b1.status == MissionStatus.BLOCKED
    assert b2.status == MissionStatus.BLOCKED
    assert _backlog(svc) == 2
    # 被拖住的任务占着配额：新下发只能容纳 1 个，之后继续拒绝
    svc.submit_mission("c1")
    with pytest.raises(BacklogFull) as exc:
        svc.submit_mission("c2", depends_on=["a"])
    case_log("阻断池占住配额", input="backlog=3(b1,b2,c1)",
             expect="c2 拒绝 backlog_full，阻断池不无限增长",
             actual=exc.value.reason)
    assert exc.value.reason == "backlog_full"
    assert len(svc.list_missions()) == 4  # a + b1 + b2 + c1，总量受控


# ---- 批量解阻与重试回到待分配仍守上限 ----

def test_batch_unblock_never_exceeds_cap(store, clock, small_config, case_log):
    svc = OrchestratorService(store, config=small_config(max_backlog=3), clock=clock)
    svc.register_robot("r1", [])
    svc.submit_mission("dep")
    for i in range(2):
        svc.submit_mission(f"w{i}", depends_on=["dep"])  # 配额 3 已满
    _run_to_success(svc, "r1", "dep")  # 两个等待任务同时具备执行条件
    statuses = [svc.get_mission(f"w{i}").status for i in range(2)]
    case_log("一次性放开一批", input="dep 完成, w0/w1 同时解阻",
             expect="全部晋升 PENDING 且积压 <= 3（晋升是配额中性迁移）",
             actual=f"statuses={statuses} backlog={_backlog(svc)}")
    assert statuses == [MissionStatus.PENDING, MissionStatus.PENDING]
    assert _backlog(svc) == 2 <= 3
    # 晋升不腾出配额：新下发依旧按上限把关
    svc.submit_mission("x")  # 占满第 3 槽
    with pytest.raises(BacklogFull):
        svc.submit_mission("y")
    case_log("解阻后配额口径不变", expect="第 4 个下发仍被 backlog_full 拒绝",
             actual="rejected")


def test_retry_back_to_pending_respects_cap(store, clock, small_config, case_log):
    svc = OrchestratorService(store, config=small_config(max_backlog=2), clock=clock)
    svc.register_robot("r1", [])
    svc.submit_mission("flaky", max_attempts=2)
    _, lease = svc.acquire_mission("r1", "flaky")  # 配额释放
    svc.start_mission("r1", "flaky", lease.lease_id)
    svc.submit_mission("f1")
    svc.submit_mission("f2")  # 配额被 f1/f2 占满
    svc.fail_mission("r1", "flaky", lease.lease_id, error="boom")  # FAILED 终态不占配额
    with pytest.raises(BacklogFull) as exc:
        svc.retry_mission("flaky")
    case_log("配额满时重试", input="backlog=2(f1,f2), retry(flaky)",
             expect="拒绝 backlog_full，任务保持 FAILED 不留半截状态",
             actual=f"{exc.value.reason} status={svc.get_mission('flaky').status}")
    assert exc.value.reason == "backlog_full"
    assert svc.get_mission("flaky").status == MissionStatus.FAILED
    # 释放槽位后重试放行，且只占用一个槽位
    svc.acquire_mission("r1", "f1")
    svc.retry_mission("flaky")
    case_log("槽位释放后重试", expect="flaky 回到 PENDING，积压 <= 2",
             actual=f"status={svc.get_mission('flaky').status} backlog={_backlog(svc)}")
    assert svc.get_mission("flaky").status == MissionStatus.PENDING
    assert _backlog(svc) == 2


def test_blocked_recovery_then_unblock_respects_cap(
    store, clock, small_config, case_log
):
    """被拖住的任务因前置恢复重新具备执行条件：全程配额口径不变。"""
    svc = OrchestratorService(store, config=small_config(max_backlog=2), clock=clock)
    svc.register_robot("r1", [])
    svc.submit_mission("a", max_attempts=2)
    svc.submit_mission("b", depends_on=["a"])  # 配额 2 已满
    _, lease = svc.acquire_mission("r1", "a")
    svc.start_mission("r1", "a", lease.lease_id)
    svc.fail_mission("r1", "a", lease.lease_id, error="boom")  # b -> BLOCKED
    assert svc.get_mission("b").status == MissionStatus.BLOCKED
    svc.retry_mission("a")  # a 回 PENDING，b 恢复 WAITING_DEPS（均配额中性/占槽）
    b = svc.get_mission("b")
    case_log("前置重试恢复", expect="b BLOCKED->WAITING_DEPS，积压仍 <= 2",
             actual=f"b={b.status} backlog={_backlog(svc)}")
    assert b.status == MissionStatus.WAITING_DEPS
    assert _backlog(svc) == 2
    with pytest.raises(BacklogFull):
        svc.submit_mission("intruder")
    _run_to_success(svc, "r1", "a")  # b 解阻晋升 PENDING
    case_log("前置最终成功", expect="b 晋升 PENDING，积压仍 <= 2",
             actual=f"b={svc.get_mission('b').status} backlog={_backlog(svc)}")
    assert svc.get_mission("b").status == MissionStatus.PENDING
    assert _backlog(svc) == 1  # a 终态退出配额，只剩 b


# ---- 解阻与多设备领取并发竞争 ----

def test_concurrent_unblock_acquire_and_submit_under_cap(
    store, clock, small_config, case_log
):
    """前置完成放开一批任务时，多设备并发领取 + 并发新下发：
    不出现瞬时超限、重复分配或半截记录。"""
    cap = 3
    svc = OrchestratorService(store, config=small_config(max_backlog=cap), clock=clock)
    for i in range(4):
        svc.register_robot(f"r{i}", [])
    svc.submit_mission("dep")
    svc.submit_mission("w0", depends_on=["dep"])
    svc.submit_mission("w1", depends_on=["dep"])  # 配额 3 已满
    _, lease = svc.acquire_mission("r0", "dep")
    svc.start_mission("r0", "dep", lease.lease_id)

    barrier = threading.Barrier(5)
    acquired: list[tuple[str, str]] = []
    submit_rejected: list[str] = []
    lock = threading.Lock()

    def completer() -> None:
        barrier.wait()
        svc.complete_mission("r0", "dep", lease.lease_id)  # 放开 w0/w1

    def acquirer(rid: str) -> None:
        barrier.wait()
        while True:
            try:
                m, _ = svc.acquire_mission(rid)
                with lock:
                    acquired.append((rid, m.mission_id))
                return
            except Rejection as e:
                if e.reason != "mission_not_assignable":
                    raise
                if svc.get_mission("dep").status == MissionStatus.SUCCEEDED and all(
                    svc.get_mission(w).status != MissionStatus.PENDING
                    for w in ("w0", "w1")
                ):
                    return  # 解阻完成且都已被领走

    def submitter() -> None:
        barrier.wait()
        for i in range(8):
            try:
                svc.submit_mission(f"s{i}")
            except BacklogFull:
                with lock:
                    submit_rejected.append(f"s{i}")

    threads = [threading.Thread(target=completer)]
    threads += [threading.Thread(target=acquirer, args=(f"r{i}",)) for i in range(1, 4)]
    threads.append(threading.Thread(target=submitter))
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    got_missions = sorted(mid for _, mid in acquired)
    case_log("解阻‖领取‖下发并发", input="complete(dep) ‖ 3 acquirer ‖ 8 submit",
             expect="每个任务至多被领一次，积压 <= 3，无重复分配",
             actual=f"acquired={got_missions} rejected_submits={len(submit_rejected)}"
                    f" backlog={_backlog(svc)}")
    # 无重复分配：每个 mission_id 至多出现一次
    assert len(got_missions) == len(set(got_missions))
    # w0/w1 解阻后各被领取恰好一次
    assert got_missions.count("w0") == 1
    assert got_missions.count("w1") == 1
    # 全程不超上限，状态不自相矛盾：被领走的任务都有租约且机器人指向它
    assert _backlog(svc) <= cap
    for _, mid in acquired:
        lease = svc.get_lease(mid)
        assert lease is not None
        assert svc.get_robot(lease.robot_id).current_mission_id == mid
    # 被拒绝的下发不留记录
    ids = {m.mission_id for m in svc.list_missions()}
    for rid in submit_rejected:
        assert rid not in ids


# ---- 重启/重放后等待池状态一致，上限继续生效 ----

def test_restart_replay_preserves_waiting_pool_and_cap(
    store, clock, small_config, case_log
):
    svc = OrchestratorService(store, config=small_config(max_backlog=4), clock=clock)
    svc.register_robot("r1", [])
    svc.submit_mission("done")
    svc.submit_mission("waiting", depends_on=["todo"])
    svc.submit_mission("blocked", depends_on=["dead"])
    svc.submit_mission("dead")          # 配额 4 恰好占满
    svc.cancel_mission("dead")          # blocked -> BLOCKED，dead 终态释放 1 槽
    _run_to_success(svc, "r1", "done")  # done 终态退出配额
    before = {
        m.mission_id: (m.status, tuple(m.depends_on), m.blocked_by)
        for m in svc.list_missions()
    }
    backlog_before = _backlog(svc)

    svc2 = OrchestratorService(store, config=small_config(max_backlog=4), clock=clock)
    after = {
        m.mission_id: (m.status, tuple(m.depends_on), m.blocked_by)
        for m in svc2.list_missions()
    }
    case_log("重启后等待池", expect="状态/依赖/阻断来源与故障前一致",
             actual=f"before={before} after={after}")
    assert before == after
    assert after["waiting"] == (MissionStatus.WAITING_DEPS, ("todo",), None)
    assert after["blocked"] == (MissionStatus.BLOCKED, ("dead",), "dead")
    # 不凭空多出可领取任务：可领取集合为空（waiting/blocked 均不可领取）
    acquirable = [
        m.mission_id
        for m in svc2.list_missions()
        if m.status in (MissionStatus.PENDING, MissionStatus.PREEMPTED)
    ]
    case_log("可领取集合", expect="空（无凭空可领取）", actual=f"acquirable={acquirable}")
    assert acquirable == []
    # 事件重放与快照一致
    assert store.replay() == store.load()
    # 重启后上限口径不变：积压占用与故障前相同，超限下发仍被拒
    case_log("重启后配额", expect=f"backlog={backlog_before}，占满后第 5 个下发被拒",
             actual=f"backlog={_backlog(svc2)}")
    assert _backlog(svc2) == backlog_before
    svc2.submit_mission("fits1")   # 占用 1 槽
    svc2.submit_mission("fits2")   # 占用最后 1 槽，配额 4 占满
    with pytest.raises(BacklogFull):
        svc2.submit_mission("overflow", depends_on=["todo"])
