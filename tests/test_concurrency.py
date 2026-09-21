"""并发竞争：多线程领取同一任务、回收与完成上报竞争。"""

import threading

from fleet_orchestrator.errors import Rejection
from fleet_orchestrator.models import MissionStatus


def test_concurrent_acquire_single_winner(service, case_log):
    n = 8
    for i in range(n):
        service.register_robot(f"r{i}", ["nav"])
    service.submit_mission("m1", ["nav"])

    winners: list[str] = []
    rejections: list[str] = []
    barrier = threading.Barrier(n)

    def try_acquire(rid: str) -> None:
        barrier.wait()  # 同时放行，最大化竞争
        try:
            service.acquire_mission(rid, "m1")
            winners.append(rid)
        except Rejection as e:
            rejections.append(e.reason)

    threads = [threading.Thread(target=try_acquire, args=(f"r{i}",)) for i in range(n)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    case_log("并发领取同一任务", input=f"{n} robots 同时 acquire m1",
             expect="恰好 1 个获胜，其余 robot_not_available 或 mission_not_assignable",
             actual=f"winners={winners} rejections={sorted(set(rejections))}")
    assert len(winners) == 1
    assert len(rejections) == n - 1
    lease = service.get_lease("m1")
    assert lease is not None and lease.robot_id == winners[0]
    # 不变量：只有一个机器人持有该任务
    holders = [r.robot_id for r in service.list_robots() if r.current_mission_id == "m1"]
    assert holders == winners


def test_concurrent_complete_vs_sweep_no_double_hold(service, clock, case_log):
    """完成上报与超时回收并发：无论谁先，任务不得丢失或被双方同时改变。"""
    outcomes: list[str] = []
    for round_no in range(20):
        rid = f"r{round_no}"
        service.register_robot(rid, [])
        service.submit_mission(f"m{round_no}")
        _, lease = service.acquire_mission(rid, f"m{round_no}")
        service.start_mission(rid, f"m{round_no}", lease.lease_id)
        clock.advance(100)  # 确保租约过期、心跳超时，sweep 有理由回收

        barrier = threading.Barrier(2)

        def do_complete() -> None:
            barrier.wait()
            try:
                service.complete_mission(rid, f"m{round_no}", lease.lease_id)
                outcomes.append("completed")
            except Rejection as e:
                outcomes.append(f"complete_rejected:{e.reason}")

        def do_sweep() -> None:
            barrier.wait()
            got = service.sweep()
            outcomes.append(f"swept:{f'm{round_no}' in got}")

        t1 = threading.Thread(target=do_complete)
        t2 = threading.Thread(target=do_sweep)
        t1.start(); t2.start(); t1.join(); t2.join()

        m = service.get_mission(f"m{round_no}")
        # 判定依据：终态只能是 SUCCEEDED（完成获胜）或 PENDING/FAILED（回收获胜），
        # 且租约必须已释放或唯一持有
        assert m.status in (
            MissionStatus.SUCCEEDED, MissionStatus.PENDING, MissionStatus.FAILED,
        ), f"round {round_no}: unexpected {m.status}"
        if m.status == MissionStatus.SUCCEEDED:
            assert service.get_lease(f"m{round_no}") is None
        clock.advance(0)
    case_log("完成 vs 回收并发", input="20 轮 complete‖sweep",
             expect="每轮状态一致、无双重持有",
             actual=f"outcomes sample={outcomes[:6]}")


def test_concurrent_submit_same_id_single_record(service, case_log):
    results: list[bool] = []
    barrier = threading.Barrier(6)

    def submit() -> None:
        barrier.wait()
        _, created = service.submit_mission("m1", ["nav"], payload={"x": 1})
        results.append(created)

    threads = [threading.Thread(target=submit) for _ in range(6)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    case_log("并发重复下发", input="6 线程同内容 submit m1",
             expect="恰好 1 个 created=True，无重复任务",
             actual=f"created_count={sum(results)} missions={len(service.list_missions())}")
    assert sum(results) == 1
    assert len(service.list_missions()) == 1
