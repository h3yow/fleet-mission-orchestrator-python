"""优先级与抢占：进度保留、不可抢占保护、失败原因可区分。"""

import pytest

from fleet_orchestrator.errors import (
    MissionNotPreemptible,
    PreemptPriorityTooLow,
    PreemptTargetNotRunning,
)
from fleet_orchestrator.models import MissionStatus


def _running(service, robot, mission, **kw):
    service.register_robot(robot, [])
    service.submit_mission(mission, **kw)
    m, lease = service.acquire_mission(robot, mission)
    service.start_mission(robot, mission, lease.lease_id)
    return m, lease


def test_preempt_keeps_progress_and_marks(service, case_log):
    _, lease = _running(service, "r1", "m1", priority=1)
    service.report_progress("r1", "m1", lease.lease_id, {"pct": 70, "checkpoint": "c3"})
    m = service.preempt_mission("m1")
    case_log("抢占保留进度", expect="PREEMPTED 且 progress 保留",
             actual=f"status={m.status} progress={m.progress}")
    assert m.status == MissionStatus.PREEMPTED
    assert m.progress == {"pct": 70, "checkpoint": "c3"}
    assert service.get_lease("m1") is None
    assert service.get_robot("r1").current_mission_id is None

    # 被抢占任务可被重新领取并续执行
    service.register_robot("r2", [])
    m2, lease2 = service.acquire_mission("r2", "m1")
    assert m2.progress["pct"] == 70
    case_log("抢占后重分配续执行", expect="r2 领取且进度仍在",
             actual=f"holder={lease2.robot_id} progress={m2.progress}")


def test_non_preemptible_rejected(service, case_log):
    _running(service, "r1", "m1", preemptible=False)
    with pytest.raises(MissionNotPreemptible) as exc:
        service.preempt_mission("m1")
    case_log("不可抢占任务", expect="mission_not_preemptible", actual=exc.value.reason)
    assert exc.value.reason == "mission_not_preemptible"
    assert service.get_mission("m1").status == MissionStatus.RUNNING  # 未被剥夺


def test_preempt_target_not_running(service, case_log):
    service.submit_mission("m1")
    with pytest.raises(PreemptTargetNotRunning) as exc:
        service.preempt_mission("m1")
    case_log("抢占 PENDING 任务", expect="preempt_target_not_running",
             actual=exc.value.reason)
    assert exc.value.reason == "preempt_target_not_running"


def test_preempt_priority_must_be_higher(service, case_log):
    _running(service, "r1", "low", priority=5)
    service.submit_mission("challenger", priority=5)
    with pytest.raises(PreemptPriorityTooLow) as exc:
        service.preempt_mission("low", by_mission_id="challenger")
    case_log("同优先级抢占", input="challenger=5 vs target=5",
             expect="preempt_priority_too_low", actual=exc.value.reason)
    assert exc.value.reason == "preempt_priority_too_low"

    service.submit_mission("urgent", priority=9)
    m = service.preempt_mission("low", by_mission_id="urgent")
    case_log("高优先级抢占", input="urgent=9 > target=5",
             expect="抢占成功", actual=m.status)
    assert m.status == MissionStatus.PREEMPTED


def test_priority_ordering_on_acquire(service, case_log):
    service.register_robot("r1", [])
    service.submit_mission("low", priority=1)
    service.submit_mission("high", priority=9)
    service.submit_mission("mid", priority=5)
    order = []
    for i in range(3):
        service.register_robot(f"rx{i}", [])
        m, _ = service.acquire_mission(f"rx{i}")
        order.append(m.mission_id)
    case_log("优先级调度顺序", expect="[high, mid, low]", actual=order)
    assert order == ["high", "mid", "low"]


def test_fifo_within_same_priority(service, case_log):
    service.submit_mission("a", priority=1)
    service.submit_mission("b", priority=1)
    service.submit_mission("c", priority=1)
    order = []
    for i in range(3):
        service.register_robot(f"r{i}", [])
        m, _ = service.acquire_mission(f"r{i}")
        order.append(m.mission_id)
    case_log("同优先级 FIFO", expect="[a, b, c]", actual=order)
    assert order == ["a", "b", "c"]
