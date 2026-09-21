"""调度与超时参数：非法配置明确拒绝，合法更新生效。"""

import pytest

from fleet_orchestrator.config import SchedulerConfig
from fleet_orchestrator.errors import ConfigOutOfRange


def test_out_of_range_rejected(case_log):
    for key, bad in [("lease_duration_s", 0.1), ("lease_duration_s", 99999),
                     ("heartbeat_timeout_s", 0), ("max_backlog", 0),
                     ("max_attempts_default", 500)]:
        with pytest.raises(ConfigOutOfRange) as exc:
            SchedulerConfig(**{key: bad})
        case_log("配置越界", input=f"{key}={bad}",
                 expect="config_out_of_range", actual=exc.value.reason)
        assert exc.value.reason == "config_out_of_range"
        assert key in exc.value.detail  # 拒绝原因指明具体字段


def test_unknown_config_key_rejected(case_log):
    cfg = SchedulerConfig()
    with pytest.raises(ConfigOutOfRange) as exc:
        cfg.updated(nonexistent_knob=1)
    case_log("未知配置键", expect="config_out_of_range", actual=exc.value.reason)
    assert exc.value.reason == "config_out_of_range"


def test_invalid_update_leaves_config_unchanged(service, case_log):
    before = service.config
    with pytest.raises(ConfigOutOfRange):
        service.update_config(lease_duration_s=-5)
    case_log("非法更新不产生半更新", expect="配置保持原值",
             actual=service.config == before)
    assert service.config == before


def test_valid_update_applies(service, case_log):
    cfg = service.update_config(lease_duration_s=60, max_backlog=10)
    case_log("合法更新", input="lease=60, backlog=10", expect="生效",
             actual=f"lease={cfg.lease_duration_s} backlog={cfg.max_backlog}")
    assert cfg.lease_duration_s == 60
    assert cfg.max_backlog == 10
