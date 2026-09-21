"""共享测试夹具。

每个用例通过 :func:`build_service` 得到内存服务与假时钟；
``caplog`` 自动在失败时展示领域日志（输入与判定依据，见 logging_setup）。
"""
from __future__ import annotations

import logging

import pytest

from app.config import SchedulerConfig
from app.service import FleetService
from app.timeutil import FakeClock


def _configure_domain_logging() -> logging.Logger:
    logger = logging.getLogger("fleet.test")
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(logging.Formatter(
            "[%(levelname)s] %(message)s"))
        logger.addHandler(handler)
    logger.setLevel(logging.INFO)
    logger.propagate = False
    return logger


LOGGER = _configure_domain_logging()


@pytest.fixture
def logger():
    return LOGGER


@pytest.fixture
def clock():
    return FakeClock(1_000)


@pytest.fixture
def config():
    return SchedulerConfig(
        heartbeat_timeout_ms=100,
        lease_duration_ms=1_000,
        max_lease_duration_ms=5_000,
        max_queue_size=100,
        max_attempts=5)


@pytest.fixture
def service(clock, config):
    return FleetService.in_memory(clock=clock, config=config)
