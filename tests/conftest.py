"""测试公共设施：假时钟、临时存储、用例日志（打印输入与判定依据）。"""

from __future__ import annotations

import logging

import pytest

from fleet_orchestrator.config import SchedulerConfig
from fleet_orchestrator.service import OrchestratorService
from fleet_orchestrator.store import Store

logger = logging.getLogger("testcase")


class FakeClock:
    def __init__(self, start: float = 1_000.0) -> None:
        self.t = start

    def __call__(self) -> float:
        return self.t

    def advance(self, seconds: float) -> None:
        self.t += seconds


@pytest.fixture()
def clock() -> FakeClock:
    return FakeClock()


@pytest.fixture()
def store(tmp_path):
    return Store(tmp_path / "data")


@pytest.fixture()
def service(store, clock) -> OrchestratorService:
    return OrchestratorService(store, clock=clock)


@pytest.fixture()
def case_log():
    """用例日志：打印输入、判定依据、实际结果，便于审计失败路径。"""

    def _log(step: str, **fields) -> None:
        parts = " | ".join(f"{k}={v!r}" for k, v in fields.items())
        logger.info("[CASE] %s | %s", step, parts)
        print(f"[CASE] {step} | {parts}")

    return _log


@pytest.fixture()
def small_config():
    def _make(**overrides):
        return SchedulerConfig(**overrides)

    return _make
