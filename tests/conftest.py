import asyncio
import time

import pytest

from sandbox_pool.config import PoolConfig
from sandbox_pool.core.pool import SandboxPool
from sandbox_pool.provider.fake import FakeProvider

# 测试用的快速时间参数
FAST = dict(
    poll_interval_s=0.02,
    maintain_interval_s=0.05,
    idle_pause_after_s=0.3,
    waiter_heartbeat_timeout_s=2,
    op_timeout_s=5,
    reconcile_interval_s=0.3,
    orphan_grace_s=0.0,
    warmup_code="warm = True",
    create_cooldown_s=1,
)


async def wait_until(pred, timeout: float = 5.0, interval: float = 0.02):
    deadline = time.monotonic() + timeout
    while True:
        value = pred()
        if asyncio.iscoroutine(value):
            value = await value
        if value:
            return value
        if time.monotonic() > deadline:
            raise AssertionError("condition not met in time")
        await asyncio.sleep(interval)


@pytest.fixture
def db_url(tmp_path):
    return f"sqlite+aiosqlite:///{tmp_path}/pool.db"


@pytest.fixture
def provider():
    return FakeProvider(latency_s=0.01)


@pytest.fixture
async def make_pool(db_url, provider):
    pools = []

    async def _make(*, run_maintainer: bool = True, provider_=None, **overrides) -> SandboxPool:
        cfg = PoolConfig(db_url=db_url, **{**FAST, **overrides})
        pool = SandboxPool(cfg, provider_ or provider)
        await pool.start(run_maintainer=run_maintainer)
        pools.append(pool)
        return pool

    yield _make
    for pool in pools:
        await pool.stop()
