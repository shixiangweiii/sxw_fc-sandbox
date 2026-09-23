import asyncio
import time

from sandbox_pool.models import SandboxState
from sandbox_pool.store.db import create_engine
from sandbox_pool.store.repository import Store


async def _stores(db_url, n=2):
    stores = [Store(create_engine(db_url), "default") for _ in range(n)]
    await stores[0].init_schema()
    return stores


async def test_reserve_slot_never_exceeds_limit_across_engines(db_url):
    stores = await _stores(db_url, 3)
    now = time.time()
    results = await asyncio.gather(
        *[
            stores[i % 3].reserve_slot(template="t", now=now, owner=f"r{i}", op_deadline=now + 60, limit=5)
            for i in range(20)
        ]
    )
    assert sum(r is not None for r in results) == 5
    assert (await stores[0].count_by_state()) == {SandboxState.CREATING.value: 5}
    for s in stores:
        await s.engine.dispose()


async def test_cas_rejects_stale_version(db_url):
    (store,) = await _stores(db_url, 1)
    now = time.time()
    row = await store.reserve_slot(template="t", now=now, owner="r", op_deadline=now + 60, limit=5)
    assert await store.cas_sandbox(row["id"], [SandboxState.CREATING], now=now, expect_version=0, state=SandboxState.WARMING)
    assert not await store.cas_sandbox(row["id"], [SandboxState.WARMING], now=now, expect_version=0, state=SandboxState.READY)
    assert not await store.cas_sandbox(row["id"], [SandboxState.CREATING], now=now, state=SandboxState.READY)
    assert (await store.get_sandbox(row["id"]))["version"] == 1
    await store.engine.dispose()


async def test_enqueue_respects_queue_max_concurrently(db_url):
    stores = await _stores(db_url, 2)
    now = time.time()
    seqs = await asyncio.gather(
        *[stores[i % 2].enqueue(owner="r", now=now, deadline=now + 60, queue_max=10) for i in range(15)]
    )
    accepted = [s for s in seqs if s is not None]
    assert len(accepted) == 10
    assert await stores[0].head_seq() == min(accepted)
    for s in stores:
        await s.engine.dispose()
