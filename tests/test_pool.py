import asyncio
import time

import pytest

from sandbox_pool.models import LeaseState, QueueFull, SandboxState, WaitTimeout
from tests.conftest import wait_until


async def _counts(pool):
    return await pool.store.count_by_state()


async def test_warm_up_then_pause_all(make_pool, provider):
    pool = await make_pool()
    await wait_until(lambda: _state_count(pool, SandboxState.PAUSED, 5))
    assert provider.alive_count() == 5
    assert all(sb["state"] == "paused" for sb in provider.sandboxes.values())
    assert ("warmup", next(iter(provider.sandboxes))) in provider.calls


async def _state_count(pool, state, n):
    return (await _counts(pool)).get(state.value, 0) == n


async def test_min_hot_keeps_some_running(make_pool):
    pool = await make_pool(min_hot=2)
    await wait_until(lambda: _state_count(pool, SandboxState.PAUSED, 3))
    await asyncio.sleep(0.5)
    counts = await _counts(pool)
    assert counts.get("READY") == 2 and counts.get("PAUSED") == 3


async def test_acquire_prefers_ready(make_pool):
    pool = await make_pool(idle_pause_after_s=60)
    await wait_until(lambda: _state_count(pool, SandboxState.READY, 5))
    grant = await pool.allocator.acquire()
    assert grant.source == "ready"


async def test_acquire_resumes_paused(make_pool, provider):
    pool = await make_pool()
    await wait_until(lambda: _state_count(pool, SandboxState.PAUSED, 5))
    grant = await pool.allocator.acquire()
    assert grant.source == "resumed"
    assert provider.sandboxes[grant.sandbox_id]["state"] == "running"
    row = await pool.store.get_sandbox(grant.sandbox_row_id)
    assert row["state"] == "LEASED" and row["lease_id"] == grant.lease_id


async def test_release_destroys_and_replenishes(make_pool, provider):
    pool = await make_pool(idle_pause_after_s=60)
    await wait_until(lambda: _state_count(pool, SandboxState.READY, 5))
    grant = await pool.allocator.acquire()
    await pool.allocator.release(grant.lease_id)
    await wait_until(lambda: grant.sandbox_id not in provider.sandboxes)
    await wait_until(lambda: _state_count(pool, SandboxState.READY, 5))
    assert (await pool.allocator.get_lease(grant.lease_id))["state"] == "RELEASED"


async def test_burst_fifo_queue_and_queue_full(make_pool):
    pool = await make_pool(idle_pause_after_s=60, wait_timeout_s=20)
    await wait_until(lambda: _state_count(pool, SandboxState.READY, 5))
    alloc = pool.allocator

    first = await asyncio.gather(*[alloc.acquire() for _ in range(5)])
    assert len({g.sandbox_id for g in first}) == 5

    order: list[int] = []

    async def waiter(i):
        g = await alloc.acquire()
        order.append(i)
        return g

    tasks = []
    for i in range(10):
        tasks.append(asyncio.create_task(waiter(i)))
        await wait_until(lambda: _eq(pool.store.count_waiting(), i + 1))  # 确认入队后再发下一个
    with pytest.raises(QueueFull):
        await alloc.acquire()

    held = list(first)
    for _ in range(10):
        g = held.pop(0)
        before = len(order)  # 必须在归还前记录：归还过程中下一个请求可能已经拿到沙箱
        await alloc.release(g.lease_id)
        await wait_until(lambda: len(order) == before + 1)
        held.append(tasks[order[-1]].result())
    assert order == list(range(10))


async def _eq(coro, value):
    return (await coro) == value


async def test_wait_timeout(make_pool):
    pool = await make_pool(max_size=1, target_size=1, idle_pause_after_s=60)
    await wait_until(lambda: _state_count(pool, SandboxState.READY, 1))
    await pool.allocator.acquire()
    t0 = time.monotonic()
    with pytest.raises(WaitTimeout):
        await pool.allocator.acquire(wait_timeout_s=0.3)
    assert 0.25 < time.monotonic() - t0 < 2
    assert await pool.store.count_waiting() == 0


async def test_two_replicas_never_double_allocate_or_exceed_capacity(make_pool, provider):
    a = await make_pool(idle_pause_after_s=0.2)
    b = await make_pool(idle_pause_after_s=0.2)
    assert a.replica_id != b.replica_id
    await wait_until(lambda: _state_count(a, SandboxState.PAUSED, 5))

    peak = 0

    async def sample():
        nonlocal peak
        while True:
            peak = max(peak, provider.alive_count())
            await asyncio.sleep(0.005)

    sampler = asyncio.create_task(sample())
    results = await asyncio.gather(
        *[(a if i % 2 else b).allocator.acquire(wait_timeout_s=1.0) for i in range(8)], return_exceptions=True
    )
    sampler.cancel()
    grants = [r for r in results if not isinstance(r, Exception)]
    assert len(grants) == 5
    assert len({g.sandbox_id for g in grants}) == 5
    assert all(isinstance(r, WaitTimeout) for r in results if isinstance(r, Exception))
    assert peak <= 5


async def test_lease_expiry_reclaims(make_pool, provider):
    pool = await make_pool(idle_pause_after_s=60)
    await wait_until(lambda: _state_count(pool, SandboxState.READY, 5))
    grant = await pool.allocator.acquire(lease_ttl_s=0.3)
    await wait_until(lambda: _lease_state(pool, grant.lease_id, "EXPIRED"))
    await wait_until(lambda: grant.sandbox_id not in provider.sandboxes)
    await wait_until(lambda: _state_count(pool, SandboxState.READY, 5))


async def _lease_state(pool, lease_id, state):
    return (await pool.store.get_lease(lease_id))["state"] == state


async def test_renew_is_bounded_by_hard_deadline(make_pool):
    pool = await make_pool(idle_pause_after_s=60, lease_max_s=2)
    await wait_until(lambda: _state_count(pool, SandboxState.READY, 5))
    grant = await pool.allocator.acquire(lease_ttl_s=1)
    lease = await pool.allocator.renew(grant.lease_id, 100)
    assert lease["expires_at"] == pytest.approx(lease["hard_deadline"])


async def test_stuck_operation_taken_over(make_pool, provider):
    # 模拟某副本占了名额、在云端建好了沙箱，但还没记录 provider_id 就崩溃了
    dead = await make_pool(run_maintainer=False)
    now = time.time()
    row = await dead.store.reserve_slot(template="t", now=now, owner="dead-replica", op_deadline=now - 1, limit=5)
    leaked = await provider.create("t", {"pool": "default", "pool_row": row["id"]}, 60)

    live = await make_pool(idle_pause_after_s=60)
    await wait_until(lambda: _gone(live, row["id"]))
    await wait_until(lambda: leaked not in provider.sandboxes)  # 对账清理孤儿
    await wait_until(lambda: _state_count(live, SandboxState.READY, 5))
    assert provider.alive_count() == 5


async def _gone(pool, row_id):
    return await pool.store.get_sandbox(row_id) is None


async def test_orphan_reconcile(make_pool, provider):
    pool = await make_pool(idle_pause_after_s=60)
    orphan = await provider.create("t", {"pool": "default", "pool_row": "no-such-row"}, 60)
    other = await provider.create("t", {"pool": "someone-else"}, 60)
    await wait_until(lambda: orphan not in provider.sandboxes)
    assert other in provider.sandboxes


async def test_circuit_breaker_stops_replenish(make_pool, provider):
    provider.fail_create = 100
    pool = await make_pool(create_fail_threshold=3, create_cooldown_s=30)
    await wait_until(lambda: _gt(pool.store.kv_get("create_block_until"), time.time()))
    await asyncio.sleep(0.3)
    create_calls = sum(1 for c in provider.calls if c[0] == "create")
    await asyncio.sleep(0.5)
    assert sum(1 for c in provider.calls if c[0] == "create") == create_calls
    assert (await pool.store.event_counts()).get("circuit_open", 0) >= 1


async def _gt(coro, value):
    return (await coro) > value


async def test_resume_failure_falls_back_to_next(make_pool, provider):
    pool = await make_pool()
    await wait_until(lambda: _state_count(pool, SandboxState.PAUSED, 5))
    provider.fail_resume_ids = set(provider.sandboxes)  # 所有暂停的都恢复失败
    grant = await pool.allocator.acquire(wait_timeout_s=5)
    assert grant.sandbox_id not in provider.fail_resume_ids
    assert (await pool.store.event_counts()).get("resume_failed", 0) >= 1


async def test_lease_state_after_release_rejects_exec(make_pool):
    pool = await make_pool(idle_pause_after_s=60)
    await wait_until(lambda: _state_count(pool, SandboxState.READY, 5))
    grant = await pool.allocator.acquire()
    await pool.allocator.release(grant.lease_id)
    from sandbox_pool.models import LeaseNotActive

    with pytest.raises(LeaseNotActive):
        await pool.allocator.run_code(grant.lease_id, "1", language=None, timeout_s=5)
    assert (await pool.allocator.get_lease(grant.lease_id))["state"] == LeaseState.RELEASED.value
