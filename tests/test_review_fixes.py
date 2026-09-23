"""评审问题（docs/sandbox-pool-review.md）的针对性测试，按问题编号组织。"""

import asyncio
import sqlite3
import time

import httpx
import pytest
from sqlalchemy import insert, select
from sqlalchemy.exc import IntegrityError

from sandbox_pool.__main__ import insecure_bind
from sandbox_pool.api.app import create_app
from sandbox_pool.api.auth import Authenticator
from sandbox_pool.config import PoolConfig
from sandbox_pool.models import (
    LeaseNotActive,
    LeaseState,
    PoolDraining,
    QueueFull,
    SandboxOpError,
    SandboxState,
    WaiterState,
    WaitTimeout,
)
from sandbox_pool.provider.e2b_provider import E2BProvider, HandleCache
from sandbox_pool.store.repository import _KV_KEYS, KV_RECONCILE_AT, Store
from sandbox_pool.store.schema import leases, pool_kv, waiters
from tests.conftest import wait_until

READY, PAUSED = SandboxState.READY, SandboxState.PAUSED


async def _count_is(pool, **expected) -> bool:
    counts = await pool.store.count_by_state()
    return all(counts.get(state, 0) == n for state, n in expected.items())


async def _row_state(pool, row_id):
    row = await pool.store.get_sandbox(row_id)
    return row["state"] if row else None


async def _warm(pool, n=5):
    """不跑维护循环时手动补货到 n 个 READY。"""
    await pool.maintainer.replenish(time.time())
    await pool.lifecycle.wait_background()
    assert await _count_is(pool, READY=n)


async def _client(pool):
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(pool=pool)), base_url="http://pool")


# ---------------- H1 ----------------


async def test_h1_min_hot_sandboxes_survive_platform_idle_timeout(make_pool, provider):
    # 平台空闲超时 0.8s：没有保活时，热沙箱 0.8s 后就被平台回收、随后被对账清掉并重建
    pool = await make_pool(min_hot=2, idle_pause_after_s=0.3, idle_platform_extra_s=0.5)
    await wait_until(lambda: _count_is(pool, READY=2, PAUSED=3), timeout=10)
    hot = {r["provider_id"] for r in await pool.store.list_sandboxes([READY])}
    await asyncio.sleep(2.0)
    assert {r["provider_id"] for r in await pool.store.list_sandboxes([READY])} == hot
    for pid in hot:
        assert await provider.get_state(pid) == "running"
    assert not any(c[0] == "expired" for c in provider.calls)
    assert (await pool.store.event_counts()).get("keepalive", 0) >= 2
    grant = await pool.allocator.acquire()
    assert grant.source == "ready" and grant.sandbox_id in hot


async def test_h1_keepalive_racing_with_claim_restores_lease_timeout(make_pool, provider):
    pool = await make_pool(run_maintainer=False, max_size=1, target_size=1, idle_pause_after_s=60, idle_platform_extra_s=5)
    await _warm(pool, 1)
    (row,) = await pool.store.list_sandboxes([READY])
    # 保活的 set_timeout（65s）在途 0.3s，期间沙箱被借走并设置了借用超时（660s），保活的调用后到平台
    provider.set_timeout_delays = [0.3]
    keepalive = asyncio.create_task(pool.maintainer._keepalive(row, pool.cfg.ready_platform_timeout_s))
    await asyncio.sleep(0.05)
    grant = await pool.allocator.acquire(lease_ttl_s=600)
    await keepalive
    remaining = provider.sandboxes[grant.sandbox_id]["deadline"] - time.time()
    assert remaining > 600, f"platform timeout left {remaining:.0f}s, lease needs 600s"


# ---------------- H2 ----------------

ALICE, BOB, ADMIN = "alice-key-0000000000", "bob-key-00000000000000", "admin-key-000000000000"
KEYS = dict(api_keys=f"alice:{ALICE}, bob:{BOB}", admin_keys=f"ops:{ADMIN}")


def _h(key):
    return {"Authorization": f"Bearer {key}"}


async def test_h2_auth_and_lease_binding(make_pool):
    pool = await make_pool(idle_pause_after_s=60, **KEYS)
    await wait_until(lambda: _count_is(pool, READY=5))
    async with await _client(pool) as c:
        assert (await c.post("/v1/leases", json={})).status_code == 401
        r = await c.post("/v1/leases", json={}, headers=_h("wrong-key"))
        assert r.status_code == 401 and r.headers["www-authenticate"] == "Bearer"
        assert (await c.get("/v1/pool/stats")).status_code == 401
        assert (await c.get("/healthz")).status_code == 200  # 探活不鉴权

        r = await c.post("/v1/leases", json={}, headers=_h(ALICE))
        assert r.status_code == 200 and r.json()["client_id"] == "alice"
        lid = r.json()["lease_id"]

        # bob 看不到、也用不了 alice 的借用：与不存在一样返回 404
        attempts = [
            ("get", f"/v1/leases/{lid}", {}),
            ("post", f"/v1/leases/{lid}/renew", {"json": {}}),
            ("post", f"/v1/leases/{lid}/run_code", {"json": {"code": "1"}}),
            ("post", f"/v1/leases/{lid}/commands", {"json": {"cmd": "id"}}),
            ("put", f"/v1/leases/{lid}/files", {"params": {"path": "/tmp/x"}, "content": b"x"}),
            ("get", f"/v1/leases/{lid}/files", {"params": {"path": "/tmp/x"}}),
            ("delete", f"/v1/leases/{lid}", {}),
        ]
        for method, path, kw in attempts:
            r = await getattr(c, method)(path, headers=_h(BOB), **kw)
            assert r.status_code == 404, (method, path, r.status_code)
        r = await c.post(f"/v1/leases/{lid}/run_code", json={"code": "print(1)"}, headers=_h(ALICE))
        assert r.status_code == 200 and r.json()["stdout"] == "1\n"

        # 调试接口、排空接口仅管理员；调试接口不返回 lease_id
        assert (await c.get("/v1/sandboxes", headers=_h(ALICE))).status_code == 403
        assert (await c.post("/v1/admin/drain", headers=_h(ALICE))).status_code == 403
        rows = (await c.get("/v1/sandboxes", headers=_h(ADMIN))).json()
        assert len(rows) == 5 and all("lease_id" not in row for row in rows)
        assert (await c.get("/v1/pool/stats", headers=_h(BOB))).json()["config"]["auth_enabled"] is True

        # 管理员可以操作任何人的借用
        r = await c.delete(f"/v1/leases/{lid}", headers=_h(ADMIN))
        assert r.status_code == 200 and r.json()["state"] == "RELEASED"


def test_h2_key_parsing_and_public_bind_guard():
    auth = Authenticator("a:key-a, b:key-b", "ops:key-a")
    assert auth.identify("key-a").admin and auth.identify("key-a").name == "ops"
    assert auth.identify("key-b").name == "b" and not auth.identify("key-b").admin
    assert auth.identify("key-c") is None
    assert not Authenticator().enabled
    with pytest.raises(ValueError):
        Authenticator("no-name-key")
    assert insecure_bind("0.0.0.0", PoolConfig())
    assert not insecure_bind("127.0.0.1", PoolConfig())
    assert not insecure_bind("::1", PoolConfig())
    assert not insecure_bind("localhost", PoolConfig())
    assert not insecure_bind("0.0.0.0", PoolConfig(api_keys="a:k"))
    assert "secret-value" not in repr(PoolConfig(api_keys="a:secret-value"))


# ---------------- M1 ----------------


async def _queue_behind_ghost(pool):
    """在队首放一个不会心跳的排队记录，让随后的借用请求走排队路径。"""
    now = time.time()
    await pool.store.enqueue(owner="ghost", now=now, deadline=now + 60, queue_max=10)


async def test_m1_heartbeat_kept_while_claiming_slow_resume(make_pool, provider):
    pool = await make_pool(max_size=2, target_size=2, waiter_heartbeat_timeout_s=0.5)
    await wait_until(lambda: _count_is(pool, PAUSED=2))
    await _queue_behind_ghost(pool)
    provider.latency_s = 1.5  # 恢复耗时远超心跳超时
    task = asyncio.create_task(pool.allocator.acquire(wait_timeout_s=10))
    seen = set()
    while not task.done():  # 恢复期间自己的排队记录一直是 WAITING，不会被维护循环判为超时
        async with pool.store.read_engine.connect() as conn:
            rows = (await conn.execute(select(waiters.c.owner_replica, waiters.c.state))).all()
        seen |= {state for owner, state in rows if owner != "ghost"}
        await asyncio.sleep(0.05)
    grant = task.result()
    assert grant.source == "resumed"
    assert "TIMEOUT" not in seen and seen <= {"WAITING", "GRANTED"}
    events = await pool.store.event_counts()
    assert events.get("lease_failed", 0) == 0 and events.get("late_grant", 0) == 0


async def test_m1_grant_after_waiter_deadline_is_still_returned(make_pool, provider):
    pool = await make_pool(max_size=2, target_size=2)
    await wait_until(lambda: _count_is(pool, PAUSED=2))
    await _queue_behind_ghost(pool)
    provider.latency_s = 1.0
    grant = await pool.allocator.acquire(wait_timeout_s=0.5)  # 截止时间在恢复途中到达
    assert grant.source == "resumed"
    events = await pool.store.event_counts()
    assert events.get("late_grant") == 1 and events.get("lease_failed", 0) == 0
    assert (await pool.store.get_sandbox(grant.sandbox_row_id))["state"] == "LEASED"
    # 排队记录如实记为 GRANTED（抢的期间已被判超时，交付时改回）
    async with pool.store.read_engine.connect() as conn:
        rows = (await conn.execute(select(waiters.c.state, waiters.c.lease_id).where(waiters.c.owner_replica != "ghost"))).all()
    assert [tuple(r) for r in rows] == [("GRANTED", grant.lease_id)]


# ---------------- M2 ----------------


async def test_m2_expiry_does_not_override_concurrent_renew(make_pool, monkeypatch):
    pool = await make_pool(run_maintainer=False, idle_pause_after_s=60)
    await _warm(pool)
    grant = await pool.allocator.acquire(lease_ttl_s=1)
    stale = await pool.store.get_lease(grant.lease_id)
    await pool.allocator.renew(grant.lease_id, 600)

    # 维护循环按续期前的到期时间列出了这条借用
    async def stale_list(*_a, **_kw):
        return [dict(stale, expires_at=time.time() - 1)]

    monkeypatch.setattr(pool.store, "list_leases", stale_list)
    await pool.maintainer.expire_leases(time.time() + 5)
    assert (await pool.store.get_lease(grant.lease_id))["state"] == LeaseState.ACTIVE.value
    assert (await pool.store.get_sandbox(grant.sandbox_row_id))["state"] == SandboxState.LEASED.value


# ---------------- M3 ----------------


async def test_m3_renew_platform_failure_keeps_lease_unchanged(make_pool, provider):
    pool = await make_pool(run_maintainer=False, idle_pause_after_s=60)
    await _warm(pool)
    grant = await pool.allocator.acquire(lease_ttl_s=60)
    before = await pool.store.get_lease(grant.lease_id)
    provider.fail_set_timeout = 1
    with pytest.raises(SandboxOpError):
        await pool.allocator.renew(grant.lease_id, 600)
    after = await pool.store.get_lease(grant.lease_id)
    assert after["expires_at"] == before["expires_at"] and after["state"] == LeaseState.ACTIVE.value
    renewed = await pool.allocator.renew(grant.lease_id, 600)
    assert renewed["expires_at"] > before["expires_at"]
    remaining = provider.sandboxes[grant.sandbox_id]["deadline"] - time.time()
    assert remaining > 600


async def test_m3_renew_failure_maps_to_502(make_pool, provider):
    pool = await make_pool(idle_pause_after_s=60)
    await wait_until(lambda: _count_is(pool, READY=5))
    async with await _client(pool) as c:
        lid = (await c.post("/v1/leases", json={})).json()["lease_id"]
        provider.fail_set_timeout = 1
        r = await c.post(f"/v1/leases/{lid}/renew", json={"ttl_s": 1200})
        assert r.status_code == 502 and "lease unchanged" in r.json()["detail"]


# ---------------- M4 ----------------


async def test_m4_release_returns_after_sandbox_destroyed(make_pool, provider):
    pool = await make_pool(run_maintainer=False, idle_pause_after_s=60)
    await _warm(pool)
    grant = await pool.allocator.acquire()
    provider.kill_latency_s = 0.3  # 实测 kill 约 0.3s
    await pool.allocator.release(grant.lease_id)
    assert grant.sandbox_id not in provider.sandboxes
    assert await pool.store.get_sandbox(grant.sandbox_row_id) is None


async def test_m4_stuck_destroy_taken_over_after_destroy_timeout(make_pool, provider):
    pool = await make_pool(idle_pause_after_s=60, op_timeout_s=30, destroy_timeout_s=0.3)
    await wait_until(lambda: _count_is(pool, READY=5))
    grant = await pool.allocator.acquire()
    provider.fail_kill = 1  # kill 失败：记录停在 DESTROYING，截止时间过后由维护循环重试
    t0 = time.monotonic()
    await pool.allocator.release(grant.lease_id)
    assert await _row_state(pool, grant.sandbox_row_id) == SandboxState.DESTROYING.value
    await wait_until(lambda: _row_gone(pool, grant.sandbox_row_id), timeout=3)
    assert time.monotonic() - t0 < 3  # 按 destroy_timeout_s 接管，而不是 op_timeout_s（30s）
    assert grant.sandbox_id not in provider.sandboxes


async def _row_gone(pool, row_id):
    return await pool.store.get_sandbox(row_id) is None


# ---------------- M5 ----------------


async def test_m5_reads_not_blocked_by_open_write_transaction(db_url):
    a, b = Store.open(db_url, "default"), Store.open(db_url, "default")
    await a.init_schema()
    async with a.tx() as conn:
        await a._lock(conn)  # a 持有写锁且未提交
        t0 = time.monotonic()
        await asyncio.wait_for(b.list_sandboxes(), timeout=5)
        await asyncio.wait_for(b.count_waiting(), timeout=5)
        await asyncio.wait_for(b.kv_get("draining"), timeout=5)
        assert time.monotonic() - t0 < 1
    await a.close()
    await b.close()


# ---------------- M6 ----------------


def test_m6_handle_cache_lru_and_idle_ttl():
    now = [0.0]
    cache = HandleCache(max_size=2, idle_ttl_s=10, clock=lambda: now[0])
    cache.put("a", 1)
    cache.put("b", 2)
    assert cache.get("a") == 1  # a 变为最近使用
    cache.put("c", 3)  # 超出容量，淘汰最久未用的 b
    assert "b" not in cache and cache.get("a") == 1 and cache.get("c") == 3
    now[0] = 11
    assert cache.get("a") is None  # 空闲超时
    cache.put("d", 4)  # 放入时顺带清理其他过期项
    assert len(cache) == 1 and cache.get("d") == 4
    cache.lock("gone")
    cache.put("e", 5)  # 连接失败留下的锁也会被清理
    assert "gone" not in cache._locks

    provider = E2BProvider(api_key="unused", cache_max=1)
    provider._cache.put("sbx", object())
    provider.forget("sbx")
    assert "sbx" not in provider._cache


async def test_m6_ended_lease_forgets_cached_handle(make_pool, provider):
    pool = await make_pool(run_maintainer=False, idle_pause_after_s=60)
    await _warm(pool)
    grant = await pool.allocator.acquire()
    await pool.allocator.run_code(grant.lease_id, "x = 1", language=None, timeout_s=5)
    # 借用被其他副本结束（本副本没有经过 end_lease）
    await pool.store.cas_lease(grant.lease_id, [LeaseState.ACTIVE], state=LeaseState.RELEASED, ended_at=time.time())
    provider.calls.clear()
    with pytest.raises(LeaseNotActive):
        await pool.allocator.run_code(grant.lease_id, "x", language=None, timeout_s=5)
    assert ("forget", grant.sandbox_id) in provider.calls


# ---------------- M7 ----------------


async def test_m7_kv_keys_prepopulated_and_retry_on_insert_conflict(db_url):
    store = Store.open(db_url, "default")
    await store.init_schema()
    await store.init_schema()  # 幂等，不产生 IntegrityError
    async with store.read_engine.connect() as conn:
        keys = set((await conn.execute(select(pool_kv.c.key))).scalars())
    assert keys == set(_KV_KEYS)

    calls = []

    async def conflict_once(conn):
        calls.append(1)
        if len(calls) == 1:  # 模拟 Postgres 下两个副本并发首次插入同一个键
            raise IntegrityError("INSERT INTO pool_kv", {}, Exception("duplicate key"))
        return "ok"

    assert await store._retry(conflict_once, integrity_retry=True) == "ok"
    calls.clear()
    with pytest.raises(IntegrityError):
        await store._retry(conflict_once)
    assert await store.kv_incr("brand_new_key") == 1 and await store.kv_incr("brand_new_key") == 2
    await store.close()


async def test_m7_create_failure_releases_slot_even_if_counter_fails(make_pool, provider, monkeypatch):
    pool = await make_pool(run_maintainer=False, max_size=1, target_size=1, idle_pause_after_s=60, op_timeout_s=30)
    provider.fail_create = 1

    async def broken_incr(_key):
        raise RuntimeError("db unavailable")

    monkeypatch.setattr(pool.store, "kv_incr", broken_incr)
    await pool.maintainer.start()
    # 失败占用的名额立即释放并重新补货，而不是等 op_timeout_s（30s）后才被接管
    await wait_until(lambda: _count_is(pool, READY=1), timeout=3)
    assert (await pool.store.event_counts()).get("create_failed") == 1


# ---------------- L4 ----------------


async def test_l4_stuck_pause_and_resume_adopted_by_actual_state(make_pool, provider):
    pool = await make_pool(idle_pause_after_s=60)
    await wait_until(lambda: _count_is(pool, READY=5))
    a, b, c, d = (await pool.store.list_sandboxes([READY]))[:4]
    now = time.time()
    # 某副本在暂停 / 恢复途中崩溃：
    #   a 暂停已在平台上完成；b 的暂停请求没发出去（仍在运行）；c 恢复已完成；d 已在平台上消失
    provider.sandboxes[a["provider_id"]]["state"] = "paused"  # 先改云端状态，避免维护循环在两步之间按旧状态收回
    for row, state in ((a, SandboxState.PAUSING), (b, SandboxState.PAUSING), (c, SandboxState.RESUMING), (d, SandboxState.PAUSING)):
        assert await pool.store.cas_sandbox(
            row["id"], [READY], now=now, expect_version=row["version"], state=state, op_owner="dead", op_deadline=now - 1
        )
    del provider.sandboxes[d["provider_id"]]  # 若 d 先被按「运行中」收回，随后也会因云端消失被清理
    await wait_until(lambda: _row_state_is(pool, a["id"], "PAUSED"))
    await wait_until(lambda: _row_state_is(pool, b["id"], "READY"))
    await wait_until(lambda: _row_state_is(pool, c["id"], "READY"))
    await wait_until(lambda: _row_gone(pool, d["id"]))
    assert not any(call == ("kill", row["provider_id"]) for call in provider.calls for row in (a, b, c))
    assert (await pool.store.event_counts()).get("adopt", 0) >= 3
    # 收回为 READY 的沙箱平台超时未知，保活随即重设
    await wait_until(lambda: _platform_deadline_set(pool, b["id"]))
    await wait_until(lambda: _count_is(pool, READY=4, PAUSED=1))


async def _row_state_is(pool, row_id, state):
    return await _row_state(pool, row_id) == state


async def _platform_deadline_set(pool, row_id):
    row = await pool.store.get_sandbox(row_id)
    return row is not None and row["platform_deadline"] is not None


# ---------------- L5 ----------------


async def test_l5_periodic_task_has_single_winner_per_interval(db_url):
    stores = [Store.open(db_url, "default") for _ in range(3)]
    await stores[0].init_schema()
    now = time.time()
    wins = await asyncio.gather(
        *[s.try_periodic(KV_RECONCILE_AT, now=now, interval_s=60) for s in stores for _ in range(3)]
    )
    assert sum(wins) == 1
    assert not await stores[1].try_periodic(KV_RECONCILE_AT, now=now + 30, interval_s=60)
    assert await stores[2].try_periodic(KV_RECONCILE_AT, now=now + 61, interval_s=60)
    for s in stores:
        await s.close()


async def test_l5_orphan_killed_once_with_two_replicas(make_pool, provider):
    a = await make_pool(idle_pause_after_s=60)
    await make_pool(idle_pause_after_s=60)
    await wait_until(lambda: _count_is(a, READY=5))
    orphan = await provider.create("t", {"pool": "default", "pool_row": "no-such-row"}, 60)
    await wait_until(lambda: orphan not in provider.sandboxes)
    await asyncio.sleep(1.0)  # 再跑几个对账周期
    assert (await a.store.event_counts()).get("orphan_killed") == 1


# ---------------- L6 ----------------


async def test_l6_concurrent_pause_respects_min_hot(make_pool):
    a = await make_pool(run_maintainer=False, min_hot=2, idle_pause_after_s=0.1)
    b = await make_pool(run_maintainer=False, min_hot=2, idle_pause_after_s=0.1)
    await _warm(a)
    later = time.time() + 5
    await asyncio.gather(a.maintainer.pause_idle(later), b.maintainer.pause_idle(later))
    await a.lifecycle.wait_background()
    await b.lifecycle.wait_background()
    assert await _count_is(a, READY=2, PAUSED=3)


# ---------------- L7 ----------------


async def test_l7_purge_history_keeps_recent_and_active(db_url):
    store = Store.open(db_url, "default")
    await store.init_schema()
    now = time.time()
    old = now - 10 * 86400
    done_old = await store.enqueue(owner="r", now=old, deadline=old + 1, queue_max=10)
    await store.cas_waiter(done_old, WaiterState.WAITING, WaiterState.TIMEOUT)
    waiting_old = await store.enqueue(owner="r", now=old, deadline=now + 60, queue_max=10)
    lease = dict(pool="default", sandbox_row_id="r", sandbox_id="s", source="ready", created_at=old, expires_at=old + 1, hard_deadline=old + 2, wait_ms=0)
    async with store.tx() as conn:
        await conn.execute(insert(leases).values(id="old-ended", state="RELEASED", ended_at=old + 1, **lease))
        await conn.execute(insert(leases).values(id="old-active", state="ACTIVE", ended_at=None, **lease))
        await conn.execute(insert(leases).values(id="recent-ended", state="EXPIRED", ended_at=now, **lease))
    await store.add_event("old", now=old, replica="r")
    await store.add_event("old", now=old, replica="r")
    await store.add_event("recent", now=now, replica="r")

    purged = await store.purge_history(before=now - 7 * 86400, batch=1)  # batch=1 覆盖分批删除
    assert purged == {"waiters": 1, "leases": 1, "events": 2}
    async with store.read_engine.connect() as conn:
        assert set((await conn.execute(select(waiters.c.seq))).scalars()) == {waiting_old}
        assert set((await conn.execute(select(leases.c.id))).scalars()) == {"old-active", "recent-ended"}
    assert await store.event_counts() == {"recent": 1}
    await store.close()


# ---------------- L8 ----------------


async def test_l8_upload_size_limit(make_pool):
    pool = await make_pool(idle_pause_after_s=60, max_upload_bytes=10)
    await wait_until(lambda: _count_is(pool, READY=5))
    async with await _client(pool) as c:
        lid = (await c.post("/v1/leases", json={})).json()["lease_id"]
        url = f"/v1/leases/{lid}/files"
        assert (await c.put(url, params={"path": "/tmp/a"}, content=b"x" * 10)).status_code == 200
        r = await c.put(url, params={"path": "/tmp/b"}, content=b"x" * 11)
        assert r.status_code == 413

        async def chunks():  # 分块上传，没有 Content-Length
            for _ in range(3):
                yield b"x" * 5

        assert (await c.put(url, params={"path": "/tmp/c"}, content=chunks())).status_code == 413
        assert (await c.get(url, params={"path": "/tmp/b"})).status_code == 404


# ---------------- L9 ----------------


async def test_l9_drain_destroys_idle_and_blocks_new_leases(make_pool, provider):
    pool = await make_pool(idle_pause_after_s=0.3)
    await wait_until(lambda: _count_is(pool, PAUSED=5))
    held = await pool.allocator.acquire()
    result = await pool.drain()
    assert result["draining"] and result["destroyed"] == 4
    with pytest.raises(PoolDraining):
        await pool.allocator.acquire(wait_timeout_s=1)
    await asyncio.sleep(0.3)
    assert provider.alive_count() == 1  # 只剩借出中的，不补货
    await pool.allocator.release(held.lease_id)
    await asyncio.sleep(0.3)
    assert provider.alive_count() == 0 and await pool.store.count_by_state() == {}
    assert (await pool.stats())["draining"] is True
    await pool.undrain()
    await wait_until(lambda: _count_is(pool, PAUSED=5), timeout=10)


async def test_l9_drain_fails_queued_waiters_fast(make_pool):
    pool = await make_pool(max_size=1, target_size=1, idle_pause_after_s=60)
    await wait_until(lambda: _count_is(pool, READY=1))
    await pool.allocator.acquire()
    waiter = asyncio.create_task(pool.allocator.acquire(wait_timeout_s=30))
    await wait_until(lambda: _eq(pool.store.count_waiting(), 1))
    t0 = time.monotonic()
    async with await _client(pool) as c:  # 鉴权关闭时为匿名管理员
        assert (await c.post("/v1/admin/drain")).status_code == 200
        assert (await c.post("/v1/leases", json={})).status_code == 503
    with pytest.raises(PoolDraining):
        await waiter
    assert time.monotonic() - t0 < 2
    assert await pool.store.count_waiting() == 0


async def _eq(coro, value):
    return (await coro) == value


# ---------------- L10 ----------------


async def test_l10_strict_fifo_burst_keeps_queue_semantics(make_pool):
    pool = await make_pool(strict_fifo=True, idle_pause_after_s=60)
    await wait_until(lambda: _count_is(pool, READY=5))
    results = await asyncio.gather(
        *[pool.allocator.acquire(wait_timeout_s=0.5) for _ in range(16)], return_exceptions=True
    )
    grants = [r for r in results if not isinstance(r, Exception)]
    assert len(grants) == 5 and len({g.sandbox_id for g in grants}) == 5
    assert sum(isinstance(r, QueueFull) for r in results) == 1
    assert sum(isinstance(r, WaitTimeout) for r in results) == 10
    # 严格模式下拿到沙箱的请求也都经过了队列
    async with pool.store.read_engine.connect() as conn:
        granted = (await conn.execute(select(waiters.c.seq).where(waiters.c.state == "GRANTED"))).all()
    assert len(granted) == 5


# ---------------- 数据库迁移 ----------------


async def test_init_schema_adds_new_columns_to_existing_db(db_url, tmp_path):
    store = Store.open(db_url, "default")
    await store.init_schema()
    await store.close()
    con = sqlite3.connect(tmp_path / "pool.db")
    con.executescript(
        "ALTER TABLE sandboxes DROP COLUMN platform_deadline;"
        "ALTER TABLE leases DROP COLUMN client_id;"
        "DROP INDEX ix_events_pool_ts;"
    )
    con.close()
    store = Store.open(db_url, "default")
    await store.init_schema()
    await store.close()
    con = sqlite3.connect(tmp_path / "pool.db")
    assert "platform_deadline" in {r[1] for r in con.execute("PRAGMA table_info(sandboxes)")}
    assert "client_id" in {r[1] for r in con.execute("PRAGMA table_info(leases)")}
    assert "ix_events_pool_ts" in {r[1] for r in con.execute("PRAGMA index_list(events)")}
    con.close()


# ---------------- 端到端回归发现：经代理的 HTTP/2 连接空闲后失效 ----------------


async def test_provider_retries_once_on_stale_connection():
    from sandbox_pool.provider.e2b_provider import _NOT_SENT_ERRORS, _retry_stale

    calls = []

    def flaky(exc):
        async def call():
            calls.append(1)
            if len(calls) == 1:
                raise exc
            return "ok"

        return call

    assert await _retry_stale(flaky(httpx.WriteError(""))) == "ok" and len(calls) == 2
    calls.clear()
    assert await _retry_stale(flaky(httpx.ReadError(""))) == "ok" and len(calls) == 2
    calls.clear()
    with pytest.raises(httpx.ReadTimeout):  # 超时不重试，保证调用耗时不超过过渡态截止时间
        await _retry_stale(flaky(httpx.ReadTimeout("")))
    calls.clear()
    with pytest.raises(httpx.ReadError):  # 创建不是幂等的：请求可能已送达时不重试
        await _retry_stale(flaky(httpx.ReadError("")), retry_on=_NOT_SENT_ERRORS)
    assert len(calls) == 1
