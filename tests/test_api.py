import httpx

from sandbox_pool.api.app import create_app
from sandbox_pool.models import SandboxState
from tests.conftest import wait_until


async def _client(pool):
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(pool=pool)), base_url="http://pool")


async def _ready(pool, n):
    return (await pool.store.count_by_state()).get(SandboxState.READY.value, 0) == n


async def test_full_lease_flow(make_pool):
    pool = await make_pool(idle_pause_after_s=60)
    await wait_until(lambda: _ready(pool, 5))
    async with await _client(pool) as c:
        r = await c.post("/v1/leases", json={})
        assert r.status_code == 200, r.text
        lease = r.json()
        lid = lease["lease_id"]
        assert lease["state"] == "ACTIVE" and lease["source"] == "ready"

        r = await c.post(f"/v1/leases/{lid}/run_code", json={"code": "a = 21\nprint(a * 2)"})
        assert r.status_code == 200 and r.json()["stdout"] == "42\n"
        r = await c.post(f"/v1/leases/{lid}/run_code", json={"code": "print(a)"})
        assert r.json()["stdout"] == "21\n"  # 同一借用内解释器状态保持
        r = await c.post(f"/v1/leases/{lid}/run_code", json={"code": "1/0"})
        assert r.json()["error"]["name"] == "ZeroDivisionError"

        r = await c.post(f"/v1/leases/{lid}/commands", json={"cmd": "echo hi"})
        assert r.json()["exit_code"] == 0
        r = await c.post(f"/v1/leases/{lid}/commands", json={"cmd": "exit 3"})
        assert r.status_code == 200 and r.json()["exit_code"] == 3

        r = await c.put(f"/v1/leases/{lid}/files", params={"path": "/tmp/a.bin"}, content=b"\x00\x01data")
        assert r.status_code == 200 and r.json()["size"] == 6
        r = await c.get(f"/v1/leases/{lid}/files", params={"path": "/tmp/a.bin"})
        assert r.content == b"\x00\x01data"
        r = await c.get(f"/v1/leases/{lid}/files", params={"path": "/tmp/none"})
        assert r.status_code == 404

        r = await c.post(f"/v1/leases/{lid}/renew", json={"ttl_s": 1200})
        assert r.status_code == 200 and r.json()["expires_at"] > lease["expires_at"]

        r = await c.delete(f"/v1/leases/{lid}")
        assert r.status_code == 200 and r.json()["state"] == "RELEASED"
        r = await c.post(f"/v1/leases/{lid}/run_code", json={"code": "1"})
        assert r.status_code == 409
        assert (await c.get("/v1/leases/nope")).status_code == 404

        stats = (await c.get("/v1/pool/stats")).json()
        assert stats["lease_sources"]["ready"] == 1
        assert "acquire" in stats["latency_ms"]
        assert (await c.get("/healthz")).json()["ok"] is True


async def test_queue_full_and_wait_timeout_status_codes(make_pool):
    pool = await make_pool(max_size=1, target_size=1, queue_max=1, idle_pause_after_s=60)
    await wait_until(lambda: _ready(pool, 1))
    async with await _client(pool) as c:
        assert (await c.post("/v1/leases", json={})).status_code == 200
        import asyncio

        waiting = asyncio.create_task(c.post("/v1/leases", json={"wait_timeout_s": 1}))
        await wait_until(lambda: pool.store.count_waiting())
        r = await c.post("/v1/leases", json={"wait_timeout_s": 1})
        assert r.status_code == 429
        r = await waiting
        assert r.status_code == 504
