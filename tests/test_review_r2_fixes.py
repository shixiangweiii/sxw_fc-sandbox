"""第二轮评审复核后确认需要修的问题，按问题编号组织（结论与取舍见 docs/sandbox-pool-r2-fix-changes.md）。

R2-N1 是复核时新发现的问题，不在评审报告里。
"""

import asyncio
import sys
import time
from types import SimpleNamespace

import pytest
from e2b import TimeoutException as E2BTimeoutException
from e2b_code_interpreter import AsyncSandbox
from sqlalchemy import select

from sandbox_pool.config import PoolConfig
from sandbox_pool.models import LeaseState, SandboxState, WaiterState, WaitTimeout
from sandbox_pool.provider.base import ExecutionTimeout
from sandbox_pool.provider.e2b_provider import E2BProvider
from sandbox_pool.provider.fake import FakeProvider
from sandbox_pool.store.schema import waiters
from tests.conftest import wait_until
from tests.test_review_fixes import ADMIN, ALICE, KEYS, _client, _count_is, _h, _warm

READY = SandboxState.READY


# ---------------- R2-N1 ----------------


async def test_r2_n1_queued_claim_of_unusable_sandbox_keeps_waiting(make_pool):
    # 补货约 1.5s，比排队心跳间隔（约 0.67s）慢：修复前心跳必然先看到排队记录已不是 WAITING
    provider = FakeProvider(latency_s=1.5)
    pool = await make_pool(provider_=provider, max_size=1, target_size=1, strict_fifo=True, idle_pause_after_s=60)
    await wait_until(lambda: _count_is(pool, READY=1), timeout=10)
    # 排队请求抢到的 READY 沙箱不可用（平台瞬时错误 / 已被平台回收）：换补货的新沙箱，而不是提前 504
    provider.fail_set_timeout = 1
    grant = await pool.allocator.acquire(wait_timeout_s=20)
    assert grant.source == "ready"
    assert (await pool.store.get_lease(grant.lease_id))["state"] == "ACTIVE"
    async with pool.store._read() as conn:
        (state,) = (await conn.execute(select(waiters.c.state).where(waiters.c.lease_id == grant.lease_id))).one()
    assert state == WaiterState.GRANTED.value


# ---------------- R2-H1 ----------------


async def test_r2_h1_direct_claim_released_when_client_disconnected(make_pool):
    pool = await make_pool(idle_pause_after_s=60)
    await wait_until(lambda: _count_is(pool, READY=5))

    async def disconnected():
        return True

    # 队列为空时直接抢（不排队）：抢到后发现请求方已断开，立即归还，不让沙箱占到借用过期
    with pytest.raises(WaitTimeout):
        await pool.allocator.acquire(is_disconnected=disconnected)
    assert await pool.store.list_leases([LeaseState.ACTIVE]) == []
    assert len(await pool.store.list_leases([LeaseState.RELEASED])) == 1


# ---------------- R2-H2 ----------------


async def test_r2_h2_failed_keepalive_never_leaves_overstated_deadline(make_pool, provider):
    a = await make_pool(run_maintainer=False, max_size=1, target_size=1, idle_pause_after_s=60)
    b = await make_pool(run_maintainer=False, max_size=1, target_size=1, idle_pause_after_s=60)
    await _warm(a, 1)
    (row,) = await a.store.list_sandboxes([READY])
    timeout = a.cfg.ready_platform_timeout_s
    # 两个副本的保活都失败：A 的调用在途 0.3s，期间 B 读到 A 预写的到期时间再次保活，B 的调用先失败
    provider.set_timeout_delays = [0.3, 0.05]
    provider.fail_set_timeout = 2
    keepalive_a = asyncio.create_task(a.maintainer._keepalive(row, timeout))
    await asyncio.sleep(0.1)
    await b.maintainer._keepalive(await b.store.get_sandbox(row["id"]), timeout)
    await keepalive_a
    fresh = await a.store.get_sandbox(row["id"])
    # 平台超时没有续上：库里不能留下比实际更晚的到期时间，否则保活会长期跳过这个沙箱
    assert fresh["platform_deadline"] is None or fresh["platform_deadline"] <= row["platform_deadline"]


# ---------------- R2-M7 ----------------


async def test_r2_m7_exec_does_not_write_sandbox_row(make_pool):
    pool = await make_pool(idle_pause_after_s=60)
    await wait_until(lambda: _count_is(pool, READY=5))
    grant = await pool.allocator.acquire()
    before = await pool.store.get_sandbox(grant.sandbox_row_id)
    await asyncio.sleep(0.01)
    # 借出中的沙箱归还即销毁，不会回到 READY：代为执行时写 last_active_at 没有任何读者，白占写连接
    await pool.allocator.run_code(grant.lease_id, "1 + 1", language=None, timeout_s=5)
    await pool.allocator.run_command(grant.lease_id, "true", cwd=None, envs=None, timeout_s=5)
    await pool.allocator.write_file(grant.lease_id, "/tmp/a", b"a")
    await pool.allocator.read_file(grant.lease_id, "/tmp/a")
    assert await pool.store.get_sandbox(grant.sandbox_row_id) == before


# ---------------- R2-M12 ----------------


async def test_r2_m12_failed_destroy_on_release_is_recorded(make_pool, provider):
    pool = await make_pool(idle_pause_after_s=60)
    await wait_until(lambda: _count_is(pool, READY=5))
    grant = await pool.allocator.acquire()
    provider.fail_kill = 1
    await pool.allocator.release(grant.lease_id)
    # 归还返回时沙箱还没销毁、仍占着容量（destroy_timeout_s 后由维护循环重试）：要能在统计里看到
    assert (await pool.store.get_sandbox(grant.sandbox_row_id))["state"] == SandboxState.DESTROYING.value
    assert (await pool.stats())["events"].get("destroy_failed") == 1


# ---------------- R2-L6 ----------------


async def test_r2_l6_admin_finds_and_force_releases_stuck_lease(make_pool):
    pool = await make_pool(idle_pause_after_s=60, **KEYS)
    await wait_until(lambda: _count_is(pool, READY=5))
    async with await _client(pool) as c:
        lease = (await c.post("/v1/leases", json={}, headers=_h(ALICE))).json()
        rows = (await c.get("/v1/sandboxes", headers=_h(ADMIN))).json()
        (leased,) = [r for r in rows if r["state"] == "LEASED"]
        # 借出中的沙箱附带借用方和到期时间，仍不返回 lease_id（借用凭证）
        assert "lease_id" not in leased and lease["lease_id"] not in str(rows)
        assert leased["lease"]["client_id"] == "alice" and leased["lease"]["expires_at"] == lease["expires_at"]
        assert all(r["lease"] is None for r in rows if r["state"] != "LEASED")

        url = f"/v1/sandboxes/{leased['id']}"
        assert (await c.delete(url, headers=_h(ALICE))).status_code == 403
        r = await c.delete(url, headers=_h(ADMIN))
        assert r.status_code == 200 and r.json()["previous_state"] == "LEASED"
        # 借用已结束：借用方之后的调用返回 409
        assert (await c.get(f"/v1/leases/{lease['lease_id']}", headers=_h(ALICE))).json()["state"] == "RELEASED"
        r = await c.post(f"/v1/leases/{lease['lease_id']}/run_code", json={"code": "1"}, headers=_h(ALICE))
        assert r.status_code == 409
        assert (await c.delete(url, headers=_h(ADMIN))).status_code == 404

        # 过渡态由执行中的副本负责：返回 409，稍后重试
        ready = (await pool.store.list_sandboxes([READY]))[0]
        assert await pool.store.cas_sandbox(
            ready["id"], [READY], now=time.time(), expect_version=ready["version"],
            state=SandboxState.PAUSING, op_owner="other-replica", op_deadline=time.time() + 60,
        )
        assert (await c.delete(f"/v1/sandboxes/{ready['id']}", headers=_h(ADMIN))).status_code == 409
    # 被销毁的沙箱由补货补回
    await wait_until(lambda: _count_is(pool, READY=4, PAUSING=1))


# ---------------- R2-M5 ----------------


async def test_r2_m5_request_body_limited_before_auth(make_pool):
    pool = await make_pool(idle_pause_after_s=60, max_body_bytes=1024, **KEYS)
    await wait_until(lambda: _count_is(pool, READY=5))
    url = "/v1/leases/any/run_code"
    async with await _client(pool) as c:
        # FastAPI 在鉴权依赖之前就读完请求体：超限的请求不带 key 也要按大小拒绝
        r = await c.post(url, json={"code": "x" * 2048})
        assert r.status_code == 413 and r.json()["error"] == "PayloadTooLarge"

        async def chunks():  # 分块上传，没有 Content-Length
            for _ in range(4):
                yield b"x" * 512

        r = await c.post(url, content=chunks(), headers={"content-type": "application/json"})
        assert r.status_code == 413 and r.json()["error"] == "PayloadTooLarge"
        # 上限以内照常鉴权
        assert (await c.post(url, json={"code": "1"})).status_code == 401
        # 上传文件不受该上限约束（先鉴权，再按 max_upload_bytes 流式计数）
        lid = (await c.post("/v1/leases", json={}, headers=_h(ALICE))).json()["lease_id"]
        r = await c.put(f"/v1/leases/{lid}/files", params={"path": "/tmp/big"}, content=b"x" * 4096, headers=_h(ALICE))
        assert r.status_code == 200


# ---------------- R2-M11 ----------------


async def test_r2_m11_execution_timeout_is_a_result_not_502(make_pool, provider):
    pool = await make_pool(idle_pause_after_s=60)
    await wait_until(lambda: _count_is(pool, READY=5))
    async with await _client(pool) as c:
        lid = (await c.post("/v1/leases", json={})).json()["lease_id"]
        base = f"/v1/leases/{lid}"
        # 用户代码超时不是后端故障：按代码抛出 TimeoutError 返回 200，避免调用方把 502 当故障重试、重跑非幂等代码
        # FakeProvider 会真的 exec 代码：用万一被执行也会立即报错的代码，不要用死循环
        provider.exec_timeouts = 2
        r = await c.post(f"{base}/run_code", json={"code": "long_running()", "timeout_s": 1})
        assert r.status_code == 200 and r.json()["error"]["name"] == "TimeoutError"
        r = await c.post(f"{base}/commands", json={"cmd": "sleep 100", "timeout_s": 1})
        assert r.status_code == 200 and r.json()["exit_code"] == -1 and r.json()["error"].startswith("TimeoutError")
        # 借用不受影响，后续调用照常
        assert (await c.get(base)).json()["state"] == "ACTIVE"
        assert (await c.post(f"{base}/run_code", json={"code": "print(1)"})).json()["stdout"] == "1\n"
        # 后端故障仍是 502
        provider.sandboxes[(await c.get(base)).json()["sandbox_id"]]["state"] = "paused"
        assert (await c.post(f"{base}/run_code", json={"code": "1"})).status_code == 502
    assert (await pool.stats())["events"].get("exec_timeout") == 2


class _TimingOutHandle:
    """SDK 句柄替身：经过 delay_s 后抛出 SDK 的 TimeoutException。"""

    def __init__(self, delay_s: float):
        self.delay_s = delay_s
        self.commands = self

    async def _timeout(self):
        await asyncio.sleep(self.delay_s)
        raise E2BTimeoutException("timed out")

    async def run_code(self, code, language=None, timeout=None):
        await self._timeout()

    async def run(self, cmd, cwd=None, envs=None, timeout=None):
        await self._timeout()


async def test_r2_m11_provider_tells_execution_timeout_from_backend_timeout():
    provider = E2BProvider(api_key="k", api_url="http://api.invalid", domain="invalid")
    run_code = lambda timeout_s: provider.run_code(  # noqa: E731
        "sbx", "x", language=None, timeout_s=timeout_s, sandbox_timeout_s=60
    )
    run_command = lambda timeout_s: provider.run_command(  # noqa: E731
        "sbx", "x", cwd=None, envs=None, timeout_s=timeout_s, sandbox_timeout_s=60
    )
    for call in (run_code, run_command):
        # 调用方给的 timeout_s 用完才超时：执行超时
        provider._cache.put("sbx", _TimingOutHandle(delay_s=1.0))
        with pytest.raises(ExecutionTimeout):
            await call(1.0)
        # 远早于 timeout_s 就超时（连接 / 请求超时）：后端故障，原样抛出（→ 502）
        provider._cache.put("sbx", _TimingOutHandle(delay_s=0))
        with pytest.raises(E2BTimeoutException):
            await call(30)


# ---------------- R2-M1 ----------------


async def test_r2_m1_warmup_connect_uses_ready_platform_timeout(monkeypatch):
    seen = {}

    class Handle:
        async def run_code(self, code, timeout=None):
            return SimpleNamespace(error=None)

    async def connect(sandbox_id, timeout=None, **opts):
        seen["timeout"] = timeout
        return Handle()

    monkeypatch.setattr(AsyncSandbox, "connect", connect)
    provider = E2BProvider(api_key="k", api_url="http://api.invalid", domain="invalid")
    # 本副本没有缓存句柄（例如被 LRU 淘汰）时预热要 connect，而 connect 会重设平台超时：
    # 必须用 READY 的平台超时（关闭暂停时为 max_age_s），写死的 300s 会让沙箱 5 分钟后被平台回收
    await provider.warmup("sbx", "1", sandbox_timeout_s=21600)
    assert seen["timeout"] == 21600


# ---------------- R2-M2 ----------------


async def test_r2_m2_create_and_pause_have_request_timeouts(monkeypatch):
    seen = {}

    async def create(*args, **kw):
        seen["create"] = kw.get("request_timeout")
        return SimpleNamespace(sandbox_id="sbx")

    async def pause(sandbox_id, **kw):
        seen["pause"] = kw.get("request_timeout")

    monkeypatch.setattr(AsyncSandbox, "create", create)
    monkeypatch.setattr(AsyncSandbox, "pause", pause)
    provider = E2BProvider(api_key="k", api_url="http://api.invalid", domain="invalid")
    await provider.create("tpl", {}, 60)
    await provider.pause("sbx")
    # 不传时回落到 SDK 默认的 60s，不小于文档里端到端测试用的 POOL_OP_TIMEOUT_S=60
    assert 0 < seen["create"] < 60 and 0 < seen["pause"] < 60


def test_r2_m2_deadlines_checked_against_request_timeouts():
    from sandbox_pool.provider.e2b_provider import check_deadlines

    check_deadlines(PoolConfig())
    check_deadlines(PoolConfig(op_timeout_s=60))  # 文档里端到端测试用的配置
    with pytest.raises(ValueError, match="POOL_OP_TIMEOUT_S"):
        check_deadlines(PoolConfig(op_timeout_s=30))
    with pytest.raises(ValueError, match="POOL_RESUME_TIMEOUT_S"):
        check_deadlines(PoolConfig(resume_timeout_s=40))
    with pytest.raises(ValueError, match="POOL_DESTROY_TIMEOUT_S"):
        check_deadlines(PoolConfig(destroy_timeout_s=20))


def test_r2_m2_server_refuses_to_start_with_too_short_deadlines(monkeypatch, capsys):
    import sandbox_pool.__main__ as entry

    monkeypatch.setattr(entry.uvicorn, "run", lambda *a, **kw: pytest.fail("server must not start"))
    monkeypatch.setattr(sys, "argv", ["sandbox_pool"])
    monkeypatch.setenv("POOL_OP_TIMEOUT_S", "30")
    with pytest.raises(SystemExit) as exc:
        entry.main()
    assert exc.value.code == 2 and "POOL_OP_TIMEOUT_S" in capsys.readouterr().err
