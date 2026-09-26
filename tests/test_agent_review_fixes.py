"""agent 子系统代码评审（sxw_aicoding/代码评审/2026-09-26-opencode常驻agent子系统代码评审报告.md）的回归用例，
按问题编号（AG-H* / AG-M* / AG-L*）组织。并发类问题都构造了确定的时序，去掉修复后用例会失败。"""

import asyncio
import json
import time

import httpx
import pytest

from sandbox_pool.agent import cron
from sandbox_pool.agent.policy import Injections, build_network, parse_policy
from sandbox_pool.agent.runner import TaskRunner
from sandbox_pool.api.app import create_app
from sandbox_pool.config import PoolConfig
from sandbox_pool.core.lifecycle import Lifecycle
from sandbox_pool.models import AgentUnavailable, InvalidRequest, SandboxState, TaskState
from sandbox_pool.provider.fake import FakeProvider
from tests.conftest import AGENT_FAST, FAST, wait_until
from tests.test_agent_service import collect, final_task, serving


async def _booted(svc, user: str = "u1"):
    agent = await svc.ensure_agent("biz", user)
    await (await svc.start_message(agent, "boot")).done.wait()
    [row] = await serving(svc, agent["id"])
    return agent, row


async def _stale_task(svc, agent, row, provider, *, prompt: str = "[sleep:0.3] 崩溃副本的任务") -> str:
    """模拟另一个副本开了任务后崩溃：会话在跑，任务记录的负责副本心跳已过期。"""
    server = provider.opencode(row["provider_id"])
    sid = server.create_session("orphan")
    server.prompt(sid, prompt)
    now = svc.now()
    status, task = await svc.store.create_task(
        dict(id=f"t-{time.monotonic_ns()}", agent_id=agent["id"], client_id="biz", source="message", schedule_id=None,
             sandbox_row_id=row["id"], session_id=sid, prompt=prompt, result_text=None, error=None, usage=None,
             created_at=now, started_at=now, finished_at=None, deadline=now + 60,
             op_owner="dead-replica", op_deadline=now - 1),
        max_running=5,
    )
    assert status == "ok"
    return task["id"]


# ---------- AG-H1 出网覆盖不能放行内网 / 元数据地址 ----------


@pytest.mark.parametrize(
    "override",
    [
        {"allow_out": ["100.100.100.200"]},
        {"allow_out": ["100.100.100.0/24"]},
        {"allow_out": ["10.1.2.3"]},
        {"allow_out": ["169.254.169.254/32"]},
        {"allow_out": ["172.16.0.0/12"]},
        {"allow_out": ["0.0.0.0/0"]},
        {"allow_out": ["::ffff:10.0.0.1"]},
        {"mode": "allowlist", "allow_out": ["192.168.1.0/24", "example.com"]},
        # 开放模式本来就放行全部公网，域名放行项只会让它绕过 deny_out
        {"allow_out": ["example.com"]},
        {"mode": "open", "allow_out": ["*.github.com"]},
    ],
)
def test_ag_h1_allow_out_cannot_reopen_blocked_ranges(override):
    with pytest.raises(InvalidRequest):
        parse_policy(override)


def test_ag_h1_legit_overrides_still_accepted():
    p = parse_policy({"mode": "allowlist", "allow_out": ["*.github.com", "pypi.org", "8.8.8.0/24"]})
    assert p.allow_out == ("*.github.com", "pypi.org", "8.8.8.0/24")
    p = parse_policy({"deny_out": ["1.2.3.0/24"], "allow_out": ["1.2.3.4"]})
    assert build_network(p, Injections())["allow_out"] == ["1.2.3.4"]
    # 从白名单切回开放模式时要一并清空域名放行项
    with pytest.raises(InvalidRequest):
        parse_policy({"mode": "open"}, base=parse_policy({"mode": "allowlist", "allow_out": ["pypi.org"]}))
    assert parse_policy({"mode": "open", "allow_out": []}, base=parse_policy({"mode": "allowlist", "allow_out": ["pypi.org"]})).allow_out == ()


async def test_ag_h1_stored_legacy_override_is_sanitized_not_fatal(make_agents, provider):
    """修复之前写入的违规覆盖：读取时丢弃违规放行项（只会更严格），装配和维护循环照常工作。"""
    svc = await make_agents()
    agent = await svc.ensure_agent("biz", "u1")
    await svc.store.update_agent(agent["id"], now=svc.now(), egress={"allow_out": ["100.100.100.200", "8.8.8.8"]})
    agent = await svc.store.get_agent_by_id(agent["id"])
    assert svc.effective_policy(agent).allow_out == ("8.8.8.8",)
    assert (await collect(await svc.start_message(agent, "hi")))[-1][1]["state"] == "SUCCEEDED"
    [row] = await serving(svc, agent["id"])
    network = provider.sandboxes[row["provider_id"]]["network"]
    assert "100.100.100.200" not in network["allow_out"] and "100.100.100.200/32" in network["deny_out"]
    with pytest.raises(InvalidRequest):
        await svc.put_egress(agent, {"allow_out": ["10.0.0.0/8"]})


# ---------- AG-H2 设置热更新不能中止刚开始的任务 ----------


async def test_ag_h2_task_arriving_during_reload_waits_and_survives(make_agents, provider):
    """维护循环写配置期间业务系统发来新消息：新任务等重载完成后才开始，不会被 dispose 中止。"""
    svc = await make_agents(run_maintainer=False)
    agent, row = await _booted(svc)
    await svc.update_settings(agent, {"instructions": "新说明"})
    agent = await svc.store.get_agent_by_id(agent["id"])
    [row] = await serving(svc, agent["id"])

    orig = provider.write_files
    started: dict = {}

    async def write_files(sandbox_id, files, *, sandbox_timeout_s):
        if "task" not in started:
            started["task"] = asyncio.create_task(svc.start_message(agent, "[sleep:0.3] 新消息"))
            await asyncio.sleep(0.3)
            assert not started["task"].done()  # 在等重载，没有开始
        await orig(sandbox_id, files, sandbox_timeout_s=sandbox_timeout_s)

    provider.write_files = write_files
    await svc.maintainer.check_sandbox(row, agent, svc.now())
    runner = await started["task"]
    task = await final_task(svc, runner.task_id)
    assert task["state"] == "SUCCEEDED", task["error"]
    assert provider.opencode(row["provider_id"]).disposed == 1
    [row] = await serving(svc, agent["id"])
    assert row["config_version"] == agent["settings_version"] and row["op_owner"] is None


async def test_ag_h2_reload_skipped_while_task_started_after_snapshot(make_agents, provider):
    """维护循环读完「没有运行中任务」之后、重载之前开始的任务：重载放弃（下一轮再试），任务不受影响。"""
    svc = await make_agents(run_maintainer=False)
    agent, row = await _booted(svc)
    await svc.update_settings(agent, {"instructions": "新说明"})
    agent = await svc.store.get_agent_by_id(agent["id"])
    [row] = await serving(svc, agent["id"])

    orig = svc.store.begin_reload
    started: dict = {}

    async def begin_reload(*a, **kw):
        started["runner"] = await svc.start_message(agent, "[sleep:0.3] 新消息")
        return await orig(*a, **kw)

    svc.store.begin_reload = begin_reload
    await svc.maintainer.check_sandbox(row, agent, svc.now())
    task = await final_task(svc, started["runner"].task_id)
    assert task["state"] == "SUCCEEDED", task["error"]
    assert provider.opencode(row["provider_id"]).disposed == 0
    # 空闲之后下一轮照常应用
    svc.store.begin_reload = orig
    [row] = await serving(svc, agent["id"])
    await svc.maintainer.check_sandbox(row, agent, svc.now())
    [row] = await serving(svc, agent["id"])
    assert row["config_version"] == agent["settings_version"]
    assert provider.opencode(row["provider_id"]).disposed == 1


async def test_ag_h2_reload_marker_expires_after_crash(make_agents):
    """执行重载的副本崩溃：占用到 op_deadline 自动失效，新任务照常准入。"""
    svc = await make_agents(run_maintainer=False)
    agent, row = await _booted(svc)
    now = svc.now()
    assert await svc.store.begin_reload(row["id"], owner="dead-replica", now=now, op_deadline=now + 0.5)
    assert not await svc.store.begin_reload(row["id"], owner=svc.replica_id, now=now, op_deadline=now + 90)
    t0 = time.monotonic()
    runner = await svc.start_message(agent, "hi")
    assert time.monotonic() - t0 >= 0.3
    assert (await collect(runner))[-1][1]["state"] == "SUCCEEDED"


# ---------- AG-M1 跨副本中止记为 ABORTED ----------


async def test_ag_m1_abort_via_other_replica_is_aborted(make_agents):
    a, b = await make_agents(), await make_agents()
    agent = await a.ensure_agent("biz", "u1")
    runner = await a.start_message(agent, "[sleep:5] 长任务")
    await asyncio.wait_for(runner.subscribe().get(), 5)
    await asyncio.sleep(0.2)
    await b.abort_task(agent, runner.task_id)
    task = await final_task(a, runner.task_id)
    assert task["state"] == TaskState.ABORTED.value, task["error"]


async def test_ag_m1_abort_on_owner_replica_is_aborted(make_agents):
    svc = await make_agents()
    agent = await svc.ensure_agent("biz", "u1")
    for _ in range(3):
        runner = await svc.start_message(agent, "[sleep:5] 长任务")
        await asyncio.wait_for(runner.subscribe().get(), 5)
        await svc.abort_task(agent, runner.task_id)
        assert (await final_task(svc, runner.task_id))["state"] == TaskState.ABORTED.value


# ---------- AG-M2 清理客户端不能关掉正在使用的连接 ----------


async def test_ag_m2_prune_keeps_clients_of_live_sandboxes(make_agents, provider):
    svc = await make_agents(run_maintainer=False)
    agent = await svc.ensure_agent("biz", "u1")
    # 沙箱在维护循环一轮中间转为 ACTIVE、任务开始；本轮末尾清理客户端
    runner = await svc.start_message(agent, "[sleep:0.5] hi")
    await svc.maintainer._prune_clients()
    assert not runner.client.closed
    assert (await collect(runner))[-1][1]["state"] == "SUCCEEDED"
    [row] = await serving(svc, agent["id"])
    client = svc.client_for(row)
    await svc.reset(agent)
    await svc.maintainer._prune_clients()
    assert client.closed and row["id"] not in svc._clients


# ---------- AG-M3 同进程共用一个写引擎 ----------


async def test_ag_m3_app_shares_pool_engine(tmp_path):
    cfg = PoolConfig(db_url=f"sqlite+aiosqlite:///{tmp_path}/app.db", **{**FAST, **AGENT_FAST, "target_size": 0})
    app = create_app(cfg, provider=FakeProvider())
    async with app.router.lifespan_context(app):
        agents, pool = app.state.agents, app.state.pool
        assert agents.store.engine is pool.store.engine
        assert agents.store.read_engine is pool.store.read_engine
        transport = httpx.ASGITransport(app=app)
        async with httpx.AsyncClient(transport=transport, base_url="http://t") as c:
            r = await c.post("/v1/agents/u1/messages", json={"text": "hi", "stream": False})
            assert r.status_code == 200 and r.json()["state"] == "SUCCEEDED", r.text


# ---------- AG-M4 重连回放不缺段 ----------


async def test_ag_m4_history_merges_text_deltas(make_agents):
    svc = await make_agents(run_maintainer=False)
    agent, row = await _booted(svc)
    task = dict((await svc.store.list_tasks(agent["id"]))[0])
    runner = TaskRunner(svc, task, row, text=None)
    early = runner.subscribe()
    deltas = [f"{i:05d}" for i in range(6000)]
    for d in deltas[:3000]:
        runner._publish("text", {"delta": d})
    runner._publish("tool", {"tool": "bash", "status": "completed"})
    for d in deltas[3000:]:
        runner._publish("text", {"delta": d})
    late = runner.subscribe()
    replay = [late.get_nowait() for _ in range(late.qsize())]
    assert [k for k, _ in replay] == ["text", "tool", "text"]
    assert replay[0][1]["delta"] + replay[2][1]["delta"] == "".join(deltas)
    # 早订阅者收到的是逐条增量，且没有被合并改写
    got = [early.get_nowait() for _ in range(early.qsize())]
    assert len(got) == 6001 and "".join(d["delta"] for k, d in got if k == "text") == "".join(deltas)


# ---------- AG-M5 停机等待后台任务不能空转（Python 3.12 的 gather 对已结束任务不让出事件循环） ----------


async def test_ag_m5_wait_background_does_not_spin_on_finished_task(monkeypatch):
    lc = Lifecycle(PoolConfig(), store=None, provider=None, replica_id="r")
    finished = asyncio.create_task(asyncio.sleep(0))
    await asyncio.sleep(0.01)
    # 任务已结束，但把它移出集合的 discard 回调还没执行（刚结束时的真实状态）
    lc._tasks.add(finished)
    calls = 0
    real_gather = asyncio.gather

    def counting_gather(*a, **kw):
        nonlocal calls
        calls += 1
        if calls > 100:  # 未修复时这里是不让出事件循环的死循环，用计数让用例失败而不是卡住
            raise RuntimeError("wait_background is spinning")
        return real_gather(*a, **kw)

    monkeypatch.setattr(asyncio, "gather", counting_gather)
    await lc.wait_background()
    slow = lc.spawn(asyncio.sleep(0.05))
    await lc.wait_background()
    assert slow.done() and calls <= 2


# ---------- AG-L1 停机中不接管任务、不触发定时任务 ----------


async def test_ag_l1_stopping_replica_does_not_take_over(make_agents, provider):
    a = await make_agents(run_maintainer=False)
    agent, row = await _booted(a)
    task_id = await _stale_task(a, agent, row, provider)
    a.stopping = True
    await a.maintainer.takeover_tasks(a.now())
    assert not a.runners
    assert (await a.store.get_task(task_id))["op_owner"] == "dead-replica"
    s = await a.create_schedule(agent, {"name": "n", "prompt": "p", "every_s": 60})
    await a.store.update_schedule(s["id"], next_run_at=a.now() - 1)
    await a.maintainer.fire_schedules(a.now())
    assert (await a.store.get_schedule(s["id"]))["next_run_at"] < a.now()  # 没有被本副本推进
    b = await make_agents()
    assert (await final_task(b, task_id))["state"] == "SUCCEEDED"


async def test_ag_l1_graceful_stop_leaves_no_runner_behind(make_agents):
    a, b = await make_agents(), await make_agents()
    agent = await a.ensure_agent("biz", "u1")
    runner = await a.start_message(agent, "[sleep:1] 交接")
    await asyncio.wait_for(runner.subscribe().get(), 5)
    await a.stop(grace_s=5)
    assert not a.runners and not [t for t in a._runner_tasks if not t.done()]
    task = await final_task(b, runner.task_id)
    assert task["state"] == "SUCCEEDED" and task["op_owner"] is None


# ---------- AG-L2 接管要求心跳仍过期 ----------


async def test_ag_l2_takeover_cas_requires_stale_heartbeat(make_agents, provider):
    svc = await make_agents(run_maintainer=False)
    agent, row = await _booted(svc)
    task_id = await _stale_task(svc, agent, row, provider)
    now = svc.now()
    # 读到过期之后，原负责副本又续上了心跳
    assert await svc.store.cas_task(task_id, [TaskState.RUNNING], expect_owner="dead-replica", op_deadline=now + 60)
    assert not await svc.store.cas_task(
        task_id, [TaskState.RUNNING], expect_owner="dead-replica", expect_stale_before=now, op_owner=svc.replica_id
    )
    assert (await svc.store.get_task(task_id))["op_owner"] == "dead-replica"


# ---------- AG-L3 手动触发的失败要报错，不能报成「跳过」 ----------


async def test_ag_l3_manual_run_reports_errors(make_agents):
    svc = await make_agents(agent_max_sandboxes=1)
    await _booted(svc, "u1")  # 唯一的名额被 u1 占着
    other = await svc.ensure_agent("biz", "u2")
    s = await svc.create_schedule(other, {"name": "n", "prompt": "p", "every_s": 3600})
    with pytest.raises(AgentUnavailable):
        await svc.run_schedule_now(other, s["id"])
    assert await svc.fire_schedule(s) is None  # 自动触发仍只记事件


# ---------- AG-L4 PATCH 只有时间字段变化才重算 next_run_at ----------


async def test_ag_l4_patch_keeps_interval_phase(make_agents):
    svc = await make_agents(run_maintainer=False)
    agent = await svc.ensure_agent("biz", "u1")
    s = await svc.create_schedule(agent, {"name": "n", "prompt": "p", "every_s": 3600})
    first = s["next_run_at"]
    await asyncio.sleep(0.05)
    assert (await svc.update_schedule(agent, s["id"], {"name": "改名", "prompt": "新提示词"}))["next_run_at"] == first
    changed = await svc.update_schedule(agent, s["id"], {"every_s": 7200})
    assert changed["next_run_at"] > first
    assert (await svc.update_schedule(agent, s["id"], {"enabled": False}))["next_run_at"] is None
    assert (await svc.update_schedule(agent, s["id"], {"enabled": True}))["next_run_at"] is not None


# ---------- AG-L5 永不触发的 cron 快速失败 ----------


def test_ag_l5_never_firing_cron_fails_fast():
    t0 = time.perf_counter()
    with pytest.raises(ValueError, match="never fires"):
        cron.next_fire("0 0 31 2 *", "Asia/Shanghai", time.time())
    assert time.perf_counter() - t0 < 0.05
    # 最稀疏的合法表达式：2100 年不是闰年，2096 之后的 2 月 29 日是 2104 年
    after = time.mktime((2097, 1, 1, 0, 0, 0, 0, 0, 0))
    assert time.gmtime(cron.next_fire("0 0 29 2 *", "UTC", after)).tm_year == 2104


# ---------- AG-L14 配置了入口 IP 时访问 opencode 不走 HTTPS_PROXY ----------


async def test_ag_l14_ingress_ip_bypasses_https_proxy(monkeypatch):
    from sandbox_pool.agent.opencode import OpencodeHttpClient

    monkeypatch.setenv("HTTPS_PROXY", "http://127.0.0.1:7890")
    endpoint = "https://4096-sbx-1.cn-hangzhou.e2b.fc.aliyuncs.com"
    direct = OpencodeHttpClient(endpoint, "tok", "/w", ingress_ip="47.111.182.73")
    proxied = OpencodeHttpClient(endpoint, "tok", "/w")
    try:
        # 经代理隧道时 TLS 用目标地址（IP）做 server_hostname，证书校验必然失败，所以直连入口 IP
        assert direct.proxy is None and not direct._http._mounts
        assert direct._ext == {"sni_hostname": "4096-sbx-1.cn-hangzhou.e2b.fc.aliyuncs.com"}
        assert proxied.proxy == "http://127.0.0.1:7890" and proxied._http._mounts
    finally:
        await direct.close()
        await proxied.close()


# ---------- AG-L13 测试替身与真实行为一致 ----------


async def test_ag_l13_fake_client_fails_after_close_and_dispose_cancels(make_agents, provider):
    svc = await make_agents(run_maintainer=False)
    agent, row = await _booted(svc)
    client = provider.app_client(row["endpoint"], row["access_token"], "/home/user/workspace", ingress_ip=None)
    runner = await svc.start_message(agent, "[sleep:5] 会被 dispose 中止")
    await asyncio.wait_for(runner.subscribe().get(), 5)
    await asyncio.sleep(0.1)
    await client.dispose()
    task = await final_task(svc, runner.task_id)
    assert task["state"] == "FAILED" and "aborted" in (task["error"] or "")
    await client.close()
    with pytest.raises(RuntimeError, match="closed"):
        await client.health()
    assert json.dumps(await svc.agent_info(agent))  # 其他副本 / 客户端不受影响
    assert (await serving(svc, agent["id"]))[0]["state"] == SandboxState.ACTIVE.value
