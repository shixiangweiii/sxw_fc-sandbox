"""agent 子系统（FakeProvider + 内存版 opencode）：沙箱装配与复用、对话任务、会话、并发、断线重连、中止 / 超时、
空闲销毁、轮换、健康检查、出网策略与设置下发、定时任务、任务接管、对账。"""

import asyncio
import json

import pytest

from sandbox_pool.agent.policy import EGRESS_FILE
from sandbox_pool.models import (
    AgentUnavailable,
    InvalidRequest,
    SandboxOpError,
    SandboxState,
    TaskConflict,
    TaskState,
    TooManyTasks,
)
from tests.conftest import wait_until

WORKDIR = "/home/user/workspace"


async def collect(runner, timeout: float = 10) -> list[tuple[str, dict]]:
    q = runner.subscribe()
    out = []
    try:
        while True:
            item = await asyncio.wait_for(q.get(), timeout)
            if item is None:
                return out
            out.append(item)
    finally:
        runner.unsubscribe(q)


async def final_task(svc, task_id: str, timeout: float = 10) -> dict:
    async def done():
        t = await svc.store.get_task(task_id)
        return t if t and t["state"] != TaskState.RUNNING.value else None

    return await wait_until(done, timeout)


async def serving(svc, agent_id: str) -> list[dict]:
    return await svc.store.agent_sandboxes(agent_id, [SandboxState.ACTIVE, SandboxState.RETIRING])


def creates(provider) -> int:
    return sum(1 for c in provider.calls if c[0] == "create_app")


async def test_first_message_boots_sandbox_streams_and_reuses(make_agents, provider):
    svc = await make_agents()
    agent = await svc.ensure_agent("biz", "u1")
    runner = await svc.start_message(agent, "你好")
    events = await collect(runner)
    kinds = [k for k, _ in events]
    assert kinds[0] == "start" and kinds[-1] == "done"
    assert "".join(d["delta"] for k, d in events if k == "text") == "echo: 你好"
    done = events[-1][1]
    assert done["state"] == "SUCCEEDED" and done["result"] == "echo: 你好" and done["usage"]["output"] > 0
    task = await svc.store.get_task(runner.task_id)
    assert task["state"] == "SUCCEEDED" and task["session_id"] == events[0][1]["session_id"]

    [row] = await serving(svc, agent["id"])
    assert row["state"] == "ACTIVE" and row["endpoint"] and row["access_token"]
    assert row["network_version"] == svc.policy_version(agent) and row["config_version"] == agent["settings_version"]
    sb = provider.sandboxes[row["provider_id"]]
    # 出网：内网与元数据屏蔽、模型 Key 注入；沙箱里只有占位符
    assert "100.100.100.200/32" in sb["network"]["deny_out"]
    assert sb["network"]["rules"]["api.deepseek.com"][0]["transform"]["headers"]["Authorization"] == "Bearer sk-test-key"
    conf = json.loads(sb["files"][f"{WORKDIR}/opencode.json"])
    assert conf["provider"]["deepseek"]["options"]["apiKey"] == "injected-by-platform"
    assert b"sk-test-key" not in b"".join(sb["files"].values())
    assert json.loads(sb["files"][EGRESS_FILE])["mode"] == "open"
    assert b"egress.json" in sb["files"][f"{WORKDIR}/AGENTS.md"]

    runner2 = await svc.start_message(agent, "第二个问题")
    assert (await collect(runner2))[-1][1]["state"] == "SUCCEEDED"
    assert creates(provider) == 1


async def test_session_continuity_and_busy_session(make_agents):
    svc = await make_agents()
    agent = await svc.ensure_agent("biz", "u1")
    r1 = await svc.start_message(agent, "[sleep:0.4] 第一轮")
    start = (await asyncio.wait_for(r1.subscribe().get(), 5))[1]
    sid = start["session_id"]
    with pytest.raises(TaskConflict):
        await svc.start_message(agent, "插队", session_id=sid)
    await r1.done.wait()
    r2 = await svc.start_message(agent, "[remember]", session_id=sid)
    done = (await collect(r2))[-1][1]
    assert done["result"] == "remembered: [sleep:0.4] 第一轮"
    with pytest.raises(TaskConflict):
        await svc.start_message(agent, "x", session_id="ses_unknown")


async def test_concurrent_first_requests_on_two_replicas_create_one_sandbox(make_agents, provider):
    a, b = await make_agents(), await make_agents()
    agent = await a.ensure_agent("biz", "u1")
    r1, r2 = await asyncio.gather(a.start_message(agent, "one"), b.start_message(agent, "two"))
    assert (await collect(r1))[-1][1]["state"] == "SUCCEEDED"
    assert (await collect(r2))[-1][1]["state"] == "SUCCEEDED"
    assert creates(provider) == 1


async def test_max_running_tasks_and_capacity(make_agents):
    svc = await make_agents(agent_max_running_tasks=1, agent_max_sandboxes=1)
    agent = await svc.ensure_agent("biz", "u1")
    r = await svc.start_message(agent, "[sleep:0.5] long")
    with pytest.raises(TooManyTasks):
        await svc.start_message(agent, "another")
    other = await svc.ensure_agent("biz", "u2")
    with pytest.raises(AgentUnavailable):
        await svc.start_message(other, "no room")
    await r.done.wait()


async def test_disconnect_keeps_task_running_and_reattach(make_agents):
    svc = await make_agents()
    agent = await svc.ensure_agent("biz", "u1")
    runner = await svc.start_message(agent, "[sleep:0.3] 慢任务")
    q = runner.subscribe()
    await asyncio.wait_for(q.get(), 5)
    runner.unsubscribe(q)  # 客户端断开
    # 运行中重连（本副本）：拿到完整历史直到结束
    items = [i async for i in svc.attach(agent, runner.task_id)]
    assert items[0][0] == "start" and items[-2][0] == "done" and items[-1] is None
    task = await final_task(svc, runner.task_id)
    assert task["state"] == "SUCCEEDED" and task["result_text"] == "echo: [sleep:0.3] 慢任务"
    # 已结束的任务重连直接返回 done
    items = [i async for i in svc.attach(agent, runner.task_id)]
    assert len(items) == 1 and items[0][0] == "done" and items[0][1]["state"] == "SUCCEEDED"


async def test_reattach_from_another_replica(make_agents):
    a, b = await make_agents(), await make_agents()
    agent = await a.ensure_agent("biz", "u1")
    runner = await a.start_message(agent, "[sleep:0.6] 远程")
    await asyncio.wait_for(runner.subscribe().get(), 5)
    items = []
    async for item in b.attach(agent, runner.task_id):
        items.append(item)
        if item is None or item[0] == "done":
            break
    assert items[0][0] == "start" and items[0][1]["attached"] is True
    assert items[-1][0] == "done" and items[-1][1]["state"] == "SUCCEEDED"


async def test_abort_and_timeout(make_agents, provider):
    svc = await make_agents()
    agent = await svc.ensure_agent("biz", "u1")
    runner = await svc.start_message(agent, "[sleep:5] 会被中止")
    await asyncio.wait_for(runner.subscribe().get(), 5)
    await svc.abort_task(agent, runner.task_id)
    task = await final_task(svc, runner.task_id)
    assert task["state"] == "ABORTED"
    with pytest.raises(TaskConflict):
        await svc.abort_task(agent, runner.task_id)

    runner = await svc.start_message(agent, "[sleep:5] 会超时", max_duration_s=0.5)
    task = await final_task(svc, runner.task_id, timeout=8)
    assert task["state"] == "TIMEOUT"
    [row] = await serving(svc, agent["id"])
    # 中止接口与跟进任务的 runner 都会调用 abort（幂等），两个任务的会话都被中止过
    assert len(set(provider.opencode(row["provider_id"]).aborts)) == 2


async def test_model_error_and_auto_reject(make_agents, provider):
    svc = await make_agents()
    agent = await svc.ensure_agent("biz", "u1")
    r = await svc.start_message(agent, "[error]")
    done = (await collect(r))[-1][1]
    assert done["state"] == "FAILED" and done["error"] == "fake model error"
    r = await svc.start_message(agent, "[ask] 危险操作")
    assert (await collect(r))[-1][1]["state"] == "SUCCEEDED"
    [row] = await serving(svc, agent["id"])
    assert list(provider.opencode(row["provider_id"]).permission_replies.values()) == ["reject"]


async def test_reasoning_tool_and_retry_events(make_agents):
    svc = await make_agents()
    agent = await svc.ensure_agent("biz", "u1")
    events = await collect(await svc.start_message(agent, "[retry][reasoning][tool] 都来"))
    kinds = [k for k, _ in events]
    assert "status" in kinds and "reasoning" in kinds
    tools = [d for k, d in events if k == "tool"]
    assert [t["status"] for t in tools] == ["running", "completed"] and tools[1]["output"] == "x86_64\n"


async def test_idle_destroy_then_recreate_and_schedule_tail(make_agents, provider):
    svc = await make_agents(agent_schedule_idle_tail_s=0.3)
    agent = await svc.ensure_agent("biz", "u1")
    await svc.update_settings(agent, {"idle_destroy_after_s": 0.6})
    agent = await svc.store.get_agent_by_id(agent["id"])
    await (await svc.start_message(agent, "hi")).done.wait()
    assert await serving(svc, agent["id"])
    await wait_until(lambda: _gone(svc, agent["id"]), timeout=5)
    await (await svc.start_message(agent, "again")).done.wait()
    assert creates(provider) == 2

    # 定时任务唤醒的沙箱：结束后按更短的收尾时间销毁
    await svc.update_settings(agent, {"idle_destroy_after_s": 300})
    sched = await svc.create_schedule(agent, {"name": "s", "every_s": 3600, "prompt": "定时"})
    runner = await svc.fire_schedule(sched)
    await runner.done.wait()
    await wait_until(lambda: _gone(svc, agent["id"]), timeout=5)


async def _gone(svc, agent_id):
    return not await svc.store.agent_sandboxes(agent_id)


async def test_rotation_idle_destroys_busy_retires(make_agents, provider):
    svc = await make_agents(agent_rotate_after_s=1.0, agent_max_life_s=60, agent_task_max_duration_s=30)
    agent = await svc.ensure_agent("biz", "u1")
    await (await svc.start_message(agent, "hi")).done.wait()
    await wait_until(lambda: _gone(svc, agent["id"]), timeout=5)  # 到达轮换时间且空闲：销毁

    busy = await svc.start_message(agent, "[sleep:2] 跑着")
    await wait_until(lambda: _states(svc, agent["id"], "RETIRING"), timeout=5)
    # 旧沙箱 RETIRING 期间来的新任务去新沙箱
    fresh = await svc.start_message(agent, "新任务")
    assert fresh.sandbox["id"] != busy.sandbox["id"]
    await fresh.done.wait()
    assert (await final_task(svc, busy.task_id))["state"] == "SUCCEEDED"
    await wait_until(lambda: _no_row(svc, busy.sandbox["id"]), timeout=5)


async def _states(svc, agent_id, state):
    return any(r["state"] == state for r in await svc.store.agent_sandboxes(agent_id))


async def _no_row(svc, row_id):
    return await svc.store.get_sandbox(row_id) is None


async def test_hard_deadline_fails_running_task(make_agents):
    svc = await make_agents()
    agent = await svc.ensure_agent("biz", "u1")
    runner = await svc.start_message(agent, "[sleep:10] 到期")
    await asyncio.wait_for(runner.subscribe().get(), 5)
    row = runner.sandbox
    await svc.store.cas_sandbox(row["id"], [SandboxState.ACTIVE], now=svc.now(),
                                created_at=svc.now() - svc.cfg.agent_max_life_s - 1)
    task = await final_task(svc, runner.task_id)
    assert task["state"] == "FAILED" and "max lifetime" in task["error"]
    await wait_until(lambda: _no_row(svc, row["id"]), timeout=5)


async def test_unhealthy_sandbox_is_destroyed(make_agents, provider):
    svc = await make_agents()
    agent = await svc.ensure_agent("biz", "u1")
    runner = await svc.start_message(agent, "[sleep:10] 跑着")
    await asyncio.wait_for(runner.subscribe().get(), 5)
    provider.opencode(runner.sandbox["provider_id"]).healthy = False
    task = await final_task(svc, runner.task_id, timeout=10)
    assert task["state"] == "FAILED"
    await wait_until(lambda: _gone(svc, agent["id"]), timeout=5)


async def test_egress_override_applied_and_readable(make_agents, provider):
    svc = await make_agents()
    agent = await svc.ensure_agent("biz", "u1")
    await (await svc.start_message(agent, "hi")).done.wait()
    [row] = await serving(svc, agent["id"])
    with pytest.raises(InvalidRequest):
        await svc.put_egress(agent, {"deny_out": ["www.baidu.com"]})
    out = await svc.put_egress(agent, {"mode": "allowlist", "allow_out": ["pypi.org", "*.github.com"]})
    assert out["desired"]["mode"] == "allowlist" and out["desired"]["deny_out"] == ["0.0.0.0/0"]
    [sb_view] = out["sandboxes"]
    assert sb_view["in_sync"] is True
    assert "sk-test-key" not in json.dumps(out)
    assert sb_view["platform"]["rules"]["api.deepseek.com"] == {"Authorization": "***"}
    net = provider.sandboxes[row["provider_id"]]["network"]
    assert net["deny_out"] == ["0.0.0.0/0"] and "pypi.org" in net["allow_out"]
    egress_file = json.loads(provider.sandboxes[row["provider_id"]]["files"][EGRESS_FILE])
    assert egress_file["mode"] == "allowlist" and egress_file["version"] == out["desired"]["version"]
    # 恢复默认
    out = await svc.put_egress(agent, None)
    assert out["desired"]["mode"] == "open" and out["sandboxes"][0]["in_sync"]


async def test_egress_apply_failure_is_retried_by_maintainer(make_agents, provider):
    svc = await make_agents()
    agent = await svc.ensure_agent("biz", "u1")
    await (await svc.start_message(agent, "hi")).done.wait()
    provider.fail_update_network = 1
    out = await svc.put_egress(agent, {"deny_out": ["8.8.4.4"]})
    assert out["sandboxes"][0]["in_sync"] is False
    agent = await svc.store.get_agent_by_id(agent["id"])

    async def synced():
        [row] = await serving(svc, agent["id"])
        return row["network_version"] == svc.policy_version(agent)

    await wait_until(synced, timeout=5)


async def test_settings_change_rewrites_config_and_reloads(make_agents, provider):
    svc = await make_agents()
    agent = await svc.ensure_agent("biz", "u1")
    await (await svc.start_message(agent, "hi")).done.wait()
    info = await svc.update_settings(
        agent, {"instructions": "所有回答都用中文", "mcp": {"fs": {"type": "local", "command": ["npx", "fs"]}}}
    )
    assert info["settings_version"] == 2

    async def applied():
        [row] = await serving(svc, agent["id"])
        return row if row["config_version"] == 2 else None

    row = await wait_until(applied, timeout=5)
    files = provider.sandboxes[row["provider_id"]]["files"]
    assert "所有回答都用中文" in files[f"{WORKDIR}/AGENTS.md"].decode()
    assert "fs" in json.loads(files[f"{WORKDIR}/opencode.json"])["mcp"]
    assert provider.opencode(row["provider_id"]).disposed >= 1


async def test_schedule_validation_and_overlap_skip(make_agents):
    svc = await make_agents()
    agent = await svc.ensure_agent("biz", "u1")
    for bad in (
        {"name": "x", "prompt": "p"},
        {"name": "x", "prompt": "p", "cron": "* * * * *", "every_s": 60},
        {"name": "x", "prompt": "p", "every_s": 10},
        {"name": "x", "prompt": "p", "cron": "61 * * * *"},
        {"name": "x", "prompt": "p", "cron": "0 9 * * *", "timezone": "Mars/Base"},
    ):
        with pytest.raises(InvalidRequest):
            await svc.create_schedule(agent, bad)
    s = await svc.create_schedule(agent, {"name": "日报", "cron": "0 9 * * *", "prompt": "[sleep:0.5] 写日报"})
    assert s["next_run_at"] > svc.now() and s["overlap"] == "skip"
    first = await svc.fire_schedule(s)
    assert first is not None
    assert await svc.fire_schedule(s) is None  # 上一次还在运行：跳过
    await first.done.wait()
    tasks = await svc.store.list_tasks(agent["id"], schedule_id=s["id"])
    assert len(tasks) == 1 and tasks[0]["source"] == "schedule" and tasks[0]["state"] == "SUCCEEDED"
    s = await svc.update_schedule(agent, s["id"], {"cron": None, "every_s": 120, "enabled": False})
    assert s["every_s"] == 120 and s["enabled"] == 0 and s["next_run_at"] is None


async def test_due_schedule_fires_once_across_replicas(make_agents):
    a, b = await make_agents(), await make_agents()
    agent = await a.ensure_agent("biz", "u1")
    s = await a.create_schedule(agent, {"name": "每分钟", "every_s": 60, "prompt": "定时任务"})
    due = a.now()
    await a.store.update_schedule(s["id"], next_run_at=due)

    async def ran():
        tasks = await a.store.list_tasks(agent["id"], schedule_id=s["id"])
        return tasks if tasks and all(t["state"] != "RUNNING" for t in tasks) else None

    tasks = await wait_until(ran, timeout=10)
    await asyncio.sleep(0.5)  # 再跑几轮维护循环，确认不会重复触发
    tasks = await a.store.list_tasks(agent["id"], schedule_id=s["id"])
    assert len(tasks) == 1 and tasks[0]["result_text"] == "echo: 定时任务"
    fresh = await a.store.get_schedule(s["id"])
    # 从应触发时刻推进一个间隔，不随维护循环的延迟漂移
    assert fresh["next_run_at"] == pytest.approx(due + 60) and fresh["last_task_id"] == tasks[0]["id"]


async def test_missed_schedule_is_skipped_not_replayed(make_agents):
    svc = await make_agents()
    agent = await svc.ensure_agent("biz", "u1")
    s = await svc.create_schedule(agent, {"name": "错过", "every_s": 60, "prompt": "x"})
    await svc.store.update_schedule(s["id"], next_run_at=svc.now() - 3600)
    await wait_until(lambda: _advanced(svc, s["id"]), timeout=5)
    assert await svc.store.list_tasks(agent["id"], schedule_id=s["id"]) == []


async def _advanced(svc, sid):
    s = await svc.store.get_schedule(sid)
    return s["next_run_at"] > svc.now()


async def test_crashed_replica_task_is_taken_over(make_agents, provider):
    svc = await make_agents()
    agent = await svc.ensure_agent("biz", "u1")
    await (await svc.start_message(agent, "boot")).done.wait()
    [row] = await serving(svc, agent["id"])
    # 模拟另一个副本开了任务后崩溃：会话在跑，任务记录的负责副本心跳已过期
    server = provider.opencode(row["provider_id"])
    sid = server.create_session("orphan")
    server.prompt(sid, "[sleep:0.5] 崩溃副本的任务")
    now = svc.now()
    status, task = await svc.store.create_task(
        dict(id="t-crashed", agent_id=agent["id"], client_id="biz", source="message", schedule_id=None,
             sandbox_row_id=row["id"], session_id=sid, prompt="[sleep:0.5] 崩溃副本的任务", result_text=None,
             error=None, usage=None, created_at=now, started_at=now, finished_at=None, deadline=now + 60,
             op_owner="dead-replica", op_deadline=now - 1),
        max_running=5,
    )
    assert status == "ok"
    task = await final_task(svc, "t-crashed")
    assert task["state"] == "SUCCEEDED" and task["result_text"] == "echo: [sleep:0.5] 崩溃副本的任务"


async def test_graceful_stop_hands_task_to_other_replica(make_agents):
    a, b = await make_agents(), await make_agents()
    agent = await a.ensure_agent("biz", "u1")
    runner = await a.start_message(agent, "[sleep:1.5] 交接")
    await asyncio.wait_for(runner.subscribe().get(), 5)
    await a.stop(grace_s=5)
    task = await final_task(b, runner.task_id, timeout=10)
    assert task["state"] == "SUCCEEDED"


async def test_reconcile_kills_orphans_and_drops_vanished(make_agents, provider):
    svc = await make_agents(agent_health_interval_s=60)
    agent = await svc.ensure_agent("biz", "u1")
    await (await svc.start_message(agent, "hi")).done.wait()
    [row] = await serving(svc, agent["id"])
    orphan = await provider.create_app("tpl", {"pool": "agents"}, 60, port=4096, network={})
    await provider.kill(row["provider_id"])  # 平台侧消失
    await wait_until(lambda: orphan.sandbox_id not in provider.sandboxes, timeout=5)
    await wait_until(lambda: _gone(svc, agent["id"]), timeout=5)


async def test_reset_and_session_after_reset(make_agents):
    svc = await make_agents()
    agent = await svc.ensure_agent("biz", "u1")
    events = await collect(await svc.start_message(agent, "hi"))
    sid = events[0][1]["session_id"]
    assert (await svc.reset(agent))["destroyed"] == 1
    with pytest.raises(TaskConflict):
        await svc.start_message(agent, "继续", session_id=sid)
    assert (await collect(await svc.start_message(agent, "新会话")))[-1][1]["state"] == "SUCCEEDED"


async def test_boot_failures_retry_then_give_up(make_agents, provider):
    svc = await make_agents()
    agent = await svc.ensure_agent("biz", "u1")
    provider.fail_create_app = 1
    assert (await collect(await svc.start_message(agent, "hi")))[-1][1]["state"] == "SUCCEEDED"
    other = await svc.ensure_agent("biz", "u2")
    provider.fail_create_app = 10
    with pytest.raises(SandboxOpError, match="failed to start 3 times"):
        await svc.start_message(other, "hi")


async def test_admin_list_hides_access_token(make_agents):
    svc = await make_agents()
    agent = await svc.ensure_agent("biz", "u1")
    await (await svc.start_message(agent, "hi")).done.wait()
    listed = await svc.admin_list()
    assert listed and "access_token" not in json.dumps(listed)
    info = await svc.agent_info(agent)
    assert info["sandboxes"] and "access_token" not in info["sandboxes"][0]
