"""pi 引擎与多引擎：事件翻译、结果提取、配置渲染（纯逻辑）；pi 沙箱的对话、中止、超时、进程丢失、接管、
断线重连、设置下发、定时任务（FakeProvider + 内存版 pi 桥接进程）；引擎选择与切换；接口。

事件样本取自本机真实 DeepSeek 实测（pi 0.87.1），见 sxw_aicoding/方案设计/2026-09-26-pi引擎接入-实施方案.md 第 2 节。
"""

import asyncio
import json

import httpx
import pytest

from sandbox_pool.agent.engines.pi import PI_CONFIG_FILE, PI_MCP_FILE, PiEngine, PiTranslator, extract_result, mcp_servers
from sandbox_pool.agent.engines.base import FilesContext
from sandbox_pool.agent.engines.opencode import OpencodeEngine
from sandbox_pool.agent.policy import EGRESS_FILE, validate_settings
from sandbox_pool.agent.service import check_agent_config
from sandbox_pool.api.app import create_app
from sandbox_pool.config import PoolConfig
from sandbox_pool.models import InvalidRequest, SandboxState, TaskConflict
from tests.conftest import wait_until
from tests.test_agent_api import h, parse_sse
from tests.test_agent_service import collect, creates, final_task, serving

WORKDIR = "/home/user/workspace"
PI = dict(agent_pi_template="tpl-pi", agent_default_engine="pi")
BOTH = dict(agent_pi_template="tpl-pi")


def ev(sid: str, event: dict, run: str = "r1") -> dict:
    return {"type": "pi.event", "properties": {"sessionID": sid, "runID": run, "event": event}}


def usage(i, o, cache=0, reasoning=0, cost=0.0):
    return {"input": i, "output": o, "cacheRead": cache, "cacheWrite": 0, "reasoning": reasoning,
            "totalTokens": i + o + cache, "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "total": cost}}


# ---------- 纯逻辑：翻译 ----------


def test_pi_translator_real_event_sequence():
    tr = PiTranslator("s1")
    stream = [
        ev("s1", {"type": "agent_start"}),
        ev("s1", {"type": "message_start", "message": {"role": "user", "content": "q"}}),
        ev("s1", {"type": "message_update", "usage": usage(0, 0),
                  "assistantMessageEvent": {"type": "thinking_delta", "contentIndex": 0, "delta": "The"}}),
        ev("s1", {"type": "tool_execution_start", "toolCallId": "call_00", "toolName": "bash",
                  "args": {"command": "echo hello-pi && uname -s", "timeout": 30}}),
        ev("s1", {"type": "tool_execution_update", "toolCallId": "call_00", "toolName": "bash"}),
        ev("s1", {"type": "tool_execution_end", "toolCallId": "call_00", "toolName": "bash",
                  "result": {"content": [{"type": "text", "text": "hello-pi\nDarwin\n"}]}, "isError": False}),
        ev("s1", {"type": "auto_retry_start", "attempt": 1, "maxAttempts": 3, "delayMs": 2000, "errorMessage": "529 overloaded"}),
        ev("s1", {"type": "compaction_start", "reason": "threshold"}),
        ev("s1", {"type": "message_update", "usage": usage(0, 0),
                  "assistantMessageEvent": {"type": "text_delta", "contentIndex": 0, "delta": "hello-pi"}}),
        ev("s2", {"type": "message_update", "usage": usage(0, 0),
                  "assistantMessageEvent": {"type": "text_delta", "contentIndex": 0, "delta": "其他会话"}}),
        ev("s1", {"type": "message_end", "message": {"role": "assistant", "stopReason": "error", "errorMessage": "x"}}),
        ev("s1", {"type": "agent_end", "messages": [], "willRetry": False}),
    ]
    out = [item for e in stream for item in tr.feed(e)]
    assert out[0] == ("reasoning", {"delta": "The"})
    assert out[1][0] == "tool" and out[1][1]["status"] == "running" and "echo hello-pi" in out[1][1]["input"]
    assert out[2][1] == {"tool": "bash", "status": "completed", "title": None,
                         "input": '{"command": "echo hello-pi && uname -s", "timeout": 30}',
                         "output": "hello-pi\nDarwin\n", "error": None}
    assert out[3] == ("status", {"type": "retry", "message": "529 overloaded", "attempt": 1})
    assert out[4] == ("status", {"type": "compaction", "message": "threshold"})
    assert out[5] == ("text", {"delta": "hello-pi"})
    assert len(out) == 6
    # message_end 里的错误不算本轮错误（自动重试后可能成功）；agent_end 之后还没结束
    assert tr.errors == [] and not tr.idle
    tr.feed(ev("s1", {"type": "agent_settled"}))
    assert tr.idle


def test_pi_translator_tool_error_run_lost_and_ui_request():
    tr = PiTranslator("s1")
    tr.feed(ev("s1", {"type": "tool_execution_start", "toolCallId": "c", "toolName": "bash", "args": {"command": "sleep 30"}}))
    [(kind, data)] = tr.feed(ev("s1", {"type": "tool_execution_end", "toolCallId": "c", "toolName": "bash",
                                       "result": {"content": [{"type": "text", "text": "Command aborted"}]}, "isError": True}))
    assert data["status"] == "error" and data["error"] == "Command aborted" and data["output"] is None
    # 没见过 agent_start 的 agent_settled 不算完成（可能是订阅前的上一轮）
    tr.feed(ev("s1", {"type": "agent_settled"}))
    assert not tr.idle
    assert tr.feed({"type": "pi.ui_request", "properties": {"sessionID": "s1", "summary": "confirm: Allow?"}}) == [
        ("status", {"type": "ui_request", "message": "confirm: Allow?"})
    ]
    tr.feed({"type": "pi.run_lost", "properties": {"sessionID": "s2", "reason": "x"}})
    assert not tr.idle
    tr.feed({"type": "pi.run_lost", "properties": {"sessionID": "s1", "runID": "r1", "reason": "exit code 1"}})
    assert tr.idle and tr.errors == ["agent process exited: exit code 1"]
    assert tr.flush() == [] and tr.asks == []


# ---------- 纯逻辑：结果提取 ----------


def test_pi_extract_result_tool_turn_sums_usage_and_skips_system():
    messages = [
        {"role": "system", "content": "", "sections": {"preamble": "You are pi"}},
        {"role": "user", "content": "上一轮"},
        {"role": "assistant", "content": [{"type": "text", "text": "旧回答"}], "usage": usage(9, 9), "stopReason": "stop"},
        {"role": "user", "content": "用 bash 执行 echo"},
        {"role": "assistant", "content": [{"type": "thinking", "thinking": "..."}, {"type": "toolCall", "id": "c"}],
         "usage": usage(256, 99, cache=3584, reasoning=37, cost=0.000217), "stopReason": "toolUse"},
        {"role": "toolResult", "toolCallId": "c", "content": [{"type": "text", "text": "hello"}], "isError": False},
        {"role": "assistant", "content": [{"type": "text", "text": "输出是 "}, {"type": "text", "text": "hello"}],
         "usage": usage(246, 46, cache=3712, cost=0.000151), "stopReason": "stop"},
    ]
    text, u, error = extract_result(messages)
    assert text == "输出是 hello" and error is None
    assert u == {"input": 502, "output": 145, "reasoning": 37, "cache_read": 7296, "cache_write": 0,
                 "cost": 0.000368, "steps": 2}


def test_pi_extract_result_abort_error_and_retry_then_success():
    aborted = [
        {"role": "user", "content": "sleep"},
        {"role": "assistant", "content": [{"type": "toolCall", "id": "c"}], "usage": usage(1, 1), "stopReason": "toolUse"},
        {"role": "toolResult", "toolCallId": "c", "content": [{"type": "text", "text": "Command aborted"}], "isError": True},
        {"role": "assistant", "content": [], "usage": usage(0, 0), "stopReason": "error", "errorMessage": "This operation was aborted"},
    ]
    assert extract_result(aborted) == ("", {"input": 1, "output": 1, "reasoning": 0, "cache_read": 0, "cache_write": 0,
                                            "cost": 0.0, "steps": 2}, "This operation was aborted")
    retried = [
        {"role": "user", "content": "q"},
        {"role": "assistant", "content": [], "usage": usage(0, 0), "stopReason": "error", "errorMessage": "429"},
        {"role": "assistant", "content": [{"type": "text", "text": "ok"}], "usage": usage(3, 2), "stopReason": "stop"},
    ]
    assert extract_result(retried)[0] == "ok" and extract_result(retried)[2] is None
    assert extract_result([{"role": "user", "content": "q"}])[:1] == ("",)
    assert extract_result([{"role": "user", "content": "q"}, {"role": "assistant", "content": [],
                                                              "stopReason": "aborted"}])[2] == "aborted"


# ---------- 纯逻辑：配置渲染与校验 ----------


def _ctx(mcp: dict) -> FilesContext:
    return FilesContext(workdir=WORKDIR, max_life_h=23.5, idle_destroy_after_s=0, mcp=mcp,
                        egress={"mode": "open"}, instructions="用中文")


def test_pi_render_files_and_mcp_conversion():
    mcp = {
        "websearch": {"type": "remote", "url": "https://dashscope.aliyuncs.com/api/v1/mcps/WebSearch/mcp", "enabled": True},
        "fs": {"type": "local", "command": ["npx", "-y", "fs-mcp"], "environment": {"A": "1"}},
        "off": {"type": "remote", "url": "https://x.example/mcp", "enabled": False},
    }
    assert mcp_servers(mcp) == {
        "fs": {"command": "npx", "args": ["-y", "fs-mcp"], "directTools": True, "env": {"A": "1"}},
        "websearch": {"url": "https://dashscope.aliyuncs.com/api/v1/mcps/WebSearch/mcp", "directTools": True},
    }
    files = PiEngine(template="t", port=4096, model="deepseek/deepseek-flash", workdir=WORKDIR, thinking="low").render_files(_ctx(mcp))
    assert set(files) == {PI_CONFIG_FILE, PI_MCP_FILE, f"{WORKDIR}/AGENTS.md"}
    assert json.loads(files[PI_CONFIG_FILE]) == {"provider": "deepseek", "model": "deepseek-flash", "thinking": "low", "mcp": True}
    assert set(json.loads(files[PI_MCP_FILE])["mcpServers"]) == {"fs", "websearch"}
    md = files[f"{WORKDIR}/AGENTS.md"].decode()
    assert "服务名为前缀" in md and "websearch" in md and "curl" in md and "用中文" in md
    # 没有 MCP 时不加载扩展，mcp.json 也清空（去掉 MCP 的设置要能生效）
    files = PiEngine(template="t", port=4096, model="deepseek/deepseek-flash", workdir=WORKDIR).render_files(_ctx({}))
    assert json.loads(files[PI_CONFIG_FILE])["mcp"] is False and json.loads(files[PI_MCP_FILE]) == {"mcpServers": {}}
    # opencode 的 AGENTS.md 与改动前一致
    oc = OpencodeEngine(template="t", port=4096, model="deepseek/deepseek-flash", workdir=WORKDIR).render_files(_ctx(mcp))
    assert "联网搜索优先使用 websearch 相关工具。" in oc[f"{WORKDIR}/AGENTS.md"].decode()
    assert "curl" not in oc[f"{WORKDIR}/AGENTS.md"].decode()


def test_engine_config_checks_and_setting_validation():
    base = dict(agent_enabled=True, agent_model_api_key="k")
    with pytest.raises(ValueError, match="POOL_AGENT_PI_TEMPLATE"):
        check_agent_config(PoolConfig(**base))
    with pytest.raises(ValueError, match="DEFAULT_ENGINE"):
        check_agent_config(PoolConfig(**base, agent_pi_template="tpl-pi"))  # 默认 opencode，但没配它的模板
    check_agent_config(PoolConfig(**base, agent_pi_template="tpl-pi", agent_default_engine="pi"))
    check_agent_config(PoolConfig(**base, agent_template="tpl-opencode", agent_pi_template="tpl-pi"))
    with pytest.raises(ValueError, match="PI_THINKING"):
        check_agent_config(PoolConfig(**base, **PI, agent_pi_thinking="huge"))
    with pytest.raises(ValueError, match="PI_MODEL"):
        check_agent_config(PoolConfig(**base, **PI, agent_pi_model="flash"))
    assert validate_settings({"engine": "pi"}) == {"engine": "pi"}
    assert validate_settings({"engine": None}) == {"engine": None}
    for bad in ("PI", "pi/../x", 3, ""):
        with pytest.raises(InvalidRequest):
            validate_settings({"engine": bad})


# ---------- pi 沙箱：服务 ----------


async def test_pi_first_message_boots_streams_and_continues(make_agents, provider):
    svc = await make_agents(**PI)
    agent = await svc.ensure_agent("biz", "u1")
    runner = await svc.start_message(agent, "你好")
    events = await collect(runner)
    assert events[0][0] == "start" and events[-1][0] == "done"
    assert "".join(d["delta"] for k, d in events if k == "text") == "echo: 你好"
    done = events[-1][1]
    assert done["state"] == "SUCCEEDED" and done["result"] == "echo: 你好" and done["usage"]["cost"] > 0
    task = await svc.store.get_task(runner.task_id)
    assert task["run_id"] and task["session_id"] == events[0][1]["session_id"]

    [row] = await serving(svc, agent["id"])
    assert row["engine"] == "pi" and row["template"] == "tpl-pi"
    sb = provider.sandboxes[row["provider_id"]]
    assert sb["metadata"]["engine"] == "pi"
    files = sb["files"]
    assert f"{WORKDIR}/opencode.json" not in files
    assert json.loads(files[PI_CONFIG_FILE])["model"] == "deepseek-flash"
    assert b"egress.json" in files[f"{WORKDIR}/AGENTS.md"] and json.loads(files[EGRESS_FILE])["mode"] == "open"
    assert b"sk-test-key" not in b"".join(files.values())
    # 会话连续
    r2 = await svc.start_message(agent, "[remember]", session_id=done["session_id"])
    assert (await collect(r2))[-1][1]["result"] == "remembered: 你好"
    assert creates(provider) == 1
    assert (await svc.agent_info(agent))["engine"] == "pi"


async def test_pi_events_retry_then_success_and_model_error(make_agents):
    svc = await make_agents(**PI)
    agent = await svc.ensure_agent("biz", "u1")
    events = await collect(await svc.start_message(agent, "[retry][reasoning][tool] 都来"))
    kinds = [k for k, _ in events]
    assert "status" in kinds and "reasoning" in kinds
    assert [d["status"] for k, d in events if k == "tool"] == ["running", "completed"]
    # 重试前的失败消息不影响最终结果
    assert events[-1][1]["state"] == "SUCCEEDED"
    done = (await collect(await svc.start_message(agent, "[error]")))[-1][1]
    assert done["state"] == "FAILED" and done["error"] == "fake model error"
    with pytest.raises(InvalidRequest, match="agent parameter"):
        await svc.start_message(agent, "x", agent_name="plan")


async def test_pi_abort_and_timeout(make_agents, provider):
    svc = await make_agents(**PI)
    agent = await svc.ensure_agent("biz", "u1")
    runner = await svc.start_message(agent, "[sleep:5] 会被中止")
    await asyncio.wait_for(runner.subscribe().get(), 5)
    await svc.abort_task(agent, runner.task_id)
    task = await final_task(svc, runner.task_id)
    assert task["state"] == "ABORTED" and task["error"] == "This operation was aborted"
    runner = await svc.start_message(agent, "[sleep:5] 会超时", max_duration_s=0.5)
    assert (await final_task(svc, runner.task_id, timeout=8))["state"] == "TIMEOUT"
    [row] = await serving(svc, agent["id"])
    assert len(set(provider.pi(row["provider_id"]).aborts)) == 2


async def test_pi_process_crash_fails_task_instead_of_empty_success(make_agents):
    svc = await make_agents(**PI)
    agent = await svc.ensure_agent("biz", "u1")
    done = (await collect(await svc.start_message(agent, "[sleep:0.1][crash]")))[-1][1]
    assert done["state"] == "FAILED" and "agent process exited" in done["error"]
    # 进程丢失后会话还能用（桥接进程按会话文件重开）
    assert (await collect(await svc.start_message(agent, "again")))[-1][1]["state"] == "SUCCEEDED"


async def test_pi_bridge_restart_during_run_fails_task_via_run_state(make_agents, provider):
    """桥接进程重启：没有 pi.run_lost 事件，会话也不再忙；靠 run 查询为 unknown 判为失败。"""
    svc = await make_agents(**PI)
    agent = await svc.ensure_agent("biz", "u1")
    runner = await svc.start_message(agent, "[sleep:5] 桥接进程会重启")
    await asyncio.wait_for(runner.subscribe().get(), 5)
    [row] = await serving(svc, agent["id"])
    server = provider.pi(row["provider_id"])
    await wait_until(lambda: server.busy() or None, timeout=5)
    server.restart()
    task = await final_task(svc, runner.task_id, timeout=15)
    assert task["state"] == "FAILED" and "run unknown" in task["error"]


async def test_pi_task_of_crashed_replica_is_taken_over(make_agents, provider):
    svc = await make_agents(**PI)
    agent = await svc.ensure_agent("biz", "u1")
    await (await svc.start_message(agent, "boot")).done.wait()
    [row] = await serving(svc, agent["id"])
    server = provider.pi(row["provider_id"])

    async def orphan(task_id: str, text: str):
        sid = server.create_session("orphan")
        run_id = server.prompt(sid, text)
        now = svc.now()
        status, _ = await svc.store.create_task(
            dict(id=task_id, agent_id=agent["id"], client_id="biz", source="message", schedule_id=None,
                 sandbox_row_id=row["id"], session_id=sid, prompt=text, result_text=None, error=None, usage=None,
                 created_at=now, started_at=now, finished_at=None, deadline=now + 60, op_owner="dead-replica",
                 op_deadline=now - 1, run_id=run_id),
            max_running=5,
        )
        assert status == "ok"

    await orphan("t-ok", "[sleep:0.5] 崩溃副本的任务")
    task = await final_task(svc, "t-ok")
    assert task["state"] == "SUCCEEDED" and task["result_text"] == "echo: [sleep:0.5] 崩溃副本的任务"
    # 接管前桥接进程也重启过：这次运行已不存在，不能当成功
    await orphan("t-lost", "[sleep:5] 无人跟进")
    server.restart()
    task = await final_task(svc, "t-lost", timeout=10)
    assert task["state"] == "FAILED" and "run unknown" in task["error"]


async def test_pi_reattach_from_another_replica(make_agents):
    a, b = await make_agents(**PI), await make_agents(**PI)
    agent = await a.ensure_agent("biz", "u1")
    runner = await a.start_message(agent, "[sleep:0.6] 远程")
    await asyncio.wait_for(runner.subscribe().get(), 5)
    items = []
    async for item in b.attach(agent, runner.task_id):
        items.append(item)
        if item is None or item[0] == "done":
            break
    assert items[0][1]["attached"] is True
    assert items[-1][0] == "done" and items[-1][1]["state"] == "SUCCEEDED"
    assert "".join(d["delta"] for k, d in items if k == "text").endswith("echo: [sleep:0.6] 远程")


async def test_pi_settings_change_rewrites_config_and_reloads(make_agents, provider):
    svc = await make_agents(**PI)
    agent = await svc.ensure_agent("biz", "u1")
    await (await svc.start_message(agent, "hi")).done.wait()
    await svc.update_settings(agent, {"instructions": "所有回答都用中文",
                                      "mcp": {"websearch": {"type": "remote", "url": "https://dashscope.aliyuncs.com/mcp"}}})

    async def applied():
        [row] = await serving(svc, agent["id"])
        return row if row["config_version"] == 2 else None

    row = await wait_until(applied, timeout=5)
    files = provider.sandboxes[row["provider_id"]]["files"]
    assert "所有回答都用中文" in files[f"{WORKDIR}/AGENTS.md"].decode()
    assert json.loads(files[PI_CONFIG_FILE])["mcp"] is True
    assert "websearch" in json.loads(files[PI_MCP_FILE])["mcpServers"]
    server = provider.pi(row["provider_id"])
    assert server.disposed >= 1
    await (await svc.start_message(agent, "after")).done.wait()
    assert server.configs[-1]["mcp"] is True


async def test_pi_schedule_fires(make_agents):
    svc = await make_agents(**PI)
    agent = await svc.ensure_agent("biz", "u1")
    s = await svc.create_schedule(agent, {"name": "定时", "every_s": 60, "prompt": "定时任务"})
    await svc.store.update_schedule(s["id"], next_run_at=svc.now())

    async def ran():
        tasks = await svc.store.list_tasks(agent["id"], schedule_id=s["id"])
        return tasks if tasks and all(t["state"] != "RUNNING" for t in tasks) else None

    [task] = await wait_until(ran, timeout=10)
    assert task["state"] == "SUCCEEDED" and task["result_text"] == "echo: 定时任务"


# ---------- 引擎选择与切换 ----------


async def test_switch_engine_when_idle_replaces_sandbox(make_agents, provider):
    svc = await make_agents(**BOTH)
    agent = await svc.ensure_agent("biz", "u1")
    first = (await collect(await svc.start_message(agent, "hi")))[-1][1]
    [old] = await serving(svc, agent["id"])
    assert old["engine"] == "opencode"
    info = await svc.update_settings(agent, {"engine": "pi"})
    assert info["engine"] == "pi" and info["settings"]["engine"] == "pi"

    async def old_gone():
        rows = await svc.store.agent_sandboxes(agent["id"])
        return all(r["id"] != old["id"] for r in rows) or None

    await wait_until(old_gone, timeout=5)
    agent = await svc.store.get_agent_by_id(agent["id"])
    done = (await collect(await svc.start_message(agent, "on pi")))[-1][1]
    assert done["state"] == "SUCCEEDED"
    [new] = await serving(svc, agent["id"])
    assert new["engine"] == "pi" and creates(provider) == 2
    # 旧引擎的会话不能续聊
    with pytest.raises(TaskConflict):
        await svc.start_message(agent, "[remember]", session_id=first["session_id"])
    # 恢复默认（null）→ opencode
    info = await svc.update_settings(agent, {"engine": None})
    assert info["engine"] == "opencode" and info["settings"]["engine"] is None
    with pytest.raises(InvalidRequest, match="not enabled"):
        await svc.update_settings(agent, {"engine": "claude"})


async def test_switch_engine_with_running_task_retires_old_sandbox(make_agents, provider):
    svc = await make_agents(**BOTH)
    agent = await svc.ensure_agent("biz", "u1")
    old_runner = await svc.start_message(agent, "[sleep:1.5] 旧引擎上的长任务")
    start = (await asyncio.wait_for(old_runner.subscribe().get(), 5))[1]
    await svc.update_settings(agent, {"engine": "pi"})
    agent = await svc.store.get_agent_by_id(agent["id"])
    # 新消息不等旧任务：旧沙箱转 RETIRING，新消息在 pi 沙箱上执行
    done = (await collect(await svc.start_message(agent, "新引擎")))[-1][1]
    assert done["state"] == "SUCCEEDED"
    rows = {r["engine"]: r for r in await serving(svc, agent["id"])}
    assert rows["opencode"]["state"] == "RETIRING" and rows["pi"]["state"] == "ACTIVE"
    with pytest.raises(TaskConflict):
        await svc.start_message(agent, "续聊旧会话", session_id=start["session_id"])
    # 旧任务照常跑完，之后旧沙箱被销毁
    assert (await final_task(svc, old_runner.task_id, timeout=10))["state"] == "SUCCEEDED"

    async def only_pi():
        rows = await svc.store.agent_sandboxes(agent["id"])
        return rows if [r["engine"] for r in rows] == ["pi"] else None

    await wait_until(only_pi, timeout=5)


async def test_old_session_rejected_right_after_switch_before_maintainer_runs(make_agents):
    svc = await make_agents(run_maintainer=False, **BOTH)
    agent = await svc.ensure_agent("biz", "u1")
    sid = (await collect(await svc.start_message(agent, "hi")))[-1][1]["session_id"]
    await svc.update_settings(agent, {"engine": "pi"})
    agent = await svc.store.get_agent_by_id(agent["id"])
    [row] = await serving(svc, agent["id"])
    assert row["state"] == "ACTIVE" and row["engine"] == "opencode"  # 维护循环没跑，旧沙箱仍是 ACTIVE
    with pytest.raises(TaskConflict, match="belongs to engine opencode"):
        await svc.start_message(agent, "[remember]", session_id=sid)


async def test_two_replicas_after_switch_create_one_new_sandbox(make_agents, provider):
    a = await make_agents(run_maintainer=False, **BOTH)
    b = await make_agents(run_maintainer=False, **BOTH)
    agent = await a.ensure_agent("biz", "u1")
    await (await a.start_message(agent, "boot")).done.wait()
    await a.update_settings(agent, {"engine": "pi"})
    agent = await a.store.get_agent_by_id(agent["id"])
    r1, r2 = await asyncio.gather(a.start_message(agent, "one"), b.start_message(agent, "two"))
    assert (await collect(r1))[-1][1]["state"] == "SUCCEEDED"
    assert (await collect(r2))[-1][1]["state"] == "SUCCEEDED"
    assert [r["engine"] for r in await svc_rows(a, agent, SandboxState.ACTIVE)] == ["pi"]
    assert creates(provider) == 2


async def svc_rows(svc, agent, state):
    return await svc.store.agent_sandboxes(agent["id"], [state])


async def test_legacy_sandbox_row_without_engine_is_opencode(make_agents):
    svc = await make_agents(**BOTH)
    agent = await svc.ensure_agent("biz", "u1")
    await (await svc.start_message(agent, "hi")).done.wait()
    [row] = await serving(svc, agent["id"])
    await svc.store.cas_sandbox(row["id"], [SandboxState.ACTIVE], now=svc.now(), engine=None)
    [row] = await serving(svc, agent["id"])
    assert row["engine"] is None and svc.engine_for(row).name == "opencode"
    assert (await svc.agent_info(agent))["sandboxes"][0]["engine"] == "opencode"
    # 老记录照常服务（引擎一致，不会被当成切换）
    assert (await collect(await svc.start_message(agent, "again")))[-1][1]["state"] == "SUCCEEDED"
    assert len(await serving(svc, agent["id"])) == 1


# ---------- provider ----------


async def test_write_files_retries_proxy_tunnel_failures(monkeypatch):
    """本机 HTTP 代理对刚创建沙箱的 envd 域名偶发返回 503（请求没发出去）：写文件幂等，重试而不是让装配失败。"""
    import httpcore

    import sandbox_pool.provider.e2b_provider as ep

    monkeypatch.setattr(ep, "_WRITE_RETRY_DELAYS", (0, 0))
    written, failures = [], [httpcore.ProxyError("503 Service Unavailable")]

    class Files:
        async def write(self, path, data, request_timeout=None):
            if failures:
                raise failures.pop()
            written.append(path)

    provider = ep.E2BProvider(api_key="k", api_url="http://api.invalid", domain="invalid")
    provider._cache.put("sbx", type("H", (), {"files": Files()})())
    await provider.write_files("sbx", {"/a": b"1"}, sandbox_timeout_s=60)
    assert written == ["/a"] and not failures


# ---------- 接口 ----------


async def _client(make_pool, make_agents, **overrides):
    pool = await make_pool(target_size=0, api_keys="biz:k-biz")
    svc = await make_agents(**overrides)
    c = httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(pool=pool, agents=svc)), base_url="http://pool",
                          timeout=30, headers=h("k-biz"))
    return svc, c


async def test_api_engines_select_and_stream_on_pi(make_pool, make_agents):
    svc, c = await _client(make_pool, make_agents, **BOTH)
    async with c:
        r = await c.get("/v1/agent-engines")
        assert r.status_code == 200
        body = r.json()
        assert body["default"] == "opencode"
        assert {e["name"]: e["default"] for e in body["engines"]} == {"opencode": True, "pi": False}
        pi = next(e for e in body["engines"] if e["name"] == "pi")
        assert pi["capabilities"]["agent_param"] is False and pi["model"] == "deepseek/deepseek-flash"
        assert (await c.patch("/v1/agents/u1/settings", json={"engine": "nope"})).status_code == 400
        r = await c.patch("/v1/agents/u1/settings", json={"engine": "pi"})
        assert r.status_code == 200 and r.json()["engine"] == "pi"
        r = await c.post("/v1/agents/u1/messages", json={"text": "你好"})
        events = parse_sse(r.text)
        assert events[-1][1]["state"] == "SUCCEEDED" and events[-1][1]["result"] == "echo: 你好"
        info = (await c.get("/v1/agents/u1")).json()
        assert info["engine"] == "pi" and [s["engine"] for s in info["sandboxes"]] == ["pi"]
        assert all("access_token" not in s for s in info["sandboxes"])
        r = await c.post("/v1/agents/u1/messages", json={"text": "x", "agent": "plan", "stream": False})
        assert r.status_code == 400 and "agent parameter" in r.text


async def test_api_engines_requires_auth_and_pi_only_deployment(make_pool, make_agents):
    svc, c = await _client(make_pool, make_agents, agent_template="", **PI)
    async with c:
        assert (await c.get("/v1/agent-engines", headers={"Authorization": "Bearer wrong"})).status_code == 401
        body = (await c.get("/v1/agent-engines")).json()
        assert body == {"default": "pi", "engines": [{**svc.engines["pi"].describe(), "default": True}]}
        assert (await c.patch("/v1/agents/u1/settings", json={"engine": "opencode"})).status_code == 400
        r = await c.post("/v1/agents/u1/messages", json={"text": "hi", "stream": False})
        assert r.status_code == 200 and r.json()["state"] == "SUCCEEDED"


# ---------- 评审修复（PI-*，见 sxw_aicoding/代码评审/2026-09-26-pi引擎接入代码评审报告.md）----------

_WEBSEARCH_MCP = json.dumps({"websearch": {"type": "remote", "url": "https://dashscope.aliyuncs.com/mcp"}})


async def test_pi_h1_slash_prompt_goes_to_the_model_not_an_extension_command(make_agents, provider):
    """PI-H1：真实 pi 把以 / 开头、命中扩展命令（pi-mcp-adapter 的 /mcp 等）的提示词当命令执行，不产生运行；pi 0.87.1
    的响应不带 disposition，桥接进程把会话一直记为忙（任务挂到截止时间、中止也清不掉）。网关与 opencode 一致，一律作为
    普通提示词发送。"""
    svc = await make_agents(agent_mcp=_WEBSEARCH_MCP, **PI)
    agent = await svc.ensure_agent("biz", "u1")
    first = (await collect(await svc.start_message(agent, "你好")))[-1][1]
    runner = await svc.start_message(agent, "/mcp 有哪些工具", session_id=first["session_id"])
    done = (await collect(runner, timeout=5))[-1][1]
    assert done["state"] == "SUCCEEDED" and done["result"] == "echo:  /mcp 有哪些工具"
    [row] = await serving(svc, agent["id"])
    server = provider.pi(row["provider_id"])
    assert server.handled == [] and server.prompts[-1] == " /mcp 有哪些工具"
    # 不以 / 开头的提示词原样发送
    await (await svc.start_message(agent, "看看 /mcp 目录", session_id=first["session_id"])).done.wait()
    assert server.prompts[-1] == "看看 /mcp 目录"


async def test_pi_m1_prompt_and_session_posts_wait_longer_than_the_bridge(monkeypatch):
    """PI-M1：pi 做完 preflight（可能先压缩上下文，要调一次模型）才响应 prompt，桥接进程还可能要冷启动会话进程、等 MCP
    元数据缓存。网关先超时会把任务记为失败，而运行照常开始（请求非幂等，不能重试）。这两个 POST 的超时要长于桥接进程
    自己的处理上限；其余请求仍用普通超时。"""
    from sandbox_pool.agent.opencode import AgentHttpClient, OpencodeError

    monkeypatch.delenv("HTTPS_PROXY", raising=False)
    monkeypatch.delenv("https_proxy", raising=False)

    async def slow_bridge(reader, writer):
        head = await reader.readuntil(b"\r\n\r\n")
        length = next((int(line.split(b":", 1)[1]) for line in head.split(b"\r\n")
                       if line.lower().startswith(b"content-length:")), 0)
        if length:
            await reader.readexactly(length)
        await asyncio.sleep(0.6)
        body = b'{"id": "s1", "run_id": "r1", "disposition": "started"}'
        writer.write(b"HTTP/1.1 200 OK\r\ncontent-type: application/json\r\ncontent-length: %d\r\n"
                     b"connection: close\r\n\r\n%s" % (len(body), body))
        await writer.drain()
        writer.close()

    server = await asyncio.start_server(slow_bridge, "127.0.0.1", 0)
    port = server.sockets[0].getsockname()[1]
    client = AgentHttpClient(f"http://127.0.0.1:{port}", "tok", WORKDIR, timeout_s=0.2)
    try:
        assert await client.create_session("t") == "s1"
        assert await client.prompt_async("s1", "hi", model=None, agent=None) == "r1"
        with pytest.raises(OpencodeError, match="Timeout"):
            await client.status()
    finally:
        await client.close()
        server.close()
        await server.wait_closed()


async def test_pi_l1_sandbox_for_follows_latest_engine_not_request_snapshot(make_agents, provider):
    """PI-L1：请求拿着切换引擎之前读到的 agent（在途请求、准入失败后的重试），不能按旧快照新建旧引擎的沙箱，
    也不能把新引擎的沙箱当成「切换了引擎」转为 RETIRING。"""
    svc = await make_agents(run_maintainer=False, **BOTH)
    stale = await svc.ensure_agent("biz", "u1")  # 未设置引擎 → 默认 opencode
    await svc.update_settings(stale, {"engine": "pi"})
    row = await svc.sandbox_for(stale)
    assert row["engine"] == "pi"
    again = await svc.sandbox_for(stale)
    assert again["id"] == row["id"] and again["state"] == "ACTIVE"
    assert creates(provider) == 1
