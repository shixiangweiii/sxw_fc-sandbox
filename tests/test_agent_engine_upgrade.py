"""固定在 OpenCode 2.3.2 / Pi 1.6.0 真实进程观察到的协议差异。"""

import httpx
import pytest

from sandbox_pool.agent.engines.opencode_v2 import OpencodeV2Engine, V2Client, V2Translator, server_password
from sandbox_pool.agent.engines.pi import PiNativeEngine


def event(kind, **data):
    return {"type": "session." + kind, "data": {"sessionID": "s", **data}}


def test_v2_mid_round_recovers_full_text_and_ignores_other_sessions():
    t = V2Translator("s")
    t.mid_round = True
    part = {"assistantMessageID": "m", "ordinal": 0}
    assert not t.feed(event("text.delta", **part, delta="后半"))
    assert not t.feed(event("text.ended", **part, text="其他轮", sessionID="other"))
    assert t.feed(event("text.ended", **part, text="前后半")) == [("text", {"delta": "前后半"})]
    assert not t.feed(event("text.ended", **part, text="前后半"))
    t.feed(event("execution.interrupted"))
    assert t.idle


def test_v2_last_round_result_and_native_pi_direct_tools():
    engine = OpencodeV2Engine(template="test", port=4096, model="deepseek/deepseek-flash", workdir="/tmp")
    messages = [
        {"type": "assistant", "content": [{"type": "text", "text": "上一轮"}]},
        {"type": "user", "text": "新一轮"},
        {
            "type": "assistant",
            "tokens": {"input": 5},
            "cost": 0.1,
            "content": [{"type": "text", "text": "当前回答"}],
        },
        {"type": "idle", "outcome": "succeeded"},
    ]
    result, usage, error = engine.extract_result(messages)
    assert (result, usage["input"], usage["steps"], error, engine.last_prompt(messages)) == (
        "当前回答",
        5,
        1,
        None,
        "新一轮",
    )
    assert server_password("box1") != server_password("box2")
    pi = PiNativeEngine(template="test", port=4096, model="pi-test", workdir="/tmp")
    assert pi.render_mcp({"websearch": {"type": "remote", "url": "https://example.com/mcp"}}) == {
        "websearch": {"exposure": "direct", "url": "https://example.com/mcp"}
    }


@pytest.mark.asyncio
async def test_v2_messages_reads_next_page_until_user_without_repeating_order():
    seen = []

    async def handle(request):
        query = request.url.params
        seen.append(dict(query))
        if "cursor" not in query:
            return httpx.Response(200, json={"data": [{"type": "idle"}], "cursor": {"next": "older"}})
        assert "order" not in query
        return httpx.Response(
            200, json={"data": [{"type": "assistant"}, {"type": "user"}], "cursor": {"next": "unused"}}
        )

    c = V2Client("https://example.test", "box", "/work")
    await c._http.aclose()
    c._http = httpx.AsyncClient(base_url="https://example.test", transport=httpx.MockTransport(handle))
    try:
        assert [m["type"] for m in await c.messages("s")] == ["user", "assistant", "idle"]
        assert len(seen) == 2
    finally:
        await c.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("name", ["opencode2", "pi1"])
async def test_upgraded_engine_uses_injected_provider(make_agents, name):
    from tests.test_agent_service import collect

    svc = await make_agents(agent_default_engine=name, agent_opencode2_template="opencode-new", agent_pi1_template="pi-new")
    agent = await svc.ensure_agent("biz", "native-engine-test")
    runner = await svc.start_message(agent, "你好 [tool]")
    events = await collect(runner)
    done = events[-1][1]
    assert done["state"] == "SUCCEEDED" and "你好" in done["result"]
    assert any(k == "tool" and ev["status"] == "completed" for k, ev in events)
