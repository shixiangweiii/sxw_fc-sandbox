"""agent 子系统的纯逻辑：出网策略、凭证注入、cron、SSE 解码、事件翻译、结果提取。"""

from datetime import datetime
from zoneinfo import ZoneInfo

import pytest

from sandbox_pool.agent import cron
from sandbox_pool.agent.opencode import SSEDecoder
from sandbox_pool.agent.policy import (
    MANDATORY_DENY,
    EgressPolicy,
    build_network,
    default_policy,
    describe,
    load_injections,
    network_version,
    parse_policy,
    redact_platform_network,
    render_opencode_config,
    validate_settings,
)
from sandbox_pool.agent.runner import Translator, extract_result
from sandbox_pool.models import InvalidRequest


# ---------- 出网策略 ----------


def test_open_policy_always_denies_internal_and_metadata():
    inj = load_injections("api.deepseek.com", "sk-x", "")
    net = build_network(parse_policy({"deny_out": ["1.2.3.4"]}), inj)
    for cidr in MANDATORY_DENY:
        assert cidr in net["deny_out"]
    assert "1.2.3.4" in net["deny_out"]
    # 注入规则的域名必须出现在 allow_out 里（平台要求），开放模式下 allow_out 不限制其他地址
    assert "api.deepseek.com" in net["allow_out"]
    assert net["rules"]["api.deepseek.com"][0]["transform"]["headers"]["Authorization"] == "Bearer sk-x"


def test_allowlist_policy_denies_all_but_allowed():
    inj = load_injections("api.deepseek.com", "sk-x", "")
    net = build_network(parse_policy({"mode": "allowlist", "allow_out": ["*.github.com", "pypi.org"]}), inj)
    assert net["deny_out"] == ["0.0.0.0/0"]
    assert set(net["allow_out"]) == {"*.github.com", "pypi.org", "api.deepseek.com"}


def test_deny_out_rejects_domains_platform_only_supports_ip_cidr():
    with pytest.raises(InvalidRequest, match="only IP / CIDR"):
        parse_policy({"deny_out": ["www.baidu.com"]})
    with pytest.raises(InvalidRequest):
        parse_policy({"mode": "bogus"})
    with pytest.raises(InvalidRequest):
        parse_policy({"allow_out": ["not a domain!"]})
    with pytest.raises(InvalidRequest):
        parse_policy({"surprise": 1})


def test_overlay_overrides_only_given_fields():
    base = default_policy('{"mode": "open", "deny_out": ["8.8.8.8"]}')
    merged = parse_policy({"allow_out": ["1.1.1.1"]}, base=base)
    assert merged.mode == "open" and merged.deny_out == ("8.8.8.8",) and merged.allow_out == ("1.1.1.1",)
    assert parse_policy(None, base=base) == base


def test_version_changes_with_policy_and_secret_but_hides_secret():
    p = EgressPolicy()
    v1 = network_version(p, load_injections("api.deepseek.com", "sk-1", ""))
    v2 = network_version(p, load_injections("api.deepseek.com", "sk-2", ""))
    v3 = network_version(parse_policy({"deny_out": ["1.1.1.1"]}), load_injections("api.deepseek.com", "sk-1", ""))
    assert len({v1, v2, v3}) == 3
    desc = describe(p, load_injections("api.deepseek.com", "sk-secret", ""), v1)
    assert "sk-secret" not in str(desc)
    assert desc["injected_hosts"] == {"api.deepseek.com": ["Authorization"]}


def test_inject_spec_expands_env_and_validates():
    inj = load_injections(
        "api.deepseek.com",
        "sk-model",
        '{"dashscope.aliyuncs.com": {"Authorization": "Bearer ${WS_KEY}"}}',
        env={"WS_KEY": "ws-123"},
    )
    assert inj.rules["dashscope.aliyuncs.com"]["Authorization"] == "Bearer ws-123"
    with pytest.raises(ValueError, match="unset environment variable"):
        load_injections("h.com", "", '{"a.com": {"X": "${NOPE}"}}', env={})
    with pytest.raises(ValueError, match="exact domain"):
        load_injections("h.com", "", '{"*.a.com": {"X": "v"}}', env={})
    too_many = {f"h{i}.com": {"X": "v"} for i in range(11)}
    with pytest.raises(ValueError, match="at most 10"):
        load_injections("h.com", "", __import__("json").dumps(too_many), env={})


def test_redact_platform_network_hides_header_values():
    echoed = {
        "allow_out": ["api.deepseek.com"],
        "deny_out": list(MANDATORY_DENY),
        "rules": {"api.deepseek.com": [{"transform": {"headers": {"Authorization": "Bearer sk-secret"}}}]},
    }
    red = redact_platform_network(echoed)
    assert "sk-secret" not in str(red)
    assert red["rules"] == {"api.deepseek.com": {"Authorization": "***"}}
    assert redact_platform_network(None) is None


def test_settings_validation_and_opencode_config():
    s = validate_settings({"idle_destroy_after_s": 3600, "instructions": "说中文",
                           "mcp": {"fs": {"type": "local", "command": ["npx", "x"]}}})
    assert s["idle_destroy_after_s"] == 3600.0
    for bad in ({"idle_destroy_after_s": -1}, {"mcp": {"x": {"type": "remote", "url": "ftp://a"}}}, {"nope": 1}):
        with pytest.raises(InvalidRequest):
            validate_settings(bad)
    conf = render_opencode_config("deepseek/deepseek-flash", {"websearch": {"type": "remote", "url": "https://x/mcp"}})
    assert conf["permission"] == {"doom_loop": "deny", "external_directory": "allow", "question": "deny"}
    assert conf["provider"]["deepseek"]["options"]["apiKey"] == "injected-by-platform"
    assert conf["share"] == "disabled" and conf["autoupdate"] is False
    assert conf["mcp"]["websearch"]["enabled"] is True


# ---------- cron ----------


def _at(*args) -> float:
    return datetime(*args, tzinfo=ZoneInfo("Asia/Shanghai")).timestamp()


@pytest.mark.parametrize(
    "expr,expected",
    [
        ("* * * * *", (2026, 9, 25, 10, 31)),
        ("*/15 * * * *", (2026, 9, 25, 10, 45)),
        ("0 9 * * *", (2026, 9, 26, 9, 0)),
        ("0 9 * * 1-5", (2026, 9, 28, 9, 0)),
        ("0 12 * * 7", (2026, 9, 27, 12, 0)),
        ("0 0 29 2 *", (2028, 2, 29, 0, 0)),
        ("0 9 1 * 1", (2026, 9, 28, 9, 0)),  # 日与周都指定时按「或」匹配
    ],
)
def test_cron_next_fire(expr, expected):
    assert cron.next_fire(expr, "Asia/Shanghai", _at(2026, 9, 25, 10, 30, 15)) == _at(*expected)


def test_cron_strictly_after_and_timezone():
    t = _at(2026, 9, 25, 9, 0)
    assert cron.next_fire("0 9 * * *", "Asia/Shanghai", t) == _at(2026, 9, 26, 9, 0)
    utc = cron.next_fire("0 9 * * *", "UTC", t)
    assert datetime.fromtimestamp(utc, ZoneInfo("UTC")).hour == 9


@pytest.mark.parametrize("bad", ["* * * *", "60 * * * *", "*/0 * * * *", "a * * * *", "5-1 * * * *"])
def test_cron_rejects_invalid(bad):
    with pytest.raises(ValueError):
        cron.parse(bad)
    with pytest.raises(ValueError):
        cron.zone("Mars/Base")


# ---------- SSE ----------


def test_sse_decoder_handles_multiline_comments_and_crlf():
    d = SSEDecoder()
    lines = [": ping", "event: x", "data: {\"a\":", "data: 1}\r", "", "", "data: 2", ""]
    out = [x for x in (d.feed_line(line) for line in lines) if x is not None]
    assert out == ['{"a":\n1}', "2"]


# ---------- 事件翻译 ----------

SID = "ses_1"


def ev(type_, **props):
    return {"type": type_, "properties": {"sessionID": SID, **props}}


def test_translator_filters_user_parts_and_streams_assistant_text():
    tr = Translator(SID)
    out = []
    out += tr.feed(ev("message.updated", info={"id": "u1", "role": "user", "sessionID": SID}))
    out += tr.feed(ev("message.part.updated", part={"id": "p0", "messageID": "u1", "sessionID": SID, "type": "text", "text": "提问"}))
    out += tr.feed(ev("session.status", status={"type": "busy"}))
    out += tr.feed(ev("message.updated", info={"id": "a1", "role": "assistant", "sessionID": SID}))
    # 类型未知时先缓存增量
    out += tr.feed(ev("message.part.delta", messageID="a1", partID="p1", field="text", delta="你"))
    assert out == []
    out += tr.feed(ev("message.part.updated", part={"id": "p1", "messageID": "a1", "sessionID": SID, "type": "text", "text": "你"}))
    out += tr.feed(ev("message.part.delta", messageID="a1", partID="p1", field="text", delta="好"))
    # 只有 updated 的文本：补发剩余部分
    out += tr.feed(ev("message.part.updated", part={"id": "p1", "messageID": "a1", "sessionID": SID, "type": "text", "text": "你好！"}))
    assert [d["delta"] for k, d in out if k == "text"] == ["你", "好", "！"]
    # 其他会话的事件忽略
    assert tr.feed({"type": "message.part.delta", "properties": {"sessionID": "other", "partID": "p1", "delta": "x"}}) == []
    tr.feed(ev("session.status", status={"type": "idle"}))
    assert tr.idle


def test_translator_reasoning_tool_retry_error_and_asks():
    tr = Translator(SID)
    out = tr.feed(ev("message.part.updated", part={"id": "r1", "messageID": "a1", "type": "reasoning", "text": ""}))
    out += tr.feed(ev("message.part.delta", messageID="a1", partID="r1", delta="想"))
    tool = {"id": "t1", "messageID": "a1", "type": "tool", "tool": "bash"}
    out += tr.feed(ev("message.part.updated", part={**tool, "state": {"status": "pending"}}))
    out += tr.feed(ev("message.part.updated", part={**tool, "state": {"status": "running", "input": {"command": "ls"}}}))
    out += tr.feed(ev("message.part.updated", part={**tool, "state": {"status": "running", "input": {"command": "ls"}}}))
    out += tr.feed(ev("message.part.updated", part={**tool, "state": {"status": "completed", "output": "x" * 5000}}))
    out += tr.feed(ev("session.status", status={"type": "retry", "message": "rate limited", "attempt": 1}))
    tr.feed(ev("session.error", error={"name": "APIError", "data": {"message": "boom"}}))
    tr.feed(ev("permission.asked", id="per_1"))
    kinds = [k for k, _ in out]
    assert kinds == ["reasoning", "tool", "tool", "status"]
    assert out[2][1]["status"] == "completed" and len(out[2][1]["output"]) < 2100
    assert tr.errors == ["boom"] and tr.asks == [("permission", "per_1")]
    # idle 只在见过 busy 之后才算完成
    tr2 = Translator(SID)
    tr2.feed(ev("session.status", status={"type": "idle"}))
    assert not tr2.idle


def test_translator_flush_unknown_parts_as_text():
    tr = Translator(SID)
    tr.feed(ev("message.part.delta", messageID="a1", partID="p9", delta="残留"))
    assert tr.flush() == [("text", {"delta": "残留"})]


def test_extract_result_uses_messages_after_last_user_message():
    def msg(role, text=None, tokens=None, cost=0, error=None):
        info = {"role": role, "tokens": tokens or {}, "cost": cost}
        if error:
            info["error"] = error
        parts = [{"type": "text", "text": text}] if text is not None else [{"type": "tool"}]
        return {"info": info, "parts": parts}

    messages = [
        msg("user", "第一轮"),
        msg("assistant", "旧回答", {"input": 1000, "output": 10}),
        msg("user", "第二轮"),
        msg("assistant", None, {"input": 10, "output": 1, "cache": {"read": 5}}, cost=0.1),
        msg("assistant", "新回答", {"input": 20, "output": 2, "reasoning": 3}, cost=0.2),
    ]
    text, usage, error = extract_result(messages)
    assert text == "新回答" and error is None
    assert usage["input"] == 30 and usage["output"] == 3 and usage["reasoning"] == 3 and usage["cache_read"] == 5
    assert usage["steps"] == 2 and usage["cost"] == 0.3
    _, _, error = extract_result([msg("user", "q"), msg("assistant", "", error={"name": "X", "data": {"message": "bad"}})])
    assert error == "bad"
