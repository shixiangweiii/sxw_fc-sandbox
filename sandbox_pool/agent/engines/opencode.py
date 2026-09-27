"""opencode 引擎：沙箱里运行 opencode serve（模板见 scripts/build_opencode_template.py）。

事件结构见 sxw_aicoding/技术调研/2026-09-25-opencode云沙箱PoC验证报告.md。
"""

import json
from typing import Optional

from sandbox_pool.agent.engines.base import Engine, FilesContext, cut
from sandbox_pool.agent.policy import render_agents_md, render_opencode_config


def _event_session(ev: dict) -> Optional[str]:
    props = ev.get("properties") or {}
    return (
        props.get("sessionID")
        or (props.get("part") or {}).get("sessionID")
        or (props.get("info") or {}).get("sessionID")
    )


class Translator:
    """把 opencode 事件翻译成对外事件：(kind, data)，kind ∈ text / reasoning / tool / status。

    - 用户消息的部件（提示词本身）不输出：message.updated 先于对应部件到达，记下每条消息的角色；
    - 文本增量来自 message.part.delta；部件类型未知时先缓存，等 message.part.updated 带来类型再发出；
      只有 message.part.updated（没有增量）的文本，按已输出长度补发剩余部分；
    - 缓存的增量不是部件全文的开头：订阅时部件已在输出（断线重连、接管），错过了开头（opencode 只在部件开始与结束时
      发 message.part.updated，增量不落盘），丢弃缓存，按全文输出（R3-N1）；
    - 工具部件在状态变化时输出（running / completed / error），输入输出截断。
    """

    def __init__(self, session_id: str):
        self.session_id = session_id
        self.roles: dict[str, str] = {}
        self.part_types: dict[str, str] = {}
        self.emitted: dict[str, int] = {}
        self.pending: dict[str, list[str]] = {}
        self.tool_status: dict[str, str] = {}
        self.busy_seen = False
        self.idle = False
        self.errors: list[str] = []
        self.asks: list[tuple[str, str]] = []  # (permission | question, request id)
        self.round_starts = 0
        self.mid_round = False

    def _emit_text(self, part_id: str, kind: str, delta: str, out: list) -> None:
        if not delta:
            return
        self.emitted[part_id] = self.emitted.get(part_id, 0) + len(delta)
        out.append((kind, {"delta": delta}))

    def feed(self, ev: dict) -> list[tuple[str, dict]]:
        out: list[tuple[str, dict]] = []
        if _event_session(ev) != self.session_id:
            return out
        kind = ev.get("type")
        props = ev.get("properties") or {}
        if kind == "message.updated":
            info = props.get("info") or {}
            if info.get("id"):
                if info.get("role") == "user" and info["id"] not in self.roles:
                    self.round_starts += 1
                self.roles[info["id"]] = info.get("role", "")
        elif kind == "message.part.delta":
            if self.roles.get(props.get("messageID")) == "user" or props.get("field", "text") != "text":
                return out
            part_id = props.get("partID", "")
            ptype = self.part_types.get(part_id)
            if ptype is None:
                self.pending.setdefault(part_id, []).append(props.get("delta", ""))
            elif ptype in ("text", "reasoning"):
                self._emit_text(part_id, ptype, props.get("delta", ""), out)
        elif kind == "message.part.updated":
            part = props.get("part") or {}
            if self.roles.get(part.get("messageID")) == "user":
                return out
            part_id, ptype = part.get("id", ""), part.get("type", "")
            self.part_types[part_id] = ptype
            if ptype in ("text", "reasoning"):
                buffered = "".join(self.pending.pop(part_id, []))
                full = part.get("text") or ""
                if buffered and full and not full.startswith(buffered):
                    buffered = ""  # 部件中段的片段：以全文为准
                self._emit_text(part_id, ptype, buffered, out)
                done = self.emitted.get(part_id, 0)
                if len(full) > done:
                    self._emit_text(part_id, ptype, full[done:], out)
            elif ptype == "tool":
                state = part.get("state") or {}
                status = state.get("status")
                if status in ("running", "completed", "error") and self.tool_status.get(part_id) != status:
                    self.tool_status[part_id] = status
                    out.append(
                        (
                            "tool",
                            {
                                "tool": part.get("tool"),
                                "status": status,
                                "title": state.get("title"),
                                "input": cut(state.get("input")),
                                "output": cut(state.get("output")) if status == "completed" else None,
                                "error": cut(state.get("error")) if status == "error" else None,
                            },
                        )
                    )
            else:
                self.pending.pop(part_id, None)
        elif kind == "session.status":
            status = props.get("status") or {}
            st = status.get("type")
            if st == "busy":
                self.busy_seen = True
            elif st == "retry":
                self.busy_seen = True
                out.append(("status", {"type": "retry", "message": cut(status.get("message"), 500), "attempt": status.get("attempt")}))
            elif st == "idle" and self.busy_seen:
                self.idle = True
        elif kind == "session.error":
            err = props.get("error") or {}
            msg = (err.get("data") or {}).get("message") or err.get("name") or "session error"
            self.errors.append(cut(msg, 1000))
        elif kind in ("permission.asked", "question.asked"):
            if props.get("id"):
                self.asks.append((kind.split(".")[0], props["id"]))
        return out

    def flush(self) -> list[tuple[str, dict]]:
        """结束时仍未确定类型的缓存增量按文本输出。"""
        out: list[tuple[str, dict]] = []
        for part_id, chunks in list(self.pending.items()):
            self._emit_text(part_id, "text", "".join(chunks), out)
        self.pending.clear()
        return out


def extract_result(messages: list[dict]) -> tuple[str, dict, Optional[str]]:
    """本轮结果：最后一条用户消息之后的 assistant 消息。文本取最后一条有文本的 assistant 消息，用量求和，错误取最后一条。

    按消息顺序而不是时间戳划分，不依赖网关与沙箱之间的时钟。
    """
    last_user = -1
    for i, m in enumerate(messages):
        if (m.get("info") or {}).get("role") == "user":
            last_user = i
    assistants = [m for m in messages[last_user + 1 :] if (m.get("info") or {}).get("role") == "assistant"]
    usage = {"input": 0, "output": 0, "reasoning": 0, "cache_read": 0, "cache_write": 0, "cost": 0.0, "steps": len(assistants)}
    text, error = "", None
    for m in assistants:
        info = m.get("info") or {}
        tok = info.get("tokens") or {}
        cache = tok.get("cache") or {}
        usage["input"] += tok.get("input") or 0
        usage["output"] += tok.get("output") or 0
        usage["reasoning"] += tok.get("reasoning") or 0
        usage["cache_read"] += cache.get("read") or 0
        usage["cache_write"] += cache.get("write") or 0
        usage["cost"] += info.get("cost") or 0
        parts = [p.get("text") or "" for p in m.get("parts") or [] if p.get("type") == "text"]
        if any(parts):
            text = "".join(parts)
        err = info.get("error")
        if err:
            error = (err.get("data") or {}).get("message") or err.get("name") or "error"
    usage["cost"] = round(usage["cost"], 8)
    return text, usage, error


def last_prompt(messages: list[dict]) -> Optional[str]:
    """最后一条用户消息的文本（不含 opencode 自己插入的 synthetic 部件，例如 plan 模式的提示）。"""
    for m in reversed(messages):
        if (m.get("info") or {}).get("role") == "user":
            return "".join(
                p.get("text") or "" for p in m.get("parts") or [] if p.get("type") == "text" and not p.get("synthetic")
            )
    return None


class OpencodeEngine(Engine):
    name = "opencode"
    supports_agent_param = True

    def translator(self, session_id: str) -> Translator:
        return Translator(session_id)

    def extract_result(self, messages: list[dict]) -> tuple[str, dict, Optional[str]]:
        return extract_result(messages)

    def last_prompt(self, messages: list[dict]) -> Optional[str]:
        return last_prompt(messages)

    def render_files(self, ctx: FilesContext) -> dict[str, bytes]:
        return {
            f"{ctx.workdir}/opencode.json": json.dumps(
                render_opencode_config(self.model, ctx.mcp), ensure_ascii=False, indent=2
            ).encode(),
            f"{ctx.workdir}/AGENTS.md": render_agents_md(
                workdir=ctx.workdir,
                max_life_h=ctx.max_life_h,
                idle_destroy_after_s=ctx.idle_destroy_after_s,
                mcp_names=sorted(ctx.mcp),
                egress=ctx.egress,
                instructions=ctx.instructions,
            ).encode(),
        }
