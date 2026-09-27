"""pi 引擎：沙箱里运行 pi 桥接进程（agent/pi_bridge/pi-bridge.mjs），每个会话一个 `pi --mode rpc` 子进程。

- 桥接进程的控制接口沿用 opencode 的路径（网关用同一个 HTTP 客户端）；事件是 pi 的原生事件，套在
  {"type": "pi.event", "properties": {"sessionID", "runID", "event"}} 里；进程丢失发 pi.run_lost，扩展 UI 请求发
  pi.ui_request。
- pi 事件与消息结构见 sxw_aicoding/方案设计/2026-09-26-pi引擎接入-实施方案.md 第 2 节（本机真实 DeepSeek 实测）。
- 写进沙箱的文件：工作目录的 AGENTS.md（pi 从工作目录读）、桥接进程的配置 pi.json（模型、思考级别、是否加载 MCP
  扩展，每次起会话进程时读取）、pi-mcp-adapter 的 mcp.json。
"""

import json
from typing import Optional

from sandbox_pool.agent.engines.base import Engine, FilesContext, cut
from sandbox_pool.agent.policy import render_agents_md

# 桥接进程每次起会话进程时读取；路径与模板里的 run.sh 一致（scripts/build_pi_template.py）
PI_CONFIG_FILE = "/home/user/.agent/pi.json"
# pi-mcp-adapter 的全局配置（<pi agent 目录>/mcp.json）
PI_MCP_FILE = "/home/user/.pi/agent/mcp.json"
THINKING_LEVELS = ("off", "minimal", "low", "medium", "high", "xhigh", "max")

_PI_EXTRA_LINES = [
    "- 没有专门的网页抓取工具：需要读取网页时，用 bash 执行 curl（例如 `curl -sL <URL> | head -c 20000`）。",
]
# MCP 工具直接注册（directTools，名称为「服务名_工具名」，例如 websearch_bailian_web_search），与 opencode 下同名
_PI_MCP_HINT = "工具名以服务名为前缀；联网搜索优先使用 websearch 相关工具。"


class PiTranslator:
    """pi 事件 → 对外事件。

    - 文本 / 思考来自 message_update 的 text_delta / thinking_delta；
    - 工具在 tool_execution_start（running）和 tool_execution_end（completed / error）时输出，输入取自 start；
    - 完成判定：agent_settled（agent_end 之后还可能有自动重试、压缩、排队消息）；
    - 不把 message_end 里的错误记为本轮错误：自动重试成功后历史里仍有失败的那条，最终错误由 extract_result 按最后一条
      assistant 判定；只有进程丢失（pi.run_lost）记为错误；
    - mid_round（断线重连、接管）：没看到 message_start 的 assistant 消息是订阅之前就开始输出的，增量缺了开头，
      而 RPC 的 message_update 不带累积内容：不转发增量，等 message_end 按全文输出（R3-N1）。
    """

    def __init__(self, session_id: str):
        self.session_id = session_id
        self.busy_seen = False
        self.idle = False
        self.errors: list[str] = []
        self.asks: list[tuple[str, str]] = []
        self.round_starts = 0
        self.mid_round = False
        self.tool_args: dict[str, object] = {}
        # 当前 assistant 消息：看到了它的 message_start / 增量没有转发（缺开头，等 message_end）
        self._msg_open = False
        self._msg_held = False

    def feed(self, ev: dict) -> list[tuple[str, dict]]:
        out: list[tuple[str, dict]] = []
        kind = ev.get("type")
        props = ev.get("properties") or {}
        if props.get("sessionID") != self.session_id:
            return out
        if kind == "pi.run_lost":
            self.busy_seen = True
            self.idle = True
            self.errors.append(cut(f"agent process exited: {props.get('reason') or 'unknown'}", 1000))
            return out
        if kind == "pi.ui_request":
            out.append(("status", {"type": "ui_request", "message": cut(props.get("summary"), 500)}))
            return out
        if kind != "pi.event":
            return out
        e = props.get("event") or {}
        t = e.get("type")
        if t == "agent_start":
            self.busy_seen = True
            self.round_starts += 1
        elif t == "message_start":
            if (e.get("message") or {}).get("role") == "assistant":
                self._msg_open = True
        elif t == "message_update":
            ame = e.get("assistantMessageEvent") or {}
            if self.mid_round and not self._msg_open:
                self._msg_held = True
            elif ame.get("type") == "text_delta" and ame.get("delta"):
                out.append(("text", {"delta": ame["delta"]}))
            elif ame.get("type") == "thinking_delta" and ame.get("delta"):
                out.append(("reasoning", {"delta": ame["delta"]}))
        elif t == "message_end":
            m = e.get("message") or {}
            if m.get("role") == "assistant":
                if self._msg_held:
                    content = m.get("content")
                    thinking = "".join(c.get("thinking") or "" for c in content or []
                                       if isinstance(c, dict) and c.get("type") == "thinking")
                    text = _content_text(content)
                    if thinking:
                        out.append(("reasoning", {"delta": thinking}))
                    if text:
                        out.append(("text", {"delta": text}))
                self._msg_open = self._msg_held = False
        elif t == "tool_execution_start":
            args = e.get("args")
            self.tool_args[e.get("toolCallId") or ""] = args
            out.append(("tool", {"tool": e.get("toolName"), "status": "running", "title": None,
                                 "input": cut(args), "output": None, "error": None}))
        elif t == "tool_execution_end":
            text = _content_text((e.get("result") or {}).get("content"))
            failed = bool(e.get("isError"))
            out.append(("tool", {"tool": e.get("toolName"), "status": "error" if failed else "completed", "title": None,
                                 "input": cut(self.tool_args.pop(e.get("toolCallId") or "", None)),
                                 "output": None if failed else cut(text), "error": cut(text) if failed else None}))
        elif t == "auto_retry_start":
            self.busy_seen = True
            out.append(("status", {"type": "retry", "message": cut(e.get("errorMessage"), 500), "attempt": e.get("attempt")}))
        elif t == "compaction_start":
            out.append(("status", {"type": "compaction", "message": e.get("reason")}))
        elif t == "agent_settled" and self.busy_seen:
            self.idle = True
        return out

    def flush(self) -> list[tuple[str, dict]]:
        return []


def _content_text(content) -> str:
    if isinstance(content, str):
        return content
    return "".join(c.get("text") or "" for c in content or [] if isinstance(c, dict) and c.get("type") == "text")


def extract_result(messages: list[dict]) -> tuple[str, dict, Optional[str]]:
    """本轮结果：最后一条用户消息之后的 assistant 消息。文本取最后一条有文本的，用量求和；最后一条 assistant 以错误或
    中止结束时取其错误信息（中止也表现为 stopReason=error，任务状态以网关自己的中止标记为准）。"""
    last_user = -1
    for i, m in enumerate(messages):
        if m.get("role") == "user":
            last_user = i
    assistants = [m for m in messages[last_user + 1 :] if m.get("role") == "assistant"]
    usage = {"input": 0, "output": 0, "reasoning": 0, "cache_read": 0, "cache_write": 0, "cost": 0.0, "steps": len(assistants)}
    text = ""
    for m in assistants:
        u = m.get("usage") or {}
        usage["input"] += u.get("input") or 0
        usage["output"] += u.get("output") or 0
        usage["reasoning"] += u.get("reasoning") or 0
        usage["cache_read"] += u.get("cacheRead") or 0
        usage["cache_write"] += u.get("cacheWrite") or 0
        usage["cost"] += (u.get("cost") or {}).get("total") or 0
        part = _content_text(m.get("content"))
        if part:
            text = part
    usage["cost"] = round(usage["cost"], 8)
    error = None
    if assistants and assistants[-1].get("stopReason") in ("error", "aborted"):
        error = assistants[-1].get("errorMessage") or assistants[-1]["stopReason"]
    return text, usage, error


def last_prompt(messages: list[dict]) -> Optional[str]:
    for m in reversed(messages):
        if m.get("role") == "user":
            return _content_text(m.get("content"))
    return None


def mcp_servers(mcp: dict) -> dict:
    """网关的 MCP 配置（opencode 格式，policy.validate_mcp）→ pi-mcp-adapter 的 mcpServers。

    每个服务都直接注册工具（directTools）：本机实测默认的 `mcp` 代理工具要先搜索工具名，模型常猜错、多走几步
    （百炼 WebSearch：代理 6.6s、直接 4.7s）。服务数量少，全部直接注册的上下文开销可以接受。
    """
    out = {}
    for name, conf in sorted(mcp.items()):
        if conf.get("enabled", True) is False:
            continue
        if conf.get("type") == "remote":
            item: dict = {"url": conf["url"], "directTools": True}
            if conf.get("headers"):
                item["headers"] = conf["headers"]
        else:
            cmd = conf["command"]
            item = {"command": cmd[0], "args": cmd[1:], "directTools": True}
            if conf.get("environment"):
                item["env"] = conf["environment"]
        out[name] = item
    return out


class PiEngine(Engine):
    name = "pi"
    supports_agent_param = False
    has_runs = True

    def __init__(self, *, template: str, port: int, model: str, workdir: str, thinking: str = ""):
        super().__init__(template=template, port=port, model=model, workdir=workdir)
        self.thinking = thinking

    def prompt_text(self, text: str) -> str:
        """pi 把以 / 开头、命中扩展命令的提示词（加载了 pi-mcp-adapter 时有 /mcp、/pi-mcp、/mcp-auth）当作命令执行，
        不产生运行、没有 agent_settled。pi 0.87.1 的 prompt 响应不带 disposition，桥接进程把会话一直记为忙：任务挂到
        截止时间，中止也清不掉，会话从此不可用（PI-H1，本机真实桥接进程实测）。与 opencode 一致，用户消息一律作为普通
        提示词：前面加一个空格（pi 只在文本以 / 开头时尝试扩展命令、技能与提示词模板，见 agent-session.ts 的 prompt()）。"""
        return f" {text}" if text.startswith("/") else text

    def translator(self, session_id: str) -> PiTranslator:
        return PiTranslator(session_id)

    def extract_result(self, messages: list[dict]) -> tuple[str, dict, Optional[str]]:
        return extract_result(messages)

    def last_prompt(self, messages: list[dict]) -> Optional[str]:
        return last_prompt(messages)

    def render_files(self, ctx: FilesContext) -> dict[str, bytes]:
        servers = mcp_servers(ctx.mcp)
        provider, _, model = self.model.partition("/")
        config = {"provider": provider, "model": model, "thinking": self.thinking or None, "mcp": bool(servers)}
        return {
            PI_CONFIG_FILE: json.dumps(config, ensure_ascii=False, indent=2).encode(),
            PI_MCP_FILE: json.dumps({"mcpServers": servers}, ensure_ascii=False, indent=2).encode(),
            f"{ctx.workdir}/AGENTS.md": render_agents_md(
                workdir=ctx.workdir,
                max_life_h=ctx.max_life_h,
                idle_destroy_after_s=ctx.idle_destroy_after_s,
                mcp_names=sorted(servers),
                egress=ctx.egress,
                instructions=ctx.instructions,
                mcp_hint=_PI_MCP_HINT,
                extra_lines=_PI_EXTRA_LINES,
            ).encode(),
        }

    def describe(self) -> dict:
        d = super().describe()
        d["thinking"] = self.thinking or None
        d["capabilities"].update(webfetch_tool=False, permission_prompts=False, reload_keeps_running_tasks=True)
        return d
