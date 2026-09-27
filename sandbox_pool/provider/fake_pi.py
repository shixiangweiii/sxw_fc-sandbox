"""内存版 pi 桥接进程（sandbox_pool/agent/pi_bridge/pi-bridge.mjs），仅用于测试。

事件是 pi 的原生事件（结构与本机真实 DeepSeek 实测一致，见 sxw_aicoding/方案设计/2026-09-26-pi引擎接入-实施方案.md
第 2 节），套在 {"type": "pi.event", "properties": {sessionID, runID, event}} 里，与真实桥接进程相同。

与真实行为保持一致的几点（缺了它们，相关问题在单测里看不到）：
- 客户端 close 之后再调用会报错；
- 重载配置（dispose）不中止运行中的会话；
- 运行中进程崩溃：run 记为 lost 并发 pi.run_lost；桥接进程重启（restart）后旧 run 查询为 unknown；
- 中止后最后一条 assistant 为 stopReason=error、errorMessage="This operation was aborted"；
- 每条消息 message_start → （assistant：message_update 增量）→ message_end（带全文），增量事件不带累积内容；
  断线重连靠 message_start 判断是否错过了消息开头（R3-N1）；
- 以 / 开头、命中扩展命令的提示词（加载了 pi-mcp-adapter 时的 /mcp、/pi-mcp、/mcp-auth）当作命令执行，不产生运行、
  没有任何事件（pi agent-session.ts 的 prompt()，命令名按第一个空格切分）。pi 0.87.1 的 prompt 响应不带 disposition，
  桥接进程无从得知，会话一直 busy、run 一直 running，中止也清不掉（本机真实桥接进程 + pi 0.87.1 实测，PI-H1）。

提示词指令（可组合）：[sleep:秒]、[tool]、[error]、[reasoning]、[retry]、[remember]、[crash]（进程崩溃）、
[ui]（扩展对话框请求，桥接进程自动应答）、[partial]（[sleep] 之前先完成一条文本为 "PARTIAL " 的 assistant 消息，
验证运行中途重连的补发）、[round]（回复末尾加上这是会话里的第几轮，提示词相同的两轮也能分出回复属于哪一轮）。
其余回复 "echo: <提示词>"。
"""

import asyncio
import json
import re
import time
import uuid
from typing import AsyncIterator, Optional

from sandbox_pool.agent.engines.pi import PI_CONFIG_FILE
from sandbox_pool.agent.opencode import OpencodeError
from sandbox_pool.provider.base import SandboxNotFound

_CHUNK = 8
_STEP_DELAY = 0.005
# pi-mcp-adapter 注册的扩展命令（pi.json 的 mcp 为 true 时加载该扩展）
_MCP_COMMANDS = ("mcp", "pi-mcp", "mcp-auth")


def _ms() -> int:
    return int(time.time() * 1000)


def _usage(i: int, o: int, reasoning: int = 0) -> dict:
    return {"input": i, "output": o, "cacheRead": 100, "cacheWrite": 0, "reasoning": reasoning,
            "totalTokens": i + o + 100, "cost": {"input": 0, "output": 0, "cacheRead": 0, "cacheWrite": 0, "total": 0.001}}


class FakePiServer:
    def __init__(self, files: dict):
        self.files = files  # 沙箱的文件（共享 FakeProvider 里的 dict），会话进程启动时读取 pi.json
        self.sessions: dict[str, dict] = {}
        self.runs: dict[str, str] = {}
        self.tasks: dict[str, asyncio.Task] = {}
        self.subscribers: set[asyncio.Queue] = set()
        self.healthy = True
        self.alive = True
        self.disposed = 0
        self.prompts: list[str] = []
        # 被当作扩展命令执行（disposition=handled）的提示词
        self.handled: list[str] = []
        self.aborts: list[str] = []
        # 每次运行时生效的配置（pi.json），用于断言设置下发
        self.configs: list[Optional[dict]] = []

    # ---------- 基础 ----------

    def emit(self, type_: str, properties: dict) -> None:
        ev = {"type": type_, "properties": properties}
        for q in list(self.subscribers):
            q.put_nowait(ev)

    def emit_pi(self, sid: str, event: dict) -> None:
        self.emit("pi.event", {"sessionID": sid, "runID": self.sessions[sid]["run_id"], "event": event})

    def shutdown(self) -> None:
        self.alive = False
        for t in self.tasks.values():
            t.cancel()
        for q in list(self.subscribers):
            q.put_nowait(None)

    def restart(self) -> None:
        """桥接进程重启：运行中的会话进程随之退出，内存里的 run 全部丢失（会话文件还在）。"""
        for t in self.tasks.values():
            t.cancel()
        self.tasks.clear()
        self.runs.clear()
        for s in self.sessions.values():
            s["busy"] = False
            s["run_id"] = None
        for q in list(self.subscribers):
            q.put_nowait(None)

    def busy(self) -> dict:
        return {sid: {"type": "busy", "run_id": s["run_id"]} for sid, s in self.sessions.items() if s["busy"]}

    def _settle(self, sid: str, state: str) -> None:
        s = self.sessions[sid]
        if s["run_id"]:
            self.runs[s["run_id"]] = state
        s["busy"] = False

    # ---------- 会话 ----------

    def create_session(self, title: str) -> str:
        sid = str(uuid.uuid4())
        self.sessions[sid] = {"id": sid, "title": title, "messages": [], "busy": False, "run_id": None}
        return sid

    def prompt(self, sid: str, text: str) -> str:
        s = self.sessions.get(sid)
        if s is None:
            raise OpencodeError(f"session {sid} not found", 404)
        if s["busy"]:
            raise OpencodeError(f"session {sid} is busy", 409)
        self.prompts.append(text)
        raw = self.files.get(PI_CONFIG_FILE)
        config = json.loads(raw) if raw else None
        self.configs.append(config)
        run_id = str(uuid.uuid4())
        s["busy"] = True
        s["run_id"] = run_id
        self.runs[run_id] = "running"
        if text.startswith("/") and text[1:].split(" ", 1)[0] in _MCP_COMMANDS and (config or {}).get("mcp"):
            self.handled.append(text)
            return run_id
        self.tasks[sid] = asyncio.create_task(self._run(sid, text))
        return run_id

    def abort(self, sid: str) -> bool:
        self.aborts.append(sid)
        t = self.tasks.get(sid)
        if t is not None and not t.done() and self.sessions[sid]["busy"]:
            t.cancel()
            return True
        return False

    async def _stream(self, sid: str, kind: str, text: str) -> None:
        for i in range(0, len(text), _CHUNK):
            self.emit_pi(sid, {"type": "message_update", "usage": _usage(0, 0),
                               "assistantMessageEvent": {"type": kind, "contentIndex": 0, "delta": text[i : i + _CHUNK]}})
            await asyncio.sleep(_STEP_DELAY)

    async def _run(self, sid: str, text: str) -> None:
        s = self.sessions[sid]
        prev = next((m for m in reversed(s["messages"]) if m["role"] == "user"), None)
        user = {"role": "user", "content": text, "timestamp": _ms()}
        s["messages"].append(user)
        self.emit_pi(sid, {"type": "agent_start"})
        self.emit_pi(sid, {"type": "turn_start"})
        self.emit_pi(sid, {"type": "message_start", "message": user})
        self.emit_pi(sid, {"type": "message_end", "message": user})
        content: list[dict] = []
        stop, error, reasoning = "stop", None, 0
        opened = False

        def open_reply() -> None:
            """与真实 pi 一致：assistant 消息先 message_start，再增量，最后 message_end（带全文）。"""
            nonlocal opened
            if not opened:
                opened = True
                self.emit_pi(sid, {"type": "message_start", "message": {"role": "assistant", "content": [], "timestamp": _ms()}})

        try:
            if "[retry]" in text:
                failed = {"role": "assistant", "content": [], "usage": _usage(0, 0), "stopReason": "error",
                          "errorMessage": "429 rate limited", "timestamp": _ms()}
                s["messages"].append(failed)
                self.emit_pi(sid, {"type": "message_start", "message": {**failed, "content": []}})
                self.emit_pi(sid, {"type": "message_end", "message": failed})
                self.emit_pi(sid, {"type": "auto_retry_start", "attempt": 1, "maxAttempts": 3, "delayMs": 10,
                                   "errorMessage": "429 rate limited"})
                await asyncio.sleep(0.02)
                self.emit_pi(sid, {"type": "auto_retry_end", "success": True, "attempt": 1})
            if "[partial]" in text:
                self.emit_pi(sid, {"type": "message_start", "message": {"role": "assistant", "content": [], "timestamp": _ms()}})
                await self._stream(sid, "text_delta", "PARTIAL ")
                partial = {"role": "assistant", "content": [{"type": "text", "text": "PARTIAL "}], "usage": _usage(0, 8),
                           "stopReason": "toolUse", "timestamp": _ms()}
                s["messages"].append(partial)
                self.emit_pi(sid, {"type": "message_end", "message": partial})
            if "[reasoning]" in text:
                open_reply()
                await self._stream(sid, "thinking_delta", "先想一想这个问题。")
                content.append({"type": "thinking", "thinking": "先想一想这个问题。"})
                reasoning = 8
            if "[tool]" in text:
                args = {"command": "uname -m"}
                self.emit_pi(sid, {"type": "tool_execution_start", "toolCallId": "call_1", "toolName": "bash", "args": args})
                await asyncio.sleep(0.02)
                result = {"content": [{"type": "text", "text": "x86_64\n"}]}
                self.emit_pi(sid, {"type": "tool_execution_end", "toolCallId": "call_1", "toolName": "bash",
                                   "result": result, "isError": False})
                s["messages"].append({"role": "toolResult", "toolCallId": "call_1", "toolName": "bash",
                                      "content": result["content"], "isError": False, "timestamp": _ms()})
            if "[ui]" in text:
                self.emit("pi.ui_request", {"sessionID": sid, "runID": s["run_id"], "method": "confirm",
                                            "summary": "confirm: Allow?", "answered": True})
            m = re.search(r"\[sleep:([\d.]+)\]", text)
            if m:
                await asyncio.sleep(float(m.group(1)))
            if "[crash]" in text:
                run_id = s["run_id"]
                self._settle(sid, "lost")
                self.emit("pi.run_lost", {"sessionID": sid, "runID": run_id, "reason": "exit code 1"})
                return
            if "[error]" in text:
                stop, error = "error", "fake model error"
            else:
                if "[remember]" in text:
                    reply = f"remembered: {prev['content'] if prev else '(nothing)'}"
                else:
                    reply = f"echo: {text}"
                if "[round]" in text:
                    reply += f" #{sum(1 for m in s['messages'] if m['role'] == 'user')}"
                open_reply()
                await self._stream(sid, "text_delta", reply)
                content.append({"type": "text", "text": reply})
        except asyncio.CancelledError:
            if not self.alive or not s["busy"]:
                raise
            stop, error, content = "error", "This operation was aborted", []
        assistant = {"role": "assistant", "content": content, "usage": _usage(len(text), sum(len(c.get("text", "")) for c in content), reasoning),
                     "stopReason": stop, "timestamp": _ms(), **({"errorMessage": error} if error else {})}
        s["messages"].append(assistant)
        open_reply()
        self.emit_pi(sid, {"type": "message_end", "message": assistant})
        self.emit_pi(sid, {"type": "turn_end", "message": assistant, "toolResults": []})
        self.emit_pi(sid, {"type": "agent_end", "messages": [user, assistant], "willRetry": False})
        self.emit_pi(sid, {"type": "agent_settled"})
        self._settle(sid, "settled")


class FakePiClient:
    """OpencodeAPI 的内存实现（pi 桥接进程）：每次调用都确认沙箱仍在运行、令牌正确。"""

    def __init__(self, provider, sandbox_id: str, access_token: Optional[str], directory: str):
        self.provider = provider
        self.sandbox_id = sandbox_id
        self.access_token = access_token
        self.directory = directory
        self.closed = False

    def _server(self) -> FakePiServer:
        if self.closed:
            raise RuntimeError("Cannot send a request, as the client has been closed.")
        try:
            sb = self.provider._running(self.sandbox_id)
        except (SandboxNotFound, RuntimeError) as e:
            raise OpencodeError(f"sandbox {self.sandbox_id} unreachable: {e}") from e
        if sb.get("access_token") != self.access_token:
            raise OpencodeError("forbidden", 403)
        server: FakePiServer = sb["pi"]
        if not server.healthy:
            raise OpencodeError("pi bridge unavailable", 502)
        return server

    async def health(self) -> dict:
        self._server()
        return {"healthy": True, "engine": "pi", "version": "fake"}

    async def create_session(self, title: str) -> str:
        return self._server().create_session(title)

    async def prompt_async(self, session_id, text, *, model, agent) -> Optional[str]:
        return self._server().prompt(session_id, text)

    async def run_state(self, session_id: str, run_id: str) -> str:
        return self._server().runs.get(run_id, "unknown")

    async def abort(self, session_id: str) -> bool:
        return self._server().abort(session_id)

    async def status(self) -> dict:
        return self._server().busy()

    async def messages(self, session_id: str) -> list[dict]:
        server = self._server()
        if session_id not in server.sessions:
            raise OpencodeError(f"session {session_id} not found", 404)
        return json.loads(json.dumps(server.sessions[session_id]["messages"]))

    async def dispose(self) -> None:
        # 与真实桥接进程一致：只重启空闲会话的进程，不中止运行中的会话
        self._server().disposed += 1

    async def reply_permission(self, request_id: str, reply: str) -> None:
        raise OpencodeError("not supported by pi bridge", 404)

    async def reject_question(self, request_id: str) -> None:
        raise OpencodeError("not supported by pi bridge", 404)

    async def events(self) -> AsyncIterator[dict]:
        server = self._server()
        q: asyncio.Queue = asyncio.Queue()
        server.subscribers.add(q)
        try:
            yield {"type": "server.connected", "properties": {}}
            while True:
                ev = await q.get()
                if ev is None:
                    raise OpencodeError("connection closed")
                yield ev
        finally:
            server.subscribers.discard(q)

    async def close(self) -> None:
        self.closed = True
