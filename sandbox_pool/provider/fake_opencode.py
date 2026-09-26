"""内存版 opencode server，仅用于测试。事件结构与实测一致（见 sxw_aicoding/技术调研/…PoC验证报告.md）。

与真实行为保持一致的两点（缺了它们，相关并发问题在单测里看不到）：客户端 close 之后再调用会报错；
dispose 会取消所有运行中的会话。

提示词里的指令模拟不同行为（可组合）：
- [sleep:秒]   处理这么久（可被中止）；
- [tool]       调用一次 bash 工具（running → completed）；
- [error]      模型报错（assistant 消息带 error，发 session.error）；
- [reasoning]  先输出一段思考；
- [retry]      先报一次限流重试（session.status retry）；
- [ask]        发起一次权限询问，等待回复（最多 5 秒）；
- [remember]   回复同一会话里上一轮的提问内容（验证会话连续）。
其余情况回复 "echo: <提示词>"。
"""

import asyncio
import json
import re
import time
import uuid
from typing import AsyncIterator, Optional

from sandbox_pool.agent.opencode import OpencodeError
from sandbox_pool.provider.base import SandboxNotFound

_CHUNK = 8
_STEP_DELAY = 0.005


def _id(prefix: str) -> str:
    return f"{prefix}_{uuid.uuid4().hex[:16]}"


def _ms() -> int:
    return int(time.time() * 1000)


class FakeOpencodeServer:
    def __init__(self, files: dict):
        self.files = files  # 沙箱的文件（共享 FakeProvider 里的 dict），读取工作目录的 opencode.json
        self.sessions: dict[str, dict] = {}
        self.subscribers: set[asyncio.Queue] = set()
        self.runs: dict[str, asyncio.Task] = {}
        self.healthy = True
        self.disposed = 0
        self.loaded_config: Optional[dict] = None
        self.permission_replies: dict[str, str] = {}
        self.question_rejects: set[str] = set()
        self.aborts: list[str] = []
        self.prompts: list[str] = []
        self.alive = True

    # ---------- 基础 ----------

    def emit(self, type_: str, properties: dict) -> None:
        ev = {"id": _id("evt"), "type": type_, "properties": properties}
        for q in list(self.subscribers):
            q.put_nowait(ev)

    def load_config(self, directory: str) -> dict:
        if self.loaded_config is None:
            raw = self.files.get(f"{directory}/opencode.json")
            self.loaded_config = json.loads(raw) if raw else {}
        return self.loaded_config

    def shutdown(self) -> None:
        self.alive = False
        for t in self.runs.values():
            t.cancel()
        for q in list(self.subscribers):
            q.put_nowait(None)

    def busy(self) -> dict:
        return {sid: {"type": s["status"]} for sid, s in self.sessions.items() if s["status"] != "idle"}

    def _set_status(self, sid: str, status: str, **extra) -> None:
        self.sessions[sid]["status"] = "idle" if status == "idle" else "busy"
        self.emit("session.status", {"sessionID": sid, "status": {"type": status, **extra}})

    # ---------- 会话 ----------

    def create_session(self, title: str) -> str:
        sid = _id("ses")
        self.sessions[sid] = {"id": sid, "title": title, "messages": [], "status": "idle"}
        self.emit("session.created", {"sessionID": sid, "info": {"id": sid, "title": title}})
        return sid

    def prompt(self, sid: str, text: str) -> None:
        if sid not in self.sessions:
            raise OpencodeError(f"session {sid} not found", 404)
        self.prompts.append(text)
        prev = self.runs.get(sid)
        if prev is not None and not prev.done():
            # 与实测一致：忙碌时的新消息并入正在运行的循环（这里简化为排在其后）
            self.runs[sid] = asyncio.create_task(self._after(prev, sid, text))
        else:
            self.runs[sid] = asyncio.create_task(self._run(sid, text))

    async def _after(self, prev: asyncio.Task, sid: str, text: str) -> None:
        await asyncio.gather(prev, return_exceptions=True)
        await self._run(sid, text)

    def abort(self, sid: str) -> bool:
        self.aborts.append(sid)
        run = self.runs.get(sid)
        if run is not None and not run.done():
            run.cancel()
            return True
        return False

    async def _stream_text(self, sid: str, mid: str, ptype: str, text: str) -> dict:
        pid = _id("prt")
        self.emit("message.part.updated", {"sessionID": sid, "part": {"id": pid, "messageID": mid, "sessionID": sid, "type": ptype, "text": ""}})
        for i in range(0, len(text), _CHUNK):
            self.emit("message.part.delta", {"sessionID": sid, "messageID": mid, "partID": pid, "field": "text", "delta": text[i : i + _CHUNK]})
            await asyncio.sleep(_STEP_DELAY)
        self.emit("message.part.updated", {"sessionID": sid, "part": {"id": pid, "messageID": mid, "sessionID": sid, "type": ptype, "text": text}})
        return {"id": pid, "type": ptype, "text": text}

    async def _run(self, sid: str, text: str) -> None:
        session = self.sessions[sid]
        prev_user = next((m for m in reversed(session["messages"]) if m["info"]["role"] == "user"), None)
        uid = _id("msg")
        user = {"info": {"id": uid, "role": "user", "sessionID": sid, "time": {"created": _ms()}},
                "parts": [{"id": _id("prt"), "type": "text", "text": text, "messageID": uid, "sessionID": sid}]}
        session["messages"].append(user)
        self.emit("message.updated", {"sessionID": sid, "info": user["info"]})
        self.emit("message.part.updated", {"sessionID": sid, "part": user["parts"][0]})
        self._set_status(sid, "busy")
        mid = _id("msg")
        info = {"id": mid, "role": "assistant", "sessionID": sid, "time": {"created": _ms()},
                "tokens": {"input": 0, "output": 0, "reasoning": 0, "cache": {"read": 0, "write": 0}}, "cost": 0}
        assistant = {"info": info, "parts": []}
        session["messages"].append(assistant)
        self.emit("message.updated", {"sessionID": sid, "info": dict(info)})
        try:
            if "[retry]" in text:
                self._set_status(sid, "retry", attempt=1, message="rate limited, retrying")
                await asyncio.sleep(0.05)
                self._set_status(sid, "busy")
            if "[reasoning]" in text:
                assistant["parts"].append(await self._stream_text(sid, mid, "reasoning", "先想一想这个问题。"))
                info["tokens"]["reasoning"] = 8
            if "[tool]" in text:
                pid = _id("prt")
                base = {"id": pid, "messageID": mid, "sessionID": sid, "type": "tool", "tool": "bash"}
                self.emit("message.part.updated", {"sessionID": sid, "part": {**base, "state": {"status": "pending"}}})
                self.emit("message.part.updated", {"sessionID": sid, "part": {**base, "state": {"status": "running", "input": {"command": "uname -m"}, "title": "uname -m"}}})
                await asyncio.sleep(0.02)
                done = {**base, "state": {"status": "completed", "input": {"command": "uname -m"}, "output": "x86_64\n", "title": "uname -m"}}
                self.emit("message.part.updated", {"sessionID": sid, "part": done})
                assistant["parts"].append(done)
            if "[ask]" in text:
                req = _id("per")
                self.emit("permission.asked", {"id": req, "sessionID": sid, "permission": "bash", "patterns": ["rm -rf /"]})
                for _ in range(500):
                    if req in self.permission_replies:
                        break
                    await asyncio.sleep(0.01)
            m = re.search(r"\[sleep:([\d.]+)\]", text)
            if m:
                await asyncio.sleep(float(m.group(1)))
            if "[error]" in text:
                info["error"] = {"name": "APIError", "data": {"message": "fake model error"}}
                self.emit("message.updated", {"sessionID": sid, "info": dict(info)})
                self.emit("session.error", {"sessionID": sid, "error": info["error"]})
            else:
                if "[remember]" in text:
                    reply = f"remembered: {prev_user['parts'][0]['text'] if prev_user else '(nothing)'}"
                else:
                    reply = f"echo: {text}"
                assistant["parts"].append(await self._stream_text(sid, mid, "text", reply))
                info["tokens"].update(input=len(text), output=len(reply), cache={"read": 100, "write": 0})
                info["cost"] = 0.001
            info["time"]["completed"] = _ms()
            self.emit("message.updated", {"sessionID": sid, "info": dict(info)})
        except asyncio.CancelledError:
            info["error"] = {"name": "MessageAbortedError", "data": {"message": "The operation was aborted."}}
            self.emit("message.updated", {"sessionID": sid, "info": dict(info)})
            self.emit("session.error", {"sessionID": sid, "error": info["error"]})
            if self.alive:
                self._set_status(sid, "idle")
            raise
        self._set_status(sid, "idle")


class FakeOpencodeClient:
    """OpencodeAPI 的内存实现：每次调用都确认沙箱仍在运行、令牌正确（与真实入口的 403 对应）。"""

    def __init__(self, provider, sandbox_id: str, access_token: Optional[str], directory: str):
        self.provider = provider
        self.sandbox_id = sandbox_id
        self.access_token = access_token
        self.directory = directory
        self.closed = False

    def _server(self) -> FakeOpencodeServer:
        if self.closed:
            # 与 httpx.AsyncClient 关闭后的行为一致
            raise RuntimeError("Cannot send a request, as the client has been closed.")
        try:
            sb = self.provider._running(self.sandbox_id)
        except (SandboxNotFound, RuntimeError) as e:
            raise OpencodeError(f"sandbox {self.sandbox_id} unreachable: {e}") from e
        if sb.get("access_token") != self.access_token:
            raise OpencodeError("forbidden", 403)
        server: FakeOpencodeServer = sb["opencode"]
        if not server.healthy:
            raise OpencodeError("opencode unavailable", 502)
        server.load_config(self.directory)
        return server

    async def health(self) -> dict:
        self._server()
        return {"healthy": True, "version": "fake"}

    async def create_session(self, title: str) -> str:
        return self._server().create_session(title)

    async def prompt_async(self, session_id, text, *, model, agent) -> None:
        self._server().prompt(session_id, text)

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
        server = self._server()
        server.disposed += 1
        server.loaded_config = None
        # 与真实 opencode 一致：实例销毁时取消所有运行中的会话（session/run-state.ts 的 finalizer）
        for run in list(server.runs.values()):
            run.cancel()

    async def reply_permission(self, request_id: str, reply: str) -> None:
        self._server().permission_replies[request_id] = reply

    async def reject_question(self, request_id: str) -> None:
        self._server().question_rejects.add(request_id)

    async def events(self) -> AsyncIterator[dict]:
        server = self._server()
        q: asyncio.Queue = asyncio.Queue()
        server.subscribers.add(q)
        try:
            yield {"id": _id("evt"), "type": "server.connected", "properties": {}}
            while True:
                ev = await q.get()
                if ev is None:
                    raise OpencodeError("connection closed")
                yield ev
        finally:
            server.subscribers.discard(q)

    async def close(self) -> None:
        self.closed = True
