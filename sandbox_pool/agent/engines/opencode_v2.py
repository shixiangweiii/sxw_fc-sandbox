"""OpenCode 2.x 的原生 API。协议由沙箱记录的引擎名区分。"""

import hashlib
import hmac
import json

import httpx

from sandbox_pool.agent.engines.base import cut
from sandbox_pool.agent.engines.opencode import OpencodeEngine, Translator
from sandbox_pool.agent.opencode import AgentHttpClient


def server_password(token: str) -> str:
    """同一实例的不同网关副本可推导相同应用密码，模板中无需保存密码。"""
    if not token:
        raise ValueError("OpenCode 2 需要安全沙箱流量令牌")
    return hmac.new(token.encode(), b"sxw-fc/opencode-v2/basic", hashlib.sha256).hexdigest()


class V2Client(AgentHttpClient):
    events_path = "/api/event"

    def __init__(self, endpoint, access_token, directory, *, ingress_ip=None):
        super().__init__(endpoint, access_token, directory, ingress_ip=ingress_ip)
        self._http.auth = httpx.BasicAuth("opencode", server_password(access_token))
        self.directory = directory
        self._params = {}

    async def health(self):
        info = (await self._request("GET", "/api/info", idempotent=True)).json()
        return {"healthy": str(info.get("version", "")).startswith("2."), "version": info.get("version")}

    async def create_session(self, title):
        r = await self._request(
            "POST",
            "/api/session",
            json_body={"title": title, "location": {"directory": self.directory}},
            idempotent=False,
            slow=True,
        )
        return r.json()["data"]["id"]

    async def prompt_async(self, session_id, text, *, model, agent):
        path = f"/api/session/{session_id}"
        if model:
            provider, _, name = model.partition("/")
            await self._request(
                "POST", path + "/model", json_body={"model": {"providerID": provider, "id": name}}, idempotent=True
            )
        if agent:
            await self._request("POST", path + "/agent", json_body={"agent": agent}, idempotent=True)
        await self._request("POST", path + "/prompt", json_body={"text": text}, idempotent=False, slow=True)

    async def abort(self, session_id):
        r = await self._request("POST", f"/api/session/{session_id}/interrupt", idempotent=False)
        return r.json()["interrupted"]

    async def _pages(self, path, *, stop_at_user=False):
        rows = []
        params = {"limit": "100", "order": "desc"}
        while True:
            page = (await self._request("GET", path, params=params, idempotent=True)).json()
            # 指定 limit 时采用分页信封。兼容未分页数组。
            items = page["data"] if isinstance(page, dict) else page
            rows.extend(items)
            if stop_at_user and any(m.get("type") == "user" for m in items):
                break
            cursor = page.get("cursor", {}).get("next") if isinstance(page, dict) else None
            if not cursor:
                break
            params.pop("order", None)
            params["cursor"] = cursor
        return rows

    async def status(self):
        return {
            s["id"]: {"type": "busy"}
            for s in await self._pages("/api/session")
            if s["time"].get("busy", 0) > s["time"].get("idle", 0)
        }

    async def messages(self, session_id):
        rows = await self._pages(f"/api/session/{session_id}/message", stop_at_user=True)
        return list(reversed(rows))

    async def dispose(self):
        await self._request(
            "POST", "/api/location/reload", params={"location[directory]": self.directory}, idempotent=True
        )

    async def reply_permission(self, request_id, reply):
        session, rid = request_id.split("/", 1)
        await self._request(
            "POST",
            f"/api/session/{session}/permission/{rid}/reply",
            json_body={"decision": reply},
            idempotent=False,
        )

    async def reject_question(self, request_id):
        session, rid = request_id.split("/", 1)
        await self._request("DELETE", f"/api/session/{session}/form/{rid}", idempotent=True)


class V2Translator(Translator):
    def __init__(self, session_id):
        super().__init__(session_id)
        self.tool_names = {}

    def feed(self, ev):
        data = ev.get("data") or {}
        kind = ev.get("type", "")
        form = data.get("form") or {}
        if (data.get("sessionID") or form.get("sessionID")) != self.session_id:
            return []
        if kind == "session.execution.started":
            self.busy_seen = True
            self.round_starts += 1
        elif kind in ("session.execution.succeeded", "session.execution.failed", "session.execution.interrupted"):
            self.idle = True
            if kind == "session.execution.failed":
                self.errors.append((data.get("error") or {}).get("message") or "OpenCode execution failed")
        elif kind.startswith(("session.text.", "session.reasoning.")):
            _, part, phase = kind.split(".")
            key = (data.get("assistantMessageID"), data.get("ordinal"), part)
            if phase == "started":
                self.emitted[key] = 0
            elif phase == "delta" and (not self.mid_round or key in self.emitted):
                out = []
                self._emit_text(key, part, data.get("delta", ""), out)
                return out
            elif phase == "ended":
                out = []
                self._emit_text(key, part, (data.get("text") or "")[self.emitted.get(key, 0) :], out)
                return out
        elif kind == "session.tool.input.started":
            self.tool_names[data["id"]] = data["name"]
        elif kind in ("session.tool.called", "session.tool.success", "session.tool.failed"):
            status = {
                "session.tool.called": "running",
                "session.tool.success": "completed",
                "session.tool.failed": "error",
            }[kind]
            content = data.get("content", [])
            output = "\n".join(p.get("text", "") for p in content if p.get("type") == "text")
            return [
                (
                    "tool",
                    {
                        "tool": self.tool_names.get(data["id"]),
                        "status": status,
                        "input": cut(data.get("input")),
                        "output": cut(output),
                        "error": cut(data.get("error")),
                    },
                )
            ]
        elif kind == "permission.asked":
            self.asks.append(("permission", f"{self.session_id}/{data['id']}"))
        elif kind == "form.created":
            self.asks.append(("question", f"{self.session_id}/{form['id']}"))
        return []


def native_mcp_config(config):
    servers = config.get("mcp", {})
    config["mcp"] = {"servers": {name: {**server, "codemode": False} for name, server in servers.items()}}
    return config


class OpencodeV2Engine(OpencodeEngine):
    name = "opencode2"

    def render_files(self, ctx):
        files = super().render_files(ctx)
        path = f"{ctx.workdir}/opencode.json"
        files[path] = json.dumps(native_mcp_config(json.loads(files[path])), ensure_ascii=False).encode()
        return files

    def translator(self, session_id):
        return V2Translator(session_id)

    def last_prompt(self, messages):
        return next((m.get("text", "") for m in reversed(messages) if m["type"] == "user"), None)

    def extract_result(self, messages):
        current = []
        for m in messages:
            if m["type"] == "user":
                current = []
            else:
                current.append(m)
        # 复用网关对单轮用量和错误的聚合。V2 的消息部件名为 content。
        normalized = []
        for m in current:
            if m["type"] not in ("assistant", "idle"):
                continue
            err = m.get("error") or {}
            info = {"role": "assistant", "tokens": m.get("tokens") or {}, "cost": m.get("cost") or 0}
            if err:
                info["error"] = {"data": {"message": err.get("message") or err.get("type")}}
            if m["type"] == "assistant" or err:
                normalized.append({"info": info, "parts": m.get("content", [])})
        return super().extract_result(normalized)
