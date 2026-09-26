"""网关访问沙箱内 opencode server 的 HTTP 客户端。

- 经平台入口 https://<port>-<sandbox_id>.<domain> 访问，每个请求带流量令牌 e2b-traffic-access-token。
- 所有请求带 ?directory=<工作目录>：opencode 按目录加载项目配置（opencode.json、AGENTS.md）。
- 不读系统代理（macOS 的系统代理对沙箱域名返回 503，实测），只用显式配置的 HTTPS_PROXY。
- 本机 DNS 被代理的 fake-ip 接管时，到沙箱域名的连接约 1/3 失败（实测）：配置 ingress_ip 后直连平台入口 IP，
  TLS SNI 与 Host 仍用沙箱域名。每个沙箱一个连接池，不同沙箱不复用带着别的 SNI 的连接。
  配置了 ingress_ip 就不走 HTTPS_PROXY：经代理隧道时 httpcore 用目标地址（IP）做 TLS 的 server_hostname、
  忽略 sni_hostname，证书校验必然失败（实测 CERTIFICATE_VERIFY_FAILED: IP address mismatch）。
- 建连失败（ConnectError / ConnectTimeout）说明请求没发出去，任何方法都重试；幂等的 GET 读失败也重试；
  非幂等的 POST 读失败不重试（请求可能已生效）。
"""

import asyncio
import json
import logging
import os
from typing import Any, AsyncIterator, Optional, Protocol

import httpx

log = logging.getLogger(__name__)

_CONNECT_ERRORS = (httpx.ConnectError, httpx.ConnectTimeout)
# 幂等请求额外重试的错误：读响应时连接断开
_IDEMPOTENT_ERRORS = _CONNECT_ERRORS + (httpx.ReadError, httpx.RemoteProtocolError, httpx.WriteError)
_RETRY_DELAYS = (0.3, 0.8, 1.5, 3.0, 5.0)


class OpencodeError(Exception):
    """opencode server 返回错误或不可达。"""

    def __init__(self, message: str, status: Optional[int] = None):
        super().__init__(message)
        self.status = status


class OpencodeAPI(Protocol):
    """AgentService / TaskRunner 依赖的 opencode 能力；测试用内存实现（provider/fake.py 的 FakeOpencode）。"""

    async def health(self) -> dict: ...

    async def create_session(self, title: str) -> str: ...

    async def prompt_async(self, session_id: str, text: str, *, model: Optional[str], agent: Optional[str]) -> None: ...

    async def abort(self, session_id: str) -> bool: ...

    async def status(self) -> dict[str, dict]:
        """忙碌中的会话：{session_id: {type: busy|retry, ...}}；空闲会话不出现。"""

    async def messages(self, session_id: str) -> list[dict]: ...

    async def dispose(self) -> None:
        """重新加载工作目录的项目配置。"""

    async def reply_permission(self, request_id: str, reply: str) -> None: ...

    async def reject_question(self, request_id: str) -> None: ...

    def events(self) -> AsyncIterator[dict]:
        """订阅事件流：连上后第一条是 server.connected；连接断开时迭代结束或抛异常，由调用方决定是否重连。"""

    async def close(self) -> None: ...


class SSEDecoder:
    """按行解析 text/event-stream：多行 data 以换行拼接，空行结束一个事件；注释行（以 : 开头）忽略。"""

    def __init__(self):
        self._data: list[str] = []

    def feed_line(self, line: str) -> Optional[str]:
        if line.endswith("\r"):
            line = line[:-1]
        if line == "":
            if not self._data:
                return None
            data = "\n".join(self._data)
            self._data = []
            return data
        if line.startswith(":"):
            return None
        name, _, value = line.partition(":")
        if value.startswith(" "):
            value = value[1:]
        if name == "data":
            self._data.append(value)
        return None


def make_proxy() -> Optional[str]:
    return os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy") or None


class OpencodeHttpClient:
    def __init__(
        self,
        endpoint: str,
        access_token: Optional[str],
        directory: str,
        *,
        ingress_ip: Optional[str] = None,
        timeout_s: float = 30,
        sse_read_timeout_s: float = 60,
    ):
        self.endpoint = endpoint.rstrip("/")
        host = httpx.URL(self.endpoint).host
        self._ext = {"sni_hostname": host} if ingress_ip else {}
        base = f"https://{ingress_ip}" if ingress_ip else self.endpoint
        headers = {"Host": host}
        if access_token:
            headers["e2b-traffic-access-token"] = access_token
        self._params = {"directory": directory}
        self._sse_timeout = httpx.Timeout(timeout_s, read=sse_read_timeout_s)
        self.proxy = None if ingress_ip else make_proxy()
        self._http = httpx.AsyncClient(
            base_url=base,
            headers=headers,
            timeout=httpx.Timeout(timeout_s, connect=min(10.0, timeout_s)),
            trust_env=False,
            proxy=self.proxy,
        )

    async def _request(self, method: str, path: str, *, json_body: Any = None, idempotent: bool) -> httpx.Response:
        retry_on = _IDEMPOTENT_ERRORS if idempotent else _CONNECT_ERRORS
        last: Optional[Exception] = None
        for delay in (*_RETRY_DELAYS, None):
            try:
                resp = await self._http.request(method, path, params=self._params, json=json_body, extensions=self._ext)
            except retry_on as e:
                last = e
                if delay is None:
                    break
                log.info("opencode %s %s failed (%r), retrying in %.1fs", method, path, e, delay)
                await asyncio.sleep(delay)
                continue
            except httpx.HTTPError as e:
                raise OpencodeError(f"{method} {path}: {type(e).__name__}: {e}") from e
            if resp.status_code >= 400:
                raise OpencodeError(f"{method} {path}: HTTP {resp.status_code} {resp.text[:300]}", resp.status_code)
            return resp
        raise OpencodeError(f"{method} {path}: {type(last).__name__}: {last}") from last

    @staticmethod
    def _json(resp: httpx.Response) -> Any:
        return resp.json() if resp.content else None

    async def health(self) -> dict:
        return self._json(await self._request("GET", "/global/health", idempotent=True)) or {}

    async def create_session(self, title: str) -> str:
        session = self._json(await self._request("POST", "/session", json_body={"title": title}, idempotent=False))
        return session["id"]

    async def prompt_async(self, session_id: str, text: str, *, model: Optional[str], agent: Optional[str]) -> None:
        body: dict = {"parts": [{"type": "text", "text": text}]}
        if model:
            provider_id, _, model_id = model.partition("/")
            body["model"] = {"providerID": provider_id, "modelID": model_id}
        if agent:
            body["agent"] = agent
        await self._request("POST", f"/session/{session_id}/prompt_async", json_body=body, idempotent=False)

    async def abort(self, session_id: str) -> bool:
        # 中止是幂等的：重复中止同一个会话没有副作用
        return bool(self._json(await self._request("POST", f"/session/{session_id}/abort", idempotent=True)))

    async def status(self) -> dict[str, dict]:
        return self._json(await self._request("GET", "/session/status", idempotent=True)) or {}

    async def messages(self, session_id: str) -> list[dict]:
        return self._json(await self._request("GET", f"/session/{session_id}/message", idempotent=True)) or []

    async def dispose(self) -> None:
        await self._request("POST", "/instance/dispose", idempotent=True)

    async def reply_permission(self, request_id: str, reply: str) -> None:
        await self._request("POST", f"/permission/{request_id}/reply", json_body={"reply": reply}, idempotent=True)

    async def reject_question(self, request_id: str) -> None:
        await self._request("POST", f"/question/{request_id}/reject", idempotent=True)

    async def events(self) -> AsyncIterator[dict]:
        last: Optional[Exception] = None
        for delay in (*_RETRY_DELAYS, None):
            try:
                async with self._http.stream(
                    "GET", "/event", params=self._params, timeout=self._sse_timeout, extensions=self._ext
                ) as resp:
                    if resp.status_code >= 400:
                        body = (await resp.aread())[:300]
                        raise OpencodeError(f"GET /event: HTTP {resp.status_code} {body!r}", resp.status_code)
                    decoder = SSEDecoder()
                    async for line in resp.aiter_lines():
                        data = decoder.feed_line(line)
                        if data is None:
                            continue
                        try:
                            event = json.loads(data)
                        except json.JSONDecodeError:
                            log.debug("skip non-json event data: %r", data[:200])
                            continue
                        if isinstance(event, dict):
                            yield event
                    return
            except _CONNECT_ERRORS as e:
                last = e
                if delay is None:
                    break
                await asyncio.sleep(delay)
        raise OpencodeError(f"GET /event: {type(last).__name__}: {last}") from last

    async def close(self) -> None:
        await self._http.aclose()
