"""基于 E2B 异步 SDK 的后端（阿里云云沙箱兼容 E2B 协议）。

- 固定 e2b==2.31.0：更新的 SDK 走 /v2 接口，云沙箱不支持。
- SDK 使用自定义 httpx transport，不读 HTTPS_PROXY，需要显式传 proxy。
- 后台任务只用 get_info / set_timeout / pause / kill 等类方法；connect() 会续期并恢复暂停的沙箱，
  只在 resume 和代为执行时使用。
- 每个云端调用都设了请求超时，保证不超过对应过渡态的截止时间（PoolConfig 的 destroy_timeout_s /
  resume_timeout_s / op_timeout_s），健康但较慢的操作不会被其他副本误接管。
- SDK 按事件循环共享一个 HTTP/2 连接池。经 HTTP 代理出网时，空闲一段时间的连接会失效，下一个请求报
  WriteError / ReadError（异常信息为空）。管控面调用在连接类错误时重试一次（超时不重试）；
  用户代码执行（run_code 等）不重试。
"""

import asyncio
import logging
import os
import time
from collections import OrderedDict
from typing import Any, Callable, Optional

import httpx
from e2b import NotFoundException, SandboxQuery
from e2b.sandbox.commands.command_handle import CommandExitException
from e2b_code_interpreter import AsyncSandbox

from sandbox_pool.provider.base import (
    CodeResult,
    CommandResult,
    ProviderSandbox,
    SandboxNotFound,
)

log = logging.getLogger(__name__)

# 云端调用的请求超时（秒）
_KILL_TIMEOUT = 20  # < destroy_timeout_s（默认 30）
_CONNECT_TIMEOUT = 30  # 恢复 = connect + 探活，合计 < resume_timeout_s（默认 60）
_PROBE_TIMEOUT = 15
_API_TIMEOUT = 30  # set_timeout / get_info / list
_WARMUP_TIMEOUT = 60  # 创建（SDK 默认请求超时 60s）+ 预热 < op_timeout_s（默认 120）

# 连接失效类错误：请求在失效的连接上发送失败或读不到响应，换一条连接重试即可
_STALE_CONNECTION_ERRORS = (httpx.NetworkError, httpx.RemoteProtocolError)
# 请求肯定没有到达服务端的错误：非幂等的创建只在这类错误时重试
_NOT_SENT_ERRORS = (httpx.ConnectError, httpx.WriteError)


# 连接失效后的重试间隔：同一条 HTTP/2 连接上的并发请求会一起失败，稍等让连接池丢弃坏连接
_STALE_RETRY_DELAYS = (0.3, 1.0)


async def _retry_stale(call, retry_on=_STALE_CONNECTION_ERRORS):
    """连接失效时最多重试两次（连接类错误都是快速失败，不会超出过渡态截止时间）。call 是返回协程的无参函数。"""
    for delay in _STALE_RETRY_DELAYS:
        try:
            return await call()
        except retry_on as e:
            log.info("retrying in %.1fs after stale connection error: %r", delay, e)
            await asyncio.sleep(delay)
    return await call()


class HandleCache:
    """本副本的连接句柄缓存：按最近使用淘汰（LRU），空闲超过 idle_ttl_s 的也淘汰。

    句柄共享 SDK 按事件循环缓存的 transport（连接池），淘汰时只丢弃引用；
    不能调用句柄内 httpx 客户端的 aclose，否则会关掉共享的连接池。
    """

    def __init__(self, max_size: int = 64, idle_ttl_s: float = 600, clock: Callable[[], float] = time.monotonic):
        self.max_size = max_size
        self.idle_ttl_s = idle_ttl_s
        self._clock = clock
        self._items: OrderedDict[str, tuple[Any, float]] = OrderedDict()
        self._locks: dict[str, asyncio.Lock] = {}

    def __len__(self) -> int:
        return len(self._items)

    def __contains__(self, key: str) -> bool:
        return key in self._items

    def get(self, key: str) -> Any:
        item = self._items.get(key)
        if item is None:
            return None
        now = self._clock()
        if now - item[1] > self.idle_ttl_s:
            self.pop(key)
            return None
        self._items[key] = (item[0], now)
        self._items.move_to_end(key)
        return item[0]

    def put(self, key: str, value: Any) -> None:
        self._items[key] = (value, self._clock())
        self._items.move_to_end(key)
        self._evict()

    def pop(self, key: str) -> None:
        self._items.pop(key, None)
        lock = self._locks.get(key)
        if lock is not None and not lock.locked():
            del self._locks[key]

    def lock(self, key: str) -> asyncio.Lock:
        return self._locks.setdefault(key, asyncio.Lock())

    def clear(self) -> None:
        self._items.clear()
        self._locks.clear()

    def _evict(self) -> None:
        now = self._clock()
        for key in [k for k, (_, ts) in self._items.items() if now - ts > self.idle_ttl_s]:
            self.pop(key)
        while len(self._items) > self.max_size:
            self.pop(next(iter(self._items)))
        # 连接失败等情况留下的锁
        for key in [k for k, lock in self._locks.items() if k not in self._items and not lock.locked()]:
            del self._locks[key]


class E2BProvider:
    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        api_url: Optional[str] = None,
        domain: Optional[str] = None,
        proxy: Optional[str] = None,
        cache_max: int = 64,
        cache_idle_s: float = 600,
    ):
        self._opts = {
            "api_key": api_key or os.environ.get("E2B_API_KEY"),
            "api_url": api_url or os.environ.get("E2B_API_URL"),
            "domain": domain or os.environ.get("E2B_DOMAIN"),
        }
        proxy = proxy or os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")
        if proxy:
            self._opts["proxy"] = proxy
        # 本副本内的连接句柄缓存；暂停 / 恢复 / 销毁 / 借用结束时失效，另按 LRU 和空闲时间淘汰
        self._cache = HandleCache(cache_max, cache_idle_s)

    async def _handle(self, sandbox_id: str, timeout_s: float) -> AsyncSandbox:
        h = self._cache.get(sandbox_id)
        if h is not None:
            return h
        async with self._cache.lock(sandbox_id):
            h = self._cache.get(sandbox_id)
            if h is None:
                try:
                    h = await _retry_stale(
                        lambda: AsyncSandbox.connect(
                            sandbox_id, timeout=int(timeout_s), request_timeout=_CONNECT_TIMEOUT, **self._opts
                        )
                    )
                except NotFoundException as e:
                    raise SandboxNotFound(sandbox_id) from e
                self._cache.put(sandbox_id, h)
            return h

    def forget(self, sandbox_id: str) -> None:
        self._cache.pop(sandbox_id)

    async def create(self, template: str, metadata: dict[str, str], timeout_s: float) -> str:
        h = await _retry_stale(
            lambda: AsyncSandbox.create(template=template, timeout=int(timeout_s), metadata=metadata, **self._opts),
            retry_on=_NOT_SENT_ERRORS,
        )
        self._cache.put(h.sandbox_id, h)
        return h.sandbox_id

    async def warmup(self, sandbox_id: str, code: str) -> None:
        h = await self._handle(sandbox_id, 300)
        execution = await h.run_code(code, timeout=_WARMUP_TIMEOUT)
        if execution.error is not None:
            raise RuntimeError(f"warmup failed: {execution.error.name}: {execution.error.value}")

    async def pause(self, sandbox_id: str) -> None:
        self.forget(sandbox_id)
        stale = False

        async def call():
            nonlocal stale
            try:
                await AsyncSandbox.pause(sandbox_id, **self._opts)
            except _STALE_CONNECTION_ERRORS:
                stale = True
                raise

        try:
            await _retry_stale(call)
        except NotFoundException as e:
            raise SandboxNotFound(sandbox_id) from e
        except Exception:
            # 之前某次尝试可能已经送达（读响应时连接断开）：以实际状态为准
            if not stale or await self.get_state(sandbox_id) != "paused":
                raise

    async def resume(self, sandbox_id: str, timeout_s: float) -> None:
        self.forget(sandbox_id)
        h = await self._handle(sandbox_id, timeout_s)
        await _retry_stale(lambda: h.commands.run("true", timeout=_PROBE_TIMEOUT, request_timeout=_PROBE_TIMEOUT))

    async def set_timeout(self, sandbox_id: str, timeout_s: float) -> None:
        try:
            await _retry_stale(
                lambda: AsyncSandbox.set_timeout(sandbox_id, int(timeout_s), request_timeout=_API_TIMEOUT, **self._opts)
            )
        except NotFoundException as e:
            raise SandboxNotFound(sandbox_id) from e

    async def kill(self, sandbox_id: str) -> bool:
        self.forget(sandbox_id)
        try:
            return await _retry_stale(
                lambda: AsyncSandbox.kill(sandbox_id, request_timeout=_KILL_TIMEOUT, **self._opts)
            )
        except NotFoundException:
            return False

    async def get_state(self, sandbox_id: str) -> Optional[str]:
        try:
            info = await _retry_stale(
                lambda: AsyncSandbox.get_info(sandbox_id, request_timeout=_API_TIMEOUT, **self._opts)
            )
        except NotFoundException:
            return None
        return getattr(info.state, "value", str(info.state))

    async def list(self, metadata: dict[str, str]) -> list[ProviderSandbox]:
        paginator = AsyncSandbox.list(
            query=SandboxQuery(metadata=metadata), request_timeout=_API_TIMEOUT, **self._opts
        )
        out: list[ProviderSandbox] = []
        while paginator.has_next:
            for info in await _retry_stale(paginator.next_items):
                out.append(
                    ProviderSandbox(
                        sandbox_id=info.sandbox_id,
                        state=getattr(info.state, "value", str(info.state)),
                        metadata=dict(info.metadata or {}),
                        started_at=info.started_at.timestamp() if info.started_at else None,
                    )
                )
        return out

    async def run_code(self, sandbox_id, code, *, language, timeout_s, sandbox_timeout_s) -> CodeResult:
        h = await self._handle(sandbox_id, sandbox_timeout_s)
        ex = await h.run_code(code, language=language, timeout=timeout_s)
        results = []
        for r in ex.results:
            item = {k: getattr(r, k) for k in ("text", "html", "markdown", "json", "png", "jpeg", "svg") if getattr(r, k, None)}
            item["is_main_result"] = bool(getattr(r, "is_main_result", False))
            results.append(item)
        error = None
        if ex.error is not None:
            error = {"name": ex.error.name, "value": ex.error.value, "traceback": ex.error.traceback}
        return CodeResult(
            stdout="".join(ex.logs.stdout),
            stderr="".join(ex.logs.stderr),
            text=ex.text,
            results=results,
            error=error,
        )

    async def run_command(self, sandbox_id, cmd, *, cwd, envs, timeout_s, sandbox_timeout_s) -> CommandResult:
        h = await self._handle(sandbox_id, sandbox_timeout_s)
        try:
            r = await h.commands.run(cmd, cwd=cwd, envs=envs, timeout=timeout_s)
            return CommandResult(exit_code=r.exit_code, stdout=r.stdout, stderr=r.stderr, error=r.error)
        except CommandExitException as e:
            return CommandResult(exit_code=e.exit_code, stdout=e.stdout, stderr=e.stderr, error=e.error)

    async def write_file(self, sandbox_id, path, data, *, sandbox_timeout_s) -> None:
        h = await self._handle(sandbox_id, sandbox_timeout_s)
        await h.files.write(path, data)

    async def read_file(self, sandbox_id, path, *, sandbox_timeout_s) -> bytes:
        h = await self._handle(sandbox_id, sandbox_timeout_s)
        try:
            return bytes(await h.files.read(path, format="bytes"))
        except NotFoundException as e:
            raise FileNotFoundError(path) from e

    async def close(self) -> None:
        self._cache.clear()
