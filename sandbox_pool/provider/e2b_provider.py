"""基于 E2B 异步 SDK 的后端（阿里云云沙箱兼容 E2B 协议）。

- 固定 e2b==2.31.0：更新的 SDK 走 /v2 接口，云沙箱不支持。
- SDK 使用自定义 httpx transport，不读 HTTPS_PROXY，需要显式传 proxy。
- 后台任务只用 get_info / set_timeout / pause / kill 等类方法；connect() 会续期并恢复暂停的沙箱，
  只在 resume 和代为执行时使用。
- 每个云端调用都显式设了请求超时（不依赖 SDK 默认的 60s），且小于对应过渡态的截止时间（PoolConfig 的
  destroy_timeout_s / resume_timeout_s / op_timeout_s），健康但较慢的操作不会被其他副本误接管；
  启动时由 check_deadlines 校验配置。
- SDK 按事件循环共享一个 HTTP/2 连接池。经 HTTP 代理出网时，空闲一段时间的连接会失效，下一个请求报
  WriteError / ReadError（异常信息为空）。管控面调用在连接类错误时重试一次（超时不重试）；
  用户代码执行（run_code 等）不重试。
"""

import asyncio
import logging
import os
import time
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, Callable, Optional

import httpcore
import httpx
from e2b import NotFoundException, SandboxQuery, TimeoutException
from e2b.sandbox.commands.command_handle import CommandExitException
from e2b_code_interpreter import AsyncSandbox

from sandbox_pool.provider.base import (
    AppSandbox,
    CodeResult,
    CommandResult,
    ExecutionTimeout,
    ProviderSandbox,
    SandboxNotFound,
)

if TYPE_CHECKING:
    from sandbox_pool.config import PoolConfig

log = logging.getLogger(__name__)

# 云端调用的请求超时（秒），必须小于对应过渡态的截止时间（见 check_deadlines）
_KILL_TIMEOUT = 20  # 销毁 < destroy_timeout_s（默认 30）
_CONNECT_TIMEOUT = 30  # 恢复 = connect + 探活，合计 < resume_timeout_s（默认 60）
_PROBE_TIMEOUT = 15
_API_TIMEOUT = 30  # set_timeout / get_info / list
# 创建、预热、暂停 < op_timeout_s（默认 120）。进入 WARMING 时截止时间重新计算，创建与预热不累加
_CREATE_TIMEOUT = 30  # 实测 0.5~2.5s
_WARMUP_TIMEOUT = 45  # 预热代码的执行超时，实测 1~3s
_PAUSE_TIMEOUT = 45  # 实测 10~18s，多个同时暂停时更慢

# 执行超时的判定容差：调用方的 timeout_s 基本用完才算执行超时
_EXEC_TIMEOUT_TOLERANCE_S = 0.5

# 连接失效类错误：请求在失效的连接上发送失败或读不到响应，换一条连接重试即可。
# envd 调用（commands / files）经 e2b_connect 直接抛 httpcore 的异常，不会被包成 httpx 异常（实测：刚创建的沙箱
# TLS 握手被对端关闭，报 httpcore.ConnectError），两类都要算上
_STALE_CONNECTION_ERRORS = (
    httpx.NetworkError,
    httpx.RemoteProtocolError,
    httpcore.NetworkError,
    httpcore.RemoteProtocolError,
)
# 请求肯定没有到达服务端的错误：非幂等的创建只在这类错误时重试
_NOT_SENT_ERRORS = (httpx.ConnectError, httpx.WriteError, httpcore.ConnectError, httpcore.WriteError)


# 连接失效后的重试间隔：同一条 HTTP/2 连接上的并发请求会一起失败，稍等让连接池丢弃坏连接
_STALE_RETRY_DELAYS = (0.3, 1.0)
# 幂等的文件写入（agent 装配）用更长的重试
_WRITE_RETRY_DELAYS = (0.5, 1.0, 2.0, 3.0)


def check_deadlines(cfg: "PoolConfig") -> None:
    """过渡态的截止时间必须大于其中云端调用的请求超时，否则健康但较慢的调用会被其他副本判为卡死并接管。

    op_timeout_s 覆盖创建、预热、暂停；resume_timeout_s 覆盖 connect + 探活；destroy_timeout_s 覆盖 kill。
    """
    required = {
        "op_timeout_s": max(_CREATE_TIMEOUT, _WARMUP_TIMEOUT, _PAUSE_TIMEOUT),
        "resume_timeout_s": _CONNECT_TIMEOUT + _PROBE_TIMEOUT,
        "destroy_timeout_s": _KILL_TIMEOUT,
    }
    bad = [
        f"POOL_{name.upper()}={getattr(cfg, name):g} must be greater than {need}s"
        for name, need in required.items()
        if getattr(cfg, name) <= need
    ]
    if bad:
        raise ValueError("; ".join(bad) + " (the request timeout of the cloud calls it covers)")


def _execution_timed_out(started: float, timeout_s: float) -> bool:
    """SDK 的 TimeoutException 既可能是执行超过 timeout_s，也可能是连接 / 请求超时（后端故障）。

    按调用方给的 timeout_s 是否已基本用完来区分：用完了就是执行超时（代码可能已执行，不能重试），更早的属于后端故障。
    """
    return time.monotonic() - started + _EXEC_TIMEOUT_TOLERANCE_S >= timeout_s


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
            lambda: AsyncSandbox.create(
                template=template,
                timeout=int(timeout_s),
                metadata=metadata,
                request_timeout=_CREATE_TIMEOUT,
                **self._opts,
            ),
            retry_on=_NOT_SENT_ERRORS,
        )
        self._cache.put(h.sandbox_id, h)
        return h.sandbox_id

    async def warmup(self, sandbox_id: str, code: str, *, sandbox_timeout_s: float) -> None:
        # 通常直接用 create 缓存的句柄；缓存被淘汰时要 connect，按 READY 的平台超时重设（connect 会重设平台超时）
        h = await self._handle(sandbox_id, sandbox_timeout_s)
        execution = await h.run_code(code, timeout=_WARMUP_TIMEOUT)
        if execution.error is not None:
            raise RuntimeError(f"warmup failed: {execution.error.name}: {execution.error.value}")

    async def pause(self, sandbox_id: str) -> None:
        self.forget(sandbox_id)
        stale = False

        async def call():
            nonlocal stale
            try:
                await AsyncSandbox.pause(sandbox_id, request_timeout=_PAUSE_TIMEOUT, **self._opts)
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
        started = time.monotonic()
        try:
            ex = await h.run_code(code, language=language, timeout=timeout_s)
        except TimeoutException as e:
            if _execution_timed_out(started, timeout_s):
                raise ExecutionTimeout(f"execution exceeded {timeout_s:g}s") from e
            raise
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
        started = time.monotonic()
        try:
            r = await h.commands.run(cmd, cwd=cwd, envs=envs, timeout=timeout_s)
            return CommandResult(exit_code=r.exit_code, stdout=r.stdout, stderr=r.stderr, error=r.error)
        except CommandExitException as e:
            return CommandResult(exit_code=e.exit_code, stdout=e.stdout, stderr=e.stderr, error=e.error)
        except TimeoutException as e:
            if _execution_timed_out(started, timeout_s):
                raise ExecutionTimeout(f"command exceeded {timeout_s:g}s") from e
            raise

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

    # ---------- agent 子系统 ----------

    async def create_app(self, template, metadata, timeout_s, *, port, network) -> AppSandbox:
        h = await _retry_stale(
            lambda: AsyncSandbox.create(
                template=template,
                timeout=int(timeout_s),
                metadata=metadata,
                secure=True,
                network={**network, "allow_public_traffic": False},
                request_timeout=_CREATE_TIMEOUT,
                **self._opts,
            ),
            retry_on=_NOT_SENT_ERRORS,
        )
        self._cache.put(h.sandbox_id, h)
        return AppSandbox(h.sandbox_id, f"https://{h.get_host(port)}", h.traffic_access_token)

    async def update_network(self, sandbox_id: str, network: dict) -> None:
        # 全量替换，重复调用结果相同，连接失效时可以重试
        try:
            await _retry_stale(
                lambda: AsyncSandbox.update_network(sandbox_id, network, request_timeout=_API_TIMEOUT, **self._opts)
            )
        except NotFoundException as e:
            raise SandboxNotFound(sandbox_id) from e

    async def get_network(self, sandbox_id: str) -> Optional[dict]:
        try:
            info = await _retry_stale(
                lambda: AsyncSandbox.get_info(sandbox_id, request_timeout=_API_TIMEOUT, **self._opts)
            )
        except NotFoundException as e:
            raise SandboxNotFound(sandbox_id) from e
        net = getattr(info, "network", None)
        return dict(net) if net else None

    async def write_files(self, sandbox_id: str, files: dict[str, bytes], *, sandbox_timeout_s: float) -> None:
        h = await self._handle(sandbox_id, sandbox_timeout_s)
        for path, data in files.items():
            # 写文件是幂等的：连接类错误（刚创建时 envd 短暂不可达；本机代理 fake-ip 下新连接约 1/3 失败，实测）多重试几次
            for delay in (*_WRITE_RETRY_DELAYS, None):
                try:
                    await h.files.write(path, data, request_timeout=_API_TIMEOUT)
                    break
                except _STALE_CONNECTION_ERRORS as e:
                    if delay is None:
                        raise
                    log.info("write %s to %s failed (%r), retrying in %.1fs", path, sandbox_id, e, delay)
                    await asyncio.sleep(delay)

    def app_client(self, endpoint, access_token, directory, *, ingress_ip):
        from sandbox_pool.agent.opencode import OpencodeHttpClient

        return OpencodeHttpClient(endpoint, access_token, directory, ingress_ip=ingress_ip)
