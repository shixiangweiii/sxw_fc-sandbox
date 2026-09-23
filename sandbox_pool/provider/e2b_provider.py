"""基于 E2B 异步 SDK 的后端（阿里云云沙箱兼容 E2B 协议）。

- 固定 e2b==2.31.0：更新的 SDK 走 /v2 接口，云沙箱不支持。
- SDK 使用自定义 httpx transport，不读 HTTPS_PROXY，需要显式传 proxy。
- 后台任务只用 get_info / set_timeout / pause / kill 等类方法；connect() 会续期并恢复暂停的沙箱，
  只在 resume 和代为执行时使用。
"""

import asyncio
import logging
import os
from typing import Optional

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


class E2BProvider:
    def __init__(
        self,
        *,
        api_key: Optional[str] = None,
        api_url: Optional[str] = None,
        domain: Optional[str] = None,
        proxy: Optional[str] = None,
    ):
        self._opts = {
            "api_key": api_key or os.environ.get("E2B_API_KEY"),
            "api_url": api_url or os.environ.get("E2B_API_URL"),
            "domain": domain or os.environ.get("E2B_DOMAIN"),
        }
        proxy = proxy or os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")
        if proxy:
            self._opts["proxy"] = proxy
        # 本副本内的连接句柄缓存；暂停 / 销毁时失效
        self._handles: dict[str, AsyncSandbox] = {}
        self._handle_locks: dict[str, asyncio.Lock] = {}

    async def _handle(self, sandbox_id: str, timeout_s: float) -> AsyncSandbox:
        h = self._handles.get(sandbox_id)
        if h is not None:
            return h
        lock = self._handle_locks.setdefault(sandbox_id, asyncio.Lock())
        async with lock:
            h = self._handles.get(sandbox_id)
            if h is None:
                try:
                    h = await AsyncSandbox.connect(sandbox_id, timeout=int(timeout_s), **self._opts)
                except NotFoundException as e:
                    raise SandboxNotFound(sandbox_id) from e
                self._handles[sandbox_id] = h
            return h

    def _forget(self, sandbox_id: str) -> None:
        self._handles.pop(sandbox_id, None)
        self._handle_locks.pop(sandbox_id, None)

    async def create(self, template: str, metadata: dict[str, str], timeout_s: float) -> str:
        h = await AsyncSandbox.create(template=template, timeout=int(timeout_s), metadata=metadata, **self._opts)
        self._handles[h.sandbox_id] = h
        return h.sandbox_id

    async def warmup(self, sandbox_id: str, code: str) -> None:
        h = await self._handle(sandbox_id, 300)
        execution = await h.run_code(code, timeout=120)
        if execution.error is not None:
            raise RuntimeError(f"warmup failed: {execution.error.name}: {execution.error.value}")

    async def pause(self, sandbox_id: str) -> None:
        self._forget(sandbox_id)
        try:
            await AsyncSandbox.pause(sandbox_id, **self._opts)
        except NotFoundException as e:
            raise SandboxNotFound(sandbox_id) from e

    async def resume(self, sandbox_id: str, timeout_s: float) -> None:
        self._forget(sandbox_id)
        h = await self._handle(sandbox_id, timeout_s)
        await h.commands.run("true", timeout=30)

    async def set_timeout(self, sandbox_id: str, timeout_s: float) -> None:
        try:
            await AsyncSandbox.set_timeout(sandbox_id, int(timeout_s), **self._opts)
        except NotFoundException as e:
            raise SandboxNotFound(sandbox_id) from e

    async def kill(self, sandbox_id: str) -> bool:
        self._forget(sandbox_id)
        try:
            return await AsyncSandbox.kill(sandbox_id, **self._opts)
        except NotFoundException:
            return False

    async def get_state(self, sandbox_id: str) -> Optional[str]:
        try:
            info = await AsyncSandbox.get_info(sandbox_id, **self._opts)
        except NotFoundException:
            return None
        return getattr(info.state, "value", str(info.state))

    async def list(self, metadata: dict[str, str]) -> list[ProviderSandbox]:
        paginator = AsyncSandbox.list(query=SandboxQuery(metadata=metadata), **self._opts)
        out: list[ProviderSandbox] = []
        while paginator.has_next:
            for info in await paginator.next_items():
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
        self._handles.clear()
