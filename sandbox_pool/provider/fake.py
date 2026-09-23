"""内存版沙箱后端，仅用于测试。多个池实例可共享同一个 FakeProvider，模拟共享的云端。"""

import asyncio
import contextlib
import io
import time
import uuid
from typing import Optional

from sandbox_pool.provider.base import (
    CodeResult,
    CommandResult,
    ProviderSandbox,
    SandboxNotFound,
)


class FakeProvider:
    def __init__(self, *, latency_s: float = 0.0, pause_latency_s: Optional[float] = None):
        self.latency_s = latency_s
        self.pause_latency_s = latency_s if pause_latency_s is None else pause_latency_s
        self.sandboxes: dict[str, dict] = {}
        self.calls: list[tuple[str, str]] = []
        # 失败注入：剩余失败次数 / 指定沙箱失败
        self.fail_create = 0
        self.fail_resume_ids: set[str] = set()
        self.fail_warmup = 0

    def _get(self, sandbox_id: str) -> dict:
        sb = self.sandboxes.get(sandbox_id)
        if sb is None:
            raise SandboxNotFound(sandbox_id)
        return sb

    def _running(self, sandbox_id: str) -> dict:
        sb = self._get(sandbox_id)
        if sb["state"] != "running":
            raise RuntimeError(f"sandbox {sandbox_id} is {sb['state']}")
        return sb

    async def _sleep(self, s: float) -> None:
        if s:
            await asyncio.sleep(s)

    def alive_count(self) -> int:
        return len(self.sandboxes)

    async def create(self, template: str, metadata: dict[str, str], timeout_s: float) -> str:
        self.calls.append(("create", template))
        await self._sleep(self.latency_s)
        if self.fail_create > 0:
            self.fail_create -= 1
            raise RuntimeError("injected create failure")
        sid = f"fake-{uuid.uuid4().hex[:12]}"
        self.sandboxes[sid] = dict(
            state="running", metadata=dict(metadata), started_at=time.time(), ns={}, files={}, timeout=timeout_s
        )
        return sid

    async def warmup(self, sandbox_id: str, code: str) -> None:
        self.calls.append(("warmup", sandbox_id))
        if self.fail_warmup > 0:
            self.fail_warmup -= 1
            raise RuntimeError("injected warmup failure")
        self._running(sandbox_id)

    async def pause(self, sandbox_id: str) -> None:
        self.calls.append(("pause", sandbox_id))
        await self._sleep(self.pause_latency_s)
        self._running(sandbox_id)["state"] = "paused"

    async def resume(self, sandbox_id: str, timeout_s: float) -> None:
        self.calls.append(("resume", sandbox_id))
        await self._sleep(self.latency_s)
        sb = self._get(sandbox_id)
        if sandbox_id in self.fail_resume_ids:
            raise RuntimeError("injected resume failure")
        sb["state"] = "running"
        sb["timeout"] = max(sb["timeout"], timeout_s)

    async def set_timeout(self, sandbox_id: str, timeout_s: float) -> None:
        self.calls.append(("set_timeout", sandbox_id))
        self._running(sandbox_id)["timeout"] = timeout_s

    async def kill(self, sandbox_id: str) -> bool:
        self.calls.append(("kill", sandbox_id))
        return self.sandboxes.pop(sandbox_id, None) is not None

    async def get_state(self, sandbox_id: str) -> Optional[str]:
        sb = self.sandboxes.get(sandbox_id)
        return sb["state"] if sb else None

    async def list(self, metadata: dict[str, str]) -> list[ProviderSandbox]:
        out = []
        for sid, sb in self.sandboxes.items():
            if all(sb["metadata"].get(k) == v for k, v in metadata.items()):
                out.append(ProviderSandbox(sid, sb["state"], dict(sb["metadata"]), sb["started_at"]))
        return out

    async def run_code(self, sandbox_id, code, *, language, timeout_s, sandbox_timeout_s) -> CodeResult:
        sb = self._running(sandbox_id)
        out = io.StringIO()
        error = None
        try:
            with contextlib.redirect_stdout(out):
                exec(code, sb["ns"])  # noqa: S102 - 仅测试
        except Exception as e:  # noqa: BLE001
            error = {"name": type(e).__name__, "value": str(e), "traceback": ""}
        return CodeResult(stdout=out.getvalue(), stderr="", text=None, results=[], error=error)

    async def run_command(self, sandbox_id, cmd, *, cwd, envs, timeout_s, sandbox_timeout_s) -> CommandResult:
        self._running(sandbox_id)
        if cmd.startswith("exit "):
            return CommandResult(exit_code=int(cmd.split()[1]), stdout="", stderr="failed")
        return CommandResult(exit_code=0, stdout=f"ran: {cmd}\n", stderr="")

    async def write_file(self, sandbox_id, path, data, *, sandbox_timeout_s) -> None:
        self._running(sandbox_id)["files"][path] = bytes(data)

    async def read_file(self, sandbox_id, path, *, sandbox_timeout_s) -> bytes:
        files = self._running(sandbox_id)["files"]
        if path not in files:
            raise FileNotFoundError(path)
        return files[path]

    async def close(self) -> None:
        pass
