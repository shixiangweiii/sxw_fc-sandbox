"""沙箱后端抽象。池子只依赖这里的接口，便于替换后端和用 Fake 测试。"""

from dataclasses import dataclass, field
from typing import Optional, Protocol


class SandboxNotFound(Exception):
    """后端已不存在该沙箱。"""


class ExecutionTimeout(Exception):
    """代为执行（run_code / commands）超过调用方给的 timeout_s。

    与后端故障不同：代码可能已经执行了一部分，调用方不能当作故障自动重试。
    """


@dataclass
class ProviderSandbox:
    sandbox_id: str
    state: str  # running / paused
    metadata: dict = field(default_factory=dict)
    started_at: Optional[float] = None


@dataclass
class CodeResult:
    stdout: str
    stderr: str
    text: Optional[str]
    results: list[dict]
    error: Optional[dict]


@dataclass
class CommandResult:
    exit_code: int
    stdout: str
    stderr: str
    error: Optional[str] = None


class SandboxProvider(Protocol):
    async def create(self, template: str, metadata: dict[str, str], timeout_s: float) -> str: ...

    async def warmup(self, sandbox_id: str, code: str, *, sandbox_timeout_s: float) -> None:
        """在新沙箱里执行预热代码。需要重新连接时按 sandbox_timeout_s 设置平台超时。"""

    async def pause(self, sandbox_id: str) -> None: ...

    async def resume(self, sandbox_id: str, timeout_s: float) -> None:
        """恢复暂停的沙箱并探活。"""

    async def set_timeout(self, sandbox_id: str, timeout_s: float) -> None: ...

    async def kill(self, sandbox_id: str) -> bool: ...

    async def get_state(self, sandbox_id: str) -> Optional[str]:
        """只读查询（不续期、不恢复）；不存在返回 None。"""

    async def list(self, metadata: dict[str, str]) -> list[ProviderSandbox]: ...

    async def run_code(
        self, sandbox_id: str, code: str, *, language: Optional[str], timeout_s: float, sandbox_timeout_s: float
    ) -> CodeResult: ...

    async def run_command(
        self,
        sandbox_id: str,
        cmd: str,
        *,
        cwd: Optional[str],
        envs: Optional[dict[str, str]],
        timeout_s: float,
        sandbox_timeout_s: float,
    ) -> CommandResult: ...

    async def write_file(self, sandbox_id: str, path: str, data: bytes, *, sandbox_timeout_s: float) -> None: ...

    async def read_file(self, sandbox_id: str, path: str, *, sandbox_timeout_s: float) -> bytes: ...

    def forget(self, sandbox_id: str) -> None:
        """丢弃本副本为该沙箱缓存的连接（借用已结束等场景）。"""

    async def close(self) -> None: ...
