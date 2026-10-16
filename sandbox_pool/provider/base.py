"""沙箱后端抽象。池子只依赖这里的接口，便于替换后端和用 Fake 测试。"""

from dataclasses import dataclass, field
from typing import Optional, Protocol


class SandboxNotFound(Exception):
    """后端已不存在该沙箱。"""


class ExecutionTimeout(Exception):
    """代为执行（run_code / commands）超过调用方给的 timeout_s。

    与后端故障不同：代码可能已经执行了一部分，调用方不能当作故障自动重试。

    【定】它**带着已经收到的那段输出**（2026-10-16）。原来超时一律返回空
    stdout / stderr，而那恰恰是调用方最需要的东西 —— 一条跑了十分钟才超时的
    构建命令，原因就在它已经打印出来的那些行里。扔掉它，等于让「超时」与
    「什么都没发生」在调用方那里不可区分。
    """

    def __init__(self, message: str, *, stdout: str = "", stderr: str = "",
                 truncated: bool = False, stdout_bytes: int = 0, stderr_bytes: int = 0):
        super().__init__(message)
        self.stdout = stdout
        self.stderr = stderr
        self.truncated = truncated
        self.stdout_bytes = stdout_bytes
        self.stderr_bytes = stderr_bytes


@dataclass
class OutputCap:
    """按字节收集一条输出流，超过上限就停止累积，但**继续数**真实字节数。

    【定】只记长度不留内容 —— 上限的意义之一就是不让一条 `cat 大文件`
    把池子自己的内存撑爆，留着内容再截断等于没有上限。
    """

    limit: int
    text: str = ""
    total_bytes: int = 0
    truncated: bool = False

    def feed(self, chunk: str) -> None:
        data = chunk.encode("utf-8", "replace")
        self.total_bytes += len(data)
        room = self.limit - len(self.text.encode("utf-8", "replace"))
        if room <= 0:
            self.truncated = True
            return
        if len(data) > room:
            self.truncated = True
            # 按字节切会劈开多字节字符，errors="ignore" 丢掉半个字符而不是产出 U+FFFD
            self.text += data[:room].decode("utf-8", "ignore")
        else:
            self.text += chunk


def cap_text(text: str, limit: int) -> tuple[str, int, bool]:
    """事后截断（拿不到流式回调时用）。返回 (截断后的文本, 真实字节数, 是否截断)。"""
    data = text.encode("utf-8", "replace")
    if len(data) <= limit:
        return text, len(data), False
    return data[:limit].decode("utf-8", "ignore"), len(data), True


@dataclass
class ProviderSandbox:
    sandbox_id: str
    state: str  # running / paused
    metadata: dict = field(default_factory=dict)
    started_at: Optional[float] = None


@dataclass
class AppSandbox:
    """对外提供服务的沙箱（agent 子系统）：服务地址与访问它必带的平台流量令牌。"""

    sandbox_id: str
    endpoint: str  # https://<port>-<sandbox_id>.<domain>
    access_token: Optional[str]


@dataclass
class CodeResult:
    stdout: str
    stderr: str
    text: Optional[str]
    results: list[dict]
    error: Optional[dict]
    # 截断事实：truncated 为真时 stdout / stderr 不完整，*_bytes 是截断前的真实字节数
    truncated: bool = False
    stdout_bytes: int = 0
    stderr_bytes: int = 0


@dataclass
class CommandResult:
    exit_code: int
    stdout: str
    stderr: str
    error: Optional[str] = None
    truncated: bool = False
    stdout_bytes: int = 0
    stderr_bytes: int = 0


class SandboxProvider(Protocol):
    async def create(
        self,
        template: str,
        metadata: dict[str, str],
        timeout_s: float,
        *,
        network: Optional[dict] = None,
        secure: bool = False,
    ) -> str:
        """建沙箱。network 为出网规则（不含 allow_public_traffic，由实现按 secure 补）。

        【定】`allow_public_traffic=False` 平台要求同时 `secure=True`，两者必须一起传。
        """

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
        self,
        sandbox_id: str,
        code: str,
        *,
        language: Optional[str],
        timeout_s: float,
        sandbox_timeout_s: float,
        max_output_bytes: int,
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
        max_output_bytes: int,
    ) -> CommandResult: ...

    async def write_file(self, sandbox_id: str, path: str, data: bytes, *, sandbox_timeout_s: float) -> None: ...

    async def read_file(self, sandbox_id: str, path: str, *, sandbox_timeout_s: float) -> bytes: ...

    def forget(self, sandbox_id: str) -> None:
        """丢弃本副本为该沙箱缓存的连接（借用已结束等场景）。"""

    async def close(self) -> None: ...

    # ---------- agent 子系统：对外提供服务的沙箱 ----------

    async def create_app(
        self, template: str, metadata: dict[str, str], timeout_s: float, *, port: int, network: dict
    ) -> AppSandbox:
        """创建沙箱：secure=True、关闭公开访问（访问端口必须带流量令牌）、下发出网规则（network，含凭证注入）。"""

    async def update_network(self, sandbox_id: str, network: dict) -> None:
        """运行中全量替换出网规则（allow_out / deny_out / rules）。"""

    async def get_network(self, sandbox_id: str) -> Optional[dict]:
        """平台回显的网络配置。注入的凭证是明文，调用方必须脱敏；沙箱不存在抛 SandboxNotFound。"""

    async def write_files(self, sandbox_id: str, files: dict[str, bytes], *, sandbox_timeout_s: float) -> None:
        """写入多个文件（幂等，连接类错误会重试）；需要重新连接时按 sandbox_timeout_s 设置平台超时。"""

    def app_client(self, endpoint: str, access_token: Optional[str], directory: str, *, ingress_ip: Optional[str]):
        """访问沙箱内 agent 服务（opencode serve 或 pi 桥接进程）的客户端（sandbox_pool.agent.opencode.OpencodeAPI）。"""
