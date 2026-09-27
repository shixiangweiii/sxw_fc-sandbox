"""agent 引擎的公共部分：沙箱内跑哪种 agent loop（opencode / pi）。

网关访问两种沙箱用同一个 HTTP 客户端（agent/opencode.py），控制接口的路径相同；差异集中在引擎里：
事件翻译、结果提取、写进沙箱的配置文件、能力声明。
"""

import json
from dataclasses import dataclass
from typing import Any, Optional, Protocol

TRUNCATE = 2000


def cut(value: Any, limit: int = TRUNCATE) -> Any:
    """截断工具输入输出等长内容；非字符串先转 JSON。"""
    if value is None:
        return None
    if not isinstance(value, str):
        value = json.dumps(value, ensure_ascii=False)
    return value if len(value) <= limit else value[:limit] + f"...(+{len(value) - limit} chars)"


@dataclass
class FilesContext:
    """渲染沙箱内配置文件需要的信息（与引擎无关的部分）。"""

    workdir: str
    max_life_h: float
    idle_destroy_after_s: float
    # 合并后的 MCP 配置（默认 + agent 设置），格式见 policy.validate_mcp
    mcp: dict
    # 当前生效的出网策略（policy.describe 的结果）
    egress: dict
    instructions: str


class Translator(Protocol):
    """把引擎事件翻译成对外事件 (kind, data)，kind ∈ text / reasoning / tool / status。

    busy_seen：已看到会话开始运行；idle：本轮运行已结束；errors：运行中出现的错误；
    asks：需要应答的询问 (permission | question, 请求 ID)，只有 opencode 会产生；
    round_starts：看到会话开始新一轮的次数（opencode：新的用户消息；pi：agent_start）。一个会话里会先后跑多个任务，
    断线重连据此划分轮次（可以多报，例如 pi 自动重试时再次 agent_start，不能漏报）；
    mid_round：订阅时这一轮可能已在输出（断线重连、接管），错过开头的消息以全文为准（pi 等到 message_end 整条输出）。
    """

    busy_seen: bool
    idle: bool
    errors: list[str]
    asks: list[tuple[str, str]]
    round_starts: int
    mid_round: bool

    def feed(self, ev: dict) -> list[tuple[str, dict]]: ...

    def flush(self) -> list[tuple[str, dict]]: ...


class Engine:
    name = ""
    # 对话接口的 agent 参数（opencode 的 build / plan）
    supports_agent_param = False
    # 提示词受理后返回 run_id，可以查询这次运行是否还存在（进程重启后查不到）
    has_runs = False

    def __init__(self, *, template: str, port: int, model: str, workdir: str):
        self.template = template
        self.port = port
        self.model = model
        self.workdir = workdir

    @property
    def enabled(self) -> bool:
        return bool(self.template)

    def prompt_text(self, text: str) -> str:
        """发给沙箱内 agent 的提示词：用户消息一律作为普通提示词交给模型（引擎自己的命令语法要转义）。"""
        return text

    def translator(self, session_id: str) -> Translator:
        raise NotImplementedError

    def extract_result(self, messages: list[dict]) -> tuple[str, dict, Optional[str]]:
        """本轮结果：(最终文本, 用量, 错误)。"""
        raise NotImplementedError

    def last_prompt(self, messages: list[dict]) -> Optional[str]:
        """会话里最后一条用户消息的文本（与 prompt_text 发出的提示词比对）；没有用户消息时为 None。"""
        raise NotImplementedError

    def render_files(self, ctx: FilesContext) -> dict[str, bytes]:
        """写进沙箱的配置文件（不含 egress.json，它与引擎无关）：{路径: 内容}。"""
        raise NotImplementedError

    def describe(self) -> dict:
        return {
            "name": self.name,
            "model": self.model,
            "capabilities": {"agent_param": self.supports_agent_param},
        }
