import json
from typing import Optional

from pydantic import BaseModel, Field


def _json_or_none(raw) -> Optional[dict]:
    """库里存的是 JSON 文本；解析不了就当没有，不让一条坏记录把整个查询打挂。"""
    if not raw:
        return None
    try:
        v = json.loads(raw)
    except (TypeError, ValueError):
        return None
    return v if isinstance(v, dict) else None


class AcquireRequest(BaseModel):
    wait_timeout_s: Optional[float] = Field(None, ge=0, description="最长等待秒数，默认并受限于服务端配置")
    lease_ttl_s: Optional[float] = Field(None, gt=0, description="借用期限秒数，默认并受限于服务端配置")
    egress: Optional[dict] = Field(
        None,
        description=(
            "本次借用的出网策略 {mode, allow_out, deny_out}。**不传 = 彻底断网**（含 DNS）。"
            'mode="allowlist" ＋ allow_out 填域名 = 只放行这些域名；mode="open" = 放行公网'
            "（强制屏蔽的内网与元数据网段去不掉）。服务端下发后会读回平台回显核对，"
            "对不上这次借用直接失败（502）"
        ),
    )


class RenewRequest(BaseModel):
    ttl_s: Optional[float] = Field(None, gt=0)


class LeaseOut(BaseModel):
    lease_id: str
    sandbox_id: str
    state: str
    source: str
    created_at: float
    expires_at: float
    hard_deadline: float
    ended_at: Optional[float] = None
    wait_ms: Optional[float] = None
    client_id: Optional[str] = None
    # 本次借用生效的出网策略，以及**平台回显**的那一份。
    # 调用方据此自己再核对一次 —— 服务端已经核对过，但「池子说它核对过」
    # 与「我自己看见回显是对的」是两件事，而借用方拿断网当安全边界用。
    egress: Optional[dict] = None
    egress_platform: Optional[dict] = None

    @classmethod
    def from_row(cls, row: dict) -> "LeaseOut":
        scalar = {k: row[k] for k in cls.model_fields if k not in ("lease_id", "egress", "egress_platform")}
        return cls(
            lease_id=row["id"],
            egress=_json_or_none(row.get("egress")),
            egress_platform=_json_or_none(row.get("egress_platform")),
            **scalar,
        )


class RunCodeRequest(BaseModel):
    code: str
    language: Optional[str] = None
    timeout_s: float = Field(60, gt=0, le=3600)


class RunCodeResponse(BaseModel):
    stdout: str
    stderr: str
    text: Optional[str]
    results: list[dict]
    error: Optional[dict]
    # 截断事实：truncated 为真时上面两条流不完整，*_bytes 是截断前的真实字节数。
    # 【定】必须说出来 —— 调用方没有别的办法发现自己拿到的是半截输出
    truncated: bool = False
    stdout_bytes: int = 0
    stderr_bytes: int = 0


class CommandRequest(BaseModel):
    cmd: str
    cwd: Optional[str] = None
    envs: Optional[dict[str, str]] = None
    timeout_s: float = Field(60, gt=0, le=3600)


class CommandResponse(BaseModel):
    exit_code: int
    stdout: str
    stderr: str
    error: Optional[str] = None
    truncated: bool = False
    stdout_bytes: int = 0
    stderr_bytes: int = 0


# ---------- agent 子系统 ----------


class StartSandboxRequest(BaseModel):
    engine: Optional[str] = Field(None, description="启动前改用的引擎；省略则沿用已保存的设置或默认引擎")


class TemplateCacheCreateRequest(BaseModel):
    template_id: str = Field(..., min_length=1, description="要预热的模板 ID")


class DeleteInstanceRequest(BaseModel):
    row_id: Optional[str] = Field(None, description="库内沙箱行号")
    provider_id: Optional[str] = Field(None, description="云端实例号；库里没有这条记录时按孤儿删除")

    def require_one(self) -> None:
        if bool(self.row_id) == bool(self.provider_id):
            raise ValueError("provide exactly one of row_id or provider_id")


class MessageRequest(BaseModel):
    text: str = Field(..., min_length=1, max_length=200_000, description="发给 agent 的消息")
    session_id: Optional[str] = Field(None, max_length=64, description="继续已有会话；为空时新建会话")
    max_duration_s: Optional[float] = Field(None, ge=60, le=86400, description="最长执行时间，超过即中止（TIMEOUT）")
    agent: Optional[str] = Field(None, max_length=64, description="opencode 引擎的 agent（如 build / plan），默认 build；pi 引擎不支持，传了返回 400")
    stream: bool = Field(True, description="true：SSE 流式返回；false：等任务结束后返回 JSON")


class TaskOut(BaseModel):
    task_id: str
    state: str
    source: str
    session_id: Optional[str] = None
    schedule_id: Optional[str] = None
    sandbox_row_id: Optional[str] = None
    prompt: str
    result: Optional[str] = None
    error: Optional[str] = None
    usage: Optional[dict] = None
    created_at: float
    started_at: Optional[float] = None
    finished_at: Optional[float] = None
    deadline: float

    @classmethod
    def from_row(cls, row: dict) -> "TaskOut":
        return cls(
            task_id=row["id"],
            result=row.get("result_text"),
            **{k: row.get(k) for k in cls.model_fields if k not in ("task_id", "result")},
        )


class ScheduleIn(BaseModel):
    name: str = Field(..., min_length=1, max_length=128)
    prompt: str = Field(..., min_length=1, max_length=200_000)
    cron: Optional[str] = Field(None, description="5 段 cron（分 时 日 月 周），与 every_s 二选一")
    every_s: Optional[float] = Field(None, description="固定间隔秒数（>= 60），与 cron 二选一")
    timezone: Optional[str] = Field(None, description="cron 的时区，默认 Asia/Shanghai")
    enabled: Optional[bool] = True
    max_duration_s: Optional[float] = None
    overlap: Optional[str] = Field(None, description="skip（默认）：上一次还在运行时跳过；allow：照常触发")


class SchedulePatch(BaseModel):
    name: Optional[str] = None
    prompt: Optional[str] = None
    cron: Optional[str] = None
    every_s: Optional[float] = None
    timezone: Optional[str] = None
    enabled: Optional[bool] = None
    max_duration_s: Optional[float] = None
    overlap: Optional[str] = None
