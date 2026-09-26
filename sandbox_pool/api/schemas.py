from typing import Optional

from pydantic import BaseModel, Field


class AcquireRequest(BaseModel):
    wait_timeout_s: Optional[float] = Field(None, ge=0, description="最长等待秒数，默认并受限于服务端配置")
    lease_ttl_s: Optional[float] = Field(None, gt=0, description="借用期限秒数，默认并受限于服务端配置")


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

    @classmethod
    def from_row(cls, row: dict) -> "LeaseOut":
        return cls(lease_id=row["id"], **{k: row[k] for k in cls.model_fields if k != "lease_id"})


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


# ---------- agent 子系统 ----------


class MessageRequest(BaseModel):
    text: str = Field(..., min_length=1, max_length=200_000, description="发给 agent 的消息")
    session_id: Optional[str] = Field(None, max_length=64, description="继续已有会话；为空时新建会话")
    max_duration_s: Optional[float] = Field(None, ge=60, le=86400, description="最长执行时间，超过即中止（TIMEOUT）")
    agent: Optional[str] = Field(None, max_length=64, description="opencode 的 agent（如 build / plan），默认 build")
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
