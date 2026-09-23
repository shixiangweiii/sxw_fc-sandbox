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
