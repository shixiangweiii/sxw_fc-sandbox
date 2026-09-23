"""状态枚举、领域对象与异常。"""

from dataclasses import dataclass
from enum import Enum


class SandboxState(str, Enum):
    CREATING = "CREATING"
    WARMING = "WARMING"
    READY = "READY"
    PAUSING = "PAUSING"
    PAUSED = "PAUSED"
    RESUMING = "RESUMING"
    LEASED = "LEASED"
    DESTROYING = "DESTROYING"


# 有执行者正在调用云端接口的过渡态，靠 op_owner / op_deadline 做崩溃接管
TRANSITIONAL_STATES = (
    SandboxState.CREATING,
    SandboxState.WARMING,
    SandboxState.PAUSING,
    SandboxState.RESUMING,
    SandboxState.DESTROYING,
)


class LeaseState(str, Enum):
    ACTIVE = "ACTIVE"
    RELEASED = "RELEASED"
    EXPIRED = "EXPIRED"
    FAILED = "FAILED"


class WaiterState(str, Enum):
    WAITING = "WAITING"
    GRANTED = "GRANTED"
    TIMEOUT = "TIMEOUT"
    CANCELLED = "CANCELLED"


@dataclass
class LeaseGrant:
    lease_id: str
    sandbox_row_id: str
    sandbox_id: str
    expires_at: float
    hard_deadline: float
    source: str  # ready / resumed
    wait_ms: float


class PoolError(Exception):
    """池内业务异常基类。"""


class QueueFull(PoolError):
    pass


class WaitTimeout(PoolError):
    pass


class LeaseNotFound(PoolError):
    pass


class LeaseNotActive(PoolError):
    pass


class SandboxOpError(PoolError):
    pass


class SandboxRecordNotFound(PoolError):
    """池里没有这条沙箱记录（管理接口）。"""


class SandboxBusy(PoolError):
    """沙箱处于过渡态（创建、预热、暂停、恢复、销毁中），由执行中的副本负责，稍后重试。"""


class PoolDraining(PoolError):
    """池子正在排空，不接受新的借用。"""


class PayloadTooLarge(PoolError):
    pass


class Unauthorized(PoolError):
    pass


class Forbidden(PoolError):
    pass
