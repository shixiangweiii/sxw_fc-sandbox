"""表结构（SQLAlchemy Core）。时间统一用 epoch 秒（float），避免方言差异。"""

from sqlalchemy import (
    Column,
    Float,
    Index,
    Integer,
    MetaData,
    String,
    Table,
    Text,
)

metadata = MetaData()

sandboxes = Table(
    "sandboxes",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("pool", String(64), nullable=False),
    Column("provider_id", String(128)),
    Column("template", String(128), nullable=False),
    Column("state", String(16), nullable=False),
    Column("version", Integer, nullable=False, default=0),
    Column("lease_id", String(36)),
    Column("created_at", Float, nullable=False),
    Column("state_changed_at", Float, nullable=False),
    Column("last_active_at", Float, nullable=False),
    Column("op_owner", String(128)),
    Column("op_deadline", Float),
    Column("error", Text),
    # READY 沙箱在平台上的到期时间（维护循环据此续期）；其他状态为空
    Column("platform_deadline", Float),
    Index("ix_sandboxes_pool_state", "pool", "state"),
)

leases = Table(
    "leases",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("pool", String(64), nullable=False),
    Column("sandbox_row_id", String(36), nullable=False),
    Column("sandbox_id", String(128), nullable=False),
    Column("state", String(16), nullable=False),
    Column("source", String(16), nullable=False),
    Column("created_at", Float, nullable=False),
    Column("expires_at", Float, nullable=False),
    Column("hard_deadline", Float, nullable=False),
    Column("ended_at", Float),
    Column("wait_ms", Float),
    # 借用方身份（鉴权开启时为 key 的名称），只有借用方本人和管理员能操作
    Column("client_id", String(64)),
    Index("ix_leases_pool_state", "pool", "state"),
)

waiters = Table(
    "waiters",
    metadata,
    Column("seq", Integer, primary_key=True, autoincrement=True),
    Column("pool", String(64), nullable=False),
    Column("state", String(16), nullable=False),
    Column("owner_replica", String(128), nullable=False),
    Column("created_at", Float, nullable=False),
    Column("deadline", Float, nullable=False),
    Column("heartbeat_at", Float, nullable=False),
    Column("lease_id", String(36)),
    Index("ix_waiters_pool_state", "pool", "state"),
)

# 共享键值：池级锁行、熔断计数、周期任务的上次执行时间、排空开关等
pool_kv = Table(
    "pool_kv",
    metadata,
    Column("pool", String(64), primary_key=True),
    Column("key", String(64), primary_key=True),
    Column("value", Float, nullable=False, default=0),
)

events = Table(
    "events",
    metadata,
    Column("id", Integer, primary_key=True, autoincrement=True),
    Column("ts", Float, nullable=False),
    Column("pool", String(64), nullable=False),
    Column("kind", String(32), nullable=False),
    Column("sandbox_row_id", String(36)),
    Column("lease_id", String(36)),
    Column("replica", String(128)),
    Column("duration_ms", Float),
    Column("detail", Text),
    Index("ix_events_pool_kind", "pool", "kind"),
    Index("ix_events_pool_ts", "pool", "ts"),
)
