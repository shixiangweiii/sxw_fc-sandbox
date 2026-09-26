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
    # ---- agent 子系统（pool = agent_pool_name）专用，均可空 ----
    Column("agent_id", String(36)),
    # opencode 服务地址 https://<port>-<sandbox_id>.<domain>
    Column("endpoint", String(256)),
    # 平台流量令牌（访问 endpoint 必带），敏感：任何接口都不返回
    Column("access_token", Text),
    # 已下发的出网策略版本、已应用的 agent 设置版本
    Column("network_version", String(64)),
    Column("config_version", Integer),
    Column("health_failures", Integer),
    # 最后一次活动的来源（message / schedule），决定空闲销毁用哪个时长
    Column("activity_kind", String(16)),
    Index("ix_sandboxes_pool_state", "pool", "state"),
)

# 每个（调用方，用户）一个常驻 agent
agents = Table(
    "agents",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("pool", String(64), nullable=False),
    Column("client_id", String(64), nullable=False),
    Column("user_id", String(128), nullable=False),
    # JSON：idle_destroy_after_s / mcp / instructions
    Column("settings", Text, nullable=False),
    Column("settings_version", Integer, nullable=False, default=1),
    # JSON：出网策略的按 agent 覆盖（mode / allow_out / deny_out），空表示用默认
    Column("egress", Text),
    Column("created_at", Float, nullable=False),
    Column("updated_at", Float, nullable=False),
    Index("ix_agents_pool_client_user", "pool", "client_id", "user_id"),
)

# 一次对话请求或一次定时触发
tasks = Table(
    "tasks",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("pool", String(64), nullable=False),
    Column("agent_id", String(36), nullable=False),
    Column("client_id", String(64), nullable=False),
    Column("source", String(16), nullable=False),
    Column("schedule_id", String(36)),
    Column("sandbox_row_id", String(36)),
    Column("session_id", String(64)),
    Column("state", String(16), nullable=False),
    Column("prompt", Text, nullable=False),
    Column("result_text", Text),
    Column("error", Text),
    # JSON：input / output / reasoning / cache_read tokens、cost
    Column("usage", Text),
    Column("created_at", Float, nullable=False),
    Column("started_at", Float),
    Column("finished_at", Float),
    # 最长执行截止时间
    Column("deadline", Float, nullable=False),
    # 负责跟进任务的副本及其心跳截止时间（过期后由其他副本接管）
    Column("op_owner", String(128)),
    Column("op_deadline", Float),
    Column("abort_requested", Integer, nullable=False, default=0),
    Index("ix_tasks_pool_agent_created", "pool", "agent_id", "created_at"),
    Index("ix_tasks_pool_state", "pool", "state"),
)

schedules = Table(
    "schedules",
    metadata,
    Column("id", String(36), primary_key=True),
    Column("pool", String(64), nullable=False),
    Column("agent_id", String(36), nullable=False),
    Column("client_id", String(64), nullable=False),
    Column("name", String(128), nullable=False),
    # cron（5 段）与 every_s 二选一
    Column("cron", String(128)),
    Column("every_s", Float),
    Column("timezone", String(64), nullable=False),
    Column("prompt", Text, nullable=False),
    Column("enabled", Integer, nullable=False, default=1),
    Column("max_duration_s", Float),
    # skip：上一次还在运行时跳过本次；allow：照常触发
    Column("overlap", String(16), nullable=False),
    Column("next_run_at", Float),
    Column("last_run_at", Float),
    Column("last_task_id", String(36)),
    Column("created_at", Float, nullable=False),
    Column("updated_at", Float, nullable=False),
    Index("ix_schedules_pool_next", "pool", "next_run_at"),
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
