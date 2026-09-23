"""池配置。所有字段都可以用 POOL_<字段名大写> 环境变量覆盖。"""

import dataclasses
import os
from dataclasses import dataclass, field


@dataclass(frozen=True)
class PoolConfig:
    pool_name: str = "default"
    template: str = "code-interpreter-v1"
    db_url: str = "sqlite+aiosqlite:///./.data/pool.db"

    # 鉴权：逗号分隔的「名称:key」。两者都为空时关闭鉴权，仅限本地开发（见 __main__ 的监听地址保护）
    api_keys: str = field(default="", repr=False)
    admin_keys: str = field(default="", repr=False)

    # 容量：沙箱总数（借出、空闲、暂停、创建中都计入）不超过 max_size，补货目标为 target_size
    max_size: int = 5
    target_size: int = 5
    # 空闲时保持运行、不暂停的数量
    min_hot: int = 0
    pause_enabled: bool = True
    idle_pause_after_s: float = 60

    # 排队
    queue_max: int = 10
    wait_timeout_s: float = 180
    # 严格先来先服务：所有借用请求都先入队（默认队列为空时直接抢）
    strict_fifo: bool = False

    # 借用
    lease_ttl_s: float = 600
    lease_max_s: float = 3600

    # 沙箱最长寿命，到期后空闲或暂停的沙箱会被回收
    max_age_s: float = 6 * 3600
    # 新沙箱预热执行的代码；为空则不预热
    warmup_code: str = "import numpy, pandas, matplotlib"

    # 平台超时只作兜底：借出中为借用剩余时间 + margin，空闲时为 idle_pause_after_s + idle_platform_extra_s
    # （READY 沙箱由维护循环在剩余时间不足一半时续期）
    platform_timeout_margin_s: float = 60
    idle_platform_extra_s: float = 120

    # 后台维护
    poll_interval_s: float = 0.2
    maintain_interval_s: float = 1.0
    waiter_heartbeat_timeout_s: float = 5
    # 创建、预热、暂停等耗时操作的截止时间；超过即认为执行者已崩溃
    op_timeout_s: float = 120
    # 销毁、恢复的截止时间（单独设置，副本崩溃时尽快释放容量）
    destroy_timeout_s: float = 30
    resume_timeout_s: float = 60
    reconcile_interval_s: float = 60
    orphan_grace_s: float = 60
    # 连续创建失败达到阈值后暂停补货 create_cooldown_s 秒
    create_fail_threshold: int = 3
    create_cooldown_s: float = 60
    # 已结束的排队、借用记录和事件保留多久；清理周期
    history_retention_s: float = 7 * 86400
    cleanup_interval_s: float = 3600

    # 本副本的连接句柄缓存：最多缓存多少个，空闲多久淘汰
    handle_cache_max: int = 64
    handle_idle_ttl_s: float = 600
    # 上传文件的大小上限
    max_upload_bytes: int = 64 * 1024 * 1024
    # 其余请求体（JSON）的大小上限，在鉴权之前生效
    max_body_bytes: int = 1024 * 1024

    @property
    def idle_platform_timeout_s(self) -> float:
        return self.idle_pause_after_s + self.idle_platform_extra_s

    @property
    def ready_platform_timeout_s(self) -> float:
        """READY 沙箱的平台超时：开启暂停时为空闲兜底时间，否则为最长寿命。"""
        return self.idle_platform_timeout_s if self.pause_enabled else self.max_age_s

    @property
    def auth_enabled(self) -> bool:
        return bool(self.api_keys.strip() or self.admin_keys.strip())

    @classmethod
    def from_env(cls, **overrides) -> "PoolConfig":
        values = {}
        for f in dataclasses.fields(cls):
            raw = os.environ.get(f"POOL_{f.name.upper()}")
            if raw is None:
                continue
            if f.type in (bool, "bool"):
                values[f.name] = raw.strip().lower() in ("1", "true", "yes", "on")
            elif f.type in (int, "int"):
                values[f.name] = int(raw)
            elif f.type in (float, "float"):
                values[f.name] = float(raw)
            else:
                values[f.name] = raw
        values.update(overrides)
        return cls(**values)
