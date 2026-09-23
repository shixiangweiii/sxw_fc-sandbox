"""池的组装入口：一个进程（副本）一个 SandboxPool。"""

import logging
import os
import socket
import statistics
import uuid
from typing import Optional

from sandbox_pool.config import PoolConfig
from sandbox_pool.core.allocator import Allocator
from sandbox_pool.core.lifecycle import Lifecycle
from sandbox_pool.core.maintainer import Maintainer
from sandbox_pool.models import LeaseState
from sandbox_pool.provider.base import SandboxProvider
from sandbox_pool.store.repository import KV_CREATE_BLOCK_UNTIL, KV_DRAINING, Store

log = logging.getLogger(__name__)


def new_replica_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}-{uuid.uuid4().hex[:6]}"


def _percentile(values: list[float], q: float) -> Optional[float]:
    if not values:
        return None
    if len(values) == 1:
        return round(values[0], 1)
    return round(statistics.quantiles(values, n=100, method="inclusive")[int(q) - 1], 1)


class SandboxPool:
    def __init__(
        self,
        cfg: PoolConfig,
        provider: SandboxProvider,
        *,
        store: Optional[Store] = None,
        replica_id: Optional[str] = None,
    ):
        self.cfg = cfg
        self.replica_id = replica_id or new_replica_id()
        self.store = store or Store.open(cfg.db_url, cfg.pool_name)
        self.provider = provider
        self.lifecycle = Lifecycle(cfg, self.store, provider, self.replica_id)
        self.maintainer = Maintainer(cfg, self.lifecycle)
        self.lifecycle.on_change = self.maintainer.kick
        self.allocator = Allocator(cfg, self.lifecycle, self.maintainer.kick)

    async def start(self, *, run_maintainer: bool = True) -> None:
        await self.store.init_schema()
        if run_maintainer:
            await self.maintainer.start()
        log.info("pool %s replica %s started (template=%s)", self.cfg.pool_name, self.replica_id, self.cfg.template)

    async def stop(self) -> None:
        await self.maintainer.stop()
        await self.provider.close()
        await self.store.close()

    async def drain(self) -> dict:
        """排空：全部副本停止补货和暂停，销毁空闲与已暂停的沙箱；借出中的沙箱归还后销毁、不再补货。"""
        await self.store.kv_set(KV_DRAINING, 1)
        destroyed = await self.maintainer.drain_idle()
        self.maintainer.kick()
        log.warning("pool %s draining (destroyed %d idle sandboxes)", self.cfg.pool_name, destroyed)
        return {"draining": True, "destroyed": destroyed, "sandboxes": await self.store.count_by_state()}

    async def undrain(self) -> dict:
        await self.store.kv_set(KV_DRAINING, 0)
        self.maintainer.kick()
        log.warning("pool %s resumed from draining", self.cfg.pool_name)
        return {"draining": False, "sandboxes": await self.store.count_by_state()}

    async def stats(self) -> dict:
        counts = await self.store.count_by_state()
        durations = await self.store.event_durations()
        active = await self.store.list_leases([LeaseState.ACTIVE])
        return {
            "pool": self.cfg.pool_name,
            "replica": self.replica_id,
            "template": self.cfg.template,
            "draining": await self.store.kv_get(KV_DRAINING) > 0,
            "config": {
                "max_size": self.cfg.max_size,
                "target_size": self.cfg.target_size,
                "min_hot": self.cfg.min_hot,
                "idle_pause_after_s": self.cfg.idle_pause_after_s,
                "ready_platform_timeout_s": self.cfg.ready_platform_timeout_s,
                "queue_max": self.cfg.queue_max,
                "wait_timeout_s": self.cfg.wait_timeout_s,
                "strict_fifo": self.cfg.strict_fifo,
                "lease_ttl_s": self.cfg.lease_ttl_s,
                "lease_max_s": self.cfg.lease_max_s,
                "auth_enabled": self.cfg.auth_enabled,
            },
            "sandboxes": {"total": sum(counts.values()), **counts},
            "waiting": await self.store.count_waiting(),
            "active_leases": len(active),
            "lease_sources": await self.store.lease_source_counts(),
            "create_blocked_until": await self.store.kv_get(KV_CREATE_BLOCK_UNTIL) or None,
            "events": await self.store.event_counts(),
            "latency_ms": {
                kind: {"count": len(v), "p50": _percentile(v, 50), "p99": _percentile(v, 99), "max": round(max(v), 1)}
                for kind, v in sorted(durations.items())
            },
        }
