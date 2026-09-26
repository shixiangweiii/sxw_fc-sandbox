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
from sandbox_pool.models import LeaseState, SandboxBusy, SandboxRecordNotFound, SandboxState
from sandbox_pool.provider.base import SandboxProvider
from sandbox_pool.store.repository import KV_CREATE_BLOCK_UNTIL, KV_DRAINING, Store

log = logging.getLogger(__name__)

# 管理员查看沙箱时附带的借用信息（不含 lease_id）
_ADMIN_LEASE_FIELDS = ("client_id", "source", "created_at", "expires_at", "hard_deadline")
# 管理员可以直接销毁的状态；过渡态由执行中的副本负责
_ADMIN_DESTROYABLE = (SandboxState.READY.value, SandboxState.PAUSED.value, SandboxState.LEASED.value)


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

    async def list_sandboxes(self) -> list[dict]:
        """管理员查看的沙箱记录。不含 lease_id（借用凭证）；借出中的附带借用方与到期时间，便于定位卡住的借用。"""
        active = {lease["sandbox_row_id"]: lease for lease in await self.store.list_leases([LeaseState.ACTIVE])}
        out = []
        for row in await self.store.list_sandboxes():
            item = {k: v for k, v in row.items() if k not in ("lease_id", "access_token")}
            lease = active.get(row["id"])
            item["lease"] = None if lease is None else {k: lease[k] for k in _ADMIN_LEASE_FIELDS}
            out.append(item)
        return out

    async def destroy_sandbox(self, row_id: str) -> dict:
        """管理员强制销毁一个沙箱（例如借用方断开后卡住的借用）。

        借出中的先结束借用（借用方之后访问返回 409），空闲 / 已暂停的直接销毁，之后照常补货。
        过渡态由执行中的副本负责，返回 409，稍后重试。
        """
        row = await self.store.get_sandbox(row_id)
        if row is None or row["pool"] != self.cfg.pool_name:
            raise SandboxRecordNotFound(f"sandbox {row_id} not found")
        lease_ended = False
        if row["state"] == SandboxState.LEASED.value and row["lease_id"]:
            lease_ended = await self.lifecycle.end_lease(
                row["lease_id"], LeaseState.RELEASED, "released by admin", background=False
            )
        done = lease_ended
        if not done and row["state"] in _ADMIN_DESTROYABLE:
            # 借用已不是 ACTIVE 的 LEASED 记录（维护循环也会清理）以及空闲、已暂停的沙箱
            done = await self.lifecycle.destroy(row, "destroyed by admin")
        if not done:
            raise SandboxBusy(f"sandbox {row_id} is {row['state']}, retry later")
        self.maintainer.kick()
        log.warning("sandbox %s (%s) destroyed by admin", row_id, row["state"])
        return {
            "id": row_id,
            "provider_id": row["provider_id"],
            "previous_state": row["state"],
            "lease_ended": lease_ended,
        }

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
