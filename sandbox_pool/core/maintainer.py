"""后台维护循环。每个副本都运行，同一件事靠 CAS 保证只有一个副本做成，不需要选主。

规则：后台任务绝不调用 connect()（会续期，并恢复暂停的沙箱），只用 get_info / list / pause / kill。
"""

import asyncio
import logging
import random
import time

from sandbox_pool.config import PoolConfig
from sandbox_pool.core.lifecycle import Lifecycle
from sandbox_pool.models import TRANSITIONAL_STATES, LeaseState, SandboxState
from sandbox_pool.store.schema import sandboxes

log = logging.getLogger(__name__)

_FAIL_COUNT = "create_fail_count"
_BLOCK_UNTIL = "create_block_until"


class Maintainer:
    def __init__(self, cfg: PoolConfig, lifecycle: Lifecycle):
        self.cfg = cfg
        self.lc = lifecycle
        self.store = lifecycle.store
        self.provider = lifecycle.provider
        self.replica_id = lifecycle.replica_id
        self._wake = asyncio.Event()
        self._task: asyncio.Task | None = None
        self._last_reconcile = 0.0
        self._stopping = False

    def kick(self) -> None:
        self._wake.set()

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="pool-maintainer")

    async def stop(self, grace_s: float = 30) -> None:
        """优雅停止：跑完当前一轮、等后台操作结束，超时才取消。

        直接取消正在执行数据库操作的协程，可能让连接的写事务悬挂，拖住其他副本。
        """
        self._stopping = True
        self.kick()
        if self._task is not None:
            try:
                await asyncio.wait_for(asyncio.shield(self._task), timeout=grace_s)
            except asyncio.TimeoutError:
                self._task.cancel()
                await asyncio.gather(self._task, return_exceptions=True)
            self._task = None
        try:
            await asyncio.wait_for(self.lc.wait_background(), timeout=grace_s)
        except asyncio.TimeoutError:
            await self.lc.cancel_background()

    async def _run(self) -> None:
        while not self._stopping:
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                log.exception("maintainer tick failed")
            interval = self.cfg.maintain_interval_s * random.uniform(0.8, 1.2)
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass
            self._wake.clear()

    async def tick(self) -> None:
        now = self.lc.now()
        await self.store.expire_waiters(now=now, heartbeat_timeout_s=self.cfg.waiter_heartbeat_timeout_s)
        await self.expire_leases(now)
        await self.recover_stuck(now)
        await self.fix_orphan_leased()
        await self.retire_old(now)
        await self.pause_idle(now)
        await self.replenish(now)
        if now - self._last_reconcile >= self.cfg.reconcile_interval_s:
            self._last_reconcile = now
            await self.reconcile(now)

    # ---------- 回收 ----------

    async def expire_leases(self, now: float) -> None:
        for lease in await self.store.list_leases([LeaseState.ACTIVE], expires_before=now):
            if await self.lc.end_lease(lease["id"], LeaseState.EXPIRED, "lease expired"):
                log.info("lease %s expired, sandbox reclaimed", lease["id"])

    async def recover_stuck(self, now: float) -> None:
        """过渡态超过 op_deadline：执行者大概率已崩溃，接管并销毁。"""
        for row in await self.store.list_sandboxes(TRANSITIONAL_STATES):
            if row["op_deadline"] is not None and row["op_deadline"] <= now:
                log.warning("sandbox row=%s stuck in %s (owner=%s), recovering", row["id"], row["state"], row["op_owner"])
                await self.lc.destroy(row, f"stuck in {row['state']} owned by {row['op_owner']}", background=True)

    async def fix_orphan_leased(self) -> None:
        """LEASED 但借用已不是 ACTIVE（例如结束借用后进程崩溃）。"""
        for row in await self.store.list_sandboxes([SandboxState.LEASED]):
            lease = await self.store.get_lease(row["lease_id"]) if row["lease_id"] else None
            if lease is None or lease["state"] != LeaseState.ACTIVE.value:
                await self.lc.destroy(row, "leased without active lease", background=True)

    async def retire_old(self, now: float) -> None:
        for row in await self.store.list_sandboxes([SandboxState.READY, SandboxState.PAUSED]):
            if row["created_at"] <= now - self.cfg.max_age_s:
                await self.lc.destroy(row, "max age reached", background=True)

    # ---------- 暂停 ----------

    async def pause_idle(self, now: float) -> None:
        if not self.cfg.pause_enabled or await self.store.count_waiting() > 0:
            return
        ready = await self.store.list_sandboxes([SandboxState.READY], order_by=sandboxes.c.last_active_at.asc())
        allowed = len(ready) - self.cfg.min_hot
        for row in ready:
            if allowed <= 0:
                break
            if row["last_active_at"] > now - self.cfg.idle_pause_after_s:
                continue
            ok = await self.store.cas_sandbox(
                row["id"],
                [SandboxState.READY],
                now=now,
                expect_version=row["version"],
                state=SandboxState.PAUSING,
                op_owner=self.replica_id,
                op_deadline=now + self.cfg.op_timeout_s,
            )
            if ok:
                allowed -= 1
                self.lc.spawn(self._pause_one(row))

    async def _pause_one(self, row: dict) -> None:
        t0 = time.perf_counter()
        try:
            await self.provider.pause(row["provider_id"])
        except Exception as e:  # noqa: BLE001
            log.warning("pause %s failed: %s", row["provider_id"], e)
            fresh = await self.store.get_sandbox(row["id"])
            if fresh and fresh["state"] == SandboxState.PAUSING.value and fresh["op_owner"] == self.replica_id:
                await self.lc.destroy(fresh, f"pause failed: {e}")
            return
        ok = await self.store.cas_sandbox(
            row["id"],
            [SandboxState.PAUSING],
            now=self.lc.now(),
            expect_owner=self.replica_id,
            state=SandboxState.PAUSED,
            op_owner=None,
            op_deadline=None,
        )
        if ok:
            await self.lc.event("pause", sandbox_row_id=row["id"], duration_ms=(time.perf_counter() - t0) * 1000)
            log.info("paused sandbox %s", row["provider_id"])

    # ---------- 补货 ----------

    async def replenish(self, now: float) -> None:
        if await self.store.kv_get(_BLOCK_UNTIL) > now:
            return
        limit = min(self.cfg.target_size, self.cfg.max_size)
        while True:
            row = await self.store.reserve_slot(
                template=self.cfg.template,
                now=self.lc.now(),
                owner=self.replica_id,
                op_deadline=self.lc.now() + self.cfg.op_timeout_s,
                limit=limit,
            )
            if row is None:
                return
            self.lc.spawn(self._create_one(row))

    async def _create_one(self, row: dict) -> None:
        provider_id = None
        try:
            t0 = time.perf_counter()
            timeout = self.cfg.idle_platform_timeout_s if self.cfg.pause_enabled else self.cfg.max_age_s
            provider_id = await self.provider.create(
                self.cfg.template,
                {"pool": self.cfg.pool_name, "pool_row": row["id"], "replica": self.replica_id},
                timeout,
            )
            await self.lc.event("create", sandbox_row_id=row["id"], duration_ms=(time.perf_counter() - t0) * 1000)
            now = self.lc.now()
            if not await self.store.cas_sandbox(
                row["id"],
                [SandboxState.CREATING],
                now=now,
                expect_owner=self.replica_id,
                state=SandboxState.WARMING,
                provider_id=provider_id,
                op_deadline=now + self.cfg.op_timeout_s,
            ):
                await self.provider.kill(provider_id)
                return
            if self.cfg.warmup_code:
                t1 = time.perf_counter()
                await self.provider.warmup(provider_id, self.cfg.warmup_code)
                await self.lc.event("warmup", sandbox_row_id=row["id"], duration_ms=(time.perf_counter() - t1) * 1000)
            now = self.lc.now()
            if await self.store.cas_sandbox(
                row["id"],
                [SandboxState.WARMING],
                now=now,
                expect_owner=self.replica_id,
                state=SandboxState.READY,
                last_active_at=now,
                op_owner=None,
                op_deadline=None,
            ):
                await self.store.kv_set(_FAIL_COUNT, 0)
                log.info("sandbox %s ready (row=%s)", provider_id, row["id"])
                self.lc.on_change()
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            log.warning("create sandbox failed: %s", e)
            await self.lc.event("create_failed", sandbox_row_id=row["id"], detail=str(e)[:500])
            fails = await self.store.kv_incr(_FAIL_COUNT)
            if fails >= self.cfg.create_fail_threshold:
                await self.store.kv_set(_BLOCK_UNTIL, self.lc.now() + self.cfg.create_cooldown_s)
                await self.store.kv_set(_FAIL_COUNT, 0)
                await self.lc.event("circuit_open", detail=f"{int(fails)} consecutive create failures")
                log.error("create failed %d times in a row, pausing replenish for %ss", fails, self.cfg.create_cooldown_s)
            fresh = await self.store.get_sandbox(row["id"])
            if fresh and fresh["op_owner"] == self.replica_id and fresh["state"] in (
                SandboxState.CREATING.value,
                SandboxState.WARMING.value,
            ):
                if fresh["provider_id"] is None and provider_id:
                    await self.provider.kill(provider_id)
                await self.lc.destroy(fresh, f"create failed: {e}")
            elif provider_id and (fresh is None or fresh["provider_id"] != provider_id):
                await self.provider.kill(provider_id)

    # ---------- 对账 ----------

    async def reconcile(self, now: float) -> None:
        """与云端对账：销毁库里没有记录的孤儿沙箱，清理云端已消失的记录。

        list 前后各读一次库：孤儿判断用 list 之后的记录（不会漏掉刚插入的行）；
        「已消失」判断只看 list 之前就存在且期间版本未变的记录（不会误伤刚创建好的沙箱）。
        """
        rows_before = {r["id"]: r for r in await self.store.list_sandboxes()}
        try:
            items = await self.provider.list({"pool": self.cfg.pool_name})
        except Exception:  # noqa: BLE001
            log.exception("list sandboxes failed")
            return
        rows_after = await self.store.list_sandboxes()
        by_provider = {r["provider_id"] for r in rows_after if r["provider_id"]}
        row_ids = {r["id"] for r in rows_after}
        for it in items:
            if it.sandbox_id in by_provider or it.metadata.get("pool_row") in row_ids:
                continue
            if it.started_at is not None and now - it.started_at < self.cfg.orphan_grace_s:
                continue
            log.warning("killing orphan sandbox %s", it.sandbox_id)
            await self.provider.kill(it.sandbox_id)
            await self.lc.event("orphan_killed", detail=it.sandbox_id)
        live = {it.sandbox_id for it in items}
        for r in rows_after:
            before = rows_before.get(r["id"])
            if (
                before is not None
                and before["version"] == r["version"]
                and r["state"] in (SandboxState.READY.value, SandboxState.PAUSED.value, SandboxState.LEASED.value)
                and r["provider_id"] not in live
                and now - r["state_changed_at"] >= self.cfg.orphan_grace_s
            ):
                log.warning("sandbox %s vanished from provider, dropping row", r["provider_id"])
                if r["state"] == SandboxState.LEASED.value and r["lease_id"]:
                    await self.lc.end_lease(r["lease_id"], LeaseState.FAILED, "sandbox vanished")
                else:
                    await self.lc.destroy(r, "vanished from provider", background=True)
