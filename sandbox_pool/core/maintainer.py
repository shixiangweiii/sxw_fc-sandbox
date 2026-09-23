"""后台维护循环。每个副本都运行，同一件事靠 CAS 保证只有一个副本做成，不需要选主。

规则：后台任务绝不调用 connect()（会续期，并恢复暂停的沙箱），只用 get_info / list / set_timeout / pause / kill。
"""

import asyncio
import logging
import random
import time
from typing import Optional

from sandbox_pool.config import PoolConfig
from sandbox_pool.core.lifecycle import Lifecycle
from sandbox_pool.models import TRANSITIONAL_STATES, LeaseState, SandboxState
from sandbox_pool.provider.base import SandboxNotFound
from sandbox_pool.store.repository import (
    KV_CLEANUP_AT,
    KV_CREATE_BLOCK_UNTIL,
    KV_CREATE_FAIL_COUNT,
    KV_DRAINING,
    KV_RECONCILE_AT,
)
from sandbox_pool.store.schema import sandboxes

log = logging.getLogger(__name__)

# 接管卡住的暂停 / 恢复时，云端实际状态 → 收回后的状态
_ADOPT_STATES = {"paused": SandboxState.PAUSED, "running": SandboxState.READY}


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
        self._last_cleanup = 0.0
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
        if await self.store.kv_get(KV_DRAINING) > 0:
            # 排空中：不补货、不暂停、不保活，陆续销毁空闲和已暂停的沙箱
            await self.drain_idle()
        else:
            await self.pause_idle(now)
            await self.keepalive_ready(now)
            await self.replenish(now)
        # 对账、历史清理：全池每个周期只由一个副本执行（pool_kv 上的时间戳 CAS）
        if now - self._last_reconcile >= self.cfg.reconcile_interval_s:
            self._last_reconcile = now
            if await self.store.try_periodic(KV_RECONCILE_AT, now=now, interval_s=self.cfg.reconcile_interval_s):
                await self.reconcile(now)
        if now - self._last_cleanup >= self.cfg.cleanup_interval_s:
            self._last_cleanup = now
            if await self.store.try_periodic(KV_CLEANUP_AT, now=now, interval_s=self.cfg.cleanup_interval_s):
                await self.cleanup(now)

    # ---------- 回收 ----------

    async def expire_leases(self, now: float) -> None:
        for lease in await self.store.list_leases([LeaseState.ACTIVE], expires_before=now):
            # 带上 expires_at <= now 条件：同时发生的续期成功时不会被覆盖
            if await self.lc.end_lease(lease["id"], LeaseState.EXPIRED, "lease expired", expired_before=self.lc.now()):
                log.info("lease %s expired, sandbox reclaimed", lease["id"])

    async def recover_stuck(self, now: float) -> None:
        """过渡态超过 op_deadline：执行者大概率已崩溃，接管。

        暂停 / 恢复途中崩溃的，先按云端实际状态收回（已暂停 → PAUSED，运行中 → READY）；
        查不到或状态未知的，以及创建、预热、销毁途中崩溃的，一律销毁。
        """
        for row in await self.store.list_sandboxes(TRANSITIONAL_STATES):
            if row["op_deadline"] is None or row["op_deadline"] > now:
                continue
            log.warning("sandbox row=%s stuck in %s (owner=%s), recovering", row["id"], row["state"], row["op_owner"])
            if row["state"] in (SandboxState.PAUSING.value, SandboxState.RESUMING.value) and row["provider_id"]:
                if await self._adopt(row):
                    continue
            await self.lc.destroy(row, f"stuck in {row['state']} owned by {row['op_owner']}", background=True)

    async def _adopt(self, row: dict) -> bool:
        try:
            actual = await self.provider.get_state(row["provider_id"])
        except Exception as e:  # noqa: BLE001
            log.warning("get_state %s failed: %s", row["provider_id"], e)
            return False
        target = _ADOPT_STATES.get(actual or "")
        if target is None:
            return False
        now = self.lc.now()
        ok = await self.store.cas_sandbox(
            row["id"],
            [row["state"]],
            now=now,
            expect_version=row["version"],
            state=target,
            op_owner=None,
            op_deadline=None,
            last_active_at=now,
            # 平台超时未知：READY 由保活立即重设
            platform_deadline=None,
            error=f"adopted from {row['state']} owned by {row['op_owner']}",
        )
        if ok:
            log.info("adopted sandbox %s: %s -> %s", row["provider_id"], row["state"], target.value)
            await self.lc.event("adopt", sandbox_row_id=row["id"], detail=f"{row['state']}->{target.value}")
            self.lc.on_change()
        # CAS 失败说明已被其他副本处理，同样不需要再销毁
        return True

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

    # ---------- 排空 ----------

    async def drain_idle(self) -> int:
        """销毁全部空闲和已暂停的沙箱（排空用），返回本次销毁的数量。"""
        rows = await self.store.list_sandboxes([SandboxState.READY, SandboxState.PAUSED])
        done = await asyncio.gather(*[self.lc.destroy(row, "drained") for row in rows], return_exceptions=True)
        for r in done:
            if isinstance(r, Exception):
                log.warning("drain destroy failed: %s", r)
        return sum(1 for r in done if r is True)

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
            # 本地预算只是预筛；是否低于 min_hot 在事务内按全池 READY 数量重新判断
            ok = await self.store.start_pause(
                row,
                now=now,
                owner=self.replica_id,
                op_deadline=now + self.cfg.op_timeout_s,
                min_hot=self.cfg.min_hot,
            )
            if ok:
                allowed -= 1
                self.lc.spawn(self._pause_one(row))

    async def _pause_one(self, row: dict) -> None:
        t0 = time.perf_counter()
        try:
            await self.provider.pause(row["provider_id"])
        except Exception as e:  # noqa: BLE001
            log.warning("pause %s failed: %r", row["provider_id"], e, exc_info=True)
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

    # ---------- READY 保活 ----------

    async def keepalive_ready(self, now: float) -> None:
        """READY 沙箱的平台超时剩余不足一半时续期（min_hot 保留的热沙箱、有人排队时未暂停的沙箱）。"""
        timeout = self.cfg.ready_platform_timeout_s
        for row in await self.store.list_sandboxes([SandboxState.READY]):
            deadline = row["platform_deadline"]
            if deadline is not None and deadline - now >= timeout / 2:
                continue
            await self._keepalive(row, timeout)

    async def _keepalive(self, row: dict, timeout: float) -> None:
        now = self.lc.now()
        # 先用版本号 CAS 占住：并发的借出 / 暂停 / 其他副本的保活会有一方失败
        if not await self.store.cas_sandbox(
            row["id"], [SandboxState.READY], now=now, expect_version=row["version"], platform_deadline=now + timeout
        ):
            return
        t0 = time.perf_counter()
        try:
            await self.provider.set_timeout(row["provider_id"], timeout)
        except SandboxNotFound:
            fresh = await self.store.get_sandbox(row["id"])
            if fresh and fresh["state"] == SandboxState.READY.value:
                await self.lc.destroy(fresh, "vanished from provider (keepalive)", background=True)
            return
        except Exception as e:  # noqa: BLE001
            log.warning("keepalive %s failed: %r", row["provider_id"], e, exc_info=True)
            # 没续上：到期时间视为未知（None），下一轮立即重试。不带版本条件：期间别的副本可能改过版本，
            # 带版本的回滚会静默失败，留下比实际更晚的到期时间，保活会长期跳过这个沙箱
            await self.store.cas_sandbox(row["id"], [SandboxState.READY], now=self.lc.now(), platform_deadline=None)
            return
        fresh = await self.store.get_sandbox(row["id"])
        await self.lc.event("keepalive", sandbox_row_id=row["id"], duration_ms=(time.perf_counter() - t0) * 1000)
        if fresh is None or fresh["version"] == row["version"] + 1:
            return
        # 续期期间被借走：借用方的 set_timeout 可能先于本次到达平台、被改成了较短的空闲超时，按借用重新设置。
        # 借用方的 CAS 若发生在上面的重读之后，它的 set_timeout 必然晚于本次到达，不需要处理。
        if fresh["state"] == SandboxState.LEASED.value and fresh["lease_id"]:
            await self._restore_lease_timeout(fresh)

    async def _restore_lease_timeout(self, row: dict) -> None:
        lease = await self.store.get_lease(row["lease_id"])
        now = self.lc.now()
        if lease is None or lease["state"] != LeaseState.ACTIVE.value or lease["expires_at"] <= now:
            return
        try:
            await self.provider.set_timeout(
                row["provider_id"], lease["expires_at"] - now + self.cfg.platform_timeout_margin_s
            )
            log.info("restored lease timeout of sandbox %s after racing keepalive", row["provider_id"])
        except Exception as e:  # noqa: BLE001
            log.warning("restore lease timeout of %s failed: %s", row["provider_id"], e)

    # ---------- 补货 ----------

    async def replenish(self, now: float) -> None:
        if await self.store.kv_get(KV_CREATE_BLOCK_UNTIL) > now:
            return
        limit = min(self.cfg.target_size, self.cfg.max_size)
        # 每轮最多占 limit 个名额：创建失败会很快释放名额，不设上限时同一轮会反复占用、反复创建。
        # 熔断在占名额的事务内检查，熔断打开后正在进行的补货立即停止
        for _ in range(limit):
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
            timeout = self.cfg.ready_platform_timeout_s
            created_at = self.lc.now()
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
                # 已被其他副本接管
                await self._kill_quietly(provider_id)
                return
            if self.cfg.warmup_code:
                t1 = time.perf_counter()
                await self.provider.warmup(provider_id, self.cfg.warmup_code, sandbox_timeout_s=timeout)
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
                platform_deadline=created_at + timeout,
            ):
                log.info("sandbox %s ready (row=%s)", provider_id, row["id"])
                self.lc.on_change()
                try:
                    await self.store.kv_set(KV_CREATE_FAIL_COUNT, 0)
                except Exception:  # noqa: BLE001
                    log.exception("reset create fail count failed")
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            await self._on_create_failed(row, provider_id, e)

    async def _on_create_failed(self, row: dict, provider_id: Optional[str], err: Exception) -> None:
        """创建失败的善后。每一步单独保护，任何一步出错都不影响后面的步骤（尤其是释放占住的容量）。

        先累加熔断计数再释放名额：熔断在名额释放前生效，补货不会在两步之间又发起新的创建。
        """
        log.warning("create sandbox failed: %s", err)
        await self.lc.event("create_failed", sandbox_row_id=row["id"], detail=str(err)[:500])
        try:
            fails = await self.store.kv_incr(KV_CREATE_FAIL_COUNT)
            if fails >= self.cfg.create_fail_threshold:
                await self.store.kv_set(KV_CREATE_BLOCK_UNTIL, self.lc.now() + self.cfg.create_cooldown_s)
                await self.store.kv_set(KV_CREATE_FAIL_COUNT, 0)
                await self.lc.event("circuit_open", detail=f"{int(fails)} consecutive create failures")
                log.error("create failed %d times in a row, pausing replenish for %ss", fails, self.cfg.create_cooldown_s)
        except Exception:  # noqa: BLE001
            log.exception("update create circuit breaker failed")
        try:
            fresh = await self.store.get_sandbox(row["id"])
            if fresh and fresh["op_owner"] == self.replica_id and fresh["state"] in (
                SandboxState.CREATING.value,
                SandboxState.WARMING.value,
            ):
                if fresh["provider_id"] is None and provider_id:
                    await self._kill_quietly(provider_id)
                await self.lc.destroy(fresh, f"create failed: {err}")
            elif provider_id and (fresh is None or fresh["provider_id"] != provider_id):
                await self._kill_quietly(provider_id)
        except Exception:  # noqa: BLE001 - 仍未清理的记录由 op_deadline 接管，云端孤儿由对账清理
            log.exception("cleanup after create failure failed (row=%s)", row["id"])

    async def _kill_quietly(self, provider_id: str) -> None:
        try:
            await self.provider.kill(provider_id)
        except Exception:  # noqa: BLE001 - 留给对账清理
            log.exception("kill %s failed, leaving it to reconcile", provider_id)

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
            await self._kill_quietly(it.sandbox_id)
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

    # ---------- 历史清理 ----------

    async def cleanup(self, now: float) -> None:
        purged = await self.store.purge_history(before=now - self.cfg.history_retention_s)
        if any(purged.values()):
            log.info("purged history older than %ss: %s", self.cfg.history_retention_s, purged)
