"""借用分配、排队、续期、归还，以及借用期内代为执行。"""

import asyncio
import logging
import random
import time
import uuid
from typing import Awaitable, Callable, Optional

from sandbox_pool.config import PoolConfig
from sandbox_pool.core.lifecycle import Lifecycle
from sandbox_pool.models import (
    LeaseGrant,
    LeaseNotActive,
    LeaseNotFound,
    LeaseState,
    QueueFull,
    SandboxOpError,
    SandboxState,
    WaiterState,
    WaitTimeout,
)
from sandbox_pool.provider.base import CodeResult, CommandResult, SandboxNotFound
from sandbox_pool.store.schema import sandboxes

log = logging.getLogger(__name__)

# 单次 try_claim 内换候选重试的上限
_MAX_CLAIM_ATTEMPTS = 10


class Allocator:
    def __init__(self, cfg: PoolConfig, lifecycle: Lifecycle, kick: Callable[[], None]):
        self.cfg = cfg
        self.lc = lifecycle
        self.store = lifecycle.store
        self.provider = lifecycle.provider
        self.replica_id = lifecycle.replica_id
        self.kick = kick

    # ---------- 借用 ----------

    async def acquire(
        self,
        *,
        wait_timeout_s: Optional[float] = None,
        lease_ttl_s: Optional[float] = None,
        is_disconnected: Optional[Callable[[], Awaitable[bool]]] = None,
    ) -> LeaseGrant:
        start = self.lc.now()
        wait = self.cfg.wait_timeout_s if wait_timeout_s is None else min(max(0.0, wait_timeout_s), self.cfg.wait_timeout_s)
        ttl = self.cfg.lease_ttl_s if lease_ttl_s is None else min(max(1.0, lease_ttl_s), self.cfg.lease_max_s)
        deadline = start + wait

        # 队列为空时直接抢，避免无谓入队；有人排队时必须排到队尾，保证先来先服务
        if await self.store.count_waiting() == 0:
            grant = await self._try_claim(ttl, start)
            if grant:
                return grant
            self.kick()

        seq = await self.store.enqueue(
            owner=self.replica_id, now=self.lc.now(), deadline=deadline, queue_max=self.cfg.queue_max
        )
        if seq is None:
            await self.lc.event("queue_full")
            raise QueueFull(f"queue is full ({self.cfg.queue_max})")

        try:
            while True:
                now = self.lc.now()
                if now >= deadline:
                    await self.store.cas_waiter(seq, WaiterState.WAITING, WaiterState.TIMEOUT)
                    await self.lc.event("wait_timeout", duration_ms=(now - start) * 1000)
                    raise WaitTimeout(f"no sandbox available within {wait:.0f}s")
                if is_disconnected is not None and await is_disconnected():
                    await self.store.cas_waiter(seq, WaiterState.WAITING, WaiterState.CANCELLED)
                    raise WaitTimeout("client disconnected")
                if not await self.store.heartbeat(seq, now):
                    raise WaitTimeout("waiter expired")
                if await self.store.head_seq() == seq:
                    grant = await self._try_claim(ttl, start)
                    if grant:
                        if await self.store.cas_waiter(
                            seq, WaiterState.WAITING, WaiterState.GRANTED, lease_id=grant.lease_id
                        ):
                            return grant
                        await self.lc.end_lease(grant.lease_id, LeaseState.FAILED, "waiter expired before grant")
                        raise WaitTimeout("waiter expired")
                    self.kick()
                await asyncio.sleep(self.cfg.poll_interval_s)
        except asyncio.CancelledError:
            await asyncio.shield(self.store.cas_waiter(seq, WaiterState.WAITING, WaiterState.CANCELLED))
            raise

    async def _try_claim(self, ttl: float, wait_started: float) -> Optional[LeaseGrant]:
        for _ in range(_MAX_CLAIM_ATTEMPTS):
            ready = await self.store.list_sandboxes([SandboxState.READY], order_by=sandboxes.c.last_active_at.desc())
            candidates = ready or await self.store.list_sandboxes(
                [SandboxState.PAUSED], order_by=sandboxes.c.state_changed_at.desc()
            )
            if not candidates:
                return None
            # 多副本同时抢时打散，降低 CAS 冲突
            row = random.choice(candidates[:3])
            if row["state"] == SandboxState.READY.value:
                grant = await self._claim_ready(row, ttl, wait_started)
            else:
                grant = await self._claim_paused(row, ttl, wait_started)
            if grant:
                return grant
        return None

    def _new_lease(self, row: dict, ttl: float, now: float, source: str, wait_started: float) -> dict:
        return dict(
            id=str(uuid.uuid4()),
            pool=self.cfg.pool_name,
            sandbox_row_id=row["id"],
            sandbox_id=row["provider_id"],
            state=LeaseState.ACTIVE.value,
            source=source,
            created_at=now,
            expires_at=now + ttl,
            hard_deadline=now + self.cfg.lease_max_s,
            ended_at=None,
            wait_ms=(now - wait_started) * 1000,
        )

    def _grant(self, lease: dict) -> LeaseGrant:
        return LeaseGrant(
            lease_id=lease["id"],
            sandbox_row_id=lease["sandbox_row_id"],
            sandbox_id=lease["sandbox_id"],
            expires_at=lease["expires_at"],
            hard_deadline=lease["hard_deadline"],
            source=lease["source"],
            wait_ms=lease["wait_ms"],
        )

    async def _claim_ready(self, row: dict, ttl: float, wait_started: float) -> Optional[LeaseGrant]:
        now = self.lc.now()
        lease = self._new_lease(row, ttl, now, "ready", wait_started)
        if not await self.store.claim_ready(row, lease=lease, now=now):
            return None
        try:
            await self.provider.set_timeout(row["provider_id"], ttl + self.cfg.platform_timeout_margin_s)
        except Exception as e:  # noqa: BLE001 - 沙箱已失效，换一个
            log.warning("ready sandbox %s unusable: %s", row["provider_id"], e)
            await self.lc.end_lease(lease["id"], LeaseState.FAILED, f"set_timeout failed: {e}")
            return None
        await self.lc.event(
            "acquire", sandbox_row_id=row["id"], lease_id=lease["id"], duration_ms=lease["wait_ms"], detail="ready"
        )
        return self._grant(lease)

    async def _claim_paused(self, row: dict, ttl: float, wait_started: float) -> Optional[LeaseGrant]:
        now = self.lc.now()
        if not await self.store.cas_sandbox(
            row["id"],
            [SandboxState.PAUSED],
            now=now,
            expect_version=row["version"],
            state=SandboxState.RESUMING,
            op_owner=self.replica_id,
            op_deadline=now + self.cfg.op_timeout_s,
        ):
            return None
        t0 = time.perf_counter()
        try:
            await self.provider.resume(row["provider_id"], ttl + self.cfg.platform_timeout_margin_s)
        except Exception as e:  # noqa: BLE001
            log.warning("resume %s failed: %s", row["provider_id"], e)
            await self.lc.event("resume_failed", sandbox_row_id=row["id"], detail=str(e)[:500])
            fresh = await self.store.get_sandbox(row["id"])
            if fresh and fresh["state"] == SandboxState.RESUMING.value and fresh["op_owner"] == self.replica_id:
                await self.lc.destroy(fresh, f"resume failed: {e}", background=True)
            return None
        await self.lc.event("resume", sandbox_row_id=row["id"], duration_ms=(time.perf_counter() - t0) * 1000)
        now = self.lc.now()
        lease = self._new_lease(row, ttl, now, "resumed", wait_started)
        if not await self.store.finish_resume(row["id"], owner=self.replica_id, lease=lease, now=now):
            # 超过 op_deadline 已被其他副本接管销毁
            return None
        await self.lc.event(
            "acquire", sandbox_row_id=row["id"], lease_id=lease["id"], duration_ms=lease["wait_ms"], detail="resumed"
        )
        return self._grant(lease)

    # ---------- 借用管理 ----------

    async def get_lease(self, lease_id: str) -> dict:
        lease = await self.store.get_lease(lease_id)
        if lease is None:
            raise LeaseNotFound(lease_id)
        return lease

    async def renew(self, lease_id: str, ttl_s: Optional[float] = None) -> dict:
        lease = await self.get_lease(lease_id)
        now = self.lc.now()
        if lease["state"] != LeaseState.ACTIVE.value or lease["expires_at"] <= now:
            raise LeaseNotActive(f"lease {lease_id} is {lease['state']}")
        ttl = self.cfg.lease_ttl_s if ttl_s is None else max(1.0, ttl_s)
        new_expires = min(now + ttl, lease["hard_deadline"])
        if not await self.store.cas_lease(
            lease_id, [LeaseState.ACTIVE], expect_unexpired_at=now, expires_at=new_expires
        ):
            raise LeaseNotActive(f"lease {lease_id} is no longer active")
        try:
            await self.provider.set_timeout(
                lease["sandbox_id"], new_expires - now + self.cfg.platform_timeout_margin_s
            )
        except SandboxNotFound as e:
            await self.lc.end_lease(lease_id, LeaseState.FAILED, "sandbox vanished")
            raise SandboxOpError(f"sandbox {lease['sandbox_id']} not found") from e
        await self.lc.event("renew", lease_id=lease_id, sandbox_row_id=lease["sandbox_row_id"])
        return await self.get_lease(lease_id)

    async def release(self, lease_id: str) -> dict:
        await self.get_lease(lease_id)
        await self.lc.end_lease(lease_id, LeaseState.RELEASED, "released")
        self.kick()
        return await self.get_lease(lease_id)

    # ---------- 代为执行 ----------

    async def _use(self, lease_id: str) -> tuple[dict, float]:
        lease = await self.get_lease(lease_id)
        now = self.lc.now()
        if lease["state"] != LeaseState.ACTIVE.value or lease["expires_at"] <= now:
            raise LeaseNotActive(f"lease {lease_id} is {lease['state']}")
        await self.store.touch_sandbox(lease["sandbox_row_id"], now)
        return lease, lease["expires_at"] - now + self.cfg.platform_timeout_margin_s

    async def _guard(self, lease: dict, coro):
        try:
            return await coro
        except SandboxNotFound as e:
            await self.lc.end_lease(lease["id"], LeaseState.FAILED, "sandbox vanished")
            raise SandboxOpError(f"sandbox {lease['sandbox_id']} not found") from e
        except FileNotFoundError:
            raise
        except Exception as e:  # noqa: BLE001
            raise SandboxOpError(f"{type(e).__name__}: {e}") from e

    async def run_code(self, lease_id: str, code: str, *, language: Optional[str], timeout_s: float) -> CodeResult:
        lease, sb_timeout = await self._use(lease_id)
        return await self._guard(
            lease,
            self.provider.run_code(
                lease["sandbox_id"], code, language=language, timeout_s=timeout_s, sandbox_timeout_s=sb_timeout
            ),
        )

    async def run_command(
        self, lease_id: str, cmd: str, *, cwd: Optional[str], envs: Optional[dict], timeout_s: float
    ) -> CommandResult:
        lease, sb_timeout = await self._use(lease_id)
        return await self._guard(
            lease,
            self.provider.run_command(
                lease["sandbox_id"], cmd, cwd=cwd, envs=envs, timeout_s=timeout_s, sandbox_timeout_s=sb_timeout
            ),
        )

    async def write_file(self, lease_id: str, path: str, data: bytes) -> None:
        lease, sb_timeout = await self._use(lease_id)
        await self._guard(lease, self.provider.write_file(lease["sandbox_id"], path, data, sandbox_timeout_s=sb_timeout))

    async def read_file(self, lease_id: str, path: str) -> bytes:
        lease, sb_timeout = await self._use(lease_id)
        return await self._guard(lease, self.provider.read_file(lease["sandbox_id"], path, sandbox_timeout_s=sb_timeout))
