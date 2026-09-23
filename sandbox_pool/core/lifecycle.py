"""分配器和维护器共用的沙箱生命周期操作。"""

import asyncio
import logging
import time
from typing import Callable, Optional

from sandbox_pool.config import PoolConfig
from sandbox_pool.models import LeaseState, SandboxState
from sandbox_pool.provider.base import SandboxProvider
from sandbox_pool.store.repository import Store

log = logging.getLogger(__name__)


class Lifecycle:
    def __init__(self, cfg: PoolConfig, store: Store, provider: SandboxProvider, replica_id: str):
        self.cfg = cfg
        self.store = store
        self.provider = provider
        self.replica_id = replica_id
        self._tasks: set[asyncio.Task] = set()
        # 有容量释放或新沙箱就绪时回调（用于唤醒本副本的维护循环）
        self.on_change: Callable[[], None] = lambda: None

    @staticmethod
    def now() -> float:
        return time.time()

    def spawn(self, coro) -> asyncio.Task:
        task = asyncio.create_task(coro)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        return task

    async def cancel_background(self) -> None:
        for t in list(self._tasks):
            t.cancel()
        if self._tasks:
            await asyncio.gather(*self._tasks, return_exceptions=True)

    async def wait_background(self) -> None:
        while self._tasks:
            await asyncio.gather(*list(self._tasks), return_exceptions=True)

    async def event(self, kind: str, **kw) -> None:
        try:
            await self.store.add_event(kind, now=self.now(), replica=self.replica_id, **kw)
        except Exception:  # noqa: BLE001 - 审计失败不影响主流程
            log.exception("record event %s failed", kind)

    async def destroy(self, row: dict, reason: str, *, background: bool = False) -> bool:
        """把沙箱从当前状态 CAS 到 DESTROYING，然后销毁并删除记录。"""
        now = self.now()
        ok = await self.store.cas_sandbox(
            row["id"],
            [row["state"]],
            now=now,
            expect_version=row["version"],
            state=SandboxState.DESTROYING,
            op_owner=self.replica_id,
            op_deadline=now + self.cfg.op_timeout_s,
            error=reason[:500],
        )
        if not ok:
            return False
        coro = self.finish_destroy(row["id"], row.get("provider_id"), reason)
        if background:
            self.spawn(coro)
        else:
            await coro
        return True

    async def finish_destroy(self, row_id: str, provider_id: Optional[str], reason: str) -> None:
        t0 = time.perf_counter()
        if provider_id:
            try:
                await self.provider.kill(provider_id)
            except Exception:  # noqa: BLE001
                # 保留 DESTROYING 记录，op_deadline 过后由维护循环重试
                log.exception("kill %s failed, will retry", provider_id)
                return
        await self.store.delete_sandbox(row_id, expect_owner=self.replica_id)
        await self.event(
            "destroy", sandbox_row_id=row_id, duration_ms=(time.perf_counter() - t0) * 1000, detail=reason
        )
        log.info("destroyed sandbox row=%s provider=%s reason=%s", row_id, provider_id, reason)
        self.on_change()

    async def end_lease(self, lease_id: str, state: LeaseState, reason: str) -> bool:
        """结束借用（归还 / 过期 / 失败），并销毁对应沙箱。"""
        lease = await self.store.get_lease(lease_id)
        if lease is None or lease["state"] != LeaseState.ACTIVE.value:
            return False
        now = self.now()
        if not await self.store.cas_lease(lease_id, [LeaseState.ACTIVE], state=state, ended_at=now):
            return False
        now = self.now()
        ok = await self.store.cas_sandbox(
            lease["sandbox_row_id"],
            [SandboxState.LEASED],
            now=now,
            expect_lease=lease_id,
            state=SandboxState.DESTROYING,
            op_owner=self.replica_id,
            op_deadline=now + self.cfg.op_timeout_s,
            error=reason[:500],
        )
        if ok:
            self.spawn(self.finish_destroy(lease["sandbox_row_id"], lease["sandbox_id"], reason))
        await self.event(
            "lease_" + state.value.lower(),
            sandbox_row_id=lease["sandbox_row_id"],
            lease_id=lease_id,
            duration_ms=(now - lease["created_at"]) * 1000,
            detail=reason,
        )
        return True
