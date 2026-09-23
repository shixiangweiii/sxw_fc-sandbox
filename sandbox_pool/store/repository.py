"""数据访问层。

多副本安全的关键约定：
- 状态变更一律是带 expect 条件的 UPDATE（CAS），影响行数为 1 才算成功；
- 需要「先数再写」的操作（占容量、入队）先更新池级锁行，把并发写串行化。
  SQLite 下事务本身就是 BEGIN IMMEDIATE；Postgres 下 UPDATE 会拿行锁，语义一致。
"""

import asyncio
import uuid
from contextlib import asynccontextmanager
from typing import Any, Iterable, Optional

from sqlalchemy import and_, delete, func, insert, select, update
from sqlalchemy.exc import IntegrityError, OperationalError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from sandbox_pool.models import SandboxState, WaiterState
from sandbox_pool.store.schema import events, leases, metadata, pool_kv, sandboxes, waiters

_LOCK_KEY = "lock"


def _values(states: Iterable) -> list:
    return [getattr(s, "value", s) for s in states]


class Store:
    def __init__(self, engine: AsyncEngine, pool: str):
        self.engine = engine
        self.pool = pool

    # ---------- 基础设施 ----------

    @asynccontextmanager
    async def tx(self):
        """写事务，遇到数据库锁冲突时整体重试由调用方的 _retry 负责。"""
        async with self.engine.begin() as conn:
            yield conn

    async def _retry(self, fn, attempts: int = 5):
        for i in range(attempts):
            try:
                async with self.tx() as conn:
                    return await fn(conn)
            except OperationalError as e:
                msg = str(e).lower()
                if i == attempts - 1 or ("locked" not in msg and "busy" not in msg):
                    raise
                await asyncio.sleep(0.05 * (2**i))

    async def _lock(self, conn: AsyncConnection) -> None:
        await conn.execute(
            update(pool_kv)
            .where(and_(pool_kv.c.pool == self.pool, pool_kv.c.key == _LOCK_KEY))
            .values(value=pool_kv.c.value + 1)
        )

    async def init_schema(self) -> None:
        async with self.engine.begin() as conn:
            await conn.run_sync(metadata.create_all)
        try:
            async with self.tx() as conn:
                await conn.execute(insert(pool_kv).values(pool=self.pool, key=_LOCK_KEY, value=0))
        except IntegrityError:
            pass

    # ---------- sandboxes ----------

    async def reserve_slot(
        self, *, template: str, now: float, owner: str, op_deadline: float, limit: int
    ) -> Optional[dict]:
        """总数 < limit 时插入一条 CREATING 记录占住名额。"""

        async def fn(conn):
            await self._lock(conn)
            total = (
                await conn.execute(
                    select(func.count()).select_from(sandboxes).where(sandboxes.c.pool == self.pool)
                )
            ).scalar_one()
            if total >= limit:
                return None
            row = dict(
                id=str(uuid.uuid4()),
                pool=self.pool,
                provider_id=None,
                template=template,
                state=SandboxState.CREATING.value,
                version=0,
                lease_id=None,
                created_at=now,
                state_changed_at=now,
                last_active_at=now,
                op_owner=owner,
                op_deadline=op_deadline,
                error=None,
            )
            await conn.execute(insert(sandboxes).values(**row))
            return row

        return await self._retry(fn)

    async def get_sandbox(self, row_id: str) -> Optional[dict]:
        async with self.engine.connect() as conn:
            r = (await conn.execute(select(sandboxes).where(sandboxes.c.id == row_id))).mappings().first()
            return dict(r) if r else None

    async def list_sandboxes(self, states: Optional[Iterable] = None, order_by=None) -> list[dict]:
        q = select(sandboxes).where(sandboxes.c.pool == self.pool)
        if states is not None:
            q = q.where(sandboxes.c.state.in_(_values(states)))
        if order_by is not None:
            q = q.order_by(order_by)
        async with self.engine.connect() as conn:
            return [dict(r) for r in (await conn.execute(q)).mappings().all()]

    async def count_by_state(self) -> dict[str, int]:
        q = (
            select(sandboxes.c.state, func.count())
            .where(sandboxes.c.pool == self.pool)
            .group_by(sandboxes.c.state)
        )
        async with self.engine.connect() as conn:
            return {s: n for s, n in (await conn.execute(q)).all()}

    async def _cas_sandbox(
        self,
        conn: AsyncConnection,
        row_id: str,
        from_states: Iterable,
        *,
        now: float,
        expect_version: Optional[int] = None,
        expect_owner: Any = ...,
        expect_lease: Any = ...,
        **values,
    ) -> bool:
        cond = [sandboxes.c.id == row_id, sandboxes.c.state.in_(_values(from_states))]
        if expect_version is not None:
            cond.append(sandboxes.c.version == expect_version)
        if expect_owner is not ...:
            cond.append(sandboxes.c.op_owner == expect_owner)
        if expect_lease is not ...:
            cond.append(sandboxes.c.lease_id == expect_lease)
        if "state" in values:
            values["state"] = getattr(values["state"], "value", values["state"])
            values["state_changed_at"] = now
        values["version"] = sandboxes.c.version + 1
        res = await conn.execute(update(sandboxes).where(and_(*cond)).values(**values))
        return res.rowcount == 1

    async def cas_sandbox(self, row_id: str, from_states: Iterable, **kw) -> bool:
        return await self._retry(lambda conn: self._cas_sandbox(conn, row_id, from_states, **kw))

    async def delete_sandbox(self, row_id: str, *, expect_owner: str) -> bool:
        async def fn(conn):
            res = await conn.execute(
                delete(sandboxes).where(
                    and_(
                        sandboxes.c.id == row_id,
                        sandboxes.c.state == SandboxState.DESTROYING.value,
                        sandboxes.c.op_owner == expect_owner,
                    )
                )
            )
            return res.rowcount == 1

        return await self._retry(fn)

    async def touch_sandbox(self, row_id: str, now: float) -> None:
        async def fn(conn):
            await conn.execute(update(sandboxes).where(sandboxes.c.id == row_id).values(last_active_at=now))

        await self._retry(fn)

    async def claim_ready(self, row: dict, *, lease: dict, now: float) -> bool:
        """READY → LEASED，同一事务内写入借用记录。"""

        async def fn(conn):
            ok = await self._cas_sandbox(
                conn,
                row["id"],
                [SandboxState.READY],
                now=now,
                expect_version=row["version"],
                state=SandboxState.LEASED,
                lease_id=lease["id"],
                last_active_at=now,
                op_owner=None,
                op_deadline=None,
            )
            if ok:
                await conn.execute(insert(leases).values(**lease))
            return ok

        return await self._retry(fn)

    async def finish_resume(self, row_id: str, *, owner: str, lease: dict, now: float) -> bool:
        """RESUMING → LEASED，同一事务内写入借用记录。"""

        async def fn(conn):
            ok = await self._cas_sandbox(
                conn,
                row_id,
                [SandboxState.RESUMING],
                now=now,
                expect_owner=owner,
                state=SandboxState.LEASED,
                lease_id=lease["id"],
                last_active_at=now,
                op_owner=None,
                op_deadline=None,
            )
            if ok:
                await conn.execute(insert(leases).values(**lease))
            return ok

        return await self._retry(fn)

    # ---------- leases ----------

    async def get_lease(self, lease_id: str) -> Optional[dict]:
        async with self.engine.connect() as conn:
            r = (await conn.execute(select(leases).where(leases.c.id == lease_id))).mappings().first()
            return dict(r) if r else None

    async def cas_lease(self, lease_id: str, from_states: Iterable, *, expect_unexpired_at=None, **values) -> bool:
        cond = [leases.c.id == lease_id, leases.c.state.in_(_values(from_states))]
        if expect_unexpired_at is not None:
            cond.append(leases.c.expires_at > expect_unexpired_at)
        if "state" in values:
            values["state"] = getattr(values["state"], "value", values["state"])

        async def fn(conn):
            res = await conn.execute(update(leases).where(and_(*cond)).values(**values))
            return res.rowcount == 1

        return await self._retry(fn)

    async def list_leases(self, states: Iterable, expires_before: Optional[float] = None) -> list[dict]:
        q = select(leases).where(and_(leases.c.pool == self.pool, leases.c.state.in_(_values(states))))
        if expires_before is not None:
            q = q.where(leases.c.expires_at <= expires_before)
        async with self.engine.connect() as conn:
            return [dict(r) for r in (await conn.execute(q)).mappings().all()]

    async def lease_source_counts(self, limit: int = 1000) -> dict[str, int]:
        sub = (
            select(leases.c.source)
            .where(leases.c.pool == self.pool)
            .order_by(leases.c.created_at.desc())
            .limit(limit)
            .subquery()
        )
        async with self.engine.connect() as conn:
            rows = (await conn.execute(select(sub.c.source, func.count()).group_by(sub.c.source))).all()
            return {s: n for s, n in rows}

    # ---------- waiters ----------

    async def count_waiting(self) -> int:
        q = select(func.count()).select_from(waiters).where(
            and_(waiters.c.pool == self.pool, waiters.c.state == WaiterState.WAITING.value)
        )
        async with self.engine.connect() as conn:
            return (await conn.execute(q)).scalar_one()

    async def enqueue(self, *, owner: str, now: float, deadline: float, queue_max: int) -> Optional[int]:
        async def fn(conn):
            await self._lock(conn)
            n = (
                await conn.execute(
                    select(func.count())
                    .select_from(waiters)
                    .where(and_(waiters.c.pool == self.pool, waiters.c.state == WaiterState.WAITING.value))
                )
            ).scalar_one()
            if n >= queue_max:
                return None
            res = await conn.execute(
                insert(waiters).values(
                    pool=self.pool,
                    state=WaiterState.WAITING.value,
                    owner_replica=owner,
                    created_at=now,
                    deadline=deadline,
                    heartbeat_at=now,
                )
            )
            return res.inserted_primary_key[0]

        return await self._retry(fn)

    async def heartbeat(self, seq: int, now: float) -> bool:
        async def fn(conn):
            res = await conn.execute(
                update(waiters)
                .where(and_(waiters.c.seq == seq, waiters.c.state == WaiterState.WAITING.value))
                .values(heartbeat_at=now)
            )
            return res.rowcount == 1

        return await self._retry(fn)

    async def head_seq(self) -> Optional[int]:
        q = select(func.min(waiters.c.seq)).where(
            and_(waiters.c.pool == self.pool, waiters.c.state == WaiterState.WAITING.value)
        )
        async with self.engine.connect() as conn:
            return (await conn.execute(q)).scalar_one()

    async def cas_waiter(self, seq: int, from_state: WaiterState, to_state: WaiterState, **values) -> bool:
        async def fn(conn):
            res = await conn.execute(
                update(waiters)
                .where(and_(waiters.c.seq == seq, waiters.c.state == from_state.value))
                .values(state=to_state.value, **values)
            )
            return res.rowcount == 1

        return await self._retry(fn)

    async def expire_waiters(self, *, now: float, heartbeat_timeout_s: float) -> int:
        async def fn(conn):
            res = await conn.execute(
                update(waiters)
                .where(
                    and_(
                        waiters.c.pool == self.pool,
                        waiters.c.state == WaiterState.WAITING.value,
                        (waiters.c.deadline <= now) | (waiters.c.heartbeat_at <= now - heartbeat_timeout_s),
                    )
                )
                .values(state=WaiterState.TIMEOUT.value)
            )
            return res.rowcount

        return await self._retry(fn)

    # ---------- kv ----------

    async def kv_get(self, key: str, default: float = 0) -> float:
        q = select(pool_kv.c.value).where(and_(pool_kv.c.pool == self.pool, pool_kv.c.key == key))
        async with self.engine.connect() as conn:
            v = (await conn.execute(q)).scalar_one_or_none()
            return default if v is None else v

    async def kv_set(self, key: str, value: float) -> None:
        async def fn(conn):
            res = await conn.execute(
                update(pool_kv).where(and_(pool_kv.c.pool == self.pool, pool_kv.c.key == key)).values(value=value)
            )
            if res.rowcount == 0:
                await conn.execute(insert(pool_kv).values(pool=self.pool, key=key, value=value))

        await self._retry(fn)

    async def kv_incr(self, key: str) -> float:
        async def fn(conn):
            res = await conn.execute(
                update(pool_kv)
                .where(and_(pool_kv.c.pool == self.pool, pool_kv.c.key == key))
                .values(value=pool_kv.c.value + 1)
            )
            if res.rowcount == 0:
                await conn.execute(insert(pool_kv).values(pool=self.pool, key=key, value=1))
            return (
                await conn.execute(
                    select(pool_kv.c.value).where(and_(pool_kv.c.pool == self.pool, pool_kv.c.key == key))
                )
            ).scalar_one()

        return await self._retry(fn)

    # ---------- events ----------

    async def add_event(
        self,
        kind: str,
        *,
        now: float,
        replica: str,
        sandbox_row_id: Optional[str] = None,
        lease_id: Optional[str] = None,
        duration_ms: Optional[float] = None,
        detail: Optional[str] = None,
    ) -> None:
        async def fn(conn):
            await conn.execute(
                insert(events).values(
                    ts=now,
                    pool=self.pool,
                    kind=kind,
                    sandbox_row_id=sandbox_row_id,
                    lease_id=lease_id,
                    replica=replica,
                    duration_ms=duration_ms,
                    detail=detail,
                )
            )

        await self._retry(fn)

    async def event_durations(self, limit_per_kind: int = 500) -> dict[str, list[float]]:
        q = (
            select(events.c.kind, events.c.duration_ms)
            .where(and_(events.c.pool == self.pool, events.c.duration_ms.is_not(None)))
            .order_by(events.c.id.desc())
            .limit(limit_per_kind * 10)
        )
        out: dict[str, list[float]] = {}
        async with self.engine.connect() as conn:
            for kind, ms in (await conn.execute(q)).all():
                bucket = out.setdefault(kind, [])
                if len(bucket) < limit_per_kind:
                    bucket.append(ms)
        return out

    async def event_counts(self) -> dict[str, int]:
        q = select(events.c.kind, func.count()).where(events.c.pool == self.pool).group_by(events.c.kind)
        async with self.engine.connect() as conn:
            return {k: n for k, n in (await conn.execute(q)).all()}
