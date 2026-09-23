"""数据访问层。

多副本安全的关键约定：
- 状态变更一律是带 expect 条件的 UPDATE（CAS），影响行数为 1 才算成功；
- 需要「先数再写」的操作（占容量、入队、暂停前检查 min_hot）先更新池级锁行，把并发写串行化。
  SQLite 下写事务本身就是 BEGIN IMMEDIATE；Postgres 下 UPDATE 会拿行锁，语义一致；
- 只读查询走只读连接（SQLite 下为普通 BEGIN，不拿写锁），不和写操作串行。
"""

import asyncio
import uuid
from contextlib import asynccontextmanager
from typing import Any, Iterable, Optional

from sqlalchemy import and_, delete, func, insert, inspect, select, update
from sqlalchemy.exc import IntegrityError, OperationalError, ProgrammingError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from sandbox_pool.models import LeaseState, SandboxState, WaiterState
from sandbox_pool.store.db import create_engine, create_read_engine
from sandbox_pool.store.schema import events, leases, metadata, pool_kv, sandboxes, waiters

# pool_kv 中的键
KV_LOCK = "lock"
KV_CREATE_FAIL_COUNT = "create_fail_count"
KV_CREATE_BLOCK_UNTIL = "create_block_until"
KV_RECONCILE_AT = "reconcile_at"
KV_CLEANUP_AT = "cleanup_at"
KV_DRAINING = "draining"
# 初始化时预先插入，之后只做 UPDATE，避免多副本并发首次插入冲突
_KV_KEYS = (KV_LOCK, KV_CREATE_FAIL_COUNT, KV_CREATE_BLOCK_UNTIL, KV_RECONCILE_AT, KV_CLEANUP_AT, KV_DRAINING)

# 可直接分配的沙箱；RESUMING 是排队请求正在恢复、尚未交付的沙箱（交付与排队记录改为 GRANTED 在同一事务内），
# 计入可用数，排在后面的请求才能算对自己前面还有多少个沙箱
_AVAILABLE_STATES = (SandboxState.READY.value, SandboxState.PAUSED.value, SandboxState.RESUMING.value)


def _values(states: Iterable) -> list:
    return [getattr(s, "value", s) for s in states]


def _migrate(sync_conn) -> None:
    """给已有的表补上新增的可空列和索引。本地 demo 用的轻量迁移，生产环境建议用 Alembic。"""
    insp = inspect(sync_conn)
    quote = sync_conn.dialect.identifier_preparer.quote
    for table in metadata.sorted_tables:
        existing = {c["name"] for c in insp.get_columns(table.name)}
        for col in table.columns:
            if col.name in existing:
                continue
            if col.primary_key or not col.nullable:
                raise RuntimeError(f"table {table.name} lacks NOT NULL column {col.name}; migrate it manually")
            sync_conn.exec_driver_sql(
                f"ALTER TABLE {quote(table.name)} ADD COLUMN {quote(col.name)} "
                f"{col.type.compile(dialect=sync_conn.dialect)}"
            )
        indexes = {ix["name"] for ix in insp.get_indexes(table.name)}
        for ix in table.indexes:
            if ix.name not in indexes:
                ix.create(sync_conn)


class Store:
    def __init__(self, engine: AsyncEngine, pool: str, *, read_engine: Optional[AsyncEngine] = None):
        self.engine = engine
        self.read_engine = read_engine or engine
        self.pool = pool

    @classmethod
    def open(cls, db_url: str, pool: str) -> "Store":
        return cls(create_engine(db_url), pool, read_engine=create_read_engine(db_url))

    async def close(self) -> None:
        if self.read_engine is not self.engine:
            await self.read_engine.dispose()
        await self.engine.dispose()

    # ---------- 基础设施 ----------

    @asynccontextmanager
    async def tx(self):
        """写事务，遇到数据库锁冲突时整体重试由调用方的 _retry 负责。"""
        async with self.engine.begin() as conn:
            yield conn

    def _read(self):
        """只读连接。"""
        return self.read_engine.connect()

    async def _retry(self, fn, attempts: int = 5, *, integrity_retry: bool = False):
        """锁冲突时整体重试写事务。integrity_retry：并发插入同一主键时重试（重试时会走 UPDATE 分支）。"""
        for i in range(attempts):
            try:
                async with self.tx() as conn:
                    return await fn(conn)
            except IntegrityError:
                if not integrity_retry or i == attempts - 1:
                    raise
            except OperationalError as e:
                msg = str(e).lower()
                if i == attempts - 1 or ("locked" not in msg and "busy" not in msg):
                    raise
            await asyncio.sleep(0.05 * (2**i))

    async def _lock(self, conn: AsyncConnection) -> None:
        await conn.execute(
            update(pool_kv)
            .where(and_(pool_kv.c.pool == self.pool, pool_kv.c.key == KV_LOCK))
            .values(value=pool_kv.c.value + 1)
        )

    async def init_schema(self) -> None:
        for attempt in range(3):
            try:
                async with self.engine.begin() as conn:
                    await conn.run_sync(metadata.create_all)
                    await conn.run_sync(_migrate)
                break
            except (IntegrityError, OperationalError, ProgrammingError):
                # 多副本同时启动（Postgres）时可能并发建表 / 加列，重试即可看到对方的结果
                if attempt == 2:
                    raise
                await asyncio.sleep(0.2 * (attempt + 1))
        # 先查已有的键，只插入缺失的，不靠 IntegrityError 判断「已存在」。
        # 执行失败的语句会留下未关闭的 sqlite3 游标，之后可能被主线程的 GC 回收：回收时要拿该连接的
        # SQLite 互斥锁，而该连接的工作线程可能正在 BEGIN IMMEDIATE 的忙等里持有它，事件循环会被卡住，
        # 同一进程内持有写锁的协程因此无法提交，形成死锁直到 busy_timeout（单元测试里两个池同进程时实测）。
        async def fn(conn):
            existing = set(
                (await conn.execute(select(pool_kv.c.key).where(pool_kv.c.pool == self.pool))).scalars()
            )
            for key in _KV_KEYS:
                if key not in existing:
                    await conn.execute(insert(pool_kv).values(pool=self.pool, key=key, value=0))

        await self._retry(fn, integrity_retry=True)

    # ---------- sandboxes ----------

    async def reserve_slot(
        self, *, template: str, now: float, owner: str, op_deadline: float, limit: int
    ) -> Optional[dict]:
        """总数 < limit 时插入一条 CREATING 记录占住名额。排空中或创建熔断期间不占。"""

        async def fn(conn):
            await self._lock(conn)
            if await self._kv_in_tx(conn, KV_DRAINING) > 0 or await self._kv_in_tx(conn, KV_CREATE_BLOCK_UNTIL) > now:
                return None
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
                platform_deadline=None,
            )
            await conn.execute(insert(sandboxes).values(**row))
            return row

        return await self._retry(fn)

    async def get_sandbox(self, row_id: str) -> Optional[dict]:
        async with self._read() as conn:
            r = (await conn.execute(select(sandboxes).where(sandboxes.c.id == row_id))).mappings().first()
            return dict(r) if r else None

    async def list_sandboxes(self, states: Optional[Iterable] = None, order_by=None) -> list[dict]:
        q = select(sandboxes).where(sandboxes.c.pool == self.pool)
        if states is not None:
            q = q.where(sandboxes.c.state.in_(_values(states)))
        if order_by is not None:
            q = q.order_by(order_by)
        async with self._read() as conn:
            return [dict(r) for r in (await conn.execute(q)).mappings().all()]

    async def count_by_state(self) -> dict[str, int]:
        q = (
            select(sandboxes.c.state, func.count())
            .where(sandboxes.c.pool == self.pool)
            .group_by(sandboxes.c.state)
        )
        async with self._read() as conn:
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

    async def _grant_waiter(self, conn: AsyncConnection, seq: int, lease_id: str) -> None:
        """排队记录改为 GRANTED。已被判超时（抢沙箱期间截止时间到了）的也改：沙箱照常交给请求方。"""
        await conn.execute(
            update(waiters)
            .where(
                and_(
                    waiters.c.seq == seq,
                    waiters.c.state.in_([WaiterState.WAITING.value, WaiterState.TIMEOUT.value]),
                )
            )
            .values(state=WaiterState.GRANTED.value, lease_id=lease_id)
        )

    async def claim_ready(self, row: dict, *, lease: dict, now: float, waiter_seq: Optional[int] = None) -> bool:
        """READY → LEASED，同一事务内写入借用记录、交付排队记录。"""

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
                platform_deadline=None,
            )
            if ok:
                await conn.execute(insert(leases).values(**lease))
                if waiter_seq is not None:
                    await self._grant_waiter(conn, waiter_seq, lease["id"])
            return ok

        return await self._retry(fn)

    async def finish_resume(
        self, row_id: str, *, owner: str, lease: dict, now: float, waiter_seq: Optional[int] = None
    ) -> bool:
        """RESUMING → LEASED，同一事务内写入借用记录、交付排队记录。"""

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
                platform_deadline=None,
            )
            if ok:
                await conn.execute(insert(leases).values(**lease))
                if waiter_seq is not None:
                    await self._grant_waiter(conn, waiter_seq, lease["id"])
            return ok

        return await self._retry(fn)

    async def start_pause(self, row: dict, *, now: float, owner: str, op_deadline: float, min_hot: int) -> bool:
        """READY → PAUSING。同一事务内先拿池级锁行再数 READY，多个副本同时暂停也不会低于 min_hot。"""

        async def fn(conn):
            await self._lock(conn)
            ready = (
                await conn.execute(
                    select(func.count())
                    .select_from(sandboxes)
                    .where(and_(sandboxes.c.pool == self.pool, sandboxes.c.state == SandboxState.READY.value))
                )
            ).scalar_one()
            if ready <= min_hot:
                return False
            return await self._cas_sandbox(
                conn,
                row["id"],
                [SandboxState.READY],
                now=now,
                expect_version=row["version"],
                state=SandboxState.PAUSING,
                op_owner=owner,
                op_deadline=op_deadline,
                platform_deadline=None,
            )

        return await self._retry(fn)

    # ---------- leases ----------

    async def get_lease(self, lease_id: str) -> Optional[dict]:
        async with self._read() as conn:
            r = (await conn.execute(select(leases).where(leases.c.id == lease_id))).mappings().first()
            return dict(r) if r else None

    async def cas_lease(
        self,
        lease_id: str,
        from_states: Iterable,
        *,
        expect_unexpired_at: Optional[float] = None,
        expect_expired_at: Optional[float] = None,
        **values,
    ) -> bool:
        """expect_unexpired_at：要求 expires_at > 该时间（续期）；expect_expired_at：要求 expires_at <= 该时间（过期回收）。"""
        cond = [leases.c.id == lease_id, leases.c.state.in_(_values(from_states))]
        if expect_unexpired_at is not None:
            cond.append(leases.c.expires_at > expect_unexpired_at)
        if expect_expired_at is not None:
            cond.append(leases.c.expires_at <= expect_expired_at)
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
        async with self._read() as conn:
            return [dict(r) for r in (await conn.execute(q)).mappings().all()]

    async def lease_source_counts(self, limit: int = 1000) -> dict[str, int]:
        sub = (
            select(leases.c.source)
            .where(leases.c.pool == self.pool)
            .order_by(leases.c.created_at.desc())
            .limit(limit)
            .subquery()
        )
        async with self._read() as conn:
            rows = (await conn.execute(select(sub.c.source, func.count()).group_by(sub.c.source))).all()
            return {s: n for s, n in rows}

    # ---------- waiters ----------

    def _waiting(self):
        return and_(waiters.c.pool == self.pool, waiters.c.state == WaiterState.WAITING.value)

    def _available(self):
        return and_(sandboxes.c.pool == self.pool, sandboxes.c.state.in_(_AVAILABLE_STATES))

    async def count_waiting(self) -> int:
        q = select(func.count()).select_from(waiters).where(self._waiting())
        async with self._read() as conn:
            return (await conn.execute(q)).scalar_one()

    async def enqueue(
        self, *, owner: str, now: float, deadline: float, queue_max: int, count_available: bool = False
    ) -> Optional[int]:
        """入队，排队数达到上限时拒绝。

        count_available（严格先来先服务模式）：所有请求都先入队，上限改为 queue_max + 可分配的沙箱数，
        马上就能拿到沙箱的请求不占排队名额。突发 16 个请求、5 个可用时仍是 5 个拿到、10 个排队、1 个被拒。
        """

        async def fn(conn):
            await self._lock(conn)
            n = (await conn.execute(select(func.count()).select_from(waiters).where(self._waiting()))).scalar_one()
            avail = 0
            if count_available:
                avail = (
                    await conn.execute(select(func.count()).select_from(sandboxes).where(self._available()))
                ).scalar_one()
            if n >= queue_max + avail:
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
        """刷新排队心跳，返回排队记录是否仍然有效。

        GRANTED 只会由请求自己抢到沙箱时写入：交付完成前、或抢到的沙箱不可用被改回 WAITING 前，仍算有效。
        """

        async def fn(conn):
            res = await conn.execute(
                update(waiters)
                .where(
                    and_(
                        waiters.c.seq == seq,
                        waiters.c.state.in_([WaiterState.WAITING.value, WaiterState.GRANTED.value]),
                    )
                )
                .values(heartbeat_at=now)
            )
            return res.rowcount == 1

        return await self._retry(fn)

    async def head_seq(self) -> Optional[int]:
        q = select(func.min(waiters.c.seq)).where(self._waiting())
        async with self._read() as conn:
            return (await conn.execute(q)).scalar_one()

    async def queue_position(self, seq: int) -> tuple[int, int]:
        """返回（排在自己前面的排队数，可直接分配的沙箱数）。前者小于后者时可以去抢。"""
        ahead = select(func.count()).select_from(waiters).where(and_(self._waiting(), waiters.c.seq < seq))
        avail = select(func.count()).select_from(sandboxes).where(self._available())
        async with self._read() as conn:
            row = (await conn.execute(select(ahead.scalar_subquery(), avail.scalar_subquery()))).one()
            return int(row[0]), int(row[1])

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
        cond = and_(
            self._waiting(),
            (waiters.c.deadline <= now) | (waiters.c.heartbeat_at <= now - heartbeat_timeout_s),
        )
        # 先用只读连接看有没有需要清理的，没有就不发写事务（每个副本每轮都会调用）
        async with self._read() as conn:
            if (await conn.execute(select(func.count()).select_from(waiters).where(cond))).scalar_one() == 0:
                return 0

        async def fn(conn):
            res = await conn.execute(update(waiters).where(cond).values(state=WaiterState.TIMEOUT.value))
            return res.rowcount

        return await self._retry(fn)

    # ---------- kv ----------

    async def _kv_in_tx(self, conn: AsyncConnection, key: str) -> float:
        q = select(pool_kv.c.value).where(and_(pool_kv.c.pool == self.pool, pool_kv.c.key == key))
        return (await conn.execute(q)).scalar_one_or_none() or 0

    async def kv_get(self, key: str, default: float = 0) -> float:
        q = select(pool_kv.c.value).where(and_(pool_kv.c.pool == self.pool, pool_kv.c.key == key))
        async with self._read() as conn:
            v = (await conn.execute(q)).scalar_one_or_none()
            return default if v is None else v

    async def kv_set(self, key: str, value: float) -> None:
        async def fn(conn):
            res = await conn.execute(
                update(pool_kv).where(and_(pool_kv.c.pool == self.pool, pool_kv.c.key == key)).values(value=value)
            )
            if res.rowcount == 0:
                await conn.execute(insert(pool_kv).values(pool=self.pool, key=key, value=value))

        await self._retry(fn, integrity_retry=True)

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

        return await self._retry(fn, integrity_retry=True)

    async def try_periodic(self, key: str, *, now: float, interval_s: float) -> bool:
        """全池周期任务的执行权：上次执行时间 <= now - interval 时 CAS 为 now，成功的副本执行本周期。"""

        async def fn(conn):
            res = await conn.execute(
                update(pool_kv)
                .where(
                    and_(
                        pool_kv.c.pool == self.pool,
                        pool_kv.c.key == key,
                        pool_kv.c.value <= now - interval_s,
                    )
                )
                .values(value=now)
            )
            return res.rowcount == 1

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
        async with self._read() as conn:
            for kind, ms in (await conn.execute(q)).all():
                bucket = out.setdefault(kind, [])
                if len(bucket) < limit_per_kind:
                    bucket.append(ms)
        return out

    async def event_counts(self) -> dict[str, int]:
        q = select(events.c.kind, func.count()).where(events.c.pool == self.pool).group_by(events.c.kind)
        async with self._read() as conn:
            return {k: n for k, n in (await conn.execute(q)).all()}

    # ---------- 历史清理 ----------

    async def purge_history(self, *, before: float, batch: int = 500) -> dict[str, int]:
        """分批删除 before 之前已结束的排队记录、借用记录和事件；进行中的记录不动。"""
        targets = [
            (
                waiters,
                waiters.c.seq,
                and_(
                    waiters.c.pool == self.pool,
                    waiters.c.state != WaiterState.WAITING.value,
                    waiters.c.created_at < before,
                ),
            ),
            (
                leases,
                leases.c.id,
                and_(
                    leases.c.pool == self.pool,
                    leases.c.state != LeaseState.ACTIVE.value,
                    leases.c.ended_at < before,
                ),
            ),
            (events, events.c.id, and_(events.c.pool == self.pool, events.c.ts < before)),
        ]
        out: dict[str, int] = {}
        for table, key, cond in targets:
            total = 0
            while True:

                async def fn(conn, table=table, key=key, cond=cond):
                    res = await conn.execute(delete(table).where(key.in_(select(key).where(cond).limit(batch))))
                    return res.rowcount

                n = await self._retry(fn)
                total += n
                if n < batch:
                    break
            out[table.name] = total
        return out
