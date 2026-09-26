"""agent 子系统的数据访问：agent、agent 沙箱、任务、定时任务。沿用 Store 的约定（见 repository.py）：

- 状态变更一律 CAS；
- 「先查再写」（建 agent、给 agent 占沙箱名额、任务准入）在同一事务内先更新池级锁行再查询，多副本串行化；
- 正常路径上不执行预期会失败的语句（不靠主键 / 唯一约束冲突判断「已存在」）。
"""

import json
import uuid
from typing import Iterable, Optional

from sqlalchemy import and_, case, delete, func, insert, or_, select, update

from sandbox_pool.models import SandboxState, TaskState
from sandbox_pool.store.repository import Store, _values
from sandbox_pool.store.schema import agents, sandboxes, schedules, tasks

# 一个 agent 同一时刻只能有一个「在建或在服务」的沙箱；RETIRING 只跑完手头的任务
BOUND_STATES = (SandboxState.CREATING, SandboxState.WARMING, SandboxState.ACTIVE)
# 在服务、可以运行任务的沙箱
_SERVING = (SandboxState.ACTIVE, SandboxState.RETIRING)
_SERVING_VALUES = _values(_SERVING)


def _agent_row(r) -> dict:
    d = dict(r)
    d["settings"] = json.loads(d["settings"] or "{}")
    d["egress"] = json.loads(d["egress"]) if d.get("egress") else None
    return d


def _task_row(r) -> dict:
    d = dict(r)
    d["usage"] = json.loads(d["usage"]) if d.get("usage") else None
    return d


class AgentStore(Store):
    # ---------- agents ----------

    async def get_agent(self, client_id: str, user_id: str) -> Optional[dict]:
        q = select(agents).where(
            and_(agents.c.pool == self.pool, agents.c.client_id == client_id, agents.c.user_id == user_id)
        )
        async with self._read() as conn:
            r = (await conn.execute(q)).mappings().first()
            return _agent_row(r) if r else None

    async def get_agent_by_id(self, agent_id: str) -> Optional[dict]:
        async with self._read() as conn:
            r = (await conn.execute(select(agents).where(agents.c.id == agent_id))).mappings().first()
            return _agent_row(r) if r else None

    async def list_agents(self) -> list[dict]:
        q = select(agents).where(agents.c.pool == self.pool).order_by(agents.c.created_at)
        async with self._read() as conn:
            return [_agent_row(r) for r in (await conn.execute(q)).mappings().all()]

    async def ensure_agent(self, client_id: str, user_id: str, *, now: float, settings: dict) -> dict:
        found = await self.get_agent(client_id, user_id)
        if found:
            return found

        async def fn(conn):
            await self._lock(conn)
            r = (
                await conn.execute(
                    select(agents).where(
                        and_(agents.c.pool == self.pool, agents.c.client_id == client_id, agents.c.user_id == user_id)
                    )
                )
            ).mappings().first()
            if r:
                return _agent_row(r)
            row = dict(
                id=str(uuid.uuid4()),
                pool=self.pool,
                client_id=client_id,
                user_id=user_id,
                settings=json.dumps(settings, ensure_ascii=False),
                settings_version=1,
                egress=None,
                created_at=now,
                updated_at=now,
            )
            await conn.execute(insert(agents).values(**row))
            return _agent_row(row)

        return await self._retry(fn)

    async def update_agent(
        self, agent_id: str, *, now: float, settings: Optional[dict] = None, egress: object = ...
    ) -> bool:
        """settings 变更时版本号加一（维护循环据此把新设置应用到沙箱）；egress 传 None 表示恢复默认策略。"""
        values: dict = {"updated_at": now}
        if settings is not None:
            values["settings"] = json.dumps(settings, ensure_ascii=False)
            values["settings_version"] = agents.c.settings_version + 1
        if egress is not ...:
            values["egress"] = None if egress is None else json.dumps(egress, ensure_ascii=False)

        async def fn(conn):
            res = await conn.execute(update(agents).where(agents.c.id == agent_id).values(**values))
            return res.rowcount == 1

        return await self._retry(fn)

    # ---------- agent 沙箱 ----------

    async def agent_sandboxes(self, agent_id: str, states: Optional[Iterable] = None) -> list[dict]:
        q = select(sandboxes).where(and_(sandboxes.c.pool == self.pool, sandboxes.c.agent_id == agent_id))
        if states is not None:
            q = q.where(sandboxes.c.state.in_(_values(states)))
        async with self._read() as conn:
            return [dict(r) for r in (await conn.execute(q.order_by(sandboxes.c.created_at))).mappings().all()]

    async def reserve_agent_sandbox(
        self, *, agent_id: str, template: str, now: float, owner: str, op_deadline: float, limit: int
    ) -> tuple[str, Optional[dict]]:
        """给 agent 占一个沙箱名额（插入 CREATING 记录）。

        返回 ("exists", 已有记录)：该 agent 已有在建或在服务的沙箱；("full", None)：池内沙箱总数已达上限；
        ("created", 新记录)。
        """

        async def fn(conn):
            await self._lock(conn)
            existing = (
                await conn.execute(
                    select(sandboxes).where(
                        and_(
                            sandboxes.c.pool == self.pool,
                            sandboxes.c.agent_id == agent_id,
                            sandboxes.c.state.in_(_values(BOUND_STATES)),
                        )
                    )
                )
            ).mappings().first()
            if existing:
                return "exists", dict(existing)
            total = (
                await conn.execute(select(func.count()).select_from(sandboxes).where(sandboxes.c.pool == self.pool))
            ).scalar_one()
            if total >= limit:
                return "full", None
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
                agent_id=agent_id,
                health_failures=0,
                activity_kind="message",
            )
            await conn.execute(insert(sandboxes).values(**row))
            return "created", row

        return await self._retry(fn)

    async def activate_sandbox(self, row_id: str, agent_id: str, *, owner: str, now: float, **values) -> bool:
        """WARMING → ACTIVE。同一事务内把该 agent 其他 ACTIVE 记录转为 RETIRING（正常不会有，防御性处理）。"""

        async def fn(conn):
            ok = await self._cas_sandbox(
                conn,
                row_id,
                [SandboxState.WARMING],
                now=now,
                expect_owner=owner,
                state=SandboxState.ACTIVE,
                op_owner=None,
                op_deadline=None,
                last_active_at=now,
                **values,
            )
            if ok:
                await conn.execute(
                    update(sandboxes)
                    .where(
                        and_(
                            sandboxes.c.pool == self.pool,
                            sandboxes.c.agent_id == agent_id,
                            sandboxes.c.id != row_id,
                            sandboxes.c.state == SandboxState.ACTIVE.value,
                        )
                    )
                    .values(
                        state=SandboxState.RETIRING.value,
                        state_changed_at=now,
                        version=sandboxes.c.version + 1,
                    )
                )
            return ok

        return await self._retry(fn)

    async def touch_sandbox(self, row_id: str, *, now: float, kind: str) -> bool:
        """记录活动时间（空闲销毁据此判断），返回沙箱是否仍在服务（ACTIVE / RETIRING）。

        同时版本号加一：维护循环按旧快照做的空闲销毁 / 轮换 CAS 会失败，下一轮重新判断，
        不会销毁刚刚接了新任务的沙箱。
        """

        async def fn(conn):
            res = await conn.execute(
                update(sandboxes)
                .where(
                    and_(
                        sandboxes.c.id == row_id,
                        sandboxes.c.state.in_([SandboxState.ACTIVE.value, SandboxState.RETIRING.value]),
                    )
                )
                .values(
                    last_active_at=case((sandboxes.c.last_active_at < now, now), else_=sandboxes.c.last_active_at),
                    activity_kind=kind,
                    version=sandboxes.c.version + 1,
                )
            )
            return res.rowcount == 1

        return await self._retry(fn)

    # ---------- tasks ----------

    async def create_task(self, task: dict, *, max_running: int) -> tuple[str, Optional[dict]]:
        """任务准入，与重载沙箱配置（begin_reload）用同一把池级锁互斥。依次检查：

        - 沙箱不在服务（ACTIVE / RETIRING）→ ("gone", None)；
        - 指定的会话有运行中的任务 → ("session_busy", None)；该 agent 运行中任务数 >= max_running → ("too_many", None)；
        - 沙箱正在重载配置（op_owner 未过期）→ ("reloading", None)，调用方稍后重试；
        - 否则插入任务，同一事务内记录沙箱活动时间并把版本号加一：维护循环按旧快照做的空闲销毁 / 轮换 CAS 会失败，
          不会销毁刚接了新任务的沙箱。返回 ("ok", 任务)。
        """
        now = task["created_at"]

        async def fn(conn):
            await self._lock(conn)
            sb = (
                await conn.execute(
                    select(sandboxes.c.state, sandboxes.c.op_owner, sandboxes.c.op_deadline).where(
                        sandboxes.c.id == task["sandbox_row_id"]
                    )
                )
            ).first()
            if sb is None or sb.state not in _SERVING_VALUES:
                return "gone", None
            running = and_(
                tasks.c.pool == self.pool, tasks.c.agent_id == task["agent_id"], tasks.c.state == TaskState.RUNNING.value
            )
            if task.get("session_id"):
                busy = (
                    await conn.execute(
                        select(func.count()).select_from(tasks).where(and_(running, tasks.c.session_id == task["session_id"]))
                    )
                ).scalar_one()
                if busy:
                    return "session_busy", None
            n = (await conn.execute(select(func.count()).select_from(tasks).where(running))).scalar_one()
            if n >= max_running:
                return "too_many", None
            if sb.op_owner is not None and (sb.op_deadline or 0) > now:
                return "reloading", None
            row = {**task, "pool": self.pool, "state": TaskState.RUNNING.value, "abort_requested": 0}
            if isinstance(row.get("usage"), dict):
                row["usage"] = json.dumps(row["usage"])
            await conn.execute(insert(tasks).values(**row))
            await conn.execute(
                update(sandboxes)
                .where(sandboxes.c.id == task["sandbox_row_id"])
                .values(
                    last_active_at=case((sandboxes.c.last_active_at < now, now), else_=sandboxes.c.last_active_at),
                    activity_kind=task["source"],
                    version=sandboxes.c.version + 1,
                )
            )
            return "ok", _task_row(row)

        return await self._retry(fn)

    async def begin_reload(self, row_id: str, *, owner: str, now: float, op_deadline: float) -> bool:
        """开始重载 ACTIVE 沙箱的 opencode 配置（dispose 会中止沙箱里运行中的会话）。

        池级锁内确认该沙箱没有运行中任务、没有其他副本在重载，再占住 op_owner / op_deadline（在服务的沙箱平时这两列为空）；
        占住期间 create_task 返回 reloading。执行者崩溃时占用到 op_deadline 自动失效。
        """

        async def fn(conn):
            await self._lock(conn)
            busy = (
                await conn.execute(
                    select(func.count())
                    .select_from(tasks)
                    .where(and_(tasks.c.sandbox_row_id == row_id, tasks.c.state == TaskState.RUNNING.value))
                )
            ).scalar_one()
            if busy:
                return False
            res = await conn.execute(
                update(sandboxes)
                .where(
                    and_(
                        sandboxes.c.id == row_id,
                        sandboxes.c.state == SandboxState.ACTIVE.value,
                        or_(sandboxes.c.op_owner.is_(None), sandboxes.c.op_deadline < now),
                    )
                )
                .values(op_owner=owner, op_deadline=op_deadline, version=sandboxes.c.version + 1)
            )
            return res.rowcount == 1

        return await self._retry(fn)

    async def end_reload(self, row_id: str, *, owner: str, now: float, **values) -> bool:
        """结束重载，释放占用；values 为要一并写入的列（成功时的 config_version）。沙箱已被销毁时 CAS 失败。"""
        return await self.cas_sandbox(
            row_id, _SERVING, now=now, expect_owner=owner, op_owner=None, op_deadline=None, **values
        )

    async def get_task(self, task_id: str) -> Optional[dict]:
        async with self._read() as conn:
            r = (await conn.execute(select(tasks).where(tasks.c.id == task_id))).mappings().first()
            return _task_row(r) if r else None

    async def list_tasks(
        self,
        agent_id: str,
        *,
        source: Optional[str] = None,
        schedule_id: Optional[str] = None,
        state: Optional[str] = None,
        since: Optional[float] = None,
        limit: int = 50,
    ) -> list[dict]:
        cond = [tasks.c.pool == self.pool, tasks.c.agent_id == agent_id]
        if source:
            cond.append(tasks.c.source == source)
        if schedule_id:
            cond.append(tasks.c.schedule_id == schedule_id)
        if state:
            cond.append(tasks.c.state == state)
        if since is not None:
            cond.append(tasks.c.created_at >= since)
        q = select(tasks).where(and_(*cond)).order_by(tasks.c.created_at.desc()).limit(limit)
        async with self._read() as conn:
            return [_task_row(r) for r in (await conn.execute(q)).mappings().all()]

    async def cas_task(
        self,
        task_id: str,
        from_states: Iterable,
        *,
        expect_owner: object = ...,
        expect_stale_before: Optional[float] = None,
        **values,
    ) -> bool:
        """expect_stale_before：要求负责副本的心跳截止时间早于该时刻（接管用，读到过期后原副本又续上心跳时不抢）。"""
        cond = [tasks.c.id == task_id, tasks.c.state.in_(_values(from_states))]
        if expect_owner is not ...:
            cond.append(tasks.c.op_owner == expect_owner)
        if expect_stale_before is not None:
            cond.append(tasks.c.op_deadline < expect_stale_before)
        if "state" in values:
            values["state"] = getattr(values["state"], "value", values["state"])
        if isinstance(values.get("usage"), dict):
            values["usage"] = json.dumps(values["usage"])

        async def fn(conn):
            res = await conn.execute(update(tasks).where(and_(*cond)).values(**values))
            return res.rowcount == 1

        return await self._retry(fn)

    async def session_sandbox(self, agent_id: str, session_id: str) -> Optional[str]:
        """会话所在的沙箱（会话存在沙箱内的 opencode 里，只能在原沙箱继续）。"""
        q = (
            select(tasks.c.sandbox_row_id)
            .where(and_(tasks.c.pool == self.pool, tasks.c.agent_id == agent_id, tasks.c.session_id == session_id))
            .order_by(tasks.c.created_at.desc())
            .limit(1)
        )
        async with self._read() as conn:
            return (await conn.execute(q)).scalar_one_or_none()

    async def running_tasks(
        self,
        *,
        sandbox_row_id: Optional[str] = None,
        agent_id: Optional[str] = None,
        schedule_id: Optional[str] = None,
        stale_before: Optional[float] = None,
    ) -> list[dict]:
        cond = [tasks.c.pool == self.pool, tasks.c.state == TaskState.RUNNING.value]
        if sandbox_row_id:
            cond.append(tasks.c.sandbox_row_id == sandbox_row_id)
        if agent_id:
            cond.append(tasks.c.agent_id == agent_id)
        if schedule_id:
            cond.append(tasks.c.schedule_id == schedule_id)
        if stale_before is not None:
            cond.append(tasks.c.op_deadline < stale_before)
        async with self._read() as conn:
            return [_task_row(r) for r in (await conn.execute(select(tasks).where(and_(*cond)))).mappings().all()]

    # ---------- schedules ----------

    async def insert_schedule(self, row: dict) -> None:
        async def fn(conn):
            await conn.execute(insert(schedules).values(**{**row, "pool": self.pool}))

        await self._retry(fn)

    async def get_schedule(self, schedule_id: str) -> Optional[dict]:
        async with self._read() as conn:
            r = (await conn.execute(select(schedules).where(schedules.c.id == schedule_id))).mappings().first()
            return dict(r) if r else None

    async def list_schedules(self, agent_id: Optional[str] = None) -> list[dict]:
        q = select(schedules).where(schedules.c.pool == self.pool)
        if agent_id:
            q = q.where(schedules.c.agent_id == agent_id)
        async with self._read() as conn:
            return [dict(r) for r in (await conn.execute(q.order_by(schedules.c.created_at))).mappings().all()]

    async def update_schedule(self, schedule_id: str, **values) -> bool:
        async def fn(conn):
            res = await conn.execute(update(schedules).where(schedules.c.id == schedule_id).values(**values))
            return res.rowcount == 1

        return await self._retry(fn)

    async def delete_schedule(self, schedule_id: str) -> bool:
        async def fn(conn):
            res = await conn.execute(delete(schedules).where(schedules.c.id == schedule_id))
            return res.rowcount == 1

        return await self._retry(fn)

    async def due_schedules(self, now: float) -> list[dict]:
        q = select(schedules).where(
            and_(
                schedules.c.pool == self.pool,
                schedules.c.enabled == 1,
                schedules.c.next_run_at.is_not(None),
                schedules.c.next_run_at <= now,
            )
        )
        async with self._read() as conn:
            return [dict(r) for r in (await conn.execute(q)).mappings().all()]

    async def claim_schedule(self, schedule_id: str, *, expect_next: float, next_run_at: float, now: float) -> bool:
        """把本次触发推进到下一次；CAS 成功的副本负责触发（多副本下每次只触发一次）。"""

        async def fn(conn):
            res = await conn.execute(
                update(schedules)
                .where(
                    and_(
                        schedules.c.id == schedule_id,
                        schedules.c.enabled == 1,
                        schedules.c.next_run_at == expect_next,
                    )
                )
                .values(next_run_at=next_run_at, last_run_at=now, updated_at=now)
            )
            return res.rowcount == 1

        return await self._retry(fn)

    # ---------- 历史清理 ----------

    async def purge_tasks(self, *, before: float, batch: int = 500) -> int:
        cond = and_(tasks.c.pool == self.pool, tasks.c.state != TaskState.RUNNING.value, tasks.c.finished_at < before)
        total = 0
        while True:

            async def fn(conn):
                res = await conn.execute(delete(tasks).where(tasks.c.id.in_(select(tasks.c.id).where(cond).limit(batch))))
                return res.rowcount

            n = await self._retry(fn)
            total += n
            if n < batch:
                return total
