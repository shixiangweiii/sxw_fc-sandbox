"""agent 子系统的后台维护循环。每个副本都运行，同一件事靠 CAS 保证只有一个副本做成。

每轮：
- 过渡态接管：CREATING / WARMING / DESTROYING 超过 op_deadline（执行者大概率已崩溃）→ 销毁；
- 任务接管：心跳过期的 RUNNING 任务 → CAS 成为负责副本，跟进到结束；
- 在服务的沙箱（ACTIVE / RETIRING）：硬截止、轮换、空闲销毁、健康检查、平台超时续期、出网策略与设置下发；
- 定时任务：到期的用 CAS 推进 next_run_at 后触发（多副本下每次只触发一次）；
- 全池周期任务：对账、历史清理（pool_kv 时间戳 CAS，每个周期一个副本）。

agent 沙箱不暂停（按 Eco 规则）。健康检查用 HTTP 访问 /global/health，不调用 connect()。
"""

import asyncio
import logging
import random
import time
from typing import TYPE_CHECKING, Optional

from sandbox_pool.models import SandboxState, TaskState
from sandbox_pool.store.repository import KV_CLEANUP_AT, KV_RECONCILE_AT

if TYPE_CHECKING:
    from sandbox_pool.agent.service import AgentService

log = logging.getLogger(__name__)

# 服务停机期间错过的定时触发不补跑：到期超过这个时长的只推进到下一次
_MISSED_GRACE_S = 300
_TRANSITIONAL = (SandboxState.CREATING, SandboxState.WARMING, SandboxState.DESTROYING)
_SERVING = (SandboxState.ACTIVE, SandboxState.RETIRING)


class AgentMaintainer:
    def __init__(self, svc: "AgentService"):
        self.svc = svc
        self.cfg = svc.cfg
        self.store = svc.store
        self._wake = asyncio.Event()
        self._task: Optional[asyncio.Task] = None
        self._stopping = False
        self._last_health: dict[str, float] = {}
        self._last_reconcile = 0.0
        self._last_cleanup = 0.0

    def kick(self) -> None:
        self._wake.set()

    async def start(self) -> None:
        if self._task is None:
            self._task = asyncio.create_task(self._run(), name="agent-maintainer")

    async def stop(self, grace_s: float = 30) -> None:
        """事件通知 + 等待：跑完当前一轮、等后台操作结束，超时才取消（不在数据库操作中途取消）。"""
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
            await asyncio.wait_for(self.svc.lc.wait_background(), timeout=grace_s)
        except asyncio.TimeoutError:
            await self.svc.lc.cancel_background()

    async def _run(self) -> None:
        while not self._stopping:
            try:
                await self.tick()
            except asyncio.CancelledError:
                raise
            except Exception:  # noqa: BLE001
                log.exception("agent maintainer tick failed")
            interval = self.cfg.maintain_interval_s * random.uniform(0.8, 1.2)
            try:
                await asyncio.wait_for(self._wake.wait(), timeout=interval)
            except asyncio.TimeoutError:
                pass
            self._wake.clear()

    async def tick(self) -> None:
        now = self.svc.now()
        await self.recover_stuck(now)
        await self.takeover_tasks(now)
        agents = {a["id"]: a for a in await self.store.list_agents()}
        rows = await self.store.list_sandboxes(_SERVING)
        for row in rows:
            try:
                await self.check_sandbox(row, agents.get(row["agent_id"]), now)
            except Exception:  # noqa: BLE001 - 一个沙箱出错不影响其他沙箱
                log.exception("check agent sandbox %s failed", row["id"])
        await self.fire_schedules(now)
        await self._prune_clients()
        if now - self._last_reconcile >= self.cfg.reconcile_interval_s:
            self._last_reconcile = now
            if await self.store.try_periodic(KV_RECONCILE_AT, now=now, interval_s=self.cfg.reconcile_interval_s):
                await self.reconcile(now)
        if now - self._last_cleanup >= self.cfg.cleanup_interval_s:
            self._last_cleanup = now
            if await self.store.try_periodic(KV_CLEANUP_AT, now=now, interval_s=self.cfg.cleanup_interval_s):
                await self.cleanup(now)

    # ---------- 过渡态与任务接管 ----------

    async def recover_stuck(self, now: float) -> None:
        for row in await self.store.list_sandboxes(_TRANSITIONAL):
            if row["op_deadline"] is None or row["op_deadline"] > now:
                continue
            log.warning("agent sandbox row=%s stuck in %s (owner=%s), destroying", row["id"], row["state"], row["op_owner"])
            await self.svc.lc.destroy(row, f"stuck in {row['state']} owned by {row['op_owner']}", background=True)

    async def takeover_tasks(self, now: float) -> None:
        if self.svc.stopping:
            # 本副本正在停机：runner 刚把任务让出来，不能再接回来（停机后无人跟进，要等心跳过期才有人接管）
            return
        for task in await self.store.running_tasks(stale_before=now):
            if task["id"] in self.svc.runners:
                continue
            if not await self.store.cas_task(
                task["id"],
                [TaskState.RUNNING],
                expect_owner=task["op_owner"],
                expect_stale_before=now,
                op_owner=self.svc.replica_id,
                op_deadline=now + self.cfg.agent_task_takeover_s,
            ):
                continue
            row = await self.store.get_sandbox(task["sandbox_row_id"]) if task["sandbox_row_id"] else None
            if row is None or row["state"] not in (s.value for s in _SERVING) or not task["session_id"]:
                reason = "sandbox is gone" if task["session_id"] else "task lost before its session was created"
                await self.store.cas_task(
                    task["id"], [TaskState.RUNNING], expect_owner=self.svc.replica_id, state=TaskState.FAILED,
                    error=f"taken over by {self.svc.replica_id}: {reason}", finished_at=now, op_owner=None, op_deadline=None,
                )
                continue
            log.info("taking over task %s (previous owner %s)", task["id"], task["op_owner"])
            await self.svc.lc.event("task_takeover", sandbox_row_id=row["id"], detail=f"task={task['id']} from={task['op_owner']}")
            self.svc.resume_runner({**task, "op_owner": self.svc.replica_id}, row)

    # ---------- 在服务的沙箱 ----------

    async def check_sandbox(self, row: dict, agent: Optional[dict], now: float) -> None:
        if agent is None:
            await self.svc.fail_tasks(row["id"], "agent deleted")
            await self.svc.lc.destroy(row, "agent deleted", background=True)
            return
        hard = self.svc.hard_deadline(row)
        running = await self.store.running_tasks(sandbox_row_id=row["id"])
        if now >= hard:
            await self.svc.fail_tasks(row["id"], "sandbox reached its max lifetime")
            await self.svc.lc.destroy(row, "max lifetime reached", background=True)
            return
        if row["state"] == SandboxState.RETIRING.value:
            if not running:
                await self.svc.lc.destroy(row, "retired", background=True)
                return
        elif now >= row["created_at"] + self.cfg.agent_rotate_after_s:
            if not running:
                await self.svc.lc.destroy(row, "rotated", background=True)
            elif await self.store.cas_sandbox(
                row["id"], [SandboxState.ACTIVE], now=now, expect_version=row["version"], state=SandboxState.RETIRING
            ):
                await self.svc.lc.event("agent_retire", sandbox_row_id=row["id"], detail="rotation with running tasks")
            return
        else:
            idle_after = self.svc.agent_settings(agent)["idle_destroy_after_s"]
            if idle_after > 0 and not running:
                if row["activity_kind"] == "schedule":
                    idle_after = min(idle_after, self.cfg.agent_schedule_idle_tail_s)
                if now - row["last_active_at"] >= idle_after:
                    await self.svc.lc.destroy(row, f"idle for {now - row['last_active_at']:.0f}s", background=True)
                    return
        if not await self.check_health(row, now, running):
            return
        await self.keepalive(row, now, hard)
        version = self.svc.policy_version(agent)
        if row["network_version"] != version:
            try:
                await self.svc.apply_network(row, agent)
            except Exception as e:  # noqa: BLE001 - 下一轮重试
                log.warning("apply network to %s failed: %r", row["provider_id"], e)
        if (
            row["state"] == SandboxState.ACTIVE.value
            and row["config_version"] != agent["settings_version"]
            and not running
        ):
            try:
                # 有任务在跑（或其他副本正在重载）时返回 False，下一轮再试
                if await self.svc.apply_settings(row, agent):
                    await self.svc.lc.event("agent_settings_applied", sandbox_row_id=row["id"],
                                            detail=str(agent["settings_version"]))
            except Exception as e:  # noqa: BLE001 - 下一轮重试
                log.warning("apply settings to %s failed: %r", row["provider_id"], e)

    async def check_health(self, row: dict, now: float, running: list) -> bool:
        """每 agent_health_interval_s 探测一次 /global/health；连续失败达到上限就销毁（运行中任务记为失败）。"""
        if now - self._last_health.get(row["id"], 0) < self.cfg.agent_health_interval_s:
            return True
        self._last_health[row["id"]] = now
        try:
            health = await asyncio.wait_for(self.svc.client_for(row).health(), timeout=20)
            healthy = bool(health.get("healthy"))
        except Exception as e:  # noqa: BLE001
            log.info("health check of %s failed: %r", row["provider_id"], e)
            healthy = False
        failures = 0 if healthy else (row["health_failures"] or 0) + 1
        if failures != (row["health_failures"] or 0):
            await self.store.cas_sandbox(row["id"], [row["state"]], now=now, health_failures=failures)
        if failures >= self.cfg.agent_health_max_failures:
            log.warning("agent sandbox %s unhealthy %d times, destroying", row["provider_id"], failures)
            await self.svc.fail_tasks(row["id"], "sandbox became unhealthy")
            fresh = await self.store.get_sandbox(row["id"])
            if fresh is not None:
                await self.svc.lc.destroy(fresh, "unhealthy", background=True)
            return False
        return True

    async def keepalive(self, row: dict, now: float, hard: float) -> None:
        """平台超时只作兜底：剩余不足一半时续期，最长到硬截止之后一分钟。"""
        deadline = row["platform_deadline"]
        timeout = self.cfg.agent_platform_timeout_s
        if deadline is not None and deadline - now >= timeout / 2:
            return
        if deadline is not None and deadline >= hard + 60:
            return
        new_timeout = max(60.0, min(timeout, hard + 60 - now))
        try:
            await self.svc.provider.set_timeout(row["provider_id"], new_timeout)
        except Exception as e:  # noqa: BLE001 - 下一轮重试；沙箱消失由健康检查与对账处理
            log.warning("keepalive of %s failed: %r", row["provider_id"], e)
            return
        await self.store.cas_sandbox(row["id"], [row["state"]], now=now, platform_deadline=now + new_timeout)

    # ---------- 定时任务 ----------

    async def fire_schedules(self, now: float) -> None:
        if self.svc.stopping:
            # 停机中不再触发：推进 next_run_at 之后触发的任务会因停机无人跟进；留给其他副本
            return
        for s in await self.store.due_schedules(now):
            try:
                # 从本次应触发的时刻推进，不随维护循环的延迟漂移；落在过去（停机很久）时从现在起算
                next_run = self.svc._next_run(s, s["next_run_at"])
                if next_run <= now:
                    next_run = self.svc._next_run(s, now)
            except ValueError as e:
                log.warning("schedule %s invalid, disabling: %s", s["id"], e)
                await self.store.update_schedule(s["id"], enabled=0, next_run_at=None, updated_at=now)
                continue
            if not await self.store.claim_schedule(s["id"], expect_next=s["next_run_at"], next_run_at=next_run, now=now):
                continue
            if now - s["next_run_at"] > _MISSED_GRACE_S:
                await self.svc.lc.event("schedule_missed", detail=f"schedule={s['id']} due at {s['next_run_at']:.0f}")
                continue
            self.svc.lc.spawn(self.svc.fire_schedule(s))

    # ---------- 对账与清理 ----------

    async def reconcile(self, now: float) -> None:
        """与云端对账：销毁库里没有记录的孤儿沙箱；云端已消失的沙箱，运行中任务记为失败并删除记录。"""
        rows_before = {r["id"]: r for r in await self.store.list_sandboxes()}
        try:
            items = await self.svc.provider.list({"pool": self.cfg.agent_pool_name})
        except Exception:  # noqa: BLE001
            log.exception("list agent sandboxes failed")
            return
        rows_after = await self.store.list_sandboxes()
        by_provider = {r["provider_id"] for r in rows_after if r["provider_id"]}
        row_ids = {r["id"] for r in rows_after}
        for it in items:
            if it.sandbox_id in by_provider or it.metadata.get("pool_row") in row_ids:
                continue
            if it.started_at is not None and now - it.started_at < self.cfg.orphan_grace_s:
                continue
            log.warning("killing orphan agent sandbox %s", it.sandbox_id)
            await self.svc._kill_quietly(it.sandbox_id)
            await self.svc.lc.event("orphan_killed", detail=it.sandbox_id)
        live = {it.sandbox_id for it in items}
        for r in rows_after:
            before = rows_before.get(r["id"])
            if (
                before is not None
                and before["version"] == r["version"]
                and r["state"] in (s.value for s in _SERVING)
                and r["provider_id"] not in live
                and now - r["state_changed_at"] >= self.cfg.orphan_grace_s
            ):
                log.warning("agent sandbox %s vanished from provider", r["provider_id"])
                await self.svc.fail_tasks(r["id"], "sandbox vanished from provider")
                await self.svc.lc.destroy(r, "vanished from provider", background=True)

    async def cleanup(self, now: float) -> None:
        tasks_purged = await self.store.purge_tasks(before=now - self.cfg.agent_task_retention_s)
        purged = await self.store.purge_history(before=now - self.cfg.history_retention_s)
        if tasks_purged or any(purged.values()):
            log.info("agent history purged: tasks=%d %s", tasks_purged, purged)

    async def _prune_clients(self) -> None:
        """关闭已销毁沙箱的客户端。重新查询在建、在服务的全部沙箱，不用本轮开始时的快照：沙箱可能在本轮中间转为
        ACTIVE，任务已经拿着它的客户端在跑。本副本 runner 正在用的沙箱也保留。"""
        live = {r["id"] for r in await self.store.list_sandboxes((*_TRANSITIONAL[:2], *_SERVING))}
        live |= {runner.sandbox["id"] for runner in list(self.svc.runners.values())}
        for row_id in [k for k in list(self.svc._clients) if k not in live]:
            await self.svc.forget_client(row_id)
            self._last_health.pop(row_id, None)
