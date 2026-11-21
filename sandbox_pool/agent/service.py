"""agent 子系统的组装入口与请求路径：每个（调用方，用户）一个常驻 agent，引擎可选 opencode / pi（agent/engines/）。

一个进程（副本）一个 AgentService，与代码执行池（SandboxPool）共用 provider 和数据库，使用独立的池名。
多副本协调沿用仓库约定：状态存库、CAS、池级锁行，不选主。后台维护见 maintainer.py，任务执行见 runner.py。
"""

import asyncio
import json
import logging
import posixpath
import time
import uuid
from typing import AsyncIterator, Optional

from sandbox_pool.agent import cron
from sandbox_pool.agent.engines import LEGACY_ENGINE, THINKING_LEVELS, Engine, FilesContext, build_engines
from sandbox_pool.agent.maintainer import AgentMaintainer
from sandbox_pool.agent.policy import (
    EGRESS_FILE,
    EgressPolicy,
    default_mcp,
    default_policy,
    describe,
    load_injections,
    network_version,
    parse_policy,
    redact_platform_network,
    validate_settings,
    build_network,
)
from sandbox_pool.agent.runner import TaskRunner
from sandbox_pool.config import PoolConfig
from sandbox_pool.core.lifecycle import Lifecycle
from sandbox_pool.core.pool import _percentile, new_replica_id
from sandbox_pool.models import (
    AgentNotFound,
    AgentUnavailable,
    InvalidRequest,
    SandboxBusy,
    SandboxOpError,
    SandboxRecordNotFound,
    SandboxState,
    TaskConflict,
    TaskState,
    TooManyTasks,
    WaitTimeout,
)
from sandbox_pool.provider.base import SandboxNotFound, SandboxProvider
from sandbox_pool.store.agent_repo import BOUND_STATES, AgentStore

log = logging.getLogger(__name__)

# 对外展示沙箱记录时去掉的列（access_token 是访问沙箱端口的凭证）
_HIDDEN_SANDBOX_FIELDS = ("access_token", "lease_id")
_SANDBOX_POLL_S = 0.5
# 单个请求最多尝试建几次沙箱（每次失败都会销毁重建）
_MAX_BOOT_ATTEMPTS = 3
# 重载配置：整体限时（写文件 + dispose，实测 1~2 秒）；占用沙箱的时长必须更长，保证 dispose 不会在占用过期后才发出
_RELOAD_TIMEOUT_S = 60
_RELOAD_HOLD_S = 90
_RELOAD_POLL_S = 0.2
# 跟随其他副本上的任务（断线重连）时多久读一次库，看任务是否已结束
_FOLLOW_POLL_S = 1.0
MIN_EVERY_S = 60

# 装配耗时：事件 kind → 对外步骤名。agent_create / agent_boot 是原有事件，其余是分步。
_BOOT_STEP_KINDS = {
    "agent_create": "create",
    "agent_boot_password": "password",
    "agent_boot_health": "health",
    "agent_boot_files": "files",
    "agent_boot_preload": "preload",
    "agent_boot_activate": "activate",
}
BOOT_EVENT_KINDS = tuple(_BOOT_STEP_KINDS) + ("agent_boot", "agent_boot_failed")


def boot_step_order(engine: str) -> tuple[str, ...]:
    """连续分段的顺序。opencode2 在创建之后多写一次应用密码。"""
    if engine == "opencode2":
        return ("create", "password", "health", "files", "preload", "activate")
    return ("create", "health", "files", "preload", "activate")


def boot_view(events: list[dict], *, state: str, engine: str) -> dict:
    """把装配事件收成对外的 boot：已完成步骤、总耗时、正在进行的下一步。

    步骤按引擎顺序取，中间缺一段就停。进行中的那一步不在事件里。
    ACTIVE 且已有 agent_boot 时 running 为空。
    """
    by_name: dict[str, dict] = {}
    total = None
    for ev in events:
        kind = ev.get("kind")
        if kind == "agent_boot":
            total = ev.get("duration_ms")
            continue
        name = _BOOT_STEP_KINDS.get(kind or "")
        if name and name not in by_name:
            by_name[name] = ev
    order = boot_step_order(engine)
    steps = []
    for name in order:
        ev = by_name.get(name)
        if ev is None:
            break
        steps.append({"name": name, "duration_ms": ev.get("duration_ms"), "finished_at": ev.get("ts")})
    running = None
    if not (state == SandboxState.ACTIVE.value and total is not None) and len(steps) < len(order):
        running = order[len(steps)]
    return {"total_ms": total, "steps": steps, "running": running}


def check_agent_config(cfg: PoolConfig) -> None:
    """启动时校验 agent 配置，有误直接拒绝启动。"""
    if not cfg.agent_enabled:
        return
    errors = []
    if not any(e.enabled for e in build_engines(cfg).values()):
        errors.append(
            "at least one agent template is required (POOL_AGENT_TEMPLATE, POOL_AGENT_PI_TEMPLATE, "
            "POOL_AGENT_OPENCODE2_TEMPLATE or POOL_AGENT_PI1_TEMPLATE) "
            "(see scripts/build_opencode_template.py, scripts/build_pi_template.py)"
        )
    enabled = [n for n, e in build_engines(cfg).items() if e.enabled]
    if enabled and cfg.agent_default_engine not in enabled:
        errors.append(f"POOL_AGENT_DEFAULT_ENGINE must be one of the enabled engines {enabled}")
    if "/" not in cfg.agent_model:
        errors.append("POOL_AGENT_MODEL must look like provider/model")
    if "/" not in cfg.agent_pi_model:
        errors.append("POOL_AGENT_PI_MODEL must look like provider/model")
    if cfg.agent_pi_thinking and cfg.agent_pi_thinking not in THINKING_LEVELS:
        errors.append(f"POOL_AGENT_PI_THINKING must be one of {THINKING_LEVELS} or empty")
    if not (0 < cfg.agent_rotate_after_s < cfg.agent_max_life_s <= 86400):
        errors.append("require 0 < POOL_AGENT_ROTATE_AFTER_S < POOL_AGENT_MAX_LIFE_S <= 86400 (platform max lifetime)")
    if cfg.agent_boot_timeout_s < 60:
        errors.append("POOL_AGENT_BOOT_TIMEOUT_S must be >= 60 (create 30s + health wait + config writes)")
    if cfg.agent_task_takeover_s <= 2 * cfg.agent_task_heartbeat_s:
        errors.append("POOL_AGENT_TASK_TAKEOVER_S must be greater than 2 * POOL_AGENT_TASK_HEARTBEAT_S")
    if cfg.agent_platform_timeout_s < 600:
        errors.append("POOL_AGENT_PLATFORM_TIMEOUT_S must be >= 600")
    if cfg.agent_task_max_duration_s >= cfg.agent_max_life_s:
        # 否则新建的沙箱也「剩余寿命不足」，会被反复轮换
        errors.append("POOL_AGENT_TASK_MAX_DURATION_S must be less than POOL_AGENT_MAX_LIFE_S")
    try:
        default_policy(cfg.agent_egress)
        default_mcp(cfg.agent_mcp)
        load_injections(cfg.agent_model_host, cfg.agent_model_api_key, cfg.agent_inject)
    except (ValueError, InvalidRequest) as e:
        errors.append(str(e))
    if errors:
        raise ValueError("; ".join(errors))


def sandbox_view(row: Optional[dict], cfg: PoolConfig) -> Optional[dict]:
    if row is None:
        return None
    out = {k: v for k, v in row.items() if k not in _HIDDEN_SANDBOX_FIELDS}
    out["engine"] = row.get("engine") or LEGACY_ENGINE
    out["hard_deadline"] = row["created_at"] + cfg.agent_max_life_s
    return out


class AgentService:
    def __init__(
        self,
        cfg: PoolConfig,
        provider: SandboxProvider,
        *,
        store: Optional[AgentStore] = None,
        replica_id: Optional[str] = None,
    ):
        check_agent_config(cfg)
        self.cfg = cfg
        self.replica_id = replica_id or new_replica_id()
        # 生产中由 create_app 传入与代码执行池共用引擎的 store（每进程一个 SQLite 写连接，见 store/db.py），
        # 由代码执行池负责关闭；没传时（测试模拟多副本）自己打开、自己关闭
        self._owns_store = store is None
        self.store = store or AgentStore.open(cfg.db_url, cfg.agent_pool_name)
        self.provider = provider
        self.lc = Lifecycle(cfg, self.store, provider, self.replica_id)
        # 全部引擎（含未启用的，老沙箱记录可能属于它们）；可供 agent 选择的是 enabled 的
        self.engines: dict[str, Engine] = build_engines(cfg)
        self.default_egress: EgressPolicy = default_policy(cfg.agent_egress)
        self.default_mcp: dict = default_mcp(cfg.agent_mcp)
        self.injections = load_injections(cfg.agent_model_host, cfg.agent_model_api_key, cfg.agent_inject)
        self.maintainer = AgentMaintainer(self)
        self.lc.on_change = self.maintainer.kick
        self.runners: dict[str, TaskRunner] = {}
        self._runner_tasks: set[asyncio.Task] = set()
        self._clients: dict[str, object] = {}
        self._boot_errors: dict[str, str] = {}
        self._just_resumed: set[str] = set()
        # 停机中：维护循环不再接管任务、不再触发定时任务
        self.stopping = False

    @staticmethod
    def now() -> float:
        return time.time()

    async def start(self, *, run_maintainer: bool = True) -> None:
        await self.store.init_schema()
        if run_maintainer:
            await self.maintainer.start()
        log.info("agent service replica %s started (engines=%s, default=%s)", self.replica_id,
                 {n: e.template for n, e in self.engines.items() if e.enabled}, self.cfg.agent_default_engine)

    async def stop(self, grace_s: float = 30) -> None:
        """先让本副本的 runner 让出任务（其他副本接管，不中止 opencode 里的任务），再停维护循环。

        先置停机标志：否则维护循环会把 runner 刚让出的任务接回本副本。维护循环停下之前，已经在后台触发的定时任务
        或进行中的请求仍可能起新 runner，所以维护循环停下后再让出一次。
        """
        self.stopping = True
        await self._release_runners(grace_s)
        await self.maintainer.stop(grace_s)
        await self._release_runners(grace_s)
        for client in list(self._clients.values()):
            try:
                await client.close()
            except Exception:  # noqa: BLE001
                pass
        self._clients.clear()
        if self._owns_store:
            await self.store.close()

    async def _release_runners(self, grace_s: float) -> None:
        for runner in list(self.runners.values()):
            runner.stop()
        if self._runner_tasks:
            done, pending = await asyncio.wait(list(self._runner_tasks), timeout=grace_s)
            for t in pending:
                t.cancel()
            await asyncio.gather(*pending, return_exceptions=True)

    # ---------- 基础 ----------

    def enabled_engines(self) -> dict[str, Engine]:
        return {n: e for n, e in self.engines.items() if e.enabled}

    def engine_for(self, row: dict) -> Engine:
        """沙箱记录上的引擎（老记录为空，视为 opencode）。"""
        return self.engines[row.get("engine") or LEGACY_ENGINE]

    def agent_engine(self, agent: dict) -> Engine:
        """agent 设置的引擎；没设置、或设置的引擎已停用时用默认引擎。"""
        name = (agent.get("settings") or {}).get("engine")
        engine = self.engines.get(name) if name else None
        if engine is None or not engine.enabled:
            engine = self.engines[self.cfg.agent_default_engine]
        return engine

    def engines_info(self) -> dict:
        return {
            "default": self.cfg.agent_default_engine,
            "engines": [
                {**e.describe(), "default": n == self.cfg.agent_default_engine}
                for n, e in self.enabled_engines().items()
            ],
        }

    def client_for(self, row: dict):
        client = self._clients.get(row["id"])
        if client is None:
            opts = {"ingress_ip": self.cfg.agent_ingress_ip or None}
            if row.get("engine") == "opencode2":
                opts["protocol"] = "opencode2"
            client = self.provider.app_client(row["endpoint"], row["access_token"], self.cfg.agent_workdir, **opts)
            self._clients[row["id"]] = client
        return client

    async def forget_client(self, row_id: str) -> None:
        client = self._clients.pop(row_id, None)
        if client is not None:
            try:
                await client.close()
            except Exception:  # noqa: BLE001
                pass

    def effective_policy(self, agent: dict) -> EgressPolicy:
        # 库里的覆盖可能早于现行校验规则写入：违规的放行项丢弃，不让装配和维护循环失败
        return parse_policy(agent.get("egress"), base=self.default_egress, strict=False)

    def policy_version(self, agent: dict) -> str:
        return network_version(self.effective_policy(agent), self.injections)

    def agent_settings(self, agent: dict) -> dict:
        s = agent.get("settings") or {}
        return {
            "idle_destroy_after_s": float(s.get("idle_destroy_after_s", self.cfg.agent_idle_destroy_after_s)),
            "mcp": s.get("mcp") or {},
            "instructions": s.get("instructions") or "",
            "engine": s.get("engine"),
        }

    def hard_deadline(self, row: dict) -> float:
        return row["created_at"] + self.cfg.agent_max_life_s

    def platform_timeout_for(self, row: dict) -> float:
        """需要重新连接沙箱（connect 会重设平台超时）时用的超时：不超过硬截止。"""
        return max(60.0, min(self.cfg.agent_platform_timeout_s, self.hard_deadline(row) + 60 - self.now()))

    def runner_finished(self, runner: TaskRunner) -> None:
        self.runners.pop(runner.task_id, None)

    def _spawn_runner(self, runner: TaskRunner) -> None:
        self.runners[runner.task_id] = runner
        task = asyncio.create_task(runner.run(), name=f"agent-task-{runner.task_id}")
        self._runner_tasks.add(task)
        task.add_done_callback(self._runner_tasks.discard)

    # ---------- agent ----------

    async def ensure_agent(self, client_id: str, user_id: str) -> dict:
        if not user_id or len(user_id) > 128:
            raise InvalidRequest("user_id must be 1..128 characters")
        return await self.store.ensure_agent(client_id, user_id, now=self.now(), settings={})

    async def get_agent(self, client_id: str, user_id: str) -> dict:
        agent = await self.store.get_agent(client_id, user_id)
        if agent is None:
            raise AgentNotFound(f"agent {user_id} not found")
        return agent

    async def agent_info(self, agent: dict) -> dict:
        rows = await self.store.agent_sandboxes(agent["id"])
        running = await self.store.running_tasks(agent_id=agent["id"])
        grouped = await self.store.events_for_sandboxes([r["id"] for r in rows], BOOT_EVENT_KINDS)
        sandboxes = []
        for row in rows:
            view = sandbox_view(row, self.cfg)
            view["boot"] = boot_view(
                grouped.get(row["id"], []), state=row["state"], engine=view["engine"]
            )
            sandboxes.append(view)
        return {
            "user_id": agent["user_id"],
            "agent_id": agent["id"],
            "created_at": agent["created_at"],
            "settings": self.agent_settings(agent),
            "settings_version": agent["settings_version"],
            # 当前生效的引擎（设置为空时是默认引擎）
            "engine": self.agent_engine(agent).name,
            "egress_override": agent.get("egress"),
            "sandboxes": sandboxes,
            "running_tasks": [t["id"] for t in running],
        }

    async def update_settings(self, agent: dict, patch: dict) -> dict:
        """修改设置。改引擎（engine）等于换沙箱：维护循环和下一次请求会把旧引擎的沙箱转为 RETIRING（跑完手头任务后
        销毁），新请求在新引擎的沙箱上执行；旧会话不能续聊。"""
        checked = validate_settings(patch)
        engine = checked.get("engine")
        if engine is not None and engine not in self.enabled_engines():
            raise InvalidRequest(f"engine {engine!r} is not enabled; enabled engines: {sorted(self.enabled_engines())}")
        # 合并以库里最新的设置为准（存储层同一事务内读、合并、写）：传入的 agent 是请求开始时读的，其间可能有其他
        # 请求改了别的字段（R3-03）。engine 为 None 表示恢复默认，从设置里删除
        if not await self.store.patch_settings(agent["id"], checked, now=self.now()):
            raise AgentNotFound(f"agent {agent['user_id']} not found")
        self.maintainer.kick()
        return await self.agent_info(await self.store.get_agent_by_id(agent["id"]))

    async def start_sandbox(self, agent: dict, *, engine: Optional[str] = None) -> dict:
        """把该 agent 的沙箱做到 ACTIVE 再返回。不建会话、不发消息。

        带 engine 且与已保存的值不同时先改设置。已经是同引擎的 ACTIVE 时直接返回，fresh 为 false。
        """
        started = self.now()
        if engine is not None and (agent.get("settings") or {}).get("engine") != engine:
            await self.update_settings(agent, {"engine": engine})
            agent = await self.store.get_agent_by_id(agent["id"]) or agent
        try:
            row = await self.sandbox_for(agent)
        except SandboxOpError as exc:
            exc.attempts = await self.boot_attempt_views(exc.attempt_ids, self.agent_engine(agent).name)
            raise
        events = (await self.store.events_for_sandboxes(
            [row["id"]], BOOT_EVENT_KINDS + ("agent_resume",)
        )).get(row["id"], [])
        boot = boot_view(events, state=row["state"], engine=row.get("engine") or LEGACY_ENGINE)
        boot_ts = next((ev["ts"] for ev in events if ev["kind"] == "agent_boot"), None)
        resumed = bool(row.pop("_resumed", False))
        view = sandbox_view(row, self.cfg)
        view["boot"] = boot
        return {
            "fresh": boot_ts is not None and boot_ts >= started,
            "resumed": resumed,
            "sandbox": view,
            "boot": boot,
        }

    async def boot_attempt_views(self, row_ids: list[str], engine: str) -> list[dict]:
        """失败尝试：沙箱行可能已经删掉，步骤和错误从事件里读。"""
        grouped = await self.store.events_for_sandboxes(row_ids, BOOT_EVENT_KINDS)
        out = []
        for row_id in row_ids:
            evs = grouped.get(row_id, [])
            failed = next((ev for ev in evs if ev["kind"] == "agent_boot_failed"), None)
            view = boot_view(evs, state=SandboxState.CREATING.value, engine=engine)
            out.append({
                "sandbox_row_id": row_id,
                "steps": view["steps"],
                "error": None if failed is None else failed.get("detail"),
            })
        return out

    # ---------- 沙箱 ----------

    async def sandbox_for(self, agent: dict, *, min_remaining_s: float = 0, wait_s: Optional[float] = None) -> dict:
        """该 agent 可接新任务的 ACTIVE 沙箱；没有就创建（后台）并等待就绪。

        剩余寿命不足 min_remaining_s、或引擎与 agent 设置不一致（切换了引擎）的沙箱转为 RETIRING（跑完手头任务后
        销毁），新任务去新沙箱。
        """
        wait_s = self.cfg.agent_wait_sandbox_s if wait_s is None else wait_s
        deadline = self.now() + wait_s
        # 新沙箱至少要能满足要求，否则每个沙箱都会被判为剩余寿命不足
        min_remaining_s = min(min_remaining_s, self.cfg.agent_max_life_s * 0.9)
        attempts = 0
        # 本次创建或等到过的沙箱行。失败时交给启动接口，行删掉后事件还在。
        seen: list[str] = []

        def note(row_id: Optional[str]) -> None:
            if row_id and row_id not in seen:
                seen.append(row_id)

        while True:
            now = self.now()
            rows = await self.store.agent_sandboxes(
                agent["id"], (*BOUND_STATES, SandboxState.PAUSED, SandboxState.PAUSING, SandboxState.RESUMING)
            )
            active = [r for r in rows if r["state"] == SandboxState.ACTIVE.value]
            if active:
                row = active[0]
                if self.engine_for(row).name != self.agent_engine(agent).name:
                    # 传入的 agent 可能读于引擎切换之前（在途请求）：以库里最新的设置为准，否则会把新引擎的沙箱当成
                    # 「切换了引擎」转为 RETIRING，再按旧快照建旧引擎的沙箱（PI-L1）
                    agent = await self.store.get_agent_by_id(agent["id"]) or agent
                engine = self.agent_engine(agent)
                if self.engine_for(row).name != engine.name:
                    if await self.store.cas_sandbox(
                        row["id"], [SandboxState.ACTIVE], now=now, expect_version=row["version"], state=SandboxState.RETIRING
                    ):
                        log.info("sandbox %s retiring: agent switched engine to %s", row["provider_id"], engine.name)
                        await self.lc.event("agent_engine_switch", sandbox_row_id=row["id"],
                                            detail=f"{self.engine_for(row).name} -> {engine.name}")
                    continue
                if min_remaining_s and self.hard_deadline(row) - now < min_remaining_s:
                    if await self.store.cas_sandbox(
                        row["id"], [SandboxState.ACTIVE], now=now, expect_version=row["version"], state=SandboxState.RETIRING
                    ):
                        log.info("sandbox %s retiring: remaining life < %ss", row["provider_id"], min_remaining_s)
                        await self.lc.event("agent_retire", sandbox_row_id=row["id"], detail="remaining life too short")
                    continue
                if row["id"] in self._just_resumed:
                    self._just_resumed.discard(row["id"])
                    row = {**row, "_resumed": True}
                return row
            paused = [r for r in rows if r["state"] == SandboxState.PAUSED.value]
            if paused and not any(r["state"] == SandboxState.RESUMING.value for r in rows):
                row = paused[0]
                agent = await self.store.get_agent_by_id(agent["id"]) or agent
                engine = self.agent_engine(agent)
                if self.engine_for(row).name != engine.name or (
                    min_remaining_s and self.hard_deadline(row) - now < min_remaining_s
                ):
                    await self.lc.destroy(row, "paused sandbox cannot take the next task")
                    continue
                await self._resume_one(row)
                continue
            if not rows:
                if attempts >= _MAX_BOOT_ATTEMPTS:
                    raise SandboxOpError(
                        f"agent sandbox failed to start {attempts} times: {self._boot_errors.get(agent['id'], 'unknown')}",
                        attempt_ids=seen,
                    )
                # 建沙箱按库里最新的引擎设置（同上，PI-L1）
                agent = await self.store.get_agent_by_id(agent["id"]) or agent
                engine = self.agent_engine(agent)
                status, row = await self.store.reserve_agent_sandbox(
                    agent_id=agent["id"],
                    template=engine.template,
                    engine=engine.name,
                    now=now,
                    owner=self.replica_id,
                    op_deadline=now + self.cfg.agent_boot_timeout_s,
                    limit=self.cfg.agent_max_sandboxes,
                )
                if status == "full":
                    raise AgentUnavailable(f"agent sandbox capacity reached ({self.cfg.agent_max_sandboxes})")
                if status == "created":
                    attempts += 1
                    note(row["id"])
                    self.lc.spawn(self._boot(row, agent))
                elif status == "exists" and row is not None:
                    note(row["id"])
            else:
                for item in rows:
                    note(item["id"])
            if now >= deadline:
                raise WaitTimeout(f"agent sandbox not ready within {wait_s:.0f}s")
            await asyncio.sleep(_SANDBOX_POLL_S)

    async def pause_sandbox(self, agent: dict) -> dict:
        """把该用户当前的 ACTIVE 沙箱休眠。有运行中任务时不暂停。已经休眠则原样返回。"""
        rows = await self.store.agent_sandboxes(agent["id"], [SandboxState.ACTIVE, SandboxState.PAUSED])
        paused = [r for r in rows if r["state"] == SandboxState.PAUSED.value]
        active = [r for r in rows if r["state"] == SandboxState.ACTIVE.value]
        if not active:
            if paused:
                return {"id": paused[0]["id"], "provider_id": paused[0].get("provider_id"), "state": "PAUSED", "changed": False}
            raise AgentNotFound(f"agent {agent['user_id']} has no sandbox to pause")
        row = active[0]
        if await self.store.running_tasks(sandbox_row_id=row["id"]):
            raise TaskConflict("sandbox has a running task; wait or abort it before pausing")
        now = self.now()
        if not await self.store.cas_sandbox(
            row["id"], [SandboxState.ACTIVE], now=now, expect_version=row["version"],
            state=SandboxState.PAUSING, op_owner=self.replica_id, op_deadline=now + self.cfg.resume_timeout_s,
        ):
            raise SandboxBusy(f"sandbox {row['id']} changed, retry later")
        try:
            await self.provider.pause(row["provider_id"])
        except Exception as e:  # noqa: BLE001
            fresh = await self.store.get_sandbox(row["id"])
            if fresh and fresh["state"] == SandboxState.PAUSING.value and fresh["op_owner"] == self.replica_id:
                actual = None
                try:
                    actual = await self.provider.get_state(row["provider_id"])
                except Exception:  # noqa: BLE001
                    actual = None
                if actual == "paused":
                    await self._mark_paused(fresh)
                    return {"id": row["id"], "provider_id": row.get("provider_id"), "state": "PAUSED", "changed": True}
                await self.lc.destroy(fresh, f"pause failed: {e}")
            raise SandboxOpError(f"pause failed: {e}") from e
        fresh = await self.store.get_sandbox(row["id"])
        if fresh is None or not await self._mark_paused(fresh):
            raise SandboxOpError(f"pause failed for sandbox {row['id']}")
        return {"id": row["id"], "provider_id": row.get("provider_id"), "state": "PAUSED", "changed": True}

    async def _mark_paused(self, row: dict) -> bool:
        now = self.now()
        ok = await self.store.cas_sandbox(
            row["id"], [SandboxState.PAUSING], now=now, expect_owner=self.replica_id,
            state=SandboxState.PAUSED, op_owner=None, op_deadline=None,
        )
        if ok:
            await self.forget_client(row["id"])
            await self.lc.event("agent_pause", sandbox_row_id=row["id"])
        return ok

    async def _resume_one(self, row: dict) -> None:
        """PAUSED → RESUMING → ACTIVE。失败时销毁，避免卡在过渡态。"""
        now = self.now()
        if not await self.store.cas_sandbox(
            row["id"], [SandboxState.PAUSED], now=now, expect_version=row["version"],
            state=SandboxState.RESUMING, op_owner=self.replica_id, op_deadline=now + self.cfg.resume_timeout_s,
        ):
            return
        try:
            await self.forget_client(row["id"])
            await self.provider.resume(row["provider_id"], self.cfg.agent_platform_timeout_s)
            await self._wait_healthy(self.client_for(row), timeout_s=self.cfg.agent_boot_timeout_s / 2)
        except Exception as e:  # noqa: BLE001
            fresh = await self.store.get_sandbox(row["id"])
            if fresh and fresh["state"] == SandboxState.RESUMING.value and fresh["op_owner"] == self.replica_id:
                await self.lc.destroy(fresh, f"resume failed: {e}")
            raise SandboxOpError(f"resume failed: {e}") from e
        now = self.now()
        if not await self.store.cas_sandbox(
            row["id"], [SandboxState.RESUMING], now=now, expect_owner=self.replica_id,
            state=SandboxState.ACTIVE, op_owner=None, op_deadline=None,
            platform_deadline=now + self.cfg.agent_platform_timeout_s,
        ):
            raise SandboxOpError(f"resume lost ownership of sandbox {row['id']}")
        self._just_resumed.add(row["id"])
        await self.lc.event("agent_resume", sandbox_row_id=row["id"])

    async def _boot(self, row: dict, agent: dict) -> None:
        """CREATING → WARMING → ACTIVE：建沙箱（出网规则 + 凭证注入）、等沙箱内 agent 服务健康、写配置文件、预加载实例。

        耗时是同一只 perf_counter 上的连续分段，本地间隙并入下一段，各段之和写入 agent_boot。
        """
        provider_id = None
        t0 = time.perf_counter()
        cursor = t0
        recorded: list[float] = []

        async def lap(kind: str) -> None:
            nonlocal cursor
            recorded.append((time.perf_counter() - cursor) * 1000)
            cursor = time.perf_counter()
            await self.lc.event(kind, sandbox_row_id=row["id"], duration_ms=recorded[-1])

        try:
            engine = self.engine_for(row)
            policy = self.effective_policy(agent)
            version = network_version(policy, self.injections)
            timeout = self.cfg.agent_platform_timeout_s
            app = await self.provider.create_app(
                engine.template,
                {"pool": self.cfg.agent_pool_name, "pool_row": row["id"], "agent": agent["id"], "replica": self.replica_id,
                 "engine": engine.name},
                timeout,
                port=engine.port,
                network=build_network(policy, self.injections),
            )
            provider_id = app.sandbox_id
            await lap("agent_create")
            now = self.now()
            values = dict(
                provider_id=app.sandbox_id,
                endpoint=app.endpoint,
                access_token=app.access_token,
                network_version=version,
                platform_deadline=now + timeout,
            )
            if not await self.store.cas_sandbox(
                row["id"],
                [SandboxState.CREATING],
                now=now,
                expect_owner=self.replica_id,
                state=SandboxState.WARMING,
                op_deadline=now + self.cfg.agent_boot_timeout_s,
                **values,
            ):
                await self._kill_quietly(app.sandbox_id)
                return
            row = {**row, **values}
            if engine.name == "opencode2":
                from sandbox_pool.agent.engines.opencode_v2 import server_password
                await self.provider.write_files(provider_id, {
                    "/home/user/.agent/server-password": server_password(app.access_token).encode()
                }, sandbox_timeout_s=timeout)
                await lap("agent_boot_password")
            client = self.client_for(row)
            await self._wait_healthy(client, timeout_s=self.cfg.agent_boot_timeout_s / 2)
            await lap("agent_boot_health")
            fresh_agent = await self.store.get_agent_by_id(agent["id"]) or agent
            await self._write_agent_files(row, fresh_agent, policy, version)
            await lap("agent_boot_files")
            # 预加载（opencode：读取工作目录的 opencode.json、连接 MCP），首个请求不必等
            await client.status()
            await lap("agent_boot_preload")
            now = self.now()
            ok = await self.store.activate_sandbox(
                row["id"],
                agent["id"],
                owner=self.replica_id,
                now=now,
                config_version=fresh_agent["settings_version"],
                health_failures=0,
            )
            if not ok:
                fresh = await self.store.get_sandbox(row["id"])
                if fresh and fresh["state"] == SandboxState.WARMING.value and fresh["op_owner"] == self.replica_id:
                    await self.lc.destroy(fresh, "activate failed")
                return
            await lap("agent_boot_activate")
            total_ms = sum(recorded)
            self._boot_errors.pop(agent["id"], None)
            await self.lc.event("agent_boot", sandbox_row_id=row["id"], duration_ms=total_ms)
            log.info("agent %s sandbox %s ready in %.1fs", agent["id"], app.sandbox_id, total_ms / 1000)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            log.warning("boot agent sandbox failed: %r", e, exc_info=True)
            self._boot_errors[agent["id"]] = f"{type(e).__name__}: {e}"[:500]
            await self.lc.event(
                "agent_boot_failed",
                sandbox_row_id=row["id"],
                detail=str(e)[:500],
                duration_ms=(time.perf_counter() - t0) * 1000,
            )
            try:
                fresh = await self.store.get_sandbox(row["id"])
                if fresh and fresh["op_owner"] == self.replica_id and fresh["state"] in (
                    SandboxState.CREATING.value,
                    SandboxState.WARMING.value,
                ):
                    if fresh["provider_id"] is None and provider_id:
                        await self._kill_quietly(provider_id)
                    await self.lc.destroy(fresh, f"boot failed: {e}")
                elif provider_id and (fresh is None or fresh["provider_id"] != provider_id):
                    await self._kill_quietly(provider_id)
            except Exception:  # noqa: BLE001 - 仍未清理的记录由 op_deadline 接管，云端孤儿由对账清理
                log.exception("cleanup after boot failure failed (row=%s)", row["id"])

    async def _wait_healthy(self, client, *, timeout_s: float) -> None:
        deadline = time.monotonic() + timeout_s
        last: Optional[Exception] = None
        while time.monotonic() < deadline:
            try:
                health = await client.health()
                if health.get("healthy"):
                    return
            except Exception as e:  # noqa: BLE001
                last = e
            await asyncio.sleep(0.5)
        raise RuntimeError(f"agent service in sandbox not healthy within {timeout_s:.0f}s: {last!r}")

    async def _kill_quietly(self, provider_id: str) -> None:
        try:
            await self.provider.kill(provider_id)
        except Exception:  # noqa: BLE001 - 留给对账清理
            log.exception("kill %s failed, leaving it to reconcile", provider_id)

    def _agent_files(
        self, engine: Optional[Engine], agent: dict, policy: EgressPolicy, version: str
    ) -> dict[str, bytes]:
        """写进沙箱的文件：egress.json（与引擎无关），engine 不为空时再加上该引擎的配置文件。"""
        egress = {**describe(policy, self.injections, version), "applied_at": self.now()}
        files = {EGRESS_FILE: json.dumps(egress, ensure_ascii=False, indent=2).encode()}
        if engine is not None:
            settings = self.agent_settings(agent)
            files.update(engine.render_files(FilesContext(
                workdir=self.cfg.agent_workdir,
                max_life_h=self.cfg.agent_max_life_s / 3600,
                idle_destroy_after_s=settings["idle_destroy_after_s"],
                mcp={**self.default_mcp, **settings["mcp"]},
                egress=egress,
                instructions=settings["instructions"],
            )))
        return files

    async def _write_agent_files(self, row: dict, agent: dict, policy: EgressPolicy, version: str) -> None:
        await self.provider.write_files(
            row["provider_id"], self._agent_files(self.engine_for(row), agent, policy, version),
            sandbox_timeout_s=self.platform_timeout_for(row),
        )

    async def apply_settings(self, row: dict, agent: dict) -> bool:
        """设置变更：重写引擎的配置文件（opencode.json / pi.json、AGENTS.md 等），让沙箱内 agent 重新加载。

        opencode 的重载（dispose）会中止沙箱里所有运行中的会话（实例销毁时取消全部会话）；pi 桥接进程只重启空闲的
        会话进程。两种引擎都先在池级锁内确认沙箱没有运行中任务并占住它，期间新任务的准入会等待
        （AgentStore.begin_reload / create_task）。整体限时，保证 dispose 不会在占用过期之后才发出。有任务在跑或其他
        副本正在重载时返回 False，维护循环下一轮再试。
        """
        now = self.now()
        if not await self.store.begin_reload(
            row["id"], owner=self.replica_id, now=now, op_deadline=now + _RELOAD_HOLD_S
        ):
            return False
        policy = self.effective_policy(agent)
        version = network_version(policy, self.injections)

        async def reload() -> None:
            await self._write_agent_files(row, agent, policy, version)
            await self.client_for(row).dispose()

        try:
            await asyncio.wait_for(reload(), timeout=_RELOAD_TIMEOUT_S)
        except Exception:
            await self.store.end_reload(row["id"], owner=self.replica_id, now=self.now())
            raise
        return await self.store.end_reload(
            row["id"], owner=self.replica_id, now=self.now(), config_version=agent["settings_version"]
        )

    async def apply_network(self, row: dict, agent: dict) -> bool:
        """把 agent 当前的出网策略下发到沙箱（全量替换），并同步沙箱内的 egress.json。"""
        policy = self.effective_policy(agent)
        version = network_version(policy, self.injections)
        await self.provider.update_network(row["provider_id"], build_network(policy, self.injections))
        await self.provider.write_files(
            row["provider_id"], self._agent_files(None, agent, policy, version),
            sandbox_timeout_s=self.platform_timeout_for(row),
        )
        ok = await self.store.cas_sandbox(row["id"], [row["state"]], now=self.now(), network_version=version)
        await self.lc.event("agent_network_applied", sandbox_row_id=row["id"], detail=version)
        return ok

    async def reset(self, agent: dict) -> dict:
        """销毁该 agent 的全部沙箱（运行中任务记为失败），下次请求重新创建。"""
        destroyed = 0
        for row in await self.store.agent_sandboxes(agent["id"]):
            if row["state"] in (SandboxState.ACTIVE.value, SandboxState.RETIRING.value, SandboxState.PAUSED.value):
                await self.fail_tasks(row["id"], "sandbox reset by user")
                if await self.lc.destroy(row, "reset by user"):
                    destroyed += 1
        self.maintainer.kick()
        return {"destroyed": destroyed}

    def _absolute_path(self, path: Optional[str], *, required: bool) -> str:
        if path is None or path == "":
            if required:
                raise InvalidRequest("path is required")
            return "/"
        if not path.startswith("/"):
            raise InvalidRequest("path must be absolute")
        norm = posixpath.normpath(path)
        if not norm.startswith("/"):
            raise InvalidRequest("path must be absolute")
        return norm

    async def _active_sandbox(self, agent: dict) -> dict:
        """只操作当前 ACTIVE 的沙箱。休眠、创建中和恢复中直接拒绝，避免 connect() 把休眠实例唤醒。"""
        rows = await self.store.agent_sandboxes(agent["id"])
        if not rows:
            raise AgentNotFound(f"agent {agent['user_id']} has no sandbox")
        active = [row for row in rows if row["state"] == SandboxState.ACTIVE.value and row.get("provider_id")]
        if not active:
            raise SandboxBusy("sandbox is not running; start it before using files")
        return active[0]

    async def list_files(self, agent: dict, path: Optional[str]) -> dict:
        path = self._absolute_path(path, required=False)
        row = await self._active_sandbox(agent)
        try:
            entries = await self.provider.list_dir(
                row["provider_id"], path, sandbox_timeout_s=self.platform_timeout_for(row),
            )
        except NotADirectoryError as e:
            raise InvalidRequest(f"{path} is a file") from e
        entries.sort(key=lambda item: (item.type != "dir", item.name))
        return {
            "path": path,
            "entries": [
                {"name": item.name, "path": item.path, "type": item.type, "size": item.size}
                for item in entries
            ],
        }

    async def read_file(self, agent: dict, path: str) -> tuple[str, bytes]:
        path = self._absolute_path(path, required=True)
        row = await self._active_sandbox(agent)
        try:
            data = await self.provider.read_file(
                row["provider_id"], path, sandbox_timeout_s=self.platform_timeout_for(row),
            )
        except IsADirectoryError as e:
            raise InvalidRequest(f"{path} is a directory") from e
        return path, data

    async def write_file(self, agent: dict, path: str, data: bytes) -> dict:
        path = self._absolute_path(path, required=True)
        row = await self._active_sandbox(agent)
        await self.provider.write_file(
            row["provider_id"], path, data, sandbox_timeout_s=self.platform_timeout_for(row),
        )
        return {"path": path, "size": len(data)}

    async def admin_destroy_sandbox(self, row_id: str) -> dict:
        """管理员销毁这一台 agent 沙箱。不删除身份、设置和任务历史。过渡态返回失败。"""
        row = await self.store.get_sandbox(row_id)
        if row is None or row["pool"] != self.cfg.agent_pool_name:
            raise SandboxRecordNotFound(f"sandbox {row_id} not found")
        if row["state"] not in (
            SandboxState.ACTIVE.value, SandboxState.RETIRING.value, SandboxState.PAUSED.value
        ):
            raise SandboxBusy(f"sandbox {row_id} is {row['state']}, retry later")
        await self.fail_tasks(row["id"], "sandbox destroyed by admin")
        if not await self.lc.destroy(row, "destroyed by admin"):
            raise SandboxBusy(f"sandbox {row_id} is {row['state']}, retry later")
        self.maintainer.kick()
        return {"id": row_id, "provider_id": row.get("provider_id"), "previous_state": row["state"]}

    async def fail_tasks(self, sandbox_row_id: str, reason: str) -> None:
        for t in await self.store.running_tasks(sandbox_row_id=sandbox_row_id):
            if await self.store.cas_task(
                t["id"], [TaskState.RUNNING], state=TaskState.FAILED, error=reason, finished_at=self.now(),
                op_owner=None, op_deadline=None,
            ):
                runner = self.runners.get(t["id"])
                if runner is not None:
                    runner.stop()

    # ---------- 出网策略 ----------

    async def get_egress(self, agent: dict) -> dict:
        policy = self.effective_policy(agent)
        version = network_version(policy, self.injections)
        out = {
            "desired": describe(policy, self.injections, version),
            "override": agent.get("egress"),
            "sandboxes": [],
        }
        for row in await self.store.agent_sandboxes(agent["id"], [SandboxState.ACTIVE, SandboxState.RETIRING]):
            item = {"sandbox_id": row["id"], "state": row["state"], "applied_version": row["network_version"],
                    "in_sync": row["network_version"] == version, "platform": None}
            try:
                item["platform"] = redact_platform_network(await self.provider.get_network(row["provider_id"]))
            except SandboxNotFound:
                item["platform"] = "not found"
            except Exception as e:  # noqa: BLE001
                item["platform_error"] = f"{type(e).__name__}: {e}"[:300]
            out["sandboxes"].append(item)
        return out

    async def put_egress(self, agent: dict, override: Optional[dict]) -> dict:
        """按 agent 覆盖出网策略（传 null 恢复默认），立即下发到在用的沙箱；下发失败由维护循环重试。"""
        if override is not None:
            parse_policy(override, base=self.default_egress)  # 校验
        await self.store.update_agent(agent["id"], now=self.now(), egress=override)
        agent = await self.store.get_agent_by_id(agent["id"])
        for row in await self.store.agent_sandboxes(agent["id"], [SandboxState.ACTIVE, SandboxState.RETIRING]):
            try:
                await self.apply_network(row, agent)
            except Exception as e:  # noqa: BLE001
                log.warning("apply network to %s failed, maintainer will retry: %r", row["provider_id"], e)
        return await self.get_egress(agent)

    # ---------- 任务 ----------

    async def start_message(
        self,
        agent: dict,
        text: str,
        *,
        session_id: Optional[str] = None,
        max_duration_s: Optional[float] = None,
        agent_name: Optional[str] = None,
    ) -> TaskRunner:
        max_d = min(max_duration_s or self.cfg.agent_task_max_duration_s, self.cfg.agent_task_max_duration_s)
        engine = self.agent_engine(agent)
        # 按请求读到的设置先查一次，不为注定被拒绝的请求建沙箱；以实际沙箱的引擎为准的检查在 _start_task
        if agent_name and not engine.supports_agent_param:
            raise InvalidRequest(f"engine {engine.name} does not support the agent parameter")
        if session_id:
            row_id = await self.store.session_sandbox(agent["id"], session_id)
            row = await self.store.get_sandbox(row_id) if row_id else None
            if row is None or row["state"] not in (SandboxState.ACTIVE.value, SandboxState.PAUSED.value):
                raise TaskConflict(
                    f"session {session_id} is no longer available (its sandbox was rotated or destroyed); start a new session"
                )
            if row["state"] == SandboxState.PAUSED.value:
                await self._resume_one(row)
                row = await self.store.get_sandbox(row["id"])
                if row is None or row["state"] != SandboxState.ACTIVE.value:
                    raise TaskConflict(f"session {session_id} is no longer available; start a new session")
            if self.engine_for(row).name != engine.name:
                # 刚切换了引擎、维护循环还没把旧沙箱转为 RETIRING：与切换后的语义一致，旧会话不能续聊
                raise TaskConflict(
                    f"session {session_id} belongs to engine {self.engine_for(row).name}, but the agent now uses "
                    f"{engine.name}; start a new session"
                )
            max_d = min(max_d, self.hard_deadline(row) - self.now() - 60)
            if max_d < 60:
                raise TaskConflict(f"the sandbox of session {session_id} is about to expire; start a new session")
            try:
                return await self._start_task(agent, row, text=text, source="message", session_id=session_id,
                                              max_duration_s=max_d, agent_name=agent_name)
            except _SandboxGone:
                raise TaskConflict(f"session {session_id} is no longer available; start a new session") from None
        for attempt in range(2):
            row = await self.sandbox_for(agent, min_remaining_s=max_d)
            try:
                return await self._start_task(agent, row, text=text, source="message", max_duration_s=max_d,
                                              agent_name=agent_name)
            except _SandboxGone:
                # 准入与维护循环的销毁撞在同一时刻：换一个沙箱再试一次
                if attempt == 1:
                    raise SandboxOpError("agent sandbox went away while starting the task, retry") from None
        raise AssertionError("unreachable")

    async def _start_task(
        self,
        agent: dict,
        row: dict,
        *,
        text: str,
        source: str,
        session_id: Optional[str] = None,
        schedule_id: Optional[str] = None,
        max_duration_s: float,
        agent_name: Optional[str] = None,
    ) -> TaskRunner:
        engine = self.engine_for(row)
        if agent_name and not engine.supports_agent_param:
            # 请求读到的设置可能早于引擎切换，sandbox_for 按最新设置选了另一种引擎的沙箱：按实际执行的引擎拒绝（R3-02）
            raise InvalidRequest(f"engine {engine.name} does not support the agent parameter")
        task_id = str(uuid.uuid4())
        wait_until = time.monotonic() + self.cfg.agent_wait_sandbox_s
        while True:
            now = self.now()
            task = dict(
                id=task_id,
                agent_id=agent["id"],
                client_id=agent["client_id"],
                source=source,
                schedule_id=schedule_id,
                sandbox_row_id=row["id"],
                session_id=session_id,
                prompt=text,
                result_text=None,
                error=None,
                usage=None,
                created_at=now,
                started_at=now,
                finished_at=None,
                deadline=now + max_duration_s,
                op_owner=self.replica_id,
                op_deadline=now + self.cfg.agent_task_takeover_s,
            )
            status, created = await self.store.create_task(task, max_running=self.cfg.agent_max_running_tasks)
            if status not in ("reloading", "aborting"):
                break
            # reloading：维护循环正在重载该沙箱的配置（dispose 会中止运行中的会话），通常 1~2 秒；
            # aborting：同一会话上一个任务的中止命令可能还在路上（中止它的副本被接管过），等占用到期（R3-01）
            if time.monotonic() >= wait_until:
                raise WaitTimeout(
                    "agent sandbox is still reloading its configuration, retry later" if status == "reloading"
                    else f"session {session_id} is still finishing the abort of its previous task, retry later"
                )
            await asyncio.sleep(_RELOAD_POLL_S)
        if status == "gone":
            raise _SandboxGone()
        if status == "too_many":
            raise TooManyTasks(f"agent already has {self.cfg.agent_max_running_tasks} running tasks")
        if status == "session_busy":
            raise TaskConflict(f"session {session_id} is running another task; attach to it or wait")
        runner = TaskRunner(self, created, row, text=text, agent_name=agent_name)
        self._spawn_runner(runner)
        return runner

    def resume_runner(self, task: dict, row: dict) -> None:
        """接管其他副本留下的任务：只跟进到结束并落库。"""
        self._spawn_runner(TaskRunner(self, task, row, text=None, resume=True))

    async def get_task(self, agent: dict, task_id: str) -> dict:
        task = await self.store.get_task(task_id)
        if task is None or task["agent_id"] != agent["id"]:
            raise AgentNotFound(f"task {task_id} not found")
        return task

    async def attach(self, agent: dict, task_id: str) -> AsyncIterator[Optional[tuple[str, dict]]]:
        """断线重连：本副本上的 runner 直接订阅；在其他副本上时自己订阅沙箱内 agent 的事件做只读翻译，
        只转发这个任务那一轮的输出，直到库里的任务结束（_follow_remote）。"""
        task = await self.get_task(agent, task_id)
        runner = self.runners.get(task_id)
        if runner is not None:
            q = runner.subscribe()
            try:
                while True:
                    item = await q.get()
                    yield item
                    if item is None:
                        return
            finally:
                runner.unsubscribe(q)
            return
        if task["state"] != TaskState.RUNNING.value:
            yield ("done", _task_done(task))
            return
        # 跟随其他副本上的任务：读库、订阅事件都在后台任务里做，这里只读队列；客户端断开时通知后台任务
        # 在两次读库之间自行退出，不在数据库操作中途取消
        out: asyncio.Queue = asyncio.Queue()
        stop = asyncio.Event()
        follower = asyncio.create_task(self._follow_remote(task, out, stop))
        try:
            while True:
                item = await out.get()
                yield item
                if item is None or item[0] == "done":
                    return
        finally:
            stop.set()
            if follower.done():
                await asyncio.gather(follower, return_exceptions=True)

    async def _follow_remote(self, task: dict, out: asyncio.Queue, stop: asyncio.Event) -> None:
        """跟随其他副本上的任务：补发本轮已有的文本，再转发翻译后的事件，直到库里的任务结束。

        一个会话里会先后跑多个任务，而事件和消息都只按会话区分，要按轮次划清归属（R3-04）：
        - 快照只在确认会话正在运行这个任务那一轮时补发（_attach_snapshot）；
        - 这一轮结束（idle / agent_settled）之后的事件不再转发；
        - 事件订阅建立时、会话开始新一轮时立即读库：任务已结束，说明之后的轮次属于后面的任务（任务在库里 RUNNING
          期间同一会话不会准入新任务），就此结束，不转发它们。
        """
        try:
            row = await self.store.get_sandbox(task["sandbox_row_id"])
            out.put_nowait(("start", {"task_id": task["id"], "session_id": task["session_id"],
                                      "sandbox_id": task["sandbox_row_id"], "attached": True}))
            if row is None or not task["session_id"]:
                while not stop.is_set():
                    fresh = await self.store.get_task(task["id"])
                    if fresh is None or fresh["state"] != TaskState.RUNNING.value:
                        out.put_nowait(("done", _task_done(fresh or task)))
                        return
                    await asyncio.sleep(_FOLLOW_POLL_S)
                return
            client = self.client_for(row)
            engine = self.engine_for(row)
            try:
                text = await self._attach_snapshot(client, engine, task)
                if text:
                    out.put_nowait(("text", {"delta": text, "snapshot": True}))
            except Exception as e:  # noqa: BLE001
                log.info("attach snapshot failed: %r", e)
            tr = engine.translator(task["session_id"])
            # 订阅之前这一轮可能已在运行（错过了开始事件）：收到结束事件即为这一轮结束；正在输出的消息错过了开头，
            # 以全文为准（R3-N1）
            tr.busy_seen = tr.mid_round = True
            events: asyncio.Queue = asyncio.Queue()

            async def read():
                try:
                    async for ev in client.events():
                        events.put_nowait(ev)
                except Exception:  # noqa: BLE001 - 以库里的任务状态为准
                    pass

            reader = asyncio.create_task(read())
            try:
                next_check = time.monotonic() + _FOLLOW_POLL_S
                rounds = 0
                while not stop.is_set():
                    try:
                        ev = await asyncio.wait_for(events.get(), timeout=_FOLLOW_POLL_S)
                    except asyncio.TimeoutError:
                        ev = None
                    items: list = []
                    check = time.monotonic() >= next_check
                    if ev is not None:
                        if ev.get("type") == "server.connected":
                            # 取快照与订阅之间任务可能已经结束、会话已开始下一个任务（错过了它开始的事件）
                            check = True
                        else:
                            ended = tr.idle
                            items = tr.feed(ev)
                            if tr.round_starts != rounds:
                                rounds = tr.round_starts
                                check = True
                            if ended:
                                items = []
                    if check:
                        next_check = time.monotonic() + _FOLLOW_POLL_S
                        fresh = await self.store.get_task(task["id"])
                        if fresh is None or fresh["state"] != TaskState.RUNNING.value:
                            out.put_nowait(("done", _task_done(fresh or task)))
                            return
                    for item in items:
                        out.put_nowait(item)
            finally:
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)
        except Exception as e:  # noqa: BLE001
            log.warning("follow task %s failed: %r", task["id"], e)
            out.put_nowait(None)

    async def _attach_snapshot(self, client, engine: Engine, task: dict) -> str:
        """重连时补发的本轮已有文本。只在确认取到的消息里最后一轮正是这个任务那一轮时补发，否则为空（R3-04）：

        - 会话不忙：这一轮还没开始（提示词未送达，会话里最后一轮是上一个任务的；连续两轮提示词相同时比对文本也分不出），
          或者已经结束（done 随后就到，带最终结果）。opencode 先写用户消息再置忙，所以忙就说明本轮用户消息已在会话里；
        - 支持 run_id 的引擎（pi）：正在运行的必须是这个任务的 run；而且桥接进程受理提示词时就记为忙，pi 回复受理之后
          还要经过几次 await 才把用户消息写进会话，这期间最后一条用户消息仍是上一轮的。所以按身份绑定：最后一条用户
          消息必须是 runner 从带同一 runID 的事件记下的那条（prompt_key），不能只比对提示词文本；
        - 其他引擎：最后一条用户消息必须是这个任务的提示词；
        - 取消息期间会话可能已换到下一轮（验收报告 3.2）：取到消息之后任务在库里仍是 RUNNING，才能说明其中没有后面
          任务的轮次——后面的任务要等本任务结束才准入，它的提示词要等准入之后才发出。
        """
        sid = task["session_id"]
        busy = (await client.status()).get(sid)
        if not busy:
            return ""
        fresh = await self.store.get_task(task["id"])
        if fresh is None or fresh["state"] != TaskState.RUNNING.value:
            return ""
        if engine.has_runs and (
            not fresh["run_id"] or busy.get("run_id") != fresh["run_id"] or not fresh["prompt_key"]
        ):
            return ""
        messages = await client.messages(sid)
        if engine.has_runs:
            if engine.last_prompt_key(messages) != fresh["prompt_key"]:
                return ""
        elif engine.last_prompt(messages) != engine.prompt_text(task["prompt"]):
            return ""
        after = await self.store.get_task(task["id"])
        if after is None or after["state"] != TaskState.RUNNING.value:
            return ""
        return engine.extract_result(messages)[0]

    async def abort_task(self, agent: dict, task_id: str) -> dict:
        """中止运行中的任务：只在库里记中止标记，中止命令由跟进该任务的 runner 下发（R3-01）。

        引擎的中止按会话生效（opencode、pi 桥接进程都没有按运行中止的接口），这里直接下发的话，命令可能在任务结束、
        同一会话开始下一个任务之后才到，中止掉别的任务。runner 下发前在库里占住会话、限时下发（见 TaskRunner._check_abort），
        占用期间同一会话不准入新任务。本副本的 runner 立即处理，其他副本的 runner 每秒查一次库，负责副本崩溃时由接管的
        副本下发。
        """
        task = await self.get_task(agent, task_id)
        if task["state"] != TaskState.RUNNING.value:
            raise TaskConflict(f"task {task_id} is already {task['state']}")
        if not await self.store.cas_task(task_id, [TaskState.RUNNING], abort_requested=1):
            task = await self.get_task(agent, task_id)
            raise TaskConflict(f"task {task_id} is already {task['state']}")
        runner = self.runners.get(task_id)
        if runner is not None:
            runner.request_abort()
        return await self.get_task(agent, task_id)

    # ---------- 定时任务 ----------

    def _next_run(self, schedule: dict, after: float) -> float:
        if schedule.get("every_s"):
            return after + float(schedule["every_s"])
        return cron.next_fire(schedule["cron"], schedule["timezone"], after)

    def _validate_schedule(self, body: dict, base: Optional[dict] = None) -> dict:
        merged = {**(base or {}), **{k: v for k, v in body.items() if v is not None or k in ("cron", "every_s")}}
        allowed = {"name", "cron", "every_s", "timezone", "prompt", "enabled", "max_duration_s", "overlap"}
        unknown = set(body) - allowed
        if unknown:
            raise InvalidRequest(f"unknown schedule fields: {sorted(unknown)}")
        name = str(merged.get("name") or "").strip()
        prompt = str(merged.get("prompt") or "").strip()
        if not name or len(name) > 128:
            raise InvalidRequest("name must be 1..128 characters")
        if not prompt:
            raise InvalidRequest("prompt is required")
        expr, every = merged.get("cron"), merged.get("every_s")
        if bool(expr) == bool(every):
            raise InvalidRequest("exactly one of cron / every_s is required")
        tz = merged.get("timezone") or "Asia/Shanghai"
        try:
            cron.zone(tz)
            if expr:
                cron.next_fire(expr, tz, self.now())
        except ValueError as e:
            raise InvalidRequest(str(e)) from e
        if every is not None and (not isinstance(every, (int, float)) or every < MIN_EVERY_S):
            raise InvalidRequest(f"every_s must be >= {MIN_EVERY_S}")
        max_d = merged.get("max_duration_s")
        if max_d is not None and (not isinstance(max_d, (int, float)) or not 60 <= max_d <= self.cfg.agent_task_max_duration_s):
            raise InvalidRequest(f"max_duration_s must be within 60..{self.cfg.agent_task_max_duration_s:g}")
        overlap = merged.get("overlap") or "skip"
        if overlap not in ("skip", "allow"):
            raise InvalidRequest("overlap must be skip or allow")
        return dict(
            name=name,
            cron=expr or None,
            every_s=float(every) if every else None,
            timezone=tz,
            prompt=prompt,
            enabled=1 if merged.get("enabled", True) else 0,
            max_duration_s=float(max_d) if max_d else None,
            overlap=overlap,
        )

    async def create_schedule(self, agent: dict, body: dict) -> dict:
        values = self._validate_schedule(body)
        now = self.now()
        row = dict(
            id=str(uuid.uuid4()),
            agent_id=agent["id"],
            client_id=agent["client_id"],
            **values,
            next_run_at=self._next_run(values, now) if values["enabled"] else None,
            last_run_at=None,
            last_task_id=None,
            created_at=now,
            updated_at=now,
        )
        await self.store.insert_schedule(row)
        self.maintainer.kick()
        return await self.store.get_schedule(row["id"])

    async def get_schedule(self, agent: dict, schedule_id: str) -> dict:
        s = await self.store.get_schedule(schedule_id)
        if s is None or s["agent_id"] != agent["id"]:
            raise AgentNotFound(f"schedule {schedule_id} not found")
        return s

    async def update_schedule(self, agent: dict, schedule_id: str, body: dict) -> dict:
        s = await self.get_schedule(agent, schedule_id)
        values = self._validate_schedule(body, base={k: s[k] for k in ("name", "cron", "every_s", "timezone", "prompt",
                                                                        "max_duration_s", "overlap")}
                                         | {"enabled": bool(s["enabled"])})
        now = self.now()
        timing = ("cron", "every_s", "timezone", "enabled")
        if not values["enabled"]:
            values["next_run_at"] = None
        elif any(values[k] != s[k] for k in timing) or s["next_run_at"] is None:
            # 只有触发时间相关的字段变化才重算；改名字、提示词等不打乱固定间隔任务的节奏
            values["next_run_at"] = self._next_run(values, now)
        await self.store.update_schedule(schedule_id, updated_at=now, **values)
        self.maintainer.kick()
        return await self.store.get_schedule(schedule_id)

    async def delete_schedule(self, agent: dict, schedule_id: str) -> dict:
        await self.get_schedule(agent, schedule_id)
        await self.store.delete_schedule(schedule_id)
        return {"deleted": schedule_id}

    async def run_schedule_now(self, agent: dict, schedule_id: str) -> Optional[dict]:
        s = await self.get_schedule(agent, schedule_id)
        runner = await self.fire_schedule(s, manual=True)
        return runner.task if runner else None

    async def fire_schedule(self, schedule: dict, *, manual: bool = False) -> Optional[TaskRunner]:
        """触发一次定时任务：overlap=skip 且上一次还在运行时跳过（返回 None）；需要时新建沙箱。

        开不了任务（并发上限、容量已满、等沙箱超时、建沙箱失败）时记事件；手动触发（manual）时把错误抛给调用方，
        接口据此返回 429 / 503 / 504 / 502，而不是和「跳过」一样的结果。
        """
        agent = await self.store.get_agent_by_id(schedule["agent_id"])
        if agent is None:
            return None
        if schedule["overlap"] == "skip" and await self.store.running_tasks(schedule_id=schedule["id"]):
            await self.lc.event("schedule_skipped", detail=f"schedule={schedule['id']} previous run still running")
            return None
        max_d = schedule["max_duration_s"] or self.cfg.agent_task_max_duration_s
        try:
            row = await self.sandbox_for(agent, min_remaining_s=max_d)
            runner = await self._start_task(agent, row, text=schedule["prompt"], source="schedule",
                                            schedule_id=schedule["id"], max_duration_s=max_d)
        except (TooManyTasks, AgentUnavailable, WaitTimeout, SandboxOpError, _SandboxGone) as e:
            log.warning("schedule %s not fired: %r", schedule["id"], e)
            await self.lc.event("schedule_failed", detail=f"schedule={schedule['id']} {type(e).__name__}: {e}"[:500])
            if manual:
                if isinstance(e, _SandboxGone):
                    raise SandboxOpError("agent sandbox went away while starting the task, retry") from None
                raise
            return None
        await self.store.update_schedule(schedule["id"], last_task_id=runner.task_id)
        await self.lc.event("schedule_fired", detail=f"schedule={schedule['id']} task={runner.task_id}")
        return runner

    # ---------- 管理员 ----------

    async def stats(self) -> dict:
        counts = await self.store.count_by_state()
        durations = await self.store.event_durations()
        return {
            "pool": self.cfg.agent_pool_name,
            "replica": self.replica_id,
            "template": self.cfg.agent_template,
            "model": self.cfg.agent_model,
            "engines": {n: {"template": e.template, "model": e.model} for n, e in self.enabled_engines().items()},
            "default_engine": self.cfg.agent_default_engine,
            "sandboxes": {"total": sum(counts.values()), **counts},
            "running_tasks": len(await self.store.running_tasks()),
            "local_runners": len(self.runners),
            "events": await self.store.event_counts(),
            "latency_ms": {
                kind: {"count": len(v), "p50": _percentile(v, 50), "p99": _percentile(v, 99), "max": round(max(v), 1)}
                for kind, v in sorted(durations.items())
            },
        }

    async def admin_list(self) -> list[dict]:
        out = []
        for agent in await self.store.list_agents():
            info = await self.agent_info(agent)
            info["client_id"] = agent["client_id"]
            out.append(info)
        return out


class _SandboxGone(Exception):
    """准入之后发现沙箱已不在服务（刚被维护循环销毁）。"""


def _task_done(task: dict) -> dict:
    return {
        "task_id": task["id"],
        "state": task["state"],
        "result": task.get("result_text"),
        "usage": task.get("usage"),
        "error": task.get("error"),
        "session_id": task.get("session_id"),
    }
