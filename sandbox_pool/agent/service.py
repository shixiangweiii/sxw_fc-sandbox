"""agent 子系统的组装入口与请求路径：每个（调用方，用户）一个常驻 opencode agent。

一个进程（副本）一个 AgentService，与代码执行池（SandboxPool）共用 provider 和数据库，使用独立的池名。
多副本协调沿用仓库约定：状态存库、CAS、池级锁行，不选主。后台维护见 maintainer.py，任务执行见 runner.py。
"""

import asyncio
import json
import logging
import time
import uuid
from typing import AsyncIterator, Optional

from sandbox_pool.agent import cron
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
    render_agents_md,
    render_opencode_config,
    validate_settings,
    build_network,
)
from sandbox_pool.agent.runner import TaskRunner, Translator, extract_result
from sandbox_pool.config import PoolConfig
from sandbox_pool.core.lifecycle import Lifecycle
from sandbox_pool.core.pool import _percentile, new_replica_id
from sandbox_pool.models import (
    AgentNotFound,
    AgentUnavailable,
    InvalidRequest,
    SandboxOpError,
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
MIN_EVERY_S = 60


def check_agent_config(cfg: PoolConfig) -> None:
    """启动时校验 agent 配置，有误直接拒绝启动。"""
    if not cfg.agent_enabled:
        return
    errors = []
    if not cfg.agent_template:
        errors.append("POOL_AGENT_TEMPLATE is required (see scripts/build_opencode_template.py)")
    if "/" not in cfg.agent_model:
        errors.append("POOL_AGENT_MODEL must look like provider/model")
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
        self.default_egress: EgressPolicy = default_policy(cfg.agent_egress)
        self.default_mcp: dict = default_mcp(cfg.agent_mcp)
        self.injections = load_injections(cfg.agent_model_host, cfg.agent_model_api_key, cfg.agent_inject)
        self.maintainer = AgentMaintainer(self)
        self.lc.on_change = self.maintainer.kick
        self.runners: dict[str, TaskRunner] = {}
        self._runner_tasks: set[asyncio.Task] = set()
        self._clients: dict[str, object] = {}
        self._boot_errors: dict[str, str] = {}
        # 停机中：维护循环不再接管任务、不再触发定时任务
        self.stopping = False

    @staticmethod
    def now() -> float:
        return time.time()

    async def start(self, *, run_maintainer: bool = True) -> None:
        await self.store.init_schema()
        if run_maintainer:
            await self.maintainer.start()
        log.info("agent service replica %s started (template=%s)", self.replica_id, self.cfg.agent_template)

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

    def client_for(self, row: dict):
        client = self._clients.get(row["id"])
        if client is None:
            client = self.provider.app_client(
                row["endpoint"], row["access_token"], self.cfg.agent_workdir, ingress_ip=self.cfg.agent_ingress_ip or None
            )
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
        return {
            "user_id": agent["user_id"],
            "agent_id": agent["id"],
            "created_at": agent["created_at"],
            "settings": self.agent_settings(agent),
            "settings_version": agent["settings_version"],
            "egress_override": agent.get("egress"),
            "sandboxes": [sandbox_view(r, self.cfg) for r in rows],
            "running_tasks": [t["id"] for t in running],
        }

    async def update_settings(self, agent: dict, patch: dict) -> dict:
        merged = {**(agent.get("settings") or {}), **validate_settings(patch)}
        await self.store.update_agent(agent["id"], now=self.now(), settings=merged)
        self.maintainer.kick()
        return await self.agent_info(await self.store.get_agent_by_id(agent["id"]))

    # ---------- 沙箱 ----------

    async def sandbox_for(self, agent: dict, *, min_remaining_s: float = 0, wait_s: Optional[float] = None) -> dict:
        """该 agent 可接新任务的 ACTIVE 沙箱；没有就创建（后台）并等待就绪。

        剩余寿命不足 min_remaining_s 的沙箱转为 RETIRING（跑完手头任务后销毁），新任务去新沙箱。
        """
        wait_s = self.cfg.agent_wait_sandbox_s if wait_s is None else wait_s
        deadline = self.now() + wait_s
        # 新沙箱至少要能满足要求，否则每个沙箱都会被判为剩余寿命不足
        min_remaining_s = min(min_remaining_s, self.cfg.agent_max_life_s * 0.9)
        attempts = 0
        while True:
            now = self.now()
            rows = await self.store.agent_sandboxes(agent["id"], BOUND_STATES)
            active = [r for r in rows if r["state"] == SandboxState.ACTIVE.value]
            if active:
                row = active[0]
                if min_remaining_s and self.hard_deadline(row) - now < min_remaining_s:
                    if await self.store.cas_sandbox(
                        row["id"], [SandboxState.ACTIVE], now=now, expect_version=row["version"], state=SandboxState.RETIRING
                    ):
                        log.info("sandbox %s retiring: remaining life < %ss", row["provider_id"], min_remaining_s)
                        await self.lc.event("agent_retire", sandbox_row_id=row["id"], detail="remaining life too short")
                    continue
                return row
            if not rows:
                if attempts >= _MAX_BOOT_ATTEMPTS:
                    raise SandboxOpError(
                        f"agent sandbox failed to start {attempts} times: {self._boot_errors.get(agent['id'], 'unknown')}"
                    )
                status, row = await self.store.reserve_agent_sandbox(
                    agent_id=agent["id"],
                    template=self.cfg.agent_template,
                    now=now,
                    owner=self.replica_id,
                    op_deadline=now + self.cfg.agent_boot_timeout_s,
                    limit=self.cfg.agent_max_sandboxes,
                )
                if status == "full":
                    raise AgentUnavailable(f"agent sandbox capacity reached ({self.cfg.agent_max_sandboxes})")
                if status == "created":
                    attempts += 1
                    self.lc.spawn(self._boot(row, agent))
            if now >= deadline:
                raise WaitTimeout(f"agent sandbox not ready within {wait_s:.0f}s")
            await asyncio.sleep(_SANDBOX_POLL_S)

    async def _boot(self, row: dict, agent: dict) -> None:
        """CREATING → WARMING → ACTIVE：建沙箱（出网规则 + 凭证注入）、等 opencode 健康、写配置文件、预加载实例。"""
        provider_id = None
        t0 = time.perf_counter()
        try:
            policy = self.effective_policy(agent)
            version = network_version(policy, self.injections)
            timeout = self.cfg.agent_platform_timeout_s
            app = await self.provider.create_app(
                self.cfg.agent_template,
                {"pool": self.cfg.agent_pool_name, "pool_row": row["id"], "agent": agent["id"], "replica": self.replica_id},
                timeout,
                port=self.cfg.agent_port,
                network=build_network(policy, self.injections),
            )
            provider_id = app.sandbox_id
            await self.lc.event("agent_create", sandbox_row_id=row["id"], duration_ms=(time.perf_counter() - t0) * 1000)
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
            client = self.client_for(row)
            await self._wait_healthy(client, timeout_s=self.cfg.agent_boot_timeout_s / 2)
            fresh_agent = await self.store.get_agent_by_id(agent["id"]) or agent
            await self._write_agent_files(row, fresh_agent, policy, version)
            # 预加载工作目录的实例（读取 opencode.json、连接 MCP），首个请求不必等
            await client.status()
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
            self._boot_errors.pop(agent["id"], None)
            await self.lc.event("agent_boot", sandbox_row_id=row["id"], duration_ms=(time.perf_counter() - t0) * 1000)
            log.info("agent %s sandbox %s ready in %.1fs", agent["id"], app.sandbox_id, time.perf_counter() - t0)
        except asyncio.CancelledError:
            raise
        except Exception as e:  # noqa: BLE001
            log.warning("boot agent sandbox failed: %r", e, exc_info=True)
            self._boot_errors[agent["id"]] = f"{type(e).__name__}: {e}"[:500]
            await self.lc.event("agent_boot_failed", sandbox_row_id=row["id"], detail=str(e)[:500])
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
        raise RuntimeError(f"opencode not healthy within {timeout_s:.0f}s: {last!r}")

    async def _kill_quietly(self, provider_id: str) -> None:
        try:
            await self.provider.kill(provider_id)
        except Exception:  # noqa: BLE001 - 留给对账清理
            log.exception("kill %s failed, leaving it to reconcile", provider_id)

    def _agent_files(self, agent: dict, policy: EgressPolicy, version: str, *, with_config: bool) -> dict[str, bytes]:
        egress = {**describe(policy, self.injections, version), "applied_at": self.now()}
        files = {EGRESS_FILE: json.dumps(egress, ensure_ascii=False, indent=2).encode()}
        if with_config:
            settings = self.agent_settings(agent)
            mcp = {**self.default_mcp, **settings["mcp"]}
            files[f"{self.cfg.agent_workdir}/opencode.json"] = json.dumps(
                render_opencode_config(self.cfg.agent_model, mcp), ensure_ascii=False, indent=2
            ).encode()
            files[f"{self.cfg.agent_workdir}/AGENTS.md"] = render_agents_md(
                workdir=self.cfg.agent_workdir,
                max_life_h=self.cfg.agent_max_life_s / 3600,
                idle_destroy_after_s=settings["idle_destroy_after_s"],
                mcp_names=sorted(mcp),
                egress=egress,
                instructions=settings["instructions"],
            ).encode()
        return files

    async def _write_agent_files(self, row: dict, agent: dict, policy: EgressPolicy, version: str) -> None:
        await self.provider.write_files(
            row["provider_id"], self._agent_files(agent, policy, version, with_config=True),
            sandbox_timeout_s=self.platform_timeout_for(row),
        )

    async def apply_settings(self, row: dict, agent: dict) -> bool:
        """设置变更：重写 opencode.json / AGENTS.md，重载工作目录实例。

        重载（dispose）会中止沙箱里所有运行中的会话（opencode 实例销毁时取消全部会话），所以先在池级锁内确认沙箱
        没有运行中任务并占住它，期间新任务的准入会等待（AgentStore.begin_reload / create_task）。整体限时，保证 dispose
        不会在占用过期之后才发出。有任务在跑或其他副本正在重载时返回 False，维护循环下一轮再试。
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
            row["provider_id"], self._agent_files(agent, policy, version, with_config=False),
            sandbox_timeout_s=self.platform_timeout_for(row),
        )
        ok = await self.store.cas_sandbox(row["id"], [row["state"]], now=self.now(), network_version=version)
        await self.lc.event("agent_network_applied", sandbox_row_id=row["id"], detail=version)
        return ok

    async def reset(self, agent: dict) -> dict:
        """销毁该 agent 的全部沙箱（运行中任务记为失败），下次请求重新创建。"""
        destroyed = 0
        for row in await self.store.agent_sandboxes(agent["id"]):
            if row["state"] in (SandboxState.ACTIVE.value, SandboxState.RETIRING.value):
                await self.fail_tasks(row["id"], "sandbox reset by user")
                if await self.lc.destroy(row, "reset by user"):
                    destroyed += 1
        self.maintainer.kick()
        return {"destroyed": destroyed}

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
        if session_id:
            row_id = await self.store.session_sandbox(agent["id"], session_id)
            row = await self.store.get_sandbox(row_id) if row_id else None
            if row is None or row["state"] != SandboxState.ACTIVE.value:
                raise TaskConflict(
                    f"session {session_id} is no longer available (its sandbox was rotated or destroyed); start a new session"
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
            if status != "reloading":
                break
            # 维护循环正在重载该沙箱的配置（dispose 会中止运行中的会话），通常 1~2 秒
            if time.monotonic() >= wait_until:
                raise WaitTimeout("agent sandbox is still reloading its configuration, retry later")
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
        """断线重连：本副本上的 runner 直接订阅；在其他副本上时自己订阅 opencode 事件做只读翻译，直到库里的任务结束。"""
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
                    await asyncio.sleep(1.0)
                return
            client = self.client_for(row)
            try:
                # 已产生的输出：取当前消息补发一次文本
                text, _, _ = extract_result(await client.messages(task["session_id"]))
                if text:
                    out.put_nowait(("text", {"delta": text, "snapshot": True}))
            except Exception as e:  # noqa: BLE001
                log.info("attach snapshot failed: %r", e)
            tr = Translator(task["session_id"])
            tr.busy_seen = True
            events: asyncio.Queue = asyncio.Queue()

            async def read():
                try:
                    async for ev in client.events():
                        events.put_nowait(ev)
                except Exception:  # noqa: BLE001 - 以库里的任务状态为准
                    pass

            reader = asyncio.create_task(read())
            try:
                next_check = time.monotonic() + 1.0
                while not stop.is_set():
                    try:
                        ev = await asyncio.wait_for(events.get(), timeout=1.0)
                        for item in tr.feed(ev):
                            out.put_nowait(item)
                    except asyncio.TimeoutError:
                        pass
                    if time.monotonic() >= next_check:
                        next_check = time.monotonic() + 1.0
                        fresh = await self.store.get_task(task["id"])
                        if fresh is None or fresh["state"] != TaskState.RUNNING.value:
                            out.put_nowait(("done", _task_done(fresh or task)))
                            return
            finally:
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)
        except Exception as e:  # noqa: BLE001
            log.warning("follow task %s failed: %r", task["id"], e)
            out.put_nowait(None)

    async def abort_task(self, agent: dict, task_id: str) -> dict:
        task = await self.get_task(agent, task_id)
        if task["state"] != TaskState.RUNNING.value:
            raise TaskConflict(f"task {task_id} is already {task['state']}")
        await self.store.cas_task(task_id, [TaskState.RUNNING], abort_requested=1)
        runner = self.runners.get(task_id)
        if runner is not None:
            runner.request_abort()
        row = await self.store.get_sandbox(task["sandbox_row_id"])
        if row is not None and task["session_id"]:
            try:
                await self.client_for(row).abort(task["session_id"])
            except Exception as e:  # noqa: BLE001 - 跟进任务的 runner 会按 abort_requested 再次中止
                log.warning("abort session %s failed: %r", task["session_id"], e)
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
