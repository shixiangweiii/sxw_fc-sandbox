from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from sandbox_pool.api.agent_routes import router as agent_router
from sandbox_pool.api.auth import Authenticator
from sandbox_pool.api.body_limit import BodyLimitMiddleware, BodyTooLarge, too_large_response
from sandbox_pool.api.routes import router
from sandbox_pool.config import PoolConfig
from sandbox_pool.core.pool import SandboxPool
from sandbox_pool.models import (
    AgentNotFound,
    AgentUnavailable,
    Forbidden,
    InvalidRequest,
    LeaseNotActive,
    LeaseNotFound,
    PayloadTooLarge,
    PoolDraining,
    PoolError,
    QueueFull,
    SandboxBusy,
    SandboxOpError,
    SandboxRecordNotFound,
    TaskConflict,
    TooManyTasks,
    Unauthorized,
    WaitTimeout,
)
from sandbox_pool.provider.base import SandboxProvider

_STATUS = {
    InvalidRequest: 400,
    Unauthorized: 401,
    Forbidden: 403,
    LeaseNotFound: 404,
    SandboxRecordNotFound: 404,
    AgentNotFound: 404,
    LeaseNotActive: 409,
    SandboxBusy: 409,
    TaskConflict: 409,
    PayloadTooLarge: 413,
    QueueFull: 429,
    TooManyTasks: 429,
    SandboxOpError: 502,
    PoolDraining: 503,
    AgentUnavailable: 503,
    WaitTimeout: 504,
}


def create_app(
    cfg: Optional[PoolConfig] = None,
    *,
    provider: Optional[SandboxProvider] = None,
    pool: Optional[SandboxPool] = None,
    agents=None,
) -> FastAPI:
    """传入 pool（及 agents）时由调用方负责其启动和停止（测试用）；否则随应用生命周期启动。

    POOL_AGENT_ENABLED=true 时同时启动 agent 子系统（AgentService），与代码执行池共用 provider。
    """
    managed = pool is None
    config = pool.cfg if pool is not None else (cfg or PoolConfig.from_env())

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if managed:
            if provider is None:
                from sandbox_pool.provider.e2b_provider import E2BProvider

                prov = E2BProvider(cache_max=config.handle_cache_max, cache_idle_s=config.handle_idle_ttl_s)
            else:
                prov = provider
            app.state.pool = SandboxPool(config, prov)
            await app.state.pool.start()
            if config.agent_enabled:
                from sandbox_pool.agent.service import AgentService
                from sandbox_pool.store.agent_repo import AgentStore

                # 共用代码执行池的数据库引擎：每进程只有一个 SQLite 写连接（见 store/db.py），由代码执行池负责关闭
                pool_store = app.state.pool.store
                store = AgentStore(pool_store.engine, config.agent_pool_name, read_engine=pool_store.read_engine)
                app.state.agents = AgentService(config, prov, store=store, replica_id=app.state.pool.replica_id)
                await app.state.agents.start()
        try:
            yield
        finally:
            if managed:
                # agent 先停（runner 让出任务给其他副本），再停代码执行池（它负责关闭共用的 provider）
                if app.state.agents is not None:
                    await app.state.agents.stop()
                await app.state.pool.stop()

    app = FastAPI(title="Sandbox Pool", version="0.2.0", lifespan=lifespan)
    app.state.auth = Authenticator.from_config(config)
    app.state.agents = agents
    if pool is not None:
        app.state.pool = pool
    app.include_router(router)
    app.include_router(agent_router)
    app.add_middleware(BodyLimitMiddleware, max_body_bytes=config.max_body_bytes)

    @app.exception_handler(BodyTooLarge)
    async def _body_too_large(_request: Request, exc: BodyTooLarge):
        return too_large_response(exc.detail)

    @app.exception_handler(PoolError)
    async def _pool_error(_request: Request, exc: PoolError):
        return JSONResponse(
            status_code=_STATUS.get(type(exc), 500),
            content={"error": type(exc).__name__, "detail": str(exc)},
            headers={"WWW-Authenticate": "Bearer"} if isinstance(exc, Unauthorized) else None,
        )

    @app.exception_handler(FileNotFoundError)
    async def _file_not_found(_request: Request, exc: FileNotFoundError):
        return JSONResponse(status_code=404, content={"error": "FileNotFound", "detail": str(exc)})

    return app
