from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from sandbox_pool.api.auth import Authenticator
from sandbox_pool.api.routes import router
from sandbox_pool.config import PoolConfig
from sandbox_pool.core.pool import SandboxPool
from sandbox_pool.models import (
    Forbidden,
    LeaseNotActive,
    LeaseNotFound,
    PayloadTooLarge,
    PoolDraining,
    PoolError,
    QueueFull,
    SandboxOpError,
    Unauthorized,
    WaitTimeout,
)
from sandbox_pool.provider.base import SandboxProvider

_STATUS = {
    Unauthorized: 401,
    Forbidden: 403,
    LeaseNotFound: 404,
    LeaseNotActive: 409,
    PayloadTooLarge: 413,
    QueueFull: 429,
    SandboxOpError: 502,
    PoolDraining: 503,
    WaitTimeout: 504,
}


def create_app(
    cfg: Optional[PoolConfig] = None,
    *,
    provider: Optional[SandboxProvider] = None,
    pool: Optional[SandboxPool] = None,
) -> FastAPI:
    """传入 pool 时由调用方负责其启动和停止（测试用）；否则随应用生命周期启动。"""
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
        try:
            yield
        finally:
            if managed:
                await app.state.pool.stop()

    app = FastAPI(title="Sandbox Pool", version="0.2.0", lifespan=lifespan)
    app.state.auth = Authenticator.from_config(config)
    if pool is not None:
        app.state.pool = pool
    app.include_router(router)

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
