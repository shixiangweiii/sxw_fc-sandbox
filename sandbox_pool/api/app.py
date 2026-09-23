from contextlib import asynccontextmanager
from typing import Optional

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse

from sandbox_pool.api.routes import router
from sandbox_pool.config import PoolConfig
from sandbox_pool.core.pool import SandboxPool
from sandbox_pool.models import (
    LeaseNotActive,
    LeaseNotFound,
    PoolError,
    QueueFull,
    SandboxOpError,
    WaitTimeout,
)
from sandbox_pool.provider.base import SandboxProvider

_STATUS = {
    QueueFull: 429,
    WaitTimeout: 504,
    LeaseNotFound: 404,
    LeaseNotActive: 409,
    SandboxOpError: 502,
}


def create_app(
    cfg: Optional[PoolConfig] = None,
    *,
    provider: Optional[SandboxProvider] = None,
    pool: Optional[SandboxPool] = None,
) -> FastAPI:
    """传入 pool 时由调用方负责其启动和停止（测试用）；否则随应用生命周期启动。"""
    managed = pool is None

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if managed:
            config = cfg or PoolConfig.from_env()
            if provider is None:
                from sandbox_pool.provider.e2b_provider import E2BProvider

                prov = E2BProvider()
            else:
                prov = provider
            app.state.pool = SandboxPool(config, prov)
            await app.state.pool.start()
        try:
            yield
        finally:
            if managed:
                await app.state.pool.stop()

    app = FastAPI(title="Sandbox Pool", version="0.1.0", lifespan=lifespan)
    if pool is not None:
        app.state.pool = pool
    app.include_router(router)

    @app.exception_handler(PoolError)
    async def _pool_error(_request: Request, exc: PoolError):
        return JSONResponse(
            status_code=_STATUS.get(type(exc), 500), content={"error": type(exc).__name__, "detail": str(exc)}
        )

    @app.exception_handler(FileNotFoundError)
    async def _file_not_found(_request: Request, exc: FileNotFoundError):
        return JSONResponse(status_code=404, content={"error": "FileNotFound", "detail": str(exc)})

    return app
