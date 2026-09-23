import dataclasses

from fastapi import APIRouter, Body, Query, Request, Response

from sandbox_pool.api.schemas import (
    AcquireRequest,
    CommandRequest,
    CommandResponse,
    LeaseOut,
    RenewRequest,
    RunCodeRequest,
    RunCodeResponse,
)
from sandbox_pool.core.pool import SandboxPool

router = APIRouter()


def _pool(request: Request) -> SandboxPool:
    return request.app.state.pool


@router.get("/healthz")
async def healthz(request: Request):
    return {"ok": True, "replica": _pool(request).replica_id}


@router.post("/v1/leases", response_model=LeaseOut)
async def acquire(request: Request, body: AcquireRequest = Body(default_factory=AcquireRequest)):
    pool = _pool(request)
    grant = await pool.allocator.acquire(
        wait_timeout_s=body.wait_timeout_s,
        lease_ttl_s=body.lease_ttl_s,
        is_disconnected=request.is_disconnected,
    )
    return LeaseOut.from_row(await pool.allocator.get_lease(grant.lease_id))


@router.get("/v1/leases/{lease_id}", response_model=LeaseOut)
async def get_lease(lease_id: str, request: Request):
    return LeaseOut.from_row(await _pool(request).allocator.get_lease(lease_id))


@router.post("/v1/leases/{lease_id}/renew", response_model=LeaseOut)
async def renew(lease_id: str, request: Request, body: RenewRequest = Body(default_factory=RenewRequest)):
    return LeaseOut.from_row(await _pool(request).allocator.renew(lease_id, body.ttl_s))


@router.delete("/v1/leases/{lease_id}", response_model=LeaseOut)
async def release(lease_id: str, request: Request):
    return LeaseOut.from_row(await _pool(request).allocator.release(lease_id))


@router.post("/v1/leases/{lease_id}/run_code", response_model=RunCodeResponse)
async def run_code(lease_id: str, body: RunCodeRequest, request: Request):
    r = await _pool(request).allocator.run_code(
        lease_id, body.code, language=body.language, timeout_s=body.timeout_s
    )
    return RunCodeResponse(**dataclasses.asdict(r))


@router.post("/v1/leases/{lease_id}/commands", response_model=CommandResponse)
async def run_command(lease_id: str, body: CommandRequest, request: Request):
    r = await _pool(request).allocator.run_command(
        lease_id, body.cmd, cwd=body.cwd, envs=body.envs, timeout_s=body.timeout_s
    )
    return CommandResponse(**dataclasses.asdict(r))


@router.put("/v1/leases/{lease_id}/files")
async def write_file(lease_id: str, request: Request, path: str = Query(...)):
    data = await request.body()
    await _pool(request).allocator.write_file(lease_id, path, data)
    return {"path": path, "size": len(data)}


@router.get("/v1/leases/{lease_id}/files")
async def read_file(lease_id: str, request: Request, path: str = Query(...)):
    data = await _pool(request).allocator.read_file(lease_id, path)
    return Response(content=data, media_type="application/octet-stream")


@router.get("/v1/pool/stats")
async def stats(request: Request):
    return await _pool(request).stats()


@router.get("/v1/sandboxes")
async def list_sandboxes(request: Request):
    return await _pool(request).store.list_sandboxes()
