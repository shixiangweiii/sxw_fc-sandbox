import dataclasses

from fastapi import APIRouter, Body, Depends, Query, Request, Response

from sandbox_pool.api.auth import Caller, require_admin, require_caller
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
from sandbox_pool.models import PayloadTooLarge

router = APIRouter()


def _pool(request: Request) -> SandboxPool:
    return request.app.state.pool


async def _read_body(request: Request, limit: int) -> bytes:
    """先看 Content-Length，再流式读取并计数，超过上限返回 413，不会把超大请求体整个读进内存。"""
    declared = request.headers.get("content-length")
    if declared is not None and declared.isdigit() and int(declared) > limit:
        raise PayloadTooLarge(f"file exceeds {limit} bytes")
    buf = bytearray()
    async for chunk in request.stream():
        buf += chunk
        if len(buf) > limit:
            raise PayloadTooLarge(f"file exceeds {limit} bytes")
    return bytes(buf)


@router.get("/healthz")
async def healthz(request: Request):
    return {"ok": True, "replica": _pool(request).replica_id}


@router.post("/v1/leases", response_model=LeaseOut)
async def acquire(
    request: Request,
    body: AcquireRequest = Body(default_factory=AcquireRequest),
    caller: Caller = Depends(require_caller),
):
    pool = _pool(request)
    grant = await pool.allocator.acquire(
        wait_timeout_s=body.wait_timeout_s,
        lease_ttl_s=body.lease_ttl_s,
        is_disconnected=request.is_disconnected,
        client_id=caller.name,
    )
    return LeaseOut.from_row(await pool.allocator.get_lease(grant.lease_id))


@router.get("/v1/leases/{lease_id}", response_model=LeaseOut)
async def get_lease(lease_id: str, request: Request, caller: Caller = Depends(require_caller)):
    return LeaseOut.from_row(await _pool(request).allocator.get_lease(lease_id, owner=caller.owner))


@router.post("/v1/leases/{lease_id}/renew", response_model=LeaseOut)
async def renew(
    lease_id: str,
    request: Request,
    body: RenewRequest = Body(default_factory=RenewRequest),
    caller: Caller = Depends(require_caller),
):
    return LeaseOut.from_row(await _pool(request).allocator.renew(lease_id, body.ttl_s, owner=caller.owner))


@router.delete("/v1/leases/{lease_id}", response_model=LeaseOut)
async def release(lease_id: str, request: Request, caller: Caller = Depends(require_caller)):
    return LeaseOut.from_row(await _pool(request).allocator.release(lease_id, owner=caller.owner))


@router.post("/v1/leases/{lease_id}/run_code", response_model=RunCodeResponse)
async def run_code(lease_id: str, body: RunCodeRequest, request: Request, caller: Caller = Depends(require_caller)):
    r = await _pool(request).allocator.run_code(
        lease_id, body.code, language=body.language, timeout_s=body.timeout_s, owner=caller.owner
    )
    return RunCodeResponse(**dataclasses.asdict(r))


@router.post("/v1/leases/{lease_id}/commands", response_model=CommandResponse)
async def run_command(lease_id: str, body: CommandRequest, request: Request, caller: Caller = Depends(require_caller)):
    r = await _pool(request).allocator.run_command(
        lease_id, body.cmd, cwd=body.cwd, envs=body.envs, timeout_s=body.timeout_s, owner=caller.owner
    )
    return CommandResponse(**dataclasses.asdict(r))


@router.put("/v1/leases/{lease_id}/files")
async def write_file(
    lease_id: str, request: Request, path: str = Query(...), caller: Caller = Depends(require_caller)
):
    pool = _pool(request)
    data = await _read_body(request, pool.cfg.max_upload_bytes)
    await pool.allocator.write_file(lease_id, path, data, owner=caller.owner)
    return {"path": path, "size": len(data)}


@router.get("/v1/leases/{lease_id}/files")
async def read_file(lease_id: str, request: Request, path: str = Query(...), caller: Caller = Depends(require_caller)):
    data = await _pool(request).allocator.read_file(lease_id, path, owner=caller.owner)
    return Response(content=data, media_type="application/octet-stream")


@router.get("/v1/pool/stats", dependencies=[Depends(require_caller)])
async def stats(request: Request):
    return await _pool(request).stats()


@router.get("/v1/sandboxes", dependencies=[Depends(require_admin)])
async def list_sandboxes(request: Request):
    """调试用，仅管理员。不返回 lease_id（借用凭证）。"""
    rows = await _pool(request).store.list_sandboxes()
    return [{k: v for k, v in r.items() if k != "lease_id"} for r in rows]


@router.post("/v1/admin/drain", dependencies=[Depends(require_admin)])
async def drain(request: Request):
    return await _pool(request).drain()


@router.delete("/v1/admin/drain", dependencies=[Depends(require_admin)])
async def undrain(request: Request):
    return await _pool(request).undrain()
