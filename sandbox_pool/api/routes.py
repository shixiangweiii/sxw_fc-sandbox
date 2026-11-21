import dataclasses

from fastapi import APIRouter, Body, Depends, Query, Request, Response
from fastapi.responses import JSONResponse

from sandbox_pool.api.auth import Caller, require_admin, require_caller
from sandbox_pool.api.schemas import (
    AcquireRequest,
    CommandRequest,
    CommandResponse,
    DeleteInstanceRequest,
    LeaseOut,
    RenewRequest,
    RunCodeRequest,
    RunCodeResponse,
    TemplateCacheCreateRequest,
)
from sandbox_pool.core.inventory import list_instances
from sandbox_pool.core.pool import SandboxPool
from sandbox_pool.core.template_cache import TemplateCacheExists, client_from_env, missing_credentials
from sandbox_pool.egress import DENY_ALL, parse_policy
from sandbox_pool.models import InvalidRequest, PayloadTooLarge, SandboxRecordNotFound

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
    # 【定】策略在**进池子之前**校验：非法的 allow_out / deny_out 要在占用任何
    # 资源之前报 400，而不是借到沙箱之后才失败（失败的请求不得留下状态变化）
    grant = await pool.allocator.acquire(
        wait_timeout_s=body.wait_timeout_s,
        lease_ttl_s=body.lease_ttl_s,
        is_disconnected=request.is_disconnected,
        client_id=caller.name,
        egress=parse_policy(body.egress, DENY_ALL) if body.egress is not None else None,
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
    """仅管理员。不返回 lease_id（借用凭证）；借出中的沙箱附带借用方与到期时间（lease 字段）。"""
    return await _pool(request).list_sandboxes()


@router.delete("/v1/sandboxes/{row_id}", dependencies=[Depends(require_admin)])
async def destroy_sandbox(row_id: str, request: Request):
    """仅管理员。强制销毁沙箱（id 取自 GET /v1/sandboxes）：借出中的先结束借用。"""
    return await _pool(request).destroy_sandbox(row_id)


@router.post("/v1/admin/drain", dependencies=[Depends(require_admin)])
async def drain(request: Request):
    return await _pool(request).drain()


@router.delete("/v1/admin/drain", dependencies=[Depends(require_admin)])
async def undrain(request: Request):
    return await _pool(request).undrain()


def _agents(request: Request):
    return getattr(request.app.state, "agents", None)


def _template_cache(request: Request):
    """测试可把假客户端放到 app.state.template_cache；未设置时按环境变量现建。"""
    if hasattr(request.app.state, "template_cache"):
        return request.app.state.template_cache
    return client_from_env()


def _cache_unconfigured() -> dict:
    missing = missing_credentials()
    detail = "缺少环境变量: " + ", ".join(missing) if missing else "模板缓存客户端未配置"
    return {"configured": False, "detail": detail, "templates": []}


def _cache_unavailable():
    body = _cache_unconfigured()
    return JSONResponse(
        status_code=503,
        content={"error": "TemplateCacheUnavailable", "detail": body["detail"]},
    )


@router.get("/v1/admin/template-caches", dependencies=[Depends(require_admin)])
async def admin_template_caches(request: Request):
    """本团队模板及其缓存。未配置凭证时仍返回 200，configured 为 false。"""
    client = _template_cache(request)
    if client is None:
        return _cache_unconfigured()
    return await client.overview()


@router.post("/v1/admin/template-caches", dependencies=[Depends(require_admin)])
async def admin_create_template_cache(body: TemplateCacheCreateRequest, request: Request):
    """为模板新建缓存。已有进行中或成功的缓存时 409，不等待预热完成。"""
    client = _template_cache(request)
    if client is None:
        return _cache_unavailable()
    try:
        return await client.create(body.template_id)
    except TemplateCacheExists as exc:
        return JSONResponse(
            status_code=409,
            content={"error": "TemplateCacheExists", "detail": str(exc), "cache": exc.cache},
        )


@router.delete("/v1/admin/template-caches/{template_id}", dependencies=[Depends(require_admin)])
async def admin_delete_template_cache(template_id: str, request: Request):
    """删除该模板的缓存。释放后按容量计时的费用停止。"""
    client = _template_cache(request)
    if client is None:
        return _cache_unavailable()
    return await client.delete(template_id)


@router.get("/v1/admin/instances", dependencies=[Depends(require_admin)])
async def admin_instances(request: Request):
    """当前账号地域的运行中与休眠实例，并与代码池、agent 池的库记录对齐。"""
    return await list_instances(_pool(request), _agents(request))


@router.post("/v1/admin/instances/{row_id}/pause", dependencies=[Depends(require_admin)])
async def admin_pause(row_id: str, request: Request):
    """把代码池里空闲的 READY 沙箱暂停。agent 沙箱不能休眠。"""
    pool = _pool(request)
    row = await pool.store.get_sandbox(row_id)
    if row is None:
        raise SandboxRecordNotFound(f"sandbox {row_id} not found")
    agents = _agents(request)
    if agents is not None and row["pool"] == agents.cfg.agent_pool_name:
        raise InvalidRequest("agent sandboxes cannot be paused; delete the instance instead")
    return await pool.pause_sandbox(row_id)


@router.delete("/v1/admin/instances", dependencies=[Depends(require_admin)])
async def admin_delete_instance(body: DeleteInstanceRequest, request: Request):
    """按库内行删除代码池或 agent 沙箱；只有云端记录时按 provider_id 销毁孤儿。"""
    try:
        body.require_one()
    except ValueError as e:
        raise InvalidRequest(str(e)) from e
    pool = _pool(request)
    agents = _agents(request)
    row = await pool.store.get_sandbox(body.row_id) if body.row_id else None
    if row is None and body.provider_id:
        for source in (pool.store, agents.store if agents is not None else None):
            if source is None:
                continue
            for candidate in await source.list_sandboxes():
                if candidate.get("provider_id") == body.provider_id:
                    row = candidate
                    break
            if row is not None:
                break
    if row is None:
        if not body.provider_id:
            raise SandboxRecordNotFound("sandbox not found")
        killed = await pool.provider.kill(body.provider_id)
        return {"provider_id": body.provider_id, "kind": "orphan", "killed": killed}
    if row["pool"] == pool.cfg.pool_name:
        return await pool.destroy_sandbox(row["id"])
    if agents is not None and row["pool"] == agents.cfg.agent_pool_name:
        return await agents.admin_destroy_sandbox(row["id"])
    raise SandboxRecordNotFound(f"sandbox {row['id']} not found")
