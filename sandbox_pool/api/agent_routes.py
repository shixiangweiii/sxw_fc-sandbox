"""agent 子系统的 HTTP 接口：/v1/agents/{user_id}/...（业务系统按用户 ID 访问自己的常驻 agent）。

agent 按（调用方 key 名称，user_id）隔离：不同调用方的同名 user_id 是不同的 agent，访问别人的 agent 返回 404。
对话默认以 SSE 流式返回（text/event-stream）：start → text / reasoning / tool / status … → done，
期间每隔 POOL_AGENT_STREAM_KEEPALIVE_S 发送一次 `: keepalive` 注释。客户端断开不影响任务，可用 /tasks/{id}/stream 重连。
"""

import asyncio
import json
from typing import AsyncIterator, Optional

from fastapi import APIRouter, Body, Depends, Query, Request
from fastapi.responses import StreamingResponse

from sandbox_pool.agent.service import AgentService
from sandbox_pool.api.auth import Caller, require_admin, require_caller
from sandbox_pool.api.schemas import MessageRequest, ScheduleIn, SchedulePatch, TaskOut
from sandbox_pool.models import AgentUnavailable

router = APIRouter(prefix="/v1")

_SSE_HEADERS = {"Cache-Control": "no-cache", "X-Accel-Buffering": "no", "Connection": "keep-alive"}


def _svc(request: Request) -> AgentService:
    svc = getattr(request.app.state, "agents", None)
    if svc is None:
        raise AgentUnavailable("agent subsystem is disabled (set POOL_AGENT_ENABLED=true)")
    return svc


def _sse(kind: str, data: dict) -> str:
    return f"event: {kind}\ndata: {json.dumps(data, ensure_ascii=False)}\n\n"


async def _stream(items: AsyncIterator, keepalive_s: float) -> AsyncIterator[str]:
    """把事件迭代器格式化为 SSE；没有事件时定期发保活注释。None 表示结束。"""
    it = items.__aiter__()
    pending: Optional[asyncio.Task] = None
    try:
        while True:
            if pending is None:
                pending = asyncio.ensure_future(it.__anext__())
            done, _ = await asyncio.wait({pending}, timeout=keepalive_s)
            if not done:
                yield ": keepalive\n\n"
                continue
            try:
                item = pending.result()
            except StopAsyncIteration:
                return
            finally:
                pending = None
            if item is None:
                return
            yield _sse(*item)
    finally:
        if pending is not None:
            pending.cancel()
            await asyncio.gather(pending, return_exceptions=True)
        aclose = getattr(it, "aclose", None)
        if aclose is not None:
            await aclose()


async def _queue_items(runner) -> AsyncIterator:
    q = runner.subscribe()
    try:
        while True:
            item = await q.get()
            yield item
            if item is None:
                return
    finally:
        runner.unsubscribe(q)


@router.post("/agents/{user_id}/messages")
async def post_message(user_id: str, body: MessageRequest, request: Request, caller: Caller = Depends(require_caller)):
    svc = _svc(request)
    agent = await svc.ensure_agent(caller.name, user_id)
    runner = await svc.start_message(
        agent, body.text, session_id=body.session_id, max_duration_s=body.max_duration_s, agent_name=body.agent
    )
    if not body.stream:
        await runner.done.wait()
        return TaskOut.from_row(await svc.get_task(agent, runner.task_id))
    return StreamingResponse(
        _stream(_queue_items(runner), svc.cfg.agent_stream_keepalive_s),
        media_type="text/event-stream",
        headers=_SSE_HEADERS,
    )


@router.get("/agents/{user_id}")
async def get_agent(user_id: str, request: Request, caller: Caller = Depends(require_caller)):
    svc = _svc(request)
    return await svc.agent_info(await svc.get_agent(caller.name, user_id))


@router.patch("/agents/{user_id}/settings")
async def patch_settings(user_id: str, request: Request, body: dict = Body(...), caller: Caller = Depends(require_caller)):
    svc = _svc(request)
    agent = await svc.ensure_agent(caller.name, user_id)
    return await svc.update_settings(agent, body)


@router.delete("/agents/{user_id}/sandbox")
async def reset_sandbox(user_id: str, request: Request, caller: Caller = Depends(require_caller)):
    svc = _svc(request)
    return await svc.reset(await svc.get_agent(caller.name, user_id))


@router.get("/agents/{user_id}/egress")
async def get_egress(user_id: str, request: Request, caller: Caller = Depends(require_caller)):
    svc = _svc(request)
    return await svc.get_egress(await svc.ensure_agent(caller.name, user_id))


@router.put("/agents/{user_id}/egress")
async def put_egress(
    user_id: str, request: Request, body: Optional[dict] = Body(None), caller: Caller = Depends(require_caller)
):
    """body 为按 agent 覆盖的策略 {mode?, allow_out?, deny_out?}；传 null 恢复默认策略。"""
    svc = _svc(request)
    return await svc.put_egress(await svc.ensure_agent(caller.name, user_id), body)


@router.get("/agents/{user_id}/tasks")
async def list_tasks(
    user_id: str,
    request: Request,
    source: Optional[str] = Query(None, pattern="^(message|schedule)$"),
    schedule_id: Optional[str] = None,
    state: Optional[str] = Query(None, pattern="^(RUNNING|SUCCEEDED|FAILED|ABORTED|TIMEOUT)$"),
    since: Optional[float] = None,
    limit: int = Query(50, ge=1, le=500),
    caller: Caller = Depends(require_caller),
):
    svc = _svc(request)
    agent = await svc.get_agent(caller.name, user_id)
    rows = await svc.store.list_tasks(
        agent["id"], source=source, schedule_id=schedule_id, state=state, since=since, limit=limit
    )
    return [TaskOut.from_row(r) for r in rows]


@router.get("/agents/{user_id}/tasks/{task_id}")
async def get_task(user_id: str, task_id: str, request: Request, caller: Caller = Depends(require_caller)):
    svc = _svc(request)
    return TaskOut.from_row(await svc.get_task(await svc.get_agent(caller.name, user_id), task_id))


@router.get("/agents/{user_id}/tasks/{task_id}/stream")
async def attach_task(user_id: str, task_id: str, request: Request, caller: Caller = Depends(require_caller)):
    svc = _svc(request)
    agent = await svc.get_agent(caller.name, user_id)
    await svc.get_task(agent, task_id)  # 不存在时在开始流式输出之前返回 404
    return StreamingResponse(
        _stream(svc.attach(agent, task_id), svc.cfg.agent_stream_keepalive_s),
        media_type="text/event-stream",
        headers=_SSE_HEADERS,
    )


@router.post("/agents/{user_id}/tasks/{task_id}/abort")
async def abort_task(user_id: str, task_id: str, request: Request, caller: Caller = Depends(require_caller)):
    svc = _svc(request)
    return TaskOut.from_row(await svc.abort_task(await svc.get_agent(caller.name, user_id), task_id))


@router.post("/agents/{user_id}/schedules")
async def create_schedule(user_id: str, body: ScheduleIn, request: Request, caller: Caller = Depends(require_caller)):
    svc = _svc(request)
    agent = await svc.ensure_agent(caller.name, user_id)
    return await svc.create_schedule(agent, body.model_dump(exclude_unset=True))


@router.get("/agents/{user_id}/schedules")
async def list_schedules(user_id: str, request: Request, caller: Caller = Depends(require_caller)):
    svc = _svc(request)
    agent = await svc.get_agent(caller.name, user_id)
    return await svc.store.list_schedules(agent["id"])


@router.get("/agents/{user_id}/schedules/{schedule_id}")
async def get_schedule(user_id: str, schedule_id: str, request: Request, caller: Caller = Depends(require_caller)):
    svc = _svc(request)
    return await svc.get_schedule(await svc.get_agent(caller.name, user_id), schedule_id)


@router.patch("/agents/{user_id}/schedules/{schedule_id}")
async def patch_schedule(
    user_id: str, schedule_id: str, body: SchedulePatch, request: Request, caller: Caller = Depends(require_caller)
):
    svc = _svc(request)
    agent = await svc.get_agent(caller.name, user_id)
    return await svc.update_schedule(agent, schedule_id, body.model_dump(exclude_unset=True))


@router.delete("/agents/{user_id}/schedules/{schedule_id}")
async def delete_schedule(user_id: str, schedule_id: str, request: Request, caller: Caller = Depends(require_caller)):
    svc = _svc(request)
    return await svc.delete_schedule(await svc.get_agent(caller.name, user_id), schedule_id)


@router.post("/agents/{user_id}/schedules/{schedule_id}/run")
async def run_schedule(user_id: str, schedule_id: str, request: Request, caller: Caller = Depends(require_caller)):
    """立即触发一次（遵守 overlap 设置）；返回新建的任务，被跳过时返回 {"skipped": true}。"""
    svc = _svc(request)
    task = await svc.run_schedule_now(await svc.get_agent(caller.name, user_id), schedule_id)
    return TaskOut.from_row(task) if task else {"skipped": True}


@router.get("/admin/agents", dependencies=[Depends(require_admin)])
async def admin_agents(request: Request):
    """仅管理员：所有 agent、它们的沙箱（不含流量令牌）与运行中任务。"""
    return await _svc(request).admin_list()


@router.get("/admin/agents/stats", dependencies=[Depends(require_admin)])
async def admin_agent_stats(request: Request):
    """仅管理员：agent 池的沙箱状态统计、事件计数、各操作耗时（建沙箱、装配、任务等）。"""
    return await _svc(request).stats()
