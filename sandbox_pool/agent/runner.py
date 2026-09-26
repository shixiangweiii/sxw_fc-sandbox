"""任务执行：订阅 opencode 事件 → （新建会话）→ prompt_async → 翻译事件并发布给订阅者 → 判定完成 → 结果落库。

- 全部云端调用和写库都在 runner 自己的后台任务里；HTTP 响应端只从订阅队列读事件。客户端断开不会取消 runner，
  任务照常跑完、结果落库（也遵守「不在数据库操作中途取消协程」的约定）。
- 负责跟进任务的副本每隔 agent_task_heartbeat_s 刷新 tasks.op_deadline；副本崩溃后其他副本在过期时接管（resume=True），
  只跟进到结束并落库。
- 完成判定以事件为主（session.status 从 busy 回到 idle），同时定期轮询 /session/status 兜底（事件流断开、重连期间）。
"""

import asyncio
import json
import logging
import time
from typing import TYPE_CHECKING, Any, Optional

from sandbox_pool.models import TaskState

if TYPE_CHECKING:
    from sandbox_pool.agent.service import AgentService

log = logging.getLogger(__name__)

_TRUNCATE = 2000
# 本副本内为后来的订阅者保留的事件数（断线重连时回放）。连续的同类文本增量合并成一条，长回复也只占很少几条
_HISTORY_MAX = 5000
_MERGEABLE = ("text", "reasoning")
_STATUS_POLL_S = 5.0
# prompt 之后多久还没见到 busy，就以 /session/status 为准判定完成（防止 busy / idle 事件都丢了的情况）
_NO_BUSY_GRACE_S = 15.0


def _cut(value: Any, limit: int = _TRUNCATE) -> Any:
    if value is None:
        return None
    if not isinstance(value, str):
        value = json.dumps(value, ensure_ascii=False)
    return value if len(value) <= limit else value[:limit] + f"...(+{len(value) - limit} chars)"


def _event_session(ev: dict) -> Optional[str]:
    props = ev.get("properties") or {}
    return (
        props.get("sessionID")
        or (props.get("part") or {}).get("sessionID")
        or (props.get("info") or {}).get("sessionID")
    )


class Translator:
    """把 opencode 事件翻译成对外事件：(kind, data)，kind ∈ text / reasoning / tool / status。

    - 用户消息的部件（提示词本身）不输出：message.updated 先于对应部件到达，记下每条消息的角色；
    - 文本增量来自 message.part.delta；部件类型未知时先缓存，等 message.part.updated 带来类型再发出；
      只有 message.part.updated（没有增量）的文本，按已输出长度补发剩余部分；
    - 工具部件在状态变化时输出（running / completed / error），输入输出截断。
    """

    def __init__(self, session_id: str):
        self.session_id = session_id
        self.roles: dict[str, str] = {}
        self.part_types: dict[str, str] = {}
        self.emitted: dict[str, int] = {}
        self.pending: dict[str, list[str]] = {}
        self.tool_status: dict[str, str] = {}
        self.busy_seen = False
        self.idle = False
        self.errors: list[str] = []
        self.asks: list[tuple[str, str]] = []  # (permission | question, request id)

    def _emit_text(self, part_id: str, kind: str, delta: str, out: list) -> None:
        if not delta:
            return
        self.emitted[part_id] = self.emitted.get(part_id, 0) + len(delta)
        out.append((kind, {"delta": delta}))

    def feed(self, ev: dict) -> list[tuple[str, dict]]:
        out: list[tuple[str, dict]] = []
        if _event_session(ev) != self.session_id:
            return out
        kind = ev.get("type")
        props = ev.get("properties") or {}
        if kind == "message.updated":
            info = props.get("info") or {}
            if info.get("id"):
                self.roles[info["id"]] = info.get("role", "")
        elif kind == "message.part.delta":
            if self.roles.get(props.get("messageID")) == "user" or props.get("field", "text") != "text":
                return out
            part_id = props.get("partID", "")
            ptype = self.part_types.get(part_id)
            if ptype is None:
                self.pending.setdefault(part_id, []).append(props.get("delta", ""))
            elif ptype in ("text", "reasoning"):
                self._emit_text(part_id, ptype, props.get("delta", ""), out)
        elif kind == "message.part.updated":
            part = props.get("part") or {}
            if self.roles.get(part.get("messageID")) == "user":
                return out
            part_id, ptype = part.get("id", ""), part.get("type", "")
            self.part_types[part_id] = ptype
            if ptype in ("text", "reasoning"):
                buffered = "".join(self.pending.pop(part_id, []))
                self._emit_text(part_id, ptype, buffered, out)
                full = part.get("text") or ""
                done = self.emitted.get(part_id, 0)
                if len(full) > done:
                    self._emit_text(part_id, ptype, full[done:], out)
            elif ptype == "tool":
                state = part.get("state") or {}
                status = state.get("status")
                if status in ("running", "completed", "error") and self.tool_status.get(part_id) != status:
                    self.tool_status[part_id] = status
                    out.append(
                        (
                            "tool",
                            {
                                "tool": part.get("tool"),
                                "status": status,
                                "title": state.get("title"),
                                "input": _cut(state.get("input")),
                                "output": _cut(state.get("output")) if status == "completed" else None,
                                "error": _cut(state.get("error")) if status == "error" else None,
                            },
                        )
                    )
            else:
                self.pending.pop(part_id, None)
        elif kind == "session.status":
            status = props.get("status") or {}
            st = status.get("type")
            if st == "busy":
                self.busy_seen = True
            elif st == "retry":
                self.busy_seen = True
                out.append(("status", {"type": "retry", "message": _cut(status.get("message"), 500), "attempt": status.get("attempt")}))
            elif st == "idle" and self.busy_seen:
                self.idle = True
        elif kind == "session.error":
            err = props.get("error") or {}
            msg = (err.get("data") or {}).get("message") or err.get("name") or "session error"
            self.errors.append(_cut(msg, 1000))
        elif kind in ("permission.asked", "question.asked"):
            if props.get("id"):
                self.asks.append((kind.split(".")[0], props["id"]))
        return out

    def flush(self) -> list[tuple[str, dict]]:
        """结束时仍未确定类型的缓存增量按文本输出。"""
        out: list[tuple[str, dict]] = []
        for part_id, chunks in list(self.pending.items()):
            self._emit_text(part_id, "text", "".join(chunks), out)
        self.pending.clear()
        return out


def extract_result(messages: list[dict]) -> tuple[str, dict, Optional[str]]:
    """本轮结果：最后一条用户消息之后的 assistant 消息。文本取最后一条有文本的 assistant 消息，用量求和，错误取最后一条。

    按消息顺序而不是时间戳划分，不依赖网关与沙箱之间的时钟。
    """
    last_user = -1
    for i, m in enumerate(messages):
        if (m.get("info") or {}).get("role") == "user":
            last_user = i
    assistants = [m for m in messages[last_user + 1 :] if (m.get("info") or {}).get("role") == "assistant"]
    usage = {"input": 0, "output": 0, "reasoning": 0, "cache_read": 0, "cache_write": 0, "cost": 0.0, "steps": len(assistants)}
    text, error = "", None
    for m in assistants:
        info = m.get("info") or {}
        tok = info.get("tokens") or {}
        cache = tok.get("cache") or {}
        usage["input"] += tok.get("input") or 0
        usage["output"] += tok.get("output") or 0
        usage["reasoning"] += tok.get("reasoning") or 0
        usage["cache_read"] += cache.get("read") or 0
        usage["cache_write"] += cache.get("write") or 0
        usage["cost"] += info.get("cost") or 0
        parts = [p.get("text") or "" for p in m.get("parts") or [] if p.get("type") == "text"]
        if any(parts):
            text = "".join(parts)
        err = info.get("error")
        if err:
            error = (err.get("data") or {}).get("message") or err.get("name") or "error"
    usage["cost"] = round(usage["cost"], 8)
    return text, usage, error


class _LostOwnership(Exception):
    """任务已被其他副本接管或已被结束（心跳 CAS 失败）。"""


class TaskRunner:
    def __init__(
        self,
        svc: "AgentService",
        task: dict,
        sandbox: dict,
        *,
        text: Optional[str],
        agent_name: Optional[str] = None,
        resume: bool = False,
    ):
        self.svc = svc
        self.cfg = svc.cfg
        self.task = task
        self.task_id = task["id"]
        self.sandbox = sandbox
        self.text = text
        self.agent_name = agent_name
        self.resume = resume
        self.client = svc.client_for(sandbox)
        self.session_id: Optional[str] = task.get("session_id")
        self.subscribers: set[asyncio.Queue] = set()
        self.history: list[tuple[str, dict]] = []
        self.done = asyncio.Event()
        self.final: Optional[dict] = None
        self._abort = asyncio.Event()
        self._stop = asyncio.Event()
        self._abort_reason: Optional[TaskState] = None

    # ---------- 订阅 ----------

    def subscribe(self) -> asyncio.Queue:
        q: asyncio.Queue = asyncio.Queue()
        for item in self.history:
            q.put_nowait(item)
        if self.done.is_set():
            q.put_nowait(None)
        else:
            self.subscribers.add(q)
        return q

    def unsubscribe(self, q: asyncio.Queue) -> None:
        self.subscribers.discard(q)

    def _publish(self, kind: str, data: dict) -> None:
        item = (kind, data)
        last = self.history[-1] if self.history else None
        if kind in _MERGEABLE and last is not None and last[0] == kind and set(data) == {"delta"}:
            # 生成新对象：旧对象可能还在订阅者的队列里没被取走，不能原地修改
            self.history[-1] = (kind, {"delta": last[1]["delta"] + data["delta"]})
        elif len(self.history) < _HISTORY_MAX:
            self.history.append(item)
        for q in list(self.subscribers):
            q.put_nowait(item)

    # ---------- 控制 ----------

    def request_abort(self) -> None:
        self._abort.set()

    def stop(self) -> None:
        """本副本停止：不再跟进任务（不中止 opencode 里的任务），交给其他副本接管。"""
        self._stop.set()

    # ---------- 执行 ----------

    async def run(self) -> None:
        hb_stop = asyncio.Event()
        hb = asyncio.create_task(self._heartbeat(hb_stop))
        try:
            outcome = await self._execute()
            if outcome is not None:
                await self._finish(*outcome)
        except _LostOwnership:
            log.info("task %s: ownership lost, stop following", self.task_id)
            await self._publish_final_from_db()
        except Exception as e:  # noqa: BLE001
            log.warning("task %s failed: %r", self.task_id, e, exc_info=True)
            await self._finish(TaskState.FAILED, None, None, f"{type(e).__name__}: {e}"[:1000])
        finally:
            hb_stop.set()
            await asyncio.gather(hb, return_exceptions=True)
            self.done.set()
            for q in list(self.subscribers):
                q.put_nowait(None)
            self.subscribers.clear()
            self.svc.runner_finished(self)

    async def _heartbeat(self, stop: asyncio.Event) -> None:
        while True:
            try:
                await asyncio.wait_for(stop.wait(), timeout=self.cfg.agent_task_heartbeat_s)
                return
            except asyncio.TimeoutError:
                pass
            try:
                ok = await self.svc.store.cas_task(
                    self.task_id,
                    [TaskState.RUNNING],
                    expect_owner=self.svc.replica_id,
                    op_deadline=self.svc.now() + self.cfg.agent_task_takeover_s,
                )
                if not ok:
                    self._stop.set()
                    return
            except Exception:  # noqa: BLE001 - 数据库抖动时下一轮再试
                log.warning("heartbeat of task %s failed", self.task_id, exc_info=True)

    async def _execute(self) -> Optional[tuple]:
        queue: asyncio.Queue = asyncio.Queue()
        reader = asyncio.create_task(self._read_events(queue))
        try:
            await self._wait_connected(queue)
            if not self.resume:
                if not self.session_id:
                    self.session_id = await self.client.create_session(title=(self.text or "")[:60] or "task")
                    if not await self.svc.store.cas_task(
                        self.task_id, [TaskState.RUNNING], expect_owner=self.svc.replica_id, session_id=self.session_id
                    ):
                        raise _LostOwnership()
                self._publish(
                    "start",
                    {"task_id": self.task_id, "session_id": self.session_id, "sandbox_id": self.sandbox["id"]},
                )
                await self.client.prompt_async(self.session_id, self.text or "", model=None, agent=self.agent_name)
            else:
                self._publish(
                    "start",
                    {"task_id": self.task_id, "session_id": self.session_id, "sandbox_id": self.sandbox["id"], "resumed": True},
                )
            return await self._follow(queue, reader)
        finally:
            reader.cancel()
            await asyncio.gather(reader, return_exceptions=True)

    async def _read_events(self, queue: asyncio.Queue) -> None:
        """事件流读取：断开后重连（订阅期间错过的事件由状态轮询和最终取消息兜底）。"""
        failures = 0
        while True:
            try:
                async for ev in self.client.events():
                    failures = 0
                    queue.put_nowait(ev)
                failures += 1
            except asyncio.CancelledError:
                raise
            except Exception as e:  # noqa: BLE001
                failures += 1
                log.info("task %s: event stream error %r (failures=%d)", self.task_id, e, failures)
                queue.put_nowait({"type": "__stream_error__", "properties": {"error": repr(e)}})
            await asyncio.sleep(min(10.0, 0.5 * failures))

    async def _wait_connected(self, queue: asyncio.Queue) -> None:
        deadline = time.monotonic() + 30
        while True:
            try:
                ev = await asyncio.wait_for(queue.get(), timeout=max(0.1, deadline - time.monotonic()))
            except asyncio.TimeoutError:
                raise RuntimeError("opencode event stream not connected within 30s") from None
            if ev.get("type") == "server.connected":
                return
            if ev.get("type") == "__stream_error__" and time.monotonic() > deadline:
                raise RuntimeError(f"opencode event stream unavailable: {ev['properties']['error']}")

    async def _check_abort(self, *, poll_db: bool) -> None:
        """超过最长执行时间或收到中止请求时中止 opencode 会话。

        截止时间与本副本收到的中止请求每轮都查（不涉及 IO）；其他副本写库的中止请求随状态轮询查。
        """
        if self._abort_reason is not None:
            return
        reason = None
        if self.svc.now() >= self.task["deadline"]:
            reason = TaskState.TIMEOUT
        elif self._abort.is_set():
            reason = TaskState.ABORTED
        elif poll_db:
            fresh = await self.svc.store.get_task(self.task_id)
            if fresh is None or fresh["state"] != TaskState.RUNNING.value:
                raise _LostOwnership()
            if fresh["op_owner"] != self.svc.replica_id:
                raise _LostOwnership()
            if fresh["abort_requested"]:
                reason = TaskState.ABORTED
        if reason is not None:
            self._abort_reason = reason
            log.info("task %s: aborting (%s)", self.task_id, reason.value)
            try:
                await self.client.abort(self.session_id)
            except Exception as e:  # noqa: BLE001 - 以之后的状态轮询为准
                log.warning("task %s: abort failed: %r", self.task_id, e)

    async def _follow(self, queue: asyncio.Queue, reader: asyncio.Task) -> Optional[tuple]:
        tr = Translator(self.session_id)
        started = time.monotonic()
        next_poll = started + _STATUS_POLL_S
        if self.resume:
            # 接管：订阅前任务已在运行（错过了 busy），收到 idle 即完成；马上查一次状态（可能在无人跟进时已经结束）
            tr.busy_seen = True
            next_poll = started
        while True:
            if self._stop.is_set():
                await self._release()
                return None
            try:
                ev = await asyncio.wait_for(queue.get(), timeout=1.0)
            except asyncio.TimeoutError:
                ev = None
            if ev is not None:
                for kind, data in tr.feed(ev):
                    self._publish(kind, data)
                while tr.asks:
                    await self._auto_reject(*tr.asks.pop(0))
                if tr.idle:
                    break
            now = time.monotonic()
            poll = now >= next_poll
            await self._check_abort(poll_db=poll)
            if poll:
                next_poll = now + _STATUS_POLL_S
                try:
                    status = await self.client.status()
                except Exception as e:  # noqa: BLE001 - 沙箱暂时不可达：继续等，由维护循环判断沙箱是否失效
                    log.info("task %s: status poll failed: %r", self.task_id, e)
                    if not await self._sandbox_alive():
                        raise RuntimeError("sandbox is gone") from e
                    continue
                if self.session_id not in status and (tr.busy_seen or self.resume or now - started > _NO_BUSY_GRACE_S):
                    break
        for kind, data in tr.flush():
            self._publish(kind, data)
        if self._abort_reason is None and (self._abort.is_set() or await self._abort_requested_in_db()):
            # 中止请求直接调用了 opencode 的 abort，会话可能在本副本查到中止请求之前就回到 idle
            # （请求落在其他副本时，库里的 abort_requested 每轮状态轮询才查一次）
            self._abort_reason = TaskState.ABORTED
        messages = await self.client.messages(self.session_id)
        text, usage, error = extract_result(messages)
        error = error or (tr.errors[-1] if tr.errors else None)
        if self._abort_reason is not None:
            return self._abort_reason, text, usage, error or f"task {self._abort_reason.value.lower()}"
        if error:
            return TaskState.FAILED, text, usage, error
        return TaskState.SUCCEEDED, text, usage, None

    async def _abort_requested_in_db(self) -> bool:
        fresh = await self.svc.store.get_task(self.task_id)
        return bool(fresh and fresh["abort_requested"])

    async def _sandbox_alive(self) -> bool:
        row = await self.svc.store.get_sandbox(self.sandbox["id"])
        return row is not None and row["state"] in ("ACTIVE", "RETIRING")

    async def _auto_reject(self, kind: str, request_id: str) -> None:
        """无人值守：询问一律拒绝，避免任务挂起（正常配置下不会出现询问）。"""
        try:
            if kind == "permission":
                await self.client.reply_permission(request_id, "reject")
            else:
                await self.client.reject_question(request_id)
            await self.svc.lc.event("agent_auto_reject", sandbox_row_id=self.sandbox["id"], detail=f"{kind} {request_id}")
        except Exception as e:  # noqa: BLE001
            log.warning("task %s: auto reject %s %s failed: %r", self.task_id, kind, request_id, e)

    async def _release(self) -> None:
        """本副本停止：让出任务（op_deadline 置为现在），其他副本立即可以接管。"""
        await self.svc.store.cas_task(
            self.task_id, [TaskState.RUNNING], expect_owner=self.svc.replica_id, op_deadline=self.svc.now()
        )
        raise _LostOwnership()

    async def _finish(self, state: TaskState, text: Optional[str], usage: Optional[dict], error: Optional[str]) -> None:
        now = self.svc.now()
        ok = await self.svc.store.cas_task(
            self.task_id,
            [TaskState.RUNNING],
            expect_owner=self.svc.replica_id,
            state=state,
            result_text=text,
            usage=usage,
            error=error,
            finished_at=now,
            op_owner=None,
            op_deadline=None,
        )
        if not ok:
            await self._publish_final_from_db()
            return
        await self.svc.store.touch_sandbox(self.sandbox["id"], now=now, kind=self.task["source"])
        await self.svc.lc.event(
            f"task_{state.value.lower()}",
            sandbox_row_id=self.sandbox["id"],
            duration_ms=(now - (self.task.get("started_at") or self.task["created_at"])) * 1000,
            detail=f"task={self.task_id} source={self.task['source']}" + (f" error={error[:200]}" if error else ""),
        )
        self.final = {"task_id": self.task_id, "state": state.value, "result": text, "usage": usage, "error": error}
        self._publish("done", {**self.final, "session_id": self.session_id})

    async def _publish_final_from_db(self) -> None:
        row = await self.svc.store.get_task(self.task_id)
        if row and row["state"] != TaskState.RUNNING.value:
            self.final = {
                "task_id": self.task_id,
                "state": row["state"],
                "result": row["result_text"],
                "usage": row["usage"],
                "error": row["error"],
            }
            self._publish("done", {**self.final, "session_id": row["session_id"]})
