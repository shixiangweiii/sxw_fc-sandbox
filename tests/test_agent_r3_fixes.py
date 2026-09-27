"""最近三次提交代码评审（sxw_aicoding/代码评审/2026-09-27-opencode进程池与pi集成-最近三次提交代码评审报告.md）
复核后确认的问题的回归用例，按问题编号（R3-*）组织。

并发类问题都用事件门控构造了报告里的时序（两个副本共享同一个 SQLite 和 FakeProvider），并对两种引擎各跑一遍；
去掉对应修复后用例会失败（见报告第六节的反向验证）。
"""

import asyncio
import json
import time

import pytest

from sandbox_pool.models import InvalidRequest, TaskConflict, TaskState
from tests.conftest import wait_until
from tests.test_agent_service import collect, final_task, serving

ENGINES = {
    "opencode": {},
    "pi": dict(agent_pi_template="tpl-pi", agent_default_engine="pi"),
}
BOTH = dict(agent_pi_template="tpl-pi")


@pytest.fixture(params=sorted(ENGINES))
def engine(request) -> str:
    return request.param


def _server(provider, row):
    """沙箱里的内存版 agent 服务（opencode 或 pi 桥接进程）。"""
    return provider.pi(row["provider_id"]) if row["engine"] == "pi" else provider.opencode(row["provider_id"])


async def _replicas(make_agents, engine: str):
    a = await make_agents(run_maintainer=False, **ENGINES[engine])
    b = await make_agents(run_maintainer=False, **ENGINES[engine])
    return a, b


async def _started(runner) -> str:
    """等任务开始，返回会话 ID。"""
    return (await asyncio.wait_for(runner.subscribe().get(), 5))[1]["session_id"]


async def _follow(svc, agent, task_id: str, items: list) -> None:
    async for item in svc.attach(agent, task_id):
        items.append(item)


def _text(items) -> str:
    return "".join(d["delta"] for k, d in items if k == "text")


# ---------- R3-01 迟到的中止命令不能中止同一会话里后来的任务 ----------


async def test_r3_01_abort_losing_the_race_does_not_abort_next_task(make_agents, provider, engine):
    """窗口一：B 读到 T1 在运行；设置中止标记之前 T1 已正常结束、同一会话开始了 T2。
    标记没设上（T1 已结束）就不能再下发中止：修复前 B 仍按会话中止，T2 被中止。"""
    a, b = await _replicas(make_agents, engine)
    agent = await a.ensure_agent("biz", "u1")
    t1 = await a.start_message(agent, "[sleep:0.3] FIRST")
    sid = await _started(t1)
    [row] = await serving(a, agent["id"])
    server = _server(provider, row)

    entered, release = asyncio.Event(), asyncio.Event()
    orig = b.store.cas_task

    async def cas_task(task_id, from_states, **kw):
        if kw.get("abort_requested"):
            entered.set()
            await release.wait()
        return await orig(task_id, from_states, **kw)

    b.store.cas_task = cas_task
    abort = asyncio.create_task(b.abort_task(agent, t1.task_id))
    await asyncio.wait_for(entered.wait(), 5)
    assert (await final_task(a, t1.task_id))["state"] == TaskState.SUCCEEDED.value
    t2 = await a.start_message(agent, "[sleep:0.5] SECOND", session_id=sid)
    await wait_until(lambda: sid in server.busy())
    release.set()
    [outcome] = await asyncio.gather(abort, return_exceptions=True)

    task2 = await final_task(a, t2.task_id)
    assert task2["state"] == TaskState.SUCCEEDED.value, task2["error"]
    assert task2["result_text"] == "echo: [sleep:0.5] SECOND" and task2["abort_requested"] == 0
    assert not server.aborts
    # 与「已结束的任务返回 409」一致
    assert isinstance(outcome, TaskConflict), outcome


async def test_r3_01_delayed_abort_command_does_not_abort_next_task(make_agents, provider, engine):
    """窗口二：中止标记已设上，但 B 发往沙箱的中止命令迟到（例如建连重试）：期间 T1 已结束（ABORTED）、
    同一会话开始了 T2。修复后中止命令只由跟进任务的 runner 在仍持有任务时下发，B 不再直接按会话中止。"""
    a, b = await _replicas(make_agents, engine)
    agent = await a.ensure_agent("biz", "u1")
    t1 = await a.start_message(agent, "[sleep:0.3] FIRST")
    sid = await _started(t1)
    [row] = await serving(a, agent["id"])
    server = _server(provider, row)

    client = b.client_for(row)
    release = asyncio.Event()
    orig_abort = client.abort

    async def delayed_abort(session_id):
        await release.wait()
        return await orig_abort(session_id)

    client.abort = delayed_abort
    abort = asyncio.create_task(b.abort_task(agent, t1.task_id))
    assert (await final_task(a, t1.task_id))["state"] == TaskState.ABORTED.value
    t2 = await a.start_message(agent, "[sleep:0.5] SECOND", session_id=sid)
    await wait_until(lambda: sid in server.busy())
    release.set()
    await abort

    task2 = await final_task(a, t2.task_id)
    assert task2["state"] == TaskState.SUCCEEDED.value, task2["error"]
    assert task2["result_text"] == "echo: [sleep:0.5] SECOND"


async def test_r3_01_abort_via_other_replica_still_takes_effect_promptly(make_agents, provider, engine):
    """中止改由跟进任务的 runner 下发后，跨副本的中止仍然很快生效（runner 每秒查一次库里的中止标记）。"""
    a, b = await _replicas(make_agents, engine)
    agent = await a.ensure_agent("biz", "u1")
    t1 = await a.start_message(agent, "[sleep:30] LONG")
    sid = await _started(t1)
    [row] = await serving(a, agent["id"])
    t0 = time.monotonic()
    task = await b.abort_task(agent, t1.task_id)
    assert task["abort_requested"] == 1
    done = await final_task(a, t1.task_id, timeout=5)
    assert done["state"] == TaskState.ABORTED.value and time.monotonic() - t0 < 3
    assert _server(provider, row).aborts == [sid]


async def test_r3_01_runner_does_not_abort_a_task_it_no_longer_holds(make_agents, provider, engine):
    """runner 下发中止前确认仍持有任务：任务已被其他副本接管（本副本的心跳一度过期）时不再下发，
    否则命令可能落在接管方结束任务之后、同一会话开始的下一个任务上。"""
    # 心跳间隔调长：让中止先于心跳发现所有权变化
    svc = await make_agents(run_maintainer=False, agent_task_heartbeat_s=5, agent_task_takeover_s=11, **ENGINES[engine])
    agent = await svc.ensure_agent("biz", "u1")
    t1 = await svc.start_message(agent, "[sleep:3] FIRST")
    await _started(t1)
    [row] = await serving(svc, agent["id"])
    assert await svc.store.cas_task(t1.task_id, [TaskState.RUNNING], op_owner="other-replica")
    t1.request_abort()
    await asyncio.wait_for(t1.done.wait(), 5)
    assert not _server(provider, row).aborts
    assert (await svc.store.get_task(t1.task_id))["state"] == TaskState.RUNNING.value  # 留给现在的负责副本


async def test_r3_01_abort_of_orphaned_task_is_delivered_after_takeover(make_agents, provider, engine):
    """负责副本崩溃、还没人接管时收到中止：只记标记，接管的副本开始跟进后立即下发。"""
    svc = await make_agents(run_maintainer=False, **ENGINES[engine])
    agent = await svc.ensure_agent("biz", "u1")
    await (await svc.start_message(agent, "boot")).done.wait()
    [row] = await serving(svc, agent["id"])
    server = _server(provider, row)
    sid = server.create_session("orphan")
    prompt = "[sleep:30] 崩溃副本的任务"
    run_id = server.prompt(sid, prompt)
    now = svc.now()
    status, task = await svc.store.create_task(
        dict(id="t-orphan", agent_id=agent["id"], client_id="biz", source="message", schedule_id=None,
             sandbox_row_id=row["id"], session_id=sid, prompt=prompt, result_text=None, error=None, usage=None,
             created_at=now, started_at=now, finished_at=None, deadline=now + 60, op_owner="dead-replica",
             op_deadline=now - 1, run_id=run_id),
        max_running=5,
    )
    assert status == "ok"
    assert (await svc.abort_task(agent, task["id"]))["abort_requested"] == 1
    await wait_until(lambda: sid in server.busy())  # 还没人接管：没有下发
    await svc.maintainer.takeover_tasks(svc.now())
    done = await final_task(svc, task["id"], timeout=5)
    assert done["state"] == TaskState.ABORTED.value, done["error"]
    assert server.aborts == [sid]


# ---------- R3-02 引擎专属参数按实际执行的沙箱校验 ----------


async def test_r3_02_agent_param_is_checked_against_the_actual_sandbox(make_agents, provider):
    """请求读到的是切换引擎之前的设置（opencode），沙箱按最新设置建成 pi：agent 参数要按实际沙箱拒绝，
    不能静默交给不支持它的 pi。"""
    svc = await make_agents(run_maintainer=False, **BOTH)
    stale = await svc.ensure_agent("biz", "u1")  # 未设置引擎 → 默认 opencode
    await svc.update_settings(stale, {"engine": "pi"})
    fresh = await svc.get_agent("biz", "u1")
    with pytest.raises(InvalidRequest, match="agent parameter"):
        await svc.start_message(fresh, "x", agent_name="plan")
    with pytest.raises(InvalidRequest, match="agent parameter"):
        await svc.start_message(stale, "x", agent_name="plan")
    assert not await svc.store.list_tasks(stale["id"])
    [row] = await serving(svc, stale["id"])
    assert row["engine"] == "pi" and not _server(provider, row).prompts
    # 不带 agent 参数照常在 pi 上执行；opencode 下 agent 参数照常可用
    assert (await collect(await svc.start_message(stale, "hi")))[-1][1]["state"] == "SUCCEEDED"
    await svc.update_settings(stale, {"engine": "opencode"})
    runner = await svc.start_message(await svc.get_agent("biz", "u1"), "plan it", agent_name="plan")
    assert (await collect(runner))[-1][1]["state"] == "SUCCEEDED" and runner.sandbox["engine"] == "opencode"


# ---------- R3-03 并发修改设置不能互相覆盖 ----------


async def test_r3_03_patches_based_on_stale_reads_keep_each_others_fields(make_agents):
    a = await make_agents(run_maintainer=False, **BOTH)
    b = await make_agents(run_maintainer=False, **BOTH)
    snap_a = await a.ensure_agent("biz", "u1")
    snap_b = await b.get_agent("biz", "u1")  # 两个请求都读到初始设置 {}
    await a.update_settings(snap_a, {"engine": "pi"})
    info = await b.update_settings(snap_b, {"instructions": "KEEP THIS INSTRUCTION"})
    assert info["engine"] == "pi"
    stored = await a.store.get_agent_by_id(snap_a["id"])
    assert stored["settings"] == {"engine": "pi", "instructions": "KEEP THIS INSTRUCTION"}
    assert stored["settings_version"] == 3

    # 同一字段：后写的生效；engine=null 恢复默认，不影响其他字段
    await a.update_settings(snap_a, {"instructions": "A"})
    info = await b.update_settings(snap_b, {"instructions": "B", "engine": None})
    stored = await a.store.get_agent_by_id(snap_a["id"])
    assert stored["settings"] == {"instructions": "B"} and info["engine"] == "opencode"

    # 真并发：两个副本同时改不同字段
    mcp = {"docs": {"type": "remote", "url": "https://mcp.example.com/sse"}}
    await asyncio.gather(
        a.update_settings(snap_a, {"idle_destroy_after_s": 120}),
        b.update_settings(snap_b, {"mcp": mcp}),
        a.update_settings(snap_a, {"engine": "pi"}),
        b.update_settings(snap_b, {"instructions": "C"}),
    )
    stored = await a.store.get_agent_by_id(snap_a["id"])
    assert stored["settings"] == {"idle_destroy_after_s": 120.0, "mcp": mcp, "engine": "pi", "instructions": "C"}
    assert stored["settings_version"] == 9


# ---------- R3-04 跨副本重连只转发目标任务这一轮 ----------


async def test_r3_04_reattach_stream_stops_at_its_own_round(make_agents, engine):
    """A 执行 T1、B 跟随 T1；T1 结束后同一会话立即开始 T2（B 约 1 秒读一次库，还没发现 T1 已结束）。
    修复前 T1 的流里混入 T2 的输出。"""
    a, b = await _replicas(make_agents, engine)
    agent = await a.ensure_agent("biz", "u1")
    t1 = await a.start_message(agent, "[sleep:0.3] FIRST_ROUND")
    sid = await _started(t1)
    items: list = []
    follower = asyncio.create_task(_follow(b, agent, t1.task_id, items))
    assert (await final_task(a, t1.task_id))["state"] == TaskState.SUCCEEDED.value
    t2 = await a.start_message(agent, "SECOND_ROUND", session_id=sid)
    assert (await final_task(a, t2.task_id))["state"] == TaskState.SUCCEEDED.value
    await asyncio.wait_for(follower, 5)

    assert items[0][0] == "start" and items[0][1]["attached"] is True
    assert _text(items) == "echo: [sleep:0.3] FIRST_ROUND"
    assert items[-1][0] == "done" and items[-1][1]["result"] == "echo: [sleep:0.3] FIRST_ROUND"


async def test_r3_04_events_after_round_end_are_not_forwarded(make_agents, provider, engine):
    """这一轮结束（idle / agent_settled）之后、任务落库之前，同一会话再出现的输出（这里直接注入一条）不属于这个任务。"""
    a, b = await _replicas(make_agents, engine)
    agent = await a.ensure_agent("biz", "u1")
    t1 = await a.start_message(agent, "[sleep:0.3] FIRST_ROUND")
    sid = await _started(t1)
    [row] = await serving(a, agent["id"])
    server = _server(provider, row)

    entered, release = asyncio.Event(), asyncio.Event()
    orig = a.store.cas_task

    async def cas_task(task_id, from_states, **kw):
        if task_id == t1.task_id and "state" in kw:  # runner 落库终态
            entered.set()
            await release.wait()
        return await orig(task_id, from_states, **kw)

    a.store.cas_task = cas_task
    before = len(server.subscribers)
    items: list = []
    follower = asyncio.create_task(_follow(b, agent, t1.task_id, items))
    await wait_until(lambda: len(server.subscribers) > before)
    await asyncio.wait_for(entered.wait(), 5)
    if engine == "pi":
        # 带 message_start 的完整消息：跟随者看到了开头，只有「本轮结束后不再转发」能拦住它
        for event in ({"type": "message_start", "message": {"role": "assistant", "content": []}},
                      {"type": "message_update", "assistantMessageEvent": {"type": "text_delta", "delta": "STRAY"}}):
            server.emit("pi.event", {"sessionID": sid, "runID": "stray", "event": event})
    else:
        server.emit("message.part.updated", {"sessionID": sid, "part": {
            "id": "prt_stray", "messageID": "msg_stray", "sessionID": sid, "type": "text", "text": "STRAY"}})
    await asyncio.sleep(0.1)
    release.set()
    await asyncio.wait_for(follower, 5)

    assert _text(items) == "echo: [sleep:0.3] FIRST_ROUND"
    assert items[-1][0] == "done" and items[-1][1]["state"] == TaskState.SUCCEEDED.value


async def test_r3_04_reattach_after_round_ended_but_before_it_is_recorded(make_agents, provider, engine):
    """T1 在沙箱里已经结束、A 还没把结果落库时 B 重连：B 看不到 T1 的结束事件，要靠「会话开始新一轮时读库」
    发现 T1 已结束，不能把随后 T2 的输出当成 T1 的。"""
    a, b = await _replicas(make_agents, engine)
    agent = await a.ensure_agent("biz", "u1")
    t1 = await a.start_message(agent, "FIRST_ROUND")
    sid = await _started(t1)
    [row] = await serving(a, agent["id"])
    server = _server(provider, row)

    entered, release = asyncio.Event(), asyncio.Event()
    orig = a.store.cas_task

    async def cas_task(task_id, from_states, **kw):
        if task_id == t1.task_id and "state" in kw:  # runner 落库终态
            entered.set()
            await release.wait()
        return await orig(task_id, from_states, **kw)

    a.store.cas_task = cas_task
    await asyncio.wait_for(entered.wait(), 5)
    assert sid not in server.busy()
    before = len(server.subscribers)
    items: list = []
    follower = asyncio.create_task(_follow(b, agent, t1.task_id, items))
    await wait_until(lambda: len(server.subscribers) > before)
    await asyncio.sleep(0.05)  # B 处理完订阅建立时的那次读库（T1 仍是 RUNNING）
    release.set()
    assert (await final_task(a, t1.task_id))["state"] == TaskState.SUCCEEDED.value
    t2 = await a.start_message(agent, "SECOND_ROUND", session_id=sid)
    assert (await final_task(a, t2.task_id))["state"] == TaskState.SUCCEEDED.value
    await asyncio.wait_for(follower, 5)

    assert "SECOND_ROUND" not in _text(items)
    assert items[-1][0] == "done" and items[-1][1]["result"] == "echo: FIRST_ROUND"


async def test_r3_04_next_task_started_before_subscription(make_agents, provider, engine):
    """取快照之后、事件订阅建立之前 T1 结束、T2 已开始（B 错过了 T2 开始的事件）：订阅建立时读库发现 T1 已结束，
    不能把 T2 后续的输出转发给 T1。"""
    a, b = await _replicas(make_agents, engine)
    agent = await a.ensure_agent("biz", "u1")
    t1 = await a.start_message(agent, "[sleep:0.1] FIRST_ROUND")
    sid = await _started(t1)
    [row] = await serving(a, agent["id"])
    server = _server(provider, row)

    client = b.client_for(row)
    entered, release = asyncio.Event(), asyncio.Event()
    orig_events = client.events

    async def delayed_events():
        entered.set()
        await release.wait()
        async for ev in orig_events():
            yield ev

    client.events = delayed_events
    items: list = []
    follower = asyncio.create_task(_follow(b, agent, t1.task_id, items))
    await asyncio.wait_for(entered.wait(), 5)
    assert (await final_task(a, t1.task_id))["state"] == TaskState.SUCCEEDED.value
    t2 = await a.start_message(agent, "[sleep:0.2] SECOND_ROUND", session_id=sid)
    await wait_until(lambda: sid in server.busy())
    release.set()
    await asyncio.wait_for(follower, 5)
    assert (await final_task(a, t2.task_id))["state"] == TaskState.SUCCEEDED.value

    assert "SECOND_ROUND" not in _text(items)
    assert items[-1][0] == "done" and items[-1][1]["result"] == "echo: [sleep:0.1] FIRST_ROUND"


@pytest.mark.parametrize("same_prompt", [False, True], ids=["new-prompt", "same-prompt"])
async def test_r3_04_reattach_before_prompt_delivered_has_no_stale_snapshot(make_agents, provider, engine, same_prompt):
    """T3 的提示词还没送达引擎（会话里最后一轮仍是上一个任务的）时重连：不能把上一轮的回复当成 T3 的快照补发。
    same_prompt：连续两轮提示词相同（例如都是「继续」），只比对提示词文本分不出来。"""
    a, b = await _replicas(make_agents, engine)
    agent = await a.ensure_agent("biz", "u1")
    prev = "继续" if same_prompt else "SECOND_ROUND"
    this = "继续" if same_prompt else "THIRD_ROUND"
    sid = (await collect(await a.start_message(agent, prev)))[-1][1]["session_id"]
    [row] = await serving(a, agent["id"])
    server = _server(provider, row)

    client = a.client_for(row)
    entered, release = asyncio.Event(), asyncio.Event()
    orig = client.prompt_async

    async def paused(*args, **kw):
        entered.set()
        await release.wait()
        return await orig(*args, **kw)

    client.prompt_async = paused
    t3 = await a.start_message(agent, this, session_id=sid)
    await asyncio.wait_for(entered.wait(), 5)
    before = len(server.subscribers)
    items: list = []
    follower = asyncio.create_task(_follow(b, agent, t3.task_id, items))
    await wait_until(lambda: len(server.subscribers) > before)  # B 已订阅事件
    release.set()
    await asyncio.wait_for(follower, 5)

    assert not [d for k, d in items if k == "text" and d.get("snapshot")]
    assert _text(items) == f"echo: {this}"
    assert items[-1][0] == "done" and items[-1][1]["result"] == f"echo: {this}"


async def test_r3_04_reattach_mid_round_still_replays_this_rounds_text(make_agents, provider, engine):
    """正常情况不受影响：本轮运行中重连，先补发本轮已有的文本（不含上一轮），再接实时增量。"""
    a, b = await _replicas(make_agents, engine)
    agent = await a.ensure_agent("biz", "u1")
    sid = (await collect(await a.start_message(agent, "FIRST_ROUND")))[-1][1]["session_id"]
    [row] = await serving(a, agent["id"])
    server = _server(provider, row)
    prompt = "[partial][sleep:0.6] THIS_ROUND"
    t = await a.start_message(agent, prompt, session_id=sid)
    await wait_until(lambda: "PARTIAL" in json.dumps(server.sessions[sid]["messages"]))
    items: list = []
    await asyncio.wait_for(_follow(b, agent, t.task_id, items), 5)

    [snapshot] = [d for k, d in items if k == "text" and d.get("snapshot")]
    assert snapshot["delta"] == "PARTIAL "
    assert _text(items) == f"PARTIAL echo: {prompt}"
    assert items[-1][0] == "done" and items[-1][1]["state"] == TaskState.SUCCEEDED.value


# ---------- R3-N1（复核新发现）回复输出到一半时重连，正在输出的消息以全文为准 ----------


async def test_r3_n1_reattach_mid_message_replays_the_whole_message(make_agents, engine):
    """回复正在输出时跨副本重连（错过了这条消息 / 部件的开头；opencode 的增量不落盘、pi 的增量不带累积内容，快照里都
    没有它）。修复前 opencode 把缓存的中段增量与结束时的全文拼在一起（错位重复），pi 只有后半段。"""
    a, b = await _replicas(make_agents, engine)
    agent = await a.ensure_agent("biz", "u1")
    prompt = "LONG " + "x" * 1500  # 回复约 1.5k 字，内存版引擎约 1 秒输出完
    t = await a.start_message(agent, prompt)
    await wait_until(lambda: any(k == "text" for k, _ in t.history))  # A 已经收到一部分回复
    items: list = []
    await asyncio.wait_for(_follow(b, agent, t.task_id, items), 10)

    assert items[-1][0] == "done" and items[-1][1]["state"] == TaskState.SUCCEEDED.value
    assert _text(items) == items[-1][1]["result"] == f"echo: {prompt}"


def test_r3_n1_opencode_translator_prefers_full_text_over_mid_part_fragment():
    from sandbox_pool.agent.engines.opencode import Translator

    def delta(text):
        return {"type": "message.part.delta", "properties": {
            "sessionID": "s", "messageID": "m", "partID": "p", "field": "text", "delta": text}}

    def updated(text):
        return {"type": "message.part.updated", "properties": {
            "sessionID": "s", "part": {"id": "p", "messageID": "m", "sessionID": "s", "type": "text", "text": text}}}

    # 中途接入：只收到中段增量，再收到结束时的全文
    tr = Translator("s")
    out = [i for e in (delta("wor"), delta("ld"), updated("hello world")) for i in tr.feed(e)]
    assert "".join(d["delta"] for _, d in out) == "hello world"
    # 正常：开始（空文本）→ 增量 → 结束（全文），与修复前相同
    tr = Translator("s")
    out = [i for e in (updated(""), delta("hello "), delta("world"), updated("hello world")) for i in tr.feed(e)]
    assert [d["delta"] for _, d in out] == ["hello ", "world"]
    # 增量先于开始事件到达（类型未知先缓存）：缓存是全文的开头，照常输出
    tr = Translator("s")
    out = [i for e in (delta("hello "), updated("hello world")) for i in tr.feed(e)]
    assert [d["delta"] for _, d in out] == ["hello ", "world"]


def test_r3_n1_pi_translator_holds_message_joined_mid_way_until_message_end():
    from sandbox_pool.agent.engines.pi import PiTranslator

    def ev(event):
        return {"type": "pi.event", "properties": {"sessionID": "s", "runID": "r", "event": event}}

    def update(kind, text):
        return ev({"type": "message_update", "assistantMessageEvent": {"type": kind, "delta": text}})

    end = ev({"type": "message_end", "message": {"role": "assistant", "content": [
        {"type": "thinking", "thinking": "想一想"}, {"type": "text", "text": "hello world"}]}})
    start = ev({"type": "message_start", "message": {"role": "assistant", "content": []}})
    # 中途接入（mid_round）：没看到这条消息的开始，增量不转发，message_end 时整条输出；下一条消息照常实时转发
    tr = PiTranslator("s")
    tr.mid_round = True
    out = [i for e in (update("text_delta", "world"), end, start, update("text_delta", "next")) for i in tr.feed(e)]
    assert out == [("reasoning", {"delta": "想一想"}), ("text", {"delta": "hello world"}), ("text", {"delta": "next"})]
    # runner 从头跟进（不是 mid_round）：与修复前相同，增量实时转发，message_end 不重复输出
    tr = PiTranslator("s")
    out = [i for e in (start, update("text_delta", "hello "), update("text_delta", "world"), end) for i in tr.feed(e)]
    assert out == [("text", {"delta": "hello "}), ("text", {"delta": "world"})]
