"""agent 子系统的 HTTP 接口：SSE / JSON 两种返回、任务与重连、出网策略、设置、定时任务、错误码、鉴权隔离。"""

import json

import httpx

from sandbox_pool.api.app import create_app


def parse_sse(text: str) -> list[tuple[str, dict]]:
    events = []
    for block in text.split("\n\n"):
        kind, data = None, []
        for line in block.split("\n"):
            if line.startswith("event:"):
                kind = line[6:].strip()
            elif line.startswith("data:"):
                data.append(line[5:].strip())
        if kind:
            events.append((kind, json.loads("\n".join(data))))
    return events


async def _setup(make_pool, make_agents, *, auth: bool = False, **agent_overrides):
    keys = dict(api_keys="biz:k-biz,other:k-other", admin_keys="ops:k-admin") if auth else {}
    pool = await make_pool(target_size=0, **keys)
    svc = await make_agents(**agent_overrides)
    client = httpx.AsyncClient(
        transport=httpx.ASGITransport(app=create_app(pool=pool, agents=svc)), base_url="http://pool", timeout=30
    )
    return svc, client


def h(key: str) -> dict:
    return {"Authorization": f"Bearer {key}"}


async def test_message_stream_and_json(make_pool, make_agents):
    svc, c = await _setup(make_pool, make_agents)
    async with c:
        r = await c.post("/v1/agents/u1/messages", json={"text": "你好"})
        assert r.status_code == 200 and r.headers["content-type"].startswith("text/event-stream")
        events = parse_sse(r.text)
        assert events[0][0] == "start" and events[-1][0] == "done"
        assert events[-1][1]["state"] == "SUCCEEDED" and events[-1][1]["result"] == "echo: 你好"
        sid = events[0][1]["session_id"]

        r = await c.post("/v1/agents/u1/messages", json={"text": "[remember]", "session_id": sid, "stream": False})
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["state"] == "SUCCEEDED" and body["result"] == "remembered: 你好" and body["usage"]["output"] > 0

        assert (await c.post("/v1/agents/u1/messages", json={"text": ""})).status_code == 422
        r = await c.post("/v1/agents/u1/messages", json={"text": "x", "session_id": "ses_gone", "stream": False})
        assert r.status_code == 409


async def test_agent_info_tasks_reattach_and_abort(make_pool, make_agents):
    svc, c = await _setup(make_pool, make_agents)
    async with c:
        assert (await c.get("/v1/agents/u1")).status_code == 404
        task = (await c.post("/v1/agents/u1/messages", json={"text": "hi", "stream": False})).json()
        info = (await c.get("/v1/agents/u1")).json()
        assert info["user_id"] == "u1" and info["sandboxes"][0]["state"] == "ACTIVE"
        assert "access_token" not in json.dumps(info)
        tasks = (await c.get("/v1/agents/u1/tasks", params={"source": "message"})).json()
        assert [t["task_id"] for t in tasks] == [task["task_id"]]
        assert (await c.get(f"/v1/agents/u1/tasks/{task['task_id']}")).json()["result"] == "echo: hi"
        assert (await c.get("/v1/agents/u1/tasks/nope")).status_code == 404
        r = await c.get(f"/v1/agents/u1/tasks/{task['task_id']}/stream")
        assert parse_sse(r.text) == [("done", parse_sse(r.text)[0][1])] and parse_sse(r.text)[0][1]["state"] == "SUCCEEDED"
        assert (await c.post(f"/v1/agents/u1/tasks/{task['task_id']}/abort")).status_code == 409

        # 运行中的任务：中止
        agent = await svc.get_agent("anonymous", "u1")
        runner = await svc.start_message(agent, "[sleep:5] 长任务")
        r = await c.post(f"/v1/agents/u1/tasks/{runner.task_id}/abort")
        assert r.status_code == 200
        await runner.done.wait()
        assert (await c.get(f"/v1/agents/u1/tasks/{runner.task_id}")).json()["state"] == "ABORTED"

        r = await c.delete("/v1/agents/u1/sandbox")
        assert r.status_code == 200 and r.json()["destroyed"] == 1


async def test_busy_session_409_and_too_many_429(make_pool, make_agents):
    svc, c = await _setup(make_pool, make_agents, agent_max_running_tasks=1)
    async with c:
        agent = await svc.ensure_agent("anonymous", "u1")
        runner = await svc.start_message(agent, "[sleep:1] 占着")
        start = (await runner.subscribe().get())[1]
        r = await c.post("/v1/agents/u1/messages", json={"text": "插队", "session_id": start["session_id"]})
        assert r.status_code == 409
        r = await c.post("/v1/agents/u1/messages", json={"text": "新会话"})
        assert r.status_code == 429
        await runner.done.wait()


async def test_egress_and_settings_endpoints(make_pool, make_agents):
    svc, c = await _setup(make_pool, make_agents)
    async with c:
        r = await c.get("/v1/agents/u1/egress")
        assert r.status_code == 200 and r.json()["desired"]["mode"] == "open" and r.json()["sandboxes"] == []
        await c.post("/v1/agents/u1/messages", json={"text": "hi", "stream": False})
        r = await c.put("/v1/agents/u1/egress", json={"deny_out": ["evil.example.com"]})
        assert r.status_code == 400 and "IP / CIDR" in r.json()["detail"]
        r = await c.put("/v1/agents/u1/egress", json={"mode": "allowlist", "allow_out": ["pypi.org"]})
        body = r.json()
        assert r.status_code == 200 and body["desired"]["mode"] == "allowlist" and body["sandboxes"][0]["in_sync"]
        assert "sk-test-key" not in r.text
        r = await c.put("/v1/agents/u1/egress", json=None)
        assert r.json()["desired"]["mode"] == "open" and r.json()["override"] is None

        assert (await c.patch("/v1/agents/u1/settings", json={"idle_destroy_after_s": -5})).status_code == 400
        r = await c.patch("/v1/agents/u1/settings", json={"idle_destroy_after_s": 7200, "instructions": "说中文"})
        assert r.status_code == 200 and r.json()["settings"]["idle_destroy_after_s"] == 7200


async def test_schedule_endpoints(make_pool, make_agents):
    svc, c = await _setup(make_pool, make_agents)
    async with c:
        bad = await c.post("/v1/agents/u1/schedules", json={"name": "x", "prompt": "p", "cron": "* * * * *", "every_s": 60})
        assert bad.status_code == 400
        r = await c.post("/v1/agents/u1/schedules", json={"name": "日报", "prompt": "写日报", "cron": "0 9 * * 1-5"})
        assert r.status_code == 200, r.text
        sched = r.json()
        assert sched["timezone"] == "Asia/Shanghai" and sched["next_run_at"]
        assert [s["id"] for s in (await c.get("/v1/agents/u1/schedules")).json()] == [sched["id"]]
        r = await c.patch(f"/v1/agents/u1/schedules/{sched['id']}", json={"enabled": False})
        assert r.json()["enabled"] == 0 and r.json()["next_run_at"] is None
        r = await c.post(f"/v1/agents/u1/schedules/{sched['id']}/run")
        assert r.status_code == 200 and r.json()["source"] == "schedule"
        task_id = r.json()["task_id"]
        agent = await svc.get_agent("anonymous", "u1")
        runner = svc.runners.get(task_id)
        if runner is not None:
            await runner.done.wait()
        r = await c.get("/v1/agents/u1/tasks", params={"schedule_id": sched["id"]})
        assert r.json()[0]["task_id"] == task_id
        assert (await c.delete(f"/v1/agents/u1/schedules/{sched['id']}")).status_code == 200
        assert (await c.get(f"/v1/agents/u1/schedules/{sched['id']}")).status_code == 404
        assert agent["user_id"] == "u1"


async def test_auth_isolation_and_admin(make_pool, make_agents):
    svc, c = await _setup(make_pool, make_agents, auth=True)
    async with c:
        assert (await c.post("/v1/agents/u1/messages", json={"text": "hi"})).status_code == 401
        r = await c.post("/v1/agents/u1/messages", json={"text": "hi", "stream": False}, headers=h("k-biz"))
        assert r.status_code == 200
        task_id = r.json()["task_id"]
        # 另一个调用方的同名 user_id 是另一个 agent，看不到 biz 的任务
        assert (await c.get("/v1/agents/u1", headers=h("k-other"))).status_code == 404
        assert (await c.get(f"/v1/agents/u1/tasks/{task_id}", headers=h("k-other"))).status_code == 404
        assert (await c.get("/v1/admin/agents", headers=h("k-biz"))).status_code == 403
        r = await c.get("/v1/admin/agents", headers=h("k-admin"))
        assert r.status_code == 200 and r.json()[0]["client_id"] == "biz"
        assert "access_token" not in r.text
        r = await c.get("/v1/admin/agents/stats", headers=h("k-admin"))
        assert r.status_code == 200 and r.json()["sandboxes"]["ACTIVE"] == 1 and "agent_boot" in r.json()["latency_ms"]
        assert (await c.get("/v1/admin/agents/stats", headers=h("k-biz"))).status_code == 403


async def test_agent_subsystem_disabled(make_pool):
    pool = await make_pool(target_size=0)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(pool=pool)), base_url="http://pool") as c:
        r = await c.post("/v1/agents/u1/messages", json={"text": "hi"})
        assert r.status_code == 503 and r.json()["error"] == "AgentUnavailable"
