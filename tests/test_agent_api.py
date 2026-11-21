"""agent 子系统的 HTTP 接口：SSE / JSON 两种返回、任务与重连、出网策略、设置、定时任务、错误码、鉴权隔离。"""

import json

import httpx
import pytest

from sandbox_pool.agent.service import boot_view
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
        assert info["sandboxes"][0]["boot"]["total_ms"] is not None
        assert info["sandboxes"][0]["boot"]["running"] is None
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


def _creates(provider) -> int:
    return sum(1 for call in provider.calls if call[0] == "create_app")


def test_boot_view_running_step():
    create = {"kind": "agent_create", "duration_ms": 10, "ts": 1.0}
    warming = boot_view([create], state="WARMING", engine="opencode")
    assert warming["running"] == "health" and warming["total_ms"] is None
    assert warming["steps"] == [{"name": "create", "duration_ms": 10, "finished_at": 1.0}]
    assert boot_view([create], state="WARMING", engine="opencode2")["running"] == "password"
    done = [
        create,
        {"kind": "agent_boot_health", "duration_ms": 20, "ts": 2},
        {"kind": "agent_boot_files", "duration_ms": 5, "ts": 3},
        {"kind": "agent_boot_preload", "duration_ms": 4, "ts": 4},
        {"kind": "agent_boot_activate", "duration_ms": 1, "ts": 5},
        {"kind": "agent_boot", "duration_ms": 40, "ts": 5},
    ]
    view = boot_view(done, state="ACTIVE", engine="opencode")
    assert view["running"] is None and view["total_ms"] == 40
    assert [step["name"] for step in view["steps"]] == ["create", "health", "files", "preload", "activate"]


async def test_start_sandbox_reports_steps_and_reuses(make_pool, make_agents, provider):
    svc, c = await _setup(make_pool, make_agents)
    async with c:
        r = await c.post("/v1/agents/u1/sandbox", json={})
        assert r.status_code == 200, r.text
        body = r.json()
        assert body["fresh"] is True and body["sandbox"]["state"] == "ACTIVE"
        names = [step["name"] for step in body["boot"]["steps"]]
        assert names == ["create", "health", "files", "preload", "activate"]
        assert body["boot"]["running"] is None
        assert sum(step["duration_ms"] for step in body["boot"]["steps"]) == pytest.approx(body["boot"]["total_ms"])
        assert "access_token" not in r.text
        created = _creates(provider)
        assert created == 1

        again = await c.post("/v1/agents/u1/sandbox", json={})
        assert again.status_code == 200, again.text
        assert again.json()["fresh"] is False
        assert _creates(provider) == created

        msg = await c.post("/v1/agents/u1/messages", json={"text": "hi", "stream": False})
        assert msg.status_code == 200, msg.text
        assert msg.json()["state"] == "SUCCEEDED"
        assert _creates(provider) == created


async def test_start_opencode2_includes_password_step(make_pool, make_agents):
    svc, c = await _setup(make_pool, make_agents, agent_opencode2_template="tpl-oc2")
    async with c:
        r = await c.post("/v1/agents/u1/sandbox", json={"engine": "opencode2"})
        assert r.status_code == 200, r.text
        assert [step["name"] for step in r.json()["boot"]["steps"]] == [
            "create", "password", "health", "files", "preload", "activate",
        ]
        assert r.json()["sandbox"]["engine"] == "opencode2"


async def test_start_sandbox_rejects_unknown_engine_and_reports_failed_attempts(make_pool, make_agents, provider):
    svc, c = await _setup(make_pool, make_agents)
    async with c:
        bad = await c.post("/v1/agents/u1/sandbox", json={"engine": "nope"})
        assert bad.status_code == 400

        provider.fail_create_app = 10
        failed = await c.post("/v1/agents/u2/sandbox", json={})
        assert failed.status_code == 502, failed.text
        body = failed.json()
        assert body["error"] == "SandboxOpError" and "failed to start 3 times" in body["detail"]
        assert len(body["attempts"]) == 3
        for item in body["attempts"]:
            assert item["sandbox_row_id"] and item["steps"] == []
            assert "injected create_app failure" in item["error"]

        missed = await c.post("/v1/agents/u3/messages", json={"text": "hi", "stream": False})
        assert missed.status_code == 502
        assert "attempts" not in missed.json()
        assert "failed to start 3 times" in missed.json()["detail"]


async def test_console_page_is_unauthenticated(make_pool, make_agents):
    svc, c = await _setup(make_pool, make_agents, auth=True)
    async with c:
        r = await c.get("/console")
        assert r.status_code == 200
        assert "text/html" in r.headers["content-type"]
        assert "沙箱 agent 操作台" in r.text
        assert "休眠沙箱" in r.text
        assert "沙箱文件" in r.text
        assert (await c.get("/v1/agent-engines")).status_code == 401


async def test_hibernate_resumes_on_next_message(make_pool, make_agents, provider):
    pool = await make_pool(target_size=0)
    agents = await make_agents(run_maintainer=False)
    app = create_app(pool=pool, agents=agents)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://pool") as c:
        first = await c.post("/v1/agents/u1/messages", json={"text": "hi", "stream": False})
        assert first.status_code == 200, first.text
        sid = first.json()["session_id"]
        created = sum(1 for call in provider.calls if call[0] == "create_app")
        paused = await c.post("/v1/agents/u1/sandbox/pause")
        assert paused.status_code == 200, paused.text
        assert paused.json()["changed"] is True and paused.json()["state"] == "PAUSED"
        info = (await c.get("/v1/agents/u1")).json()
        assert info["sandboxes"][0]["state"] == "PAUSED"
        assert provider.sandboxes[paused.json()["provider_id"]]["state"] == "paused"
        await agents.maintainer.tick()
        assert (await c.get("/v1/agents/u1")).json()["sandboxes"][0]["state"] == "PAUSED"
        again = await c.post("/v1/agents/u1/sandbox/pause")
        assert again.status_code == 200 and again.json()["changed"] is False
        nxt = await c.post("/v1/agents/u1/messages", json={"text": "继续", "session_id": sid, "stream": False})
        assert nxt.status_code == 200, nxt.text
        assert nxt.json()["state"] == "SUCCEEDED"
        assert sum(1 for call in provider.calls if call[0] == "create_app") == created
        assert (await c.get("/v1/agents/u1")).json()["sandboxes"][0]["state"] == "ACTIVE"


async def test_sandbox_files_roundtrip_and_pause_does_not_resume(make_pool, make_agents, provider):
    pool = await make_pool(target_size=0, max_body_bytes=16)
    agents = await make_agents(run_maintainer=False, max_upload_bytes=32)
    app = create_app(pool=pool, agents=agents)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://pool") as c:
        missing = await c.get("/v1/agents/u1/files")
        assert missing.status_code == 404 and missing.json()["error"] == "AgentNotFound"

        started = await c.post("/v1/agents/u1/sandbox", json={})
        assert started.status_code == 200, started.text

        root = await c.get("/v1/agents/u1/files")
        assert root.status_code == 200, root.text
        assert root.json()["path"] == "/"
        names = {item["name"]: item["type"] for item in root.json()["entries"]}
        assert names["home"] == "dir"

        work = await c.get("/v1/agents/u1/files", params={"path": "/home/user/workspace"})
        assert work.status_code == 200, work.text
        assert {item["name"] for item in work.json()["entries"]} >= {"opencode.json", "AGENTS.md"}

        relative = await c.get("/v1/agents/u1/files", params={"path": "workspace"})
        assert relative.status_code == 400 and relative.json()["error"] == "InvalidRequest"

        payload = b"abc\x00def"
        put = await c.put(
            "/v1/agents/u1/files",
            params={"path": "/home/user/workspace/../workspace/note.bin"},
            content=payload,
        )
        assert put.status_code == 200, put.text
        assert put.json() == {"path": "/home/user/workspace/note.bin", "size": len(payload)}
        downloaded = await c.get("/v1/agents/u1/files/content", params={"path": "/home/user/workspace/note.bin"})
        assert downloaded.status_code == 200 and downloaded.content == payload

        listed_file = await c.get("/v1/agents/u1/files", params={"path": "/home/user/workspace/note.bin"})
        assert listed_file.status_code == 400 and listed_file.json()["error"] == "InvalidRequest"
        directory = await c.get("/v1/agents/u1/files/content", params={"path": "/home/user/workspace"})
        assert directory.status_code == 400 and directory.json()["error"] == "InvalidRequest"
        absent = await c.get("/v1/agents/u1/files", params={"path": "/no/such"})
        assert absent.status_code == 404 and absent.json()["error"] == "FileNotFound"

        small = await c.put("/v1/agents/u1/files", params={"path": "/tmp/ok.bin"}, content=b"x" * 20)
        assert small.status_code == 200, small.text
        big = await c.put("/v1/agents/u1/files", params={"path": "/tmp/big.bin"}, content=b"x" * 40)
        assert big.status_code == 413 and big.json()["error"] == "PayloadTooLarge"

        paused = await c.post("/v1/agents/u1/sandbox/pause")
        assert paused.status_code == 200, paused.text
        blocked = await c.get("/v1/agents/u1/files")
        assert blocked.status_code == 409 and blocked.json()["error"] == "SandboxBusy"
        pid = paused.json()["provider_id"]
        assert provider.sandboxes[pid]["state"] == "paused"
        assert not any(call[0] == "resume" for call in provider.calls)


async def test_agent_subsystem_disabled(make_pool):
    pool = await make_pool(target_size=0)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=create_app(pool=pool)), base_url="http://pool") as c:
        r = await c.post("/v1/agents/u1/messages", json={"text": "hi"})
        assert r.status_code == 503 and r.json()["error"] == "AgentUnavailable"
