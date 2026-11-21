"""管理员实例清单、暂停与删除。FakeProvider，不访问云端。"""

import time

import httpx

from sandbox_pool.api.app import create_app
from sandbox_pool.models import SandboxState


def h(key: str) -> dict:
    return {"Authorization": f"Bearer {key}"}


async def _ready_row(pool):
    await pool.maintainer.replenish(time.time())
    await pool.lifecycle.wait_background()
    rows = await pool.store.list_sandboxes([SandboxState.READY])
    assert len(rows) == 1
    return rows[0]


def _by(items, **cond):
    for item in items:
        if all(item.get(k) == v for k, v in cond.items()):
            return item
    raise AssertionError(cond)


async def test_inventory_pause_missing_and_orphan(make_pool, provider):
    pool = await make_pool(target_size=1, max_size=1, run_maintainer=False)
    app = create_app(pool=pool)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://pool") as c:
        row = await _ready_row(pool)
        listed = (await c.get("/v1/admin/instances")).json()
        item = _by(listed, row_id=row["id"])
        assert item["kind"] == "code" and item["cloud_state"] == "running" and item["db_state"] == "READY"
        assert "access_token" not in str(listed) and "lease_id" not in str(listed)

        paused = await c.post(f"/v1/admin/instances/{row['id']}/pause")
        assert paused.status_code == 200, paused.text
        assert paused.json()["state"] == "PAUSED"
        again = await c.post(f"/v1/admin/instances/{row['id']}/pause")
        assert again.status_code == 200 and again.json()["changed"] is False
        item = _by((await c.get("/v1/admin/instances")).json(), row_id=row["id"])
        assert item["cloud_state"] == "paused" and item["db_state"] == "PAUSED" and item["kind"] == "code"

        await provider.kill(row["provider_id"])
        item = _by((await c.get("/v1/admin/instances")).json(), row_id=row["id"])
        assert item["kind"] == "missing" and item["cloud_state"] is None
        gone = await c.request("DELETE", "/v1/admin/instances", json={"row_id": row["id"]})
        assert gone.status_code == 200, gone.text
        assert await pool.store.get_sandbox(row["id"]) is None

        orphan = await provider.create("tpl", {"pool": "stray"}, 60)
        item = _by((await c.get("/v1/admin/instances")).json(), provider_id=orphan)
        assert item["kind"] == "orphan" and item["cloud_state"] == "running" and item["row_id"] is None
        killed = await c.request("DELETE", "/v1/admin/instances", json={"provider_id": orphan})
        assert killed.status_code == 200 and killed.json()["killed"] is True
        assert orphan not in provider.sandboxes


async def test_pause_rejects_leased_and_delete_ends_lease(make_pool):
    pool = await make_pool(target_size=1, max_size=1, idle_pause_after_s=3600, run_maintainer=False)
    app = create_app(pool=pool)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://pool") as c:
        await _ready_row(pool)
        grant = await pool.allocator.acquire()
        denied = await c.post(f"/v1/admin/instances/{grant.sandbox_row_id}/pause")
        assert denied.status_code == 409
        deleted = await c.request("DELETE", "/v1/admin/instances", json={"row_id": grant.sandbox_row_id})
        assert deleted.status_code == 200, deleted.text
        assert deleted.json()["lease_ended"] is True
        assert grant.sandbox_id not in provider_of(pool)
        assert (await pool.allocator.get_lease(grant.lease_id))["state"] == "RELEASED"


def provider_of(pool):
    return pool.provider.sandboxes


async def test_transitional_delete_conflicts(make_pool):
    pool = await make_pool(target_size=0, max_size=5, run_maintainer=False)
    app = create_app(pool=pool)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://pool") as c:
        row = await pool.store.reserve_slot(
            template="t", now=time.time(), owner="admin-test", op_deadline=time.time() + 30, limit=5, cap=5
        )
        denied = await c.request("DELETE", "/v1/admin/instances", json={"row_id": row["id"]})
        assert denied.status_code == 409
        both = await c.request("DELETE", "/v1/admin/instances", json={"row_id": row["id"], "provider_id": "x"})
        assert both.status_code == 400


async def test_agent_pause_rejected_and_delete_keeps_identity(make_pool, make_agents):
    pool = await make_pool(target_size=0, api_keys="biz:k-biz", admin_keys="ops:k-admin")
    agents = await make_agents(run_maintainer=False)
    app = create_app(pool=pool, agents=agents)
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://pool") as c:
        assert (await c.get("/admin")).status_code == 200
        assert "沙箱实例管理" in (await c.get("/admin")).text
        assert (await c.get("/v1/admin/instances", headers=h("k-biz"))).status_code == 403
        created = await c.post(
            "/v1/agents/u1/messages", json={"text": "hi", "stream": False}, headers=h("k-biz")
        )
        assert created.status_code == 200, created.text
        task_id = created.json()["task_id"]
        listed = (await c.get("/v1/admin/instances", headers=h("k-admin"))).json()
        assert "access_token" not in str(listed) and "lease_id" not in str(listed)
        item = _by(listed, kind="agent")
        assert item["user_id"] == "u1" and item["cloud_state"] == "running" and item["db_state"] == "ACTIVE"
        denied = await c.post(f"/v1/admin/instances/{item['row_id']}/pause", headers=h("k-admin"))
        assert denied.status_code == 400
        deleted = await c.request(
            "DELETE", "/v1/admin/instances", json={"row_id": item["row_id"]}, headers=h("k-admin")
        )
        assert deleted.status_code == 200, deleted.text
        assert (await c.get(f"/v1/agents/u1/tasks/{task_id}", headers=h("k-biz"))).status_code == 200
        info = (await c.get("/v1/agents/u1", headers=h("k-biz"))).json()
        assert info["user_id"] == "u1" and info["sandboxes"] == []
        assert item["provider_id"] not in pool.provider.sandboxes
