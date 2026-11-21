"""管理页模板缓存。假的 POP 客户端，不访问云端。"""

from types import SimpleNamespace

import httpx

from sandbox_pool.api.app import create_app
from sandbox_pool.core.template_cache import TemplateCaches, join_views


def _tpl(template_id, name):
    return SimpleNamespace(template_id=template_id, name=name)


def _cache(template_id, status, **extra):
    defaults = dict(progress=None, image_size_mb=None, created_time=None, ready_time=None, status_reason=None)
    defaults.update(extra)
    return SimpleNamespace(template_id=template_id, status=status, **defaults)


class FakeSdk:
    def __init__(self):
        self.templates = [_tpl("tpl-a", "alpha")]
        self.caches = []
        self.creates = []
        self.deletes = []
        self.template_tokens = []

    def list_templates(self, request):
        self.template_tokens.append(request.next_token)
        if request.next_token == "page-2":
            templates, token = [_tpl("tpl-b", "beta")], ""
        elif self.pages:
            templates, token = [_tpl("tpl-a", "alpha")], "page-2"
        else:
            templates, token = self.templates, None
        return SimpleNamespace(body=SimpleNamespace(code="200", templates=templates, next_token=token))

    def list_template_cache(self, request):
        items = self.caches
        if request.template_id:
            items = [item for item in items if item.template_id == request.template_id]
        return SimpleNamespace(body=SimpleNamespace(code="200", template_caches=items, next_token=None))

    def create_template_cache(self, request):
        template_id = request.body.template_id
        self.creates.append(template_id)
        created = _cache(template_id, "InProgress", progress=10, image_size_mb=512)
        self.caches.append(created)
        return SimpleNamespace(body=SimpleNamespace(code="200", message="success", template_cache=created))

    def delete_template_cache(self, template_id, request):
        self.deletes.append((template_id, request.team_id))
        return SimpleNamespace(body=SimpleNamespace(code="200", message="success"))

    pages = False


def _app(pool, sdk=None):
    app = create_app(pool=pool)
    if sdk is not None:
        app.state.template_cache = TemplateCaches(sdk, "team-1")
    return app


def _client(app):
    return httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://pool")


async def test_template_cache_unconfigured_and_page(make_pool, monkeypatch):
    for name in (
        "ALIBABA_CLOUD_ACCESS_KEY_ID",
        "ALIBABA_CLOUD_ACCESS_KEY_SECRET",
        "FCSANDBOX_REGION_ID",
        "FCSANDBOX_TEAM_ID",
    ):
        monkeypatch.delenv(name, raising=False)
    pool = await make_pool(target_size=0, run_maintainer=False)
    app = _app(pool)
    async with _client(app) as c:
        listed = await c.get("/v1/admin/template-caches")
        assert listed.status_code == 200, listed.text
        body = listed.json()
        assert body["configured"] is False and body["templates"] == []
        assert "ALIBABA_CLOUD_ACCESS_KEY_ID" in body["detail"]
        created = await c.post("/v1/admin/template-caches", json={"template_id": "tpl-a"})
        assert created.status_code == 503 and created.json()["error"] == "TemplateCacheUnavailable"
        removed = await c.delete("/v1/admin/template-caches/tpl-a")
        assert removed.status_code == 503
        page = await c.get("/admin")
        assert page.status_code == 200 and "模板缓存" in page.text
        assert "删除后按容量计时的费用停止" in page.text


async def test_template_cache_create_blocks_success_and_allows_evicted(make_pool):
    pool = await make_pool(target_size=0, run_maintainer=False)
    sdk = FakeSdk()
    app = _app(pool, sdk)
    async with _client(app) as c:
        listed = await c.get("/v1/admin/template-caches")
        assert listed.status_code == 200, listed.text
        row = listed.json()["templates"][0]
        assert row["template_id"] == "tpl-a" and row["name"] == "alpha" and row["cache"] is None

        created = await c.post("/v1/admin/template-caches", json={"template_id": "tpl-a"})
        assert created.status_code == 200, created.text
        assert created.json()["status"] == "InProgress" and sdk.creates == ["tpl-a"]

        sdk.caches = [_cache("tpl-a", "Success", image_size_mb=2048, ready_time="2026-11-21T12:00:00Z")]
        again = await c.post("/v1/admin/template-caches", json={"template_id": "tpl-a"})
        assert again.status_code == 409 and again.json()["error"] == "TemplateCacheExists"
        assert again.json()["cache"]["status"] == "Success"
        assert sdk.creates == ["tpl-a"]

        sdk.caches = [_cache("tpl-a", "Evicted")]
        renewed = await c.post("/v1/admin/template-caches", json={"template_id": "tpl-a"})
        assert renewed.status_code == 200, renewed.text
        assert sdk.creates == ["tpl-a", "tpl-a"]

        removed = await c.delete("/v1/admin/template-caches/tpl-a")
        assert removed.status_code == 200 and removed.json()["deleted"] is True
        assert sdk.deletes == [("tpl-a", "team-1")]


async def test_template_cache_pages_and_prefers_active_cache(make_pool):
    pool = await make_pool(target_size=0, run_maintainer=False)
    sdk = FakeSdk()
    sdk.pages = True
    sdk.caches = [_cache("tpl-a", "Evicted"), _cache("tpl-a", "Success", progress=100, image_size_mb=8)]
    app = _app(pool, sdk)
    async with _client(app) as c:
        body = (await c.get("/v1/admin/template-caches")).json()
    assert [row["template_id"] for row in body["templates"]] == ["tpl-a", "tpl-b"]
    assert body["templates"][0]["cache"]["status"] == "Success"
    assert sdk.template_tokens == [None, "page-2"]


def test_join_views_keeps_cache_without_template():
    rows = join_views([], [_cache("only-cache", "Failed", status_reason="pull failed")])
    assert rows == [{
        "template_id": "only-cache",
        "name": "",
        "cache": {
            "status": "Failed",
            "progress": None,
            "image_size_mb": None,
            "created_time": None,
            "ready_time": None,
            "status_reason": "pull failed",
        },
    }]
