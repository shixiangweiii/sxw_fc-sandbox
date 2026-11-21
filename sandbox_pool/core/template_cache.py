"""模板缓存：列出本团队模板并与平台缓存对齐，新建或删除。

凭证与构建模板的脚本相同，不进入 PoolConfig。缺任一必填项时不创建客户端，服务照常启动。
SDK 是同步的，对外方法放到线程里，避免堵住事件循环。
"""

import asyncio
import os
from typing import Optional

_REQUIRED = (
    "ALIBABA_CLOUD_ACCESS_KEY_ID",
    "ALIBABA_CLOUD_ACCESS_KEY_SECRET",
    "FCSANDBOX_REGION_ID",
    "FCSANDBOX_TEAM_ID",
)
_PAGE_SIZE = 100
_MAX_PAGES = 20
# 同一模板有多条记录时，页面只展示这一条：正在用的优先于已淘汰的
_STATUS_RANK = {"Success": 0, "InProgress": 1, "Deleting": 2, "Failed": 3, "Evicted": 4}
_BLOCK_CREATE = frozenset({"Success", "InProgress"})


class TemplateCacheExists(Exception):
    """该模板已有进行中或成功的缓存，不再创建一次。"""

    def __init__(self, cache: dict):
        self.cache = cache
        super().__init__(f"template cache is {cache.get('status')}")


def missing_credentials() -> list[str]:
    return [name for name in _REQUIRED if not os.environ.get(name, "").strip()]


def client_from_env() -> Optional["TemplateCaches"]:
    """四个必填项都在时返回客户端，否则 None。"""
    if missing_credentials():
        return None
    from alibabacloud_fcsandbox20260509.client import Client
    from alibabacloud_tea_openapi import models as open_api_models

    config = open_api_models.Config(
        access_key_id=os.environ["ALIBABA_CLOUD_ACCESS_KEY_ID"].strip(),
        access_key_secret=os.environ["ALIBABA_CLOUD_ACCESS_KEY_SECRET"].strip(),
        security_token=os.environ.get("ALIBABA_CLOUD_SECURITY_TOKEN") or None,
        region_id=os.environ["FCSANDBOX_REGION_ID"].strip(),
    )
    endpoint = os.environ.get("FCSANDBOX_ENDPOINT", "").strip()
    if endpoint:
        config.endpoint = endpoint
    return TemplateCaches(Client(config), os.environ["FCSANDBOX_TEAM_ID"].strip())


def _field(obj, name: str):
    if isinstance(obj, dict):
        return obj.get(name)
    return getattr(obj, name, None)


def cache_view(raw) -> Optional[dict]:
    if raw is None:
        return None
    return {
        "status": _field(raw, "status"),
        "progress": _field(raw, "progress"),
        "image_size_mb": _field(raw, "image_size_mb"),
        "created_time": _field(raw, "created_time"),
        "ready_time": _field(raw, "ready_time"),
        "status_reason": _field(raw, "status_reason"),
    }


def pick_cache(items: list) -> Optional[dict]:
    views = [view for view in (cache_view(item) for item in items) if view is not None]
    if not views:
        return None
    return min(views, key=lambda item: _STATUS_RANK.get(item.get("status"), 9))


def join_views(templates: list, caches: list) -> list[dict]:
    """模板列表为主。缓存里多出来的模板 ID 也保留，避免已有缓存被漏掉。"""
    by_template: dict[str, list] = {}
    for item in caches:
        template_id = _field(item, "template_id")
        if template_id:
            by_template.setdefault(template_id, []).append(item)
    rows = []
    seen = set()
    for item in templates:
        template_id = _field(item, "template_id")
        if not template_id or template_id in seen:
            continue
        seen.add(template_id)
        rows.append({
            "template_id": template_id,
            "name": _field(item, "name") or "",
            "cache": pick_cache(by_template.get(template_id, [])),
        })
    for template_id, items in by_template.items():
        if template_id in seen:
            continue
        rows.append({"template_id": template_id, "name": "", "cache": pick_cache(items)})
    rows.sort(key=lambda row: (row["name"], row["template_id"]))
    return rows


def _api_body(response):
    body = getattr(response, "body", response)
    code = getattr(body, "code", None)
    if code not in (None, "200", 200):
        raise RuntimeError(getattr(body, "message", None) or f"template cache api code {code}")
    return body


class TemplateCaches:
    def __init__(self, sdk, team_id: str):
        self._sdk = sdk
        self.team_id = team_id

    async def overview(self) -> dict:
        templates = await asyncio.to_thread(self._overview)
        return {"configured": True, "detail": "", "templates": templates}

    async def create(self, template_id: str) -> dict:
        return await asyncio.to_thread(self._create, template_id)

    async def delete(self, template_id: str) -> dict:
        await asyncio.to_thread(self._delete, template_id)
        return {"template_id": template_id, "deleted": True}

    def _overview(self) -> list[dict]:
        return join_views(self._templates(), self._caches())

    def _create(self, template_id: str) -> dict:
        current = pick_cache(self._caches(template_id))
        if current and current.get("status") in _BLOCK_CREATE:
            raise TemplateCacheExists(current)
        from alibabacloud_fcsandbox20260509 import models

        response = self._sdk.create_template_cache(models.CreateTemplateCacheRequest(
            body=models.CreateTemplateCacheInput(team_id=self.team_id, template_id=template_id),
        ))
        return cache_view(_api_body(response).template_cache) or {"status": None}

    def _delete(self, template_id: str) -> None:
        from alibabacloud_fcsandbox20260509 import models

        _api_body(self._sdk.delete_template_cache(
            template_id, models.DeleteTemplateCacheRequest(team_id=self.team_id),
        ))

    def _templates(self) -> list:
        from alibabacloud_fcsandbox20260509 import models

        def call(token):
            return self._sdk.list_templates(models.ListTemplatesRequest(
                team_id=self.team_id, max_results=_PAGE_SIZE, next_token=token,
            ))

        return [item for body in self._pages(call) for item in (body.templates or [])]

    def _caches(self, template_id: Optional[str] = None) -> list:
        from alibabacloud_fcsandbox20260509 import models

        def call(token):
            return self._sdk.list_template_cache(models.ListTemplateCacheRequest(
                team_id=self.team_id, template_id=template_id, max_results=_PAGE_SIZE, next_token=token,
            ))

        return [item for body in self._pages(call) for item in (body.template_caches or [])]

    def _pages(self, call) -> list:
        token = None
        seen: set[str] = set()
        pages = []
        for _ in range(_MAX_PAGES):
            body = _api_body(call(token))
            pages.append(body)
            nxt = (getattr(body, "next_token", None) or "").strip()
            if not nxt or nxt in seen:
                break
            seen.add(nxt)
            token = nxt
        return pages
