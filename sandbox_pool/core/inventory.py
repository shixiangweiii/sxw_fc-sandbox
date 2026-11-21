"""管理员实例清单：云端 running / paused 与两个池的库记录按 provider_id 对齐。

只读查询走 list_account，不调用 connect()。输出不含 access_token 和 lease_id。
"""

from typing import Optional

from sandbox_pool.models import LeaseState

# 与 SandboxPool.list_sandboxes 附带的借用字段一致，不含 lease_id
_LEASE_FIELDS = ("client_id", "source", "created_at", "expires_at", "hard_deadline")


def _view(
    *,
    provider_id: Optional[str],
    cloud_state: Optional[str],
    db_state: Optional[str],
    pool: Optional[str],
    kind: str,
    row_id: Optional[str],
    template: Optional[str],
    engine: Optional[str],
    started_at: Optional[float],
    client_id: Optional[str],
    user_id: Optional[str],
    lease: Optional[dict],
) -> dict:
    return {
        "provider_id": provider_id,
        "cloud_state": cloud_state,
        "db_state": db_state,
        "pool": pool,
        "kind": kind,
        "row_id": row_id,
        "template": template,
        "engine": engine,
        "started_at": started_at,
        "client_id": client_id,
        "user_id": user_id,
        "lease": lease,
    }


def _from_row(row: dict, *, kind: str, lease: Optional[dict], agent: Optional[dict]) -> dict:
    lease_view = None if lease is None else {k: lease[k] for k in _LEASE_FIELDS}
    client_id = None
    user_id = None
    if agent is not None:
        client_id = agent.get("client_id")
        user_id = agent.get("user_id")
    elif lease_view is not None:
        client_id = lease_view["client_id"]
    return _view(
        provider_id=row.get("provider_id"),
        cloud_state=None,
        db_state=row["state"],
        pool=row["pool"],
        kind=kind,
        row_id=row["id"],
        template=row.get("template"),
        engine=row.get("engine"),
        started_at=row.get("created_at"),
        client_id=client_id,
        user_id=user_id,
        lease=lease_view,
    )


async def list_instances(pool, agents=None) -> list[dict]:
    """云端有记录的在前；库里有、云端没有的标成 missing；反过来标成 orphan。"""
    cloud = await pool.provider.list_account()
    leases = {item["sandbox_row_id"]: item for item in await pool.store.list_leases([LeaseState.ACTIVE])}
    indexed: dict[str, dict] = {}
    no_provider: list[dict] = []

    def take(row: dict, kind: str, lease: Optional[dict], agent: Optional[dict]) -> None:
        view = _from_row(row, kind=kind, lease=lease, agent=agent)
        if view["provider_id"]:
            indexed[view["provider_id"]] = view
        else:
            no_provider.append(view)

    for row in await pool.store.list_sandboxes():
        take(row, "code", leases.get(row["id"]), None)
    if agents is not None:
        agents_by_id = {item["id"]: item for item in await agents.store.list_agents()}
        for row in await agents.store.list_sandboxes():
            take(row, "agent", None, agents_by_id.get(row.get("agent_id")))

    out: list[dict] = []
    seen: set[str] = set()
    for inst in cloud:
        seen.add(inst.sandbox_id)
        current = indexed.get(inst.sandbox_id)
        if current is None:
            out.append(
                _view(
                    provider_id=inst.sandbox_id,
                    cloud_state=inst.state,
                    db_state=None,
                    pool=(inst.metadata or {}).get("pool"),
                    kind="orphan",
                    row_id=None,
                    template=None,
                    engine=(inst.metadata or {}).get("engine"),
                    started_at=inst.started_at,
                    client_id=None,
                    user_id=None,
                    lease=None,
                )
            )
            continue
        merged = dict(current)
        merged["cloud_state"] = inst.state
        if inst.started_at is not None:
            merged["started_at"] = inst.started_at
        out.append(merged)
    for provider_id, view in indexed.items():
        if provider_id not in seen:
            missing = dict(view)
            missing["kind"] = "missing"
            missing["cloud_state"] = None
            out.append(missing)
    for view in no_provider:
        missing = dict(view)
        missing["kind"] = "missing"
        missing["cloud_state"] = None
        out.append(missing)
    return out
