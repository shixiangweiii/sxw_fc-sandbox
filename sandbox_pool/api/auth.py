"""调用方鉴权（Bearer Token）。

- `POOL_API_KEYS`：调用方，格式「名称:key,名称:key」；`POOL_ADMIN_KEYS`：管理员，格式相同。
- 两者都为空时关闭鉴权（仅限本地开发），所有请求视为匿名管理员。
- 调用方只能访问自己的借用（借用记录的 client_id 为 key 的名称）；管理员可以访问全部借用和管理接口。
"""

import hmac
from dataclasses import dataclass
from typing import Optional

from fastapi import Depends, Request

from sandbox_pool.config import PoolConfig
from sandbox_pool.models import Forbidden, Unauthorized


@dataclass(frozen=True)
class Caller:
    name: str
    admin: bool

    @property
    def owner(self) -> Optional[str]:
        """访问借用时的归属限制；管理员不受限。"""
        return None if self.admin else self.name


ANONYMOUS = Caller("anonymous", admin=True)


def parse_keys(spec: str) -> list[tuple[str, str]]:
    out = []
    for item in spec.split(","):
        item = item.strip()
        if not item:
            continue
        name, sep, key = item.partition(":")
        name, key = name.strip(), key.strip()
        if not sep or not name or not key:
            raise ValueError("API key entries must look like name:key")
        if len(name) > 64:
            raise ValueError(f"API key name too long: {name[:16]}...")
        out.append((name, key))
    return out


class Authenticator:
    def __init__(self, api_keys: str = "", admin_keys: str = ""):
        self._entries = [(key.encode(), Caller(name, admin=False)) for name, key in parse_keys(api_keys)]
        self._entries += [(key.encode(), Caller(name, admin=True)) for name, key in parse_keys(admin_keys)]

    @classmethod
    def from_config(cls, cfg: PoolConfig) -> "Authenticator":
        return cls(cfg.api_keys, cfg.admin_keys)

    @property
    def enabled(self) -> bool:
        return bool(self._entries)

    def identify(self, token: str) -> Optional[Caller]:
        """逐个做常量时间比较（不提前返回）；同一个 key 同时配置在两类里时按管理员算。"""
        candidate = token.encode()
        found = None
        for key, caller in self._entries:
            if hmac.compare_digest(key, candidate) and (found is None or caller.admin):
                found = caller
        return found


async def require_caller(request: Request) -> Caller:
    auth: Authenticator = request.app.state.auth
    if not auth.enabled:
        return ANONYMOUS
    scheme, _, token = request.headers.get("authorization", "").partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        raise Unauthorized("missing bearer token")
    caller = auth.identify(token.strip())
    if caller is None:
        raise Unauthorized("invalid api key")
    return caller


async def require_admin(caller: Caller = Depends(require_caller)) -> Caller:
    if not caller.admin:
        raise Forbidden("admin key required")
    return caller
