"""出网策略：校验、合并、生成平台的 network 规则。**唯一一份**。

── 它为什么在这里，而不在 agent/policy.py 里（2026-10-16 挪过来的）────────

原来整块住在 `agent/policy.py`。为 Atlas 集成给**代码执行池**加借用级出网策略时，
`core/allocator.py` 与 `core/maintainer.py` 也要用它 —— 而 `core → agent` 是
反向依赖：agent 子系统是可选的（POOL_AGENT_ENABLED），core 是它的下层。

抄第二份更糟：强制屏蔽网段、`allow_out` 与屏蔽段的重叠判定、域名只在白名单模式
下有意义这几条，是实测逼出来的（见 docs/fc-agent-sandbox-notes.md）。两份迟早分叉，
而分叉的表现是「两条路径对同一个策略给出不同答案」，两边都不会报错。

`agent/policy.py` 从这里 import，名字仍然绑定在它的命名空间里，所以
`from sandbox_pool.agent.policy import parse_policy` 这类既有写法照常可用。
"""

import ipaddress
import logging
import re
from dataclasses import dataclass
from typing import Optional

from sandbox_pool.models import InvalidRequest

log = logging.getLogger(__name__)

# 开放模式下必须屏蔽的内网与元数据地址，调用方的覆盖不能去掉
MANDATORY_DENY = ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "169.254.0.0/16", "100.100.100.200/32")
MODES = ("open", "allowlist")
ALL_TRAFFIC = "0.0.0.0/0"
MAX_LIST_ITEMS = 200

_DOMAIN = re.compile(r"^(\*\.)?([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z][a-z0-9-]{0,62}$")


def _is_ip_or_cidr(value: str) -> bool:
    try:
        ipaddress.ip_network(value, strict=False)
        return True
    except ValueError:
        return False


_MANDATORY_NETS = tuple(ipaddress.ip_network(n) for n in MANDATORY_DENY)
_IPV4_MAPPED = ipaddress.ip_network("::ffff:0:0/96")


def _overlaps_mandatory(value: str) -> bool:
    """放行项与强制屏蔽网段重叠（平台规则 allow_out 优先于 deny_out，重叠即等于去掉了屏蔽）。"""
    net = ipaddress.ip_network(value, strict=False)
    if net.version == 6:
        return net.overlaps(_IPV4_MAPPED)
    return any(net.overlaps(m) for m in _MANDATORY_NETS)


def _is_domain(value: str) -> bool:
    return len(value) <= 253 and bool(_DOMAIN.match(value))


def _dedup(items) -> list[str]:
    out: list[str] = []
    for x in items:
        if x not in out:
            out.append(x)
    return out


@dataclass(frozen=True)
class EgressPolicy:
    mode: str = "open"
    allow_out: tuple[str, ...] = ()
    deny_out: tuple[str, ...] = ()

    def to_dict(self) -> dict:
        return {"mode": self.mode, "allow_out": list(self.allow_out), "deny_out": list(self.deny_out)}


def parse_policy(obj: Optional[dict], base: Optional[EgressPolicy] = None, *, strict: bool = True) -> EgressPolicy:
    """校验并合并策略：obj 中出现的字段覆盖 base。deny_out 只能是 IP / CIDR。

    allow_out 在平台上优先于 deny_out，所以：
    - IP / CIDR 不能与强制屏蔽的内网、元数据网段重叠（否则等于去掉屏蔽），IPv4 映射的 IPv6 地址一律拒绝；
    - 域名（支持 *. 通配）只在白名单模式下有意义；开放模式本来就放行全部公网，域名放行项只会让它绕过 deny_out，拒绝。

    strict=False 用于读取库里已存的覆盖（可能早于上述规则写入）：违规的放行项直接丢弃（只会更严格）、记日志，
    不抛错，避免一条旧记录让该 agent 的装配和维护循环一直失败。格式错误仍然抛错。
    """
    base = base or EgressPolicy()
    if obj is None:
        return base
    if not isinstance(obj, dict):
        raise InvalidRequest("egress policy must be an object")
    unknown = set(obj) - {"mode", "allow_out", "deny_out"}
    if unknown:
        raise InvalidRequest(f"unknown egress fields: {sorted(unknown)}")
    mode = obj.get("mode", base.mode)
    if mode not in MODES:
        raise InvalidRequest(f"egress mode must be one of {MODES}")

    def items(key: str) -> Optional[list[str]]:
        if key not in obj:
            return None
        value = obj[key]
        if not isinstance(value, list) or not all(isinstance(v, str) for v in value):
            raise InvalidRequest(f"{key} must be a list of strings")
        if len(value) > MAX_LIST_ITEMS:
            raise InvalidRequest(f"{key} has more than {MAX_LIST_ITEMS} items")
        return [v.strip().lower() for v in value if v.strip()]

    allow = items("allow_out")
    deny = items("deny_out")
    for v in allow or []:
        if not (_is_domain(v) or _is_ip_or_cidr(v)):
            raise InvalidRequest(f"invalid allow_out entry {v!r}: expected a domain, IP or CIDR")
    for v in deny or []:
        if not _is_ip_or_cidr(v):
            raise InvalidRequest(f"invalid deny_out entry {v!r}: only IP / CIDR are supported (the platform rejects domains)")
    allow_out = tuple(_dedup(allow)) if allow is not None else base.allow_out
    # 合并之后再查放行项：模式可能来自 obj，allow_out 来自 base
    rejected = {}
    for v in allow_out:
        if _is_ip_or_cidr(v):
            if _overlaps_mandatory(v):
                rejected[v] = f"overlaps the always-blocked private / metadata ranges {list(MANDATORY_DENY)}"
        elif mode == "open":
            rejected[v] = (
                "domains are only allowed in allowlist mode "
                "(open mode already allows all public traffic; a domain entry would only bypass deny_out)"
            )
    if rejected:
        if strict:
            v, why = next(iter(rejected.items()))
            raise InvalidRequest(f"allow_out entry {v!r} {why}")
        log.warning("dropping disallowed allow_out entries of a stored egress policy: %s", sorted(rejected))
        allow_out = tuple(v for v in allow_out if v not in rejected)
    return EgressPolicy(
        mode=mode,
        allow_out=allow_out,
        deny_out=tuple(_dedup(deny)) if deny is not None else base.deny_out,
    )


def policy_network(policy: EgressPolicy, *, extra_allow: tuple[str, ...] = (),
                   rules: Optional[dict] = None) -> dict:
    """按策略生成平台的 network 规则（不含 allow_public_traffic，创建时由调用方加）。

    【定】白名单模式下 deny_out 一律是 0.0.0.0/0，调用方自己的 deny_out 不参与 ——
    平台上 allow_out 优先于 deny_out，用它去否定一个已放行的域名不会生效。

    实测（scripts/probe_code_pool_network.py）：
      · allow_out 为空 ＋ deny_out=0.0.0.0/0 → **彻底断网，连 DNS 一起断**；
      · allow_out 有域名时，放行域名通、未放行域名 000、DNS 保留；
      · 但按域名的过滤发生在 80/443 的 SNI 层，**裸 TCP 连接照样连得上** ——
        拿 TCP 可达性去判「拦没拦」会得到假阴性。
    """
    if policy.mode == "open":
        deny = _dedup([*MANDATORY_DENY, *policy.deny_out])
    else:
        deny = [ALL_TRAFFIC]
    return {
        "allow_out": _dedup([*policy.allow_out, *extra_allow]),
        "deny_out": deny,
        "rules": rules or {},
    }


#: 彻底断网：池子里待借的沙箱用它创建
DENY_ALL = EgressPolicy(mode="allowlist", allow_out=(), deny_out=())
