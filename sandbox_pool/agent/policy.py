"""agent 沙箱的出网策略、凭证注入，以及写进沙箱的 opencode 配置、AGENTS.md、egress.json。

云沙箱实测约束（docs/fc-agent-sandbox-notes.md）：
- deny_out 只接受 IP / CIDR（带域名返回 400）；按域名限制只能用白名单模式（deny_out = 0.0.0.0/0 + allow_out）。
- 沙箱的 DNS 服务器是 100.100.2.136（在 100.64.0.0/10 内），所以只能单独屏蔽元数据地址，不能整段屏蔽。
- 凭证注入规则（network.rules）的域名必须同时出现在 allow_out 里；每个沙箱最多 10 个域名，值最长 2048 字节。
- get_info 会明文回显注入值，对外展示前必须脱敏。
"""

import hashlib
import ipaddress
import json
import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any, Optional

from sandbox_pool.models import InvalidRequest

log = logging.getLogger(__name__)

# 开放模式下必须屏蔽的内网与元数据地址，调用方的覆盖不能去掉
MANDATORY_DENY = ("10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "169.254.0.0/16", "100.100.100.200/32")
MODES = ("open", "allowlist")
ALL_TRAFFIC = "0.0.0.0/0"
MAX_RULE_HOSTS = 10
MAX_HEADER_VALUE = 2048
MAX_LIST_ITEMS = 200
# 沙箱内 opencode 看到的 Key：真实值由平台在出网时注入
PLACEHOLDER_KEY = "injected-by-platform"
EGRESS_FILE = "/home/user/.agent/egress.json"

_DOMAIN = re.compile(r"^(\*\.)?([a-z0-9]([a-z0-9-]{0,61}[a-z0-9])?\.)+[a-z][a-z0-9-]{0,62}$")
_HEADER_NAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9-]{0,63}$")
_ENV_REF = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


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


def default_policy(spec: str) -> EgressPolicy:
    """配置里的默认策略（POOL_AGENT_EGRESS，JSON）；为空时是开放模式。"""
    if not spec.strip():
        return EgressPolicy()
    try:
        obj = json.loads(spec)
    except json.JSONDecodeError as e:
        raise ValueError(f"POOL_AGENT_EGRESS is not valid JSON: {e}") from e
    return parse_policy(obj)


@dataclass(frozen=True)
class Injections:
    """管理员配置的凭证注入：{域名: {请求头: 值}}。值是真实凭证，只用于下发到平台。"""

    rules: dict = field(default_factory=dict)

    @property
    def hosts(self) -> list[str]:
        return list(self.rules)

    def describe(self) -> dict[str, list[str]]:
        """对外展示：只有域名和请求头名。"""
        return {host: sorted(headers) for host, headers in self.rules.items()}

    def fingerprint(self) -> dict:
        """参与版本号计算：值只取哈希，凭证轮换时版本号随之变化。"""
        return {
            host: {name: hashlib.sha256(value.encode()).hexdigest()[:16] for name, value in sorted(headers.items())}
            for host, headers in sorted(self.rules.items())
        }


def _expand_env(value: str, env: dict) -> str:
    def repl(m: re.Match) -> str:
        name = m.group(1)
        if not env.get(name):
            raise ValueError(f"POOL_AGENT_INJECT references unset environment variable {name}")
        return env[name]

    return _ENV_REF.sub(repl, value)


def load_injections(model_host: str, model_api_key: str, inject_spec: str, env: Optional[dict] = None) -> Injections:
    """模型 Key 注入 + POOL_AGENT_INJECT。启动时调用，配置有误直接报错（拒绝启动）。"""
    env = os.environ if env is None else env
    rules: dict[str, dict[str, str]] = {}
    if model_api_key:
        rules[model_host.lower()] = {"Authorization": f"Bearer {model_api_key}"}
    if inject_spec.strip():
        try:
            extra = json.loads(inject_spec)
        except json.JSONDecodeError as e:
            raise ValueError(f"POOL_AGENT_INJECT is not valid JSON: {e}") from e
        if not isinstance(extra, dict):
            raise ValueError("POOL_AGENT_INJECT must be an object {host: {header: value}}")
        for host, headers in extra.items():
            if not isinstance(headers, dict) or not headers:
                raise ValueError(f"POOL_AGENT_INJECT[{host}] must be a non-empty object")
            rules[host.lower()] = {**rules.get(host.lower(), {}), **{k: _expand_env(str(v), env) for k, v in headers.items()}}
    if len(rules) > MAX_RULE_HOSTS:
        raise ValueError(f"at most {MAX_RULE_HOSTS} injection hosts are supported by the platform")
    for host, headers in rules.items():
        if host.startswith("*.") or not _is_domain(host):
            raise ValueError(f"injection host {host!r} must be an exact domain")
        for name, value in headers.items():
            if not _HEADER_NAME.match(name):
                raise ValueError(f"invalid header name {name!r} for {host}")
            if not value or len(value.encode()) > MAX_HEADER_VALUE:
                raise ValueError(f"header {name} for {host} must be 1..{MAX_HEADER_VALUE} bytes")
    return Injections(rules)


def build_network(policy: EgressPolicy, injections: Injections) -> dict:
    """SDK 的 network 参数（不含 allow_public_traffic，创建时由调用方加上）。"""
    if policy.mode == "open":
        deny = _dedup([*MANDATORY_DENY, *policy.deny_out])
    else:
        deny = [ALL_TRAFFIC]
    allow = _dedup([*policy.allow_out, *injections.hosts])
    return {
        "allow_out": allow,
        "deny_out": deny,
        "rules": {host: [{"transform": {"headers": dict(headers)}}] for host, headers in injections.rules.items()},
    }


def network_version(policy: EgressPolicy, injections: Injections) -> str:
    canonical = json.dumps(
        {"policy": policy.to_dict(), "injections": injections.fingerprint()}, sort_keys=True, separators=(",", ":")
    )
    return hashlib.sha256(canonical.encode()).hexdigest()[:16]


def describe(policy: EgressPolicy, injections: Injections, version: str) -> dict:
    """对外（API 与沙箱内文件）展示的生效策略，不含凭证。"""
    net = build_network(policy, injections)
    return {
        "version": version,
        "mode": policy.mode,
        "allow_out": net["allow_out"],
        "deny_out": net["deny_out"],
        "injected_hosts": injections.describe(),
        "notes": (
            "open：除 deny_out 外的公网都可访问；allowlist：只能访问 allow_out 中的域名 / IP。"
            "injected_hosts 的请求由平台在出网时自动加上凭证请求头，沙箱内看不到真实值。"
        ),
    }


def redact_platform_network(net: Any) -> Optional[dict]:
    """平台 get_info 回显的网络配置：注入值是明文，只保留请求头名。"""
    if not net:
        return None
    net = dict(net)
    out = {k: v for k, v in net.items() if k != "rules"}
    rules = net.get("rules") or {}
    redacted = {}
    for host, host_rules in rules.items():
        names: list[str] = []
        for rule in host_rules or []:
            headers = ((rule or {}).get("transform") or {}).get("headers") or {}
            names.extend(sorted(headers))
        redacted[host] = {name: "***" for name in names}
    out["rules"] = redacted
    return out


# ---------- agent 设置与沙箱内文件 ----------

SETTINGS_KEYS = ("idle_destroy_after_s", "mcp", "instructions")
MAX_INSTRUCTIONS = 20_000


def validate_mcp(mcp: Any) -> dict:
    """opencode 的 mcp 段：{名称: {type: remote, url, headers?, enabled?} | {type: local, command: [...], environment?}}。"""
    if not isinstance(mcp, dict):
        raise InvalidRequest("mcp must be an object {name: config}")
    for name, conf in mcp.items():
        if not re.match(r"^[A-Za-z0-9_-]{1,64}$", str(name)):
            raise InvalidRequest(f"invalid mcp name {name!r}")
        if not isinstance(conf, dict):
            raise InvalidRequest(f"mcp {name} must be an object")
        kind = conf.get("type")
        if kind == "remote":
            if not isinstance(conf.get("url"), str) or not conf["url"].startswith(("https://", "http://")):
                raise InvalidRequest(f"mcp {name}: remote url must start with https:// or http://")
        elif kind == "local":
            cmd = conf.get("command")
            if not isinstance(cmd, list) or not cmd or not all(isinstance(c, str) for c in cmd):
                raise InvalidRequest(f"mcp {name}: local command must be a non-empty list of strings")
        else:
            raise InvalidRequest(f"mcp {name}: type must be remote or local")
    return mcp


def validate_settings(patch: dict) -> dict:
    if not isinstance(patch, dict):
        raise InvalidRequest("settings must be an object")
    unknown = set(patch) - set(SETTINGS_KEYS)
    if unknown:
        raise InvalidRequest(f"unknown settings: {sorted(unknown)}")
    out = {}
    if "idle_destroy_after_s" in patch:
        v = patch["idle_destroy_after_s"]
        if not isinstance(v, (int, float)) or v < 0:
            raise InvalidRequest("idle_destroy_after_s must be a number >= 0 (0 = never)")
        out["idle_destroy_after_s"] = float(v)
    if "mcp" in patch:
        out["mcp"] = validate_mcp(patch["mcp"] or {})
    if "instructions" in patch:
        v = patch["instructions"] or ""
        if not isinstance(v, str) or len(v) > MAX_INSTRUCTIONS:
            raise InvalidRequest(f"instructions must be a string of at most {MAX_INSTRUCTIONS} characters")
        out["instructions"] = v
    return out


def default_mcp(spec: str) -> dict:
    if not spec.strip():
        return {}
    try:
        return validate_mcp(json.loads(spec))
    except (json.JSONDecodeError, InvalidRequest) as e:
        raise ValueError(f"POOL_AGENT_MCP is invalid: {e}") from e


def render_opencode_config(model: str, mcp: dict) -> dict:
    """写入工作目录的 opencode.json（项目级配置，opencode 首次访问该目录时加载）。"""
    provider_id = model.partition("/")[0]
    return {
        "$schema": "https://opencode.ai/config.json",
        "model": model,
        "small_model": model,
        "autoupdate": False,
        "share": "disabled",
        # 无人值守：询问类权限会让会话挂起，一律给出确定的答案
        "permission": {"doom_loop": "deny", "external_directory": "allow", "question": "deny"},
        "provider": {provider_id: {"options": {"apiKey": PLACEHOLDER_KEY}}},
        "mcp": {name: {**conf, "enabled": conf.get("enabled", True)} for name, conf in mcp.items()},
    }


def render_agents_md(*, workdir: str, max_life_h: float, idle_destroy_after_s: float, mcp_names: list[str],
                     egress: dict, instructions: str) -> str:
    idle = "不会因空闲销毁" if not idle_destroy_after_s else f"空闲 {idle_destroy_after_s / 3600:g} 小时后会被销毁"
    lines = [
        "# 运行环境说明（由沙箱池自动生成）",
        "",
        f"- 你运行在云沙箱里，工作目录是 `{workdir}`。沙箱最长存活约 {max_life_h:g} 小时，到期、{idle}，"
        "或被重置时，沙箱内的文件、安装的依赖和会话都会清除，不要把沙箱当作长期存储。",
        "- 需要交付给用户的结果，请直接写在最终回复里。",
        f"- 出网策略：{egress['mode']} 模式。当前生效的策略（随时可能更新）在 `{EGRESS_FILE}`，"
        "访问外部地址失败时先读这个文件确认是否被策略限制。",
        "- 访问内网地址和云元数据地址会被拒绝。凭证由平台在出网时自动注入，环境变量里的 Key 只是占位符，不要打印或修改。",
    ]
    if mcp_names:
        lines.append(f"- 可用的 MCP 服务：{', '.join(mcp_names)}。联网搜索优先使用 websearch 相关工具。")
    lines.append("- pip / npm 已配置国内镜像。")
    if instructions.strip():
        lines += ["", "# 用户说明", "", instructions.strip()]
    return "\n".join(lines) + "\n"
