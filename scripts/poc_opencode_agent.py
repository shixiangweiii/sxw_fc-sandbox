"""云上验证（PoC）：用现有第二代模板建一个临时沙箱，逐项验证 opencode 常驻 agent 方案的关键假设，最后销毁沙箱。

验证项：
- 流量令牌：端口访问不带令牌返回 403、带令牌返回 200；
- 出网注入：沙箱内 curl / opencode（Bun）访问 DeepSeek、百炼 MCP 时由平台注入凭证，沙箱环境变量里看不到 Key；
- 出网屏蔽：元数据地址、内网地址不可达，公网与 DNS 正常；运行中 update_network 是否生效；
- opencode：项目级 opencode.json 加载、SSE 事件格式、工具调用（bash / webfetch / 百炼 websearch MCP）、实例重载、内存；
- SSE 长连接空闲保持。

    set -a; . ./.env; set +a
    python scripts/poc_opencode_agent.py [--template <第二代模板 ID>] [--keep] [--skip-idle-sse]

百炼 MCP 的 Key 取自 BAILIAN_MCP_API_KEY，未设置时从 sxw_aicoding/百炼-mcp.txt 解析。结果写入 .data/poc-report.json，不含任何密钥。
"""

import argparse
import asyncio
import json
import os
import re
import subprocess
import sys
import time
from pathlib import Path

import httpcore
import httpx
from e2b import AsyncSandbox
from e2b.sandbox.commands.command_handle import CommandExitException

ROOT = Path(__file__).resolve().parents[1]
OPENCODE_VERSION = "1.18.32"
OPENCODE_TGZ = f"https://registry.npmmirror.com/opencode-linux-x64/-/opencode-linux-x64-{OPENCODE_VERSION}.tgz"
PORT = 4096
WORKDIR = "/home/user/workspace"
MCP_URL = "https://dashscope.aliyuncs.com/api/v1/mcps/WebSearch/mcp"
DENY_INTERNAL = ["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "169.254.0.0/16", "100.100.100.200/32"]
PLACEHOLDER = "injected-by-platform"

report: dict = {"started_at": time.strftime("%Y-%m-%d %H:%M:%S"), "checks": {}}


def record(name: str, ok, **detail) -> None:
    report["checks"][name] = {"ok": ok, **detail}
    mark = {True: "PASS", False: "FAIL", None: "INFO"}[ok]
    print(f"[{mark}] {name}: {json.dumps(detail, ensure_ascii=False)[:600]}")


def bailian_key() -> str:
    key = os.environ.get("BAILIAN_MCP_API_KEY", "").strip()
    if key:
        return key
    text = (ROOT / "sxw_aicoding" / "百炼-mcp.txt").read_text(encoding="utf-8")
    m = re.search(r"Authorization:\s*Bearer\s+(\S+)", text)
    if not m:
        raise SystemExit("百炼 MCP Key 未找到：设置 BAILIAN_MCP_API_KEY")
    return m.group(1)


def network_config(deepseek_key: str, mcp_key: str, extra_deny: list[str] | None = None) -> dict:
    return {
        "allow_out": ["api.deepseek.com", "dashscope.aliyuncs.com"],
        "deny_out": DENY_INTERNAL + (extra_deny or []),
        "rules": {
            "api.deepseek.com": [{"transform": {"headers": {"Authorization": f"Bearer {deepseek_key}"}}}],
            "dashscope.aliyuncs.com": [{"transform": {"headers": {"Authorization": f"Bearer {mcp_key}"}}}],
        },
    }


def redact_network(net) -> dict:
    """get_info 回显的网络配置里注入值是明文，只保留结构。"""
    if not net:
        return {}
    out = {k: v for k, v in dict(net).items() if k != "rules"}
    rules = dict(net).get("rules") or {}
    out["rules"] = {host: f"<{len(v)} rule(s) redacted>" for host, v in rules.items()}
    return out


async def sh(sbx: AsyncSandbox, cmd: str, *, user: str = "user", timeout: float = 60) -> tuple[int, str]:
    # 刚创建的沙箱前几次 envd 调用可能报 ConnectError(EndOfStream())（实测），连接类错误重试
    for attempt in range(10):
        try:
            r = await sbx.commands.run(cmd, user=user, timeout=timeout)
            return r.exit_code, (r.stdout + r.stderr).strip()
        except CommandExitException as e:
            return e.exit_code, ((e.stdout or "") + (e.stderr or "")).strip()
        except (httpx.NetworkError, httpx.RemoteProtocolError, httpcore.NetworkError, httpcore.RemoteProtocolError) as e:
            # envd 调用经 e2b_connect 直接抛 httpcore 的异常（实测：刚创建时 TLS 握手被对端关闭）
            if attempt == 9:
                return -1, repr(e)
            report.setdefault("envd_connect_retries", 0)
            report["envd_connect_retries"] += 1
            await asyncio.sleep(1)
    return -1, "unreachable"


def ingress_ip(host: str) -> str | None:
    """平台入口 IP：优先用 POOL_AGENT_INGRESS_IP；否则用公共 DNS（223.5.5.5）解析，绕开本机代理的 fake-ip。"""
    ip = os.environ.get("POOL_AGENT_INGRESS_IP", "").strip()
    if ip:
        return ip
    try:
        out = subprocess.run(["dig", "+short", "@223.5.5.5", host], capture_output=True, text=True, timeout=10).stdout
    except (OSError, subprocess.TimeoutExpired):
        return None
    ips = [x for x in out.split() if re.fullmatch(r"\d+\.\d+\.\d+\.\d+", x)]
    return ips[-1] if ips else None


async def with_connect_retry(call, attempts: int = 6):
    """平台入口偶发建连失败（TLS 握手被对端关闭，实测）；ConnectError 说明请求没发出去，任何方法都可以重试。"""
    for i in range(attempts):
        try:
            return await call()
        except (httpx.ConnectError, httpx.ConnectTimeout) as e:
            if i == attempts - 1:
                raise
            report["ingress_connect_retries"] = report.get("ingress_connect_retries", 0) + 1
            await asyncio.sleep(0.5 * (i + 1))


class Opencode:
    """直接经公网 URL + 流量令牌访问沙箱内 opencode server。"""

    def __init__(self, host: str, token: str, ingress_ip: str | None):
        self.host = host
        # 本机代理的 fake-ip / TUN 模式会让到沙箱域名的连接约 1/3 失败（实测），直连平台入口 IP、
        # SNI 与 Host 仍用沙箱域名；不读系统代理（会对沙箱域名返回 503），只用显式配置的 HTTPS_PROXY
        self.ext = {"sni_hostname": host} if ingress_ip else {}
        self.http = httpx.AsyncClient(
            base_url=f"https://{ingress_ip or host}",
            headers={"e2b-traffic-access-token": token, "Host": host},
            timeout=httpx.Timeout(30, read=60),
            trust_env=False,
            proxy=os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy") or None,
        )
        self.params = {"directory": WORKDIR}

    async def get(self, path: str, **params):
        r = await with_connect_retry(lambda: self.http.get(path, params={**self.params, **params}, extensions=self.ext))
        r.raise_for_status()
        return r.json()

    async def post(self, path: str, body=None):
        r = await with_connect_retry(lambda: self.http.post(path, params=self.params, json=body, extensions=self.ext))
        r.raise_for_status()
        return r.json() if r.content else None

    async def events(self, queue: asyncio.Queue, stop: asyncio.Event) -> None:
        for attempt in range(6):
            try:
                async with self.http.stream("GET", "/event", params=self.params, timeout=httpx.Timeout(30, read=90),
                                            extensions=self.ext) as r:
                    r.raise_for_status()
                    data: list[str] = []
                    async for line in r.aiter_lines():
                        if stop.is_set():
                            return
                        if line.startswith("data:"):
                            data.append(line[5:].lstrip())
                        elif line == "" and data:
                            try:
                                await queue.put(json.loads("\n".join(data)))
                            except json.JSONDecodeError:
                                pass
                            data = []
                return
            except (httpx.ConnectError, httpx.ConnectTimeout):
                if attempt == 5:
                    raise
                report["ingress_connect_retries"] = report.get("ingress_connect_retries", 0) + 1
                await asyncio.sleep(0.5 * (attempt + 1))

    async def close(self):
        await self.http.aclose()


def summarize_event(ev: dict) -> dict:
    """事件样本（截断长字符串，便于写报告）。"""

    def cut(v):
        if isinstance(v, str):
            return v[:160]
        if isinstance(v, dict):
            return {k: cut(x) for k, x in list(v.items())[:25]}
        if isinstance(v, list):
            return [cut(x) for x in v[:5]]
        return v

    return cut(ev)


async def run_prompt(oc: Opencode, text: str, *, timeout: float = 240) -> dict:
    """新建会话、提问，收集事件直到会话空闲；返回事件统计、最终文本、用量、工具调用。"""
    queue: asyncio.Queue = asyncio.Queue()
    stop = asyncio.Event()
    reader = asyncio.create_task(oc.events(queue, stop))
    try:
        first = await asyncio.wait_for(queue.get(), 20)
        session = await oc.post("/session", {"title": "poc"})
        sid = session["id"]
        t0 = time.monotonic()
        await oc.post(f"/session/{sid}/prompt_async", {"parts": [{"type": "text", "text": text}]})
        types: dict[str, int] = {}
        samples: dict[str, dict] = {"server.connected": summarize_event(first)}
        first_delta = None
        busy = False
        tools: dict[str, dict] = {}
        errors = []
        while time.monotonic() - t0 < timeout:
            try:
                ev = await asyncio.wait_for(queue.get(), 5)
            except asyncio.TimeoutError:
                st = await oc.get("/session/status")
                if busy and sid not in st:
                    break
                continue
            t = ev.get("type", "?")
            props = ev.get("properties") or {}
            ev_sid = props.get("sessionID") or (props.get("part") or {}).get("sessionID") or (props.get("info") or {}).get("sessionID")
            types[t] = types.get(t, 0) + 1
            samples.setdefault(t, summarize_event(ev))
            if ev_sid != sid:
                continue
            if t == "message.part.delta" and first_delta is None:
                first_delta = round(time.monotonic() - t0, 2)
            if t == "message.part.updated" and (props.get("part") or {}).get("type") == "tool":
                part = props["part"]
                tools[part["id"]] = {"tool": part.get("tool"), "status": (part.get("state") or {}).get("status"),
                                     "title": (part.get("state") or {}).get("title")}
            if t == "session.error":
                errors.append(summarize_event(props))
            if t == "session.status":
                st_type = (props.get("status") or {}).get("type")
                if st_type == "busy":
                    busy = True
                elif st_type == "idle" and busy:
                    break
        elapsed = round(time.monotonic() - t0, 2)
        msgs = await oc.get(f"/session/{sid}/message")
        final, usage = "", {"input": 0, "output": 0, "reasoning": 0, "cache_read": 0, "cost": 0.0}
        for m in msgs:
            info = m.get("info") or {}
            if info.get("role") != "assistant":
                continue
            tok = info.get("tokens") or {}
            usage["input"] += tok.get("input", 0)
            usage["output"] += tok.get("output", 0)
            usage["reasoning"] += tok.get("reasoning", 0)
            usage["cache_read"] += (tok.get("cache") or {}).get("read", 0)
            usage["cost"] += info.get("cost") or 0
            text_parts = [p.get("text", "") for p in m.get("parts", []) if p.get("type") == "text"]
            if text_parts:
                final = "".join(text_parts)
        return {
            "session_id": sid,
            "elapsed_s": elapsed,
            "first_delta_s": first_delta,
            "event_types": types,
            "samples": samples,
            "tools": list(tools.values()),
            "errors": errors,
            "final": final[:800],
            "usage": usage,
            "assistant_messages": sum(1 for m in msgs if (m.get("info") or {}).get("role") == "assistant"),
        }
    finally:
        stop.set()
        reader.cancel()
        await asyncio.gather(reader, return_exceptions=True)


def opencode_config(mcp_enabled: bool = True) -> dict:
    return {
        "$schema": "https://opencode.ai/config.json",
        "model": "deepseek/deepseek-flash",
        "small_model": "deepseek/deepseek-flash",
        "autoupdate": False,
        "share": "disabled",
        "permission": {"doom_loop": "deny", "external_directory": "allow", "question": "deny"},
        "provider": {"deepseek": {"options": {"apiKey": PLACEHOLDER}}},
        "mcp": {"websearch": {"type": "remote", "url": MCP_URL, "enabled": mcp_enabled}},
    }


RUN_SH = f"""#!/bin/bash
export HOME=/home/user
export OPENCODE_DISABLE_AUTOUPDATE=1 OPENCODE_DISABLE_MODELS_FETCH=1 OPENCODE_DISABLE_LSP_DOWNLOAD=1
export DEEPSEEK_API_KEY={PLACEHOLDER}
cd /home/user
while true; do
  /home/user/.opencode/bin/opencode serve --hostname 0.0.0.0 --port {PORT} >> /home/user/.agent/opencode.log 2>&1
  echo "opencode exited: $?" >> /home/user/.agent/opencode.log
  sleep 1
done
"""


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--template", default=os.environ.get("POOL_TEMPLATE", "xu76gk97q07mgohgw7q3"))
    ap.add_argument("--keep", action="store_true", help="结束后不销毁沙箱（调试用）")
    ap.add_argument("--skip-idle-sse", action="store_true")
    args = ap.parse_args()

    deepseek_key = os.environ["FCSANDBOX_OPENCODE_MODEL_API_KEY"]
    mcp_key = bailian_key()
    opts = {}
    proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")
    if proxy:
        opts["proxy"] = proxy

    t0 = time.monotonic()
    sbx = await AsyncSandbox.create(
        template=args.template,
        timeout=1800,
        metadata={"pool": "poc-opencode"},
        secure=True,
        network={**network_config(deepseek_key, mcp_key), "allow_public_traffic": False},
        request_timeout=60,
        **opts,
    )
    record("create", True, seconds=round(time.monotonic() - t0, 2), sandbox_id=sbx.sandbox_id,
           has_traffic_token=bool(sbx.traffic_access_token), sandbox_domain=sbx.sandbox_domain)
    oc = None
    try:
        # ---------- 环境 ----------
        _, out = await sh(sbx, "id; uname -m; nproc; free -m | head -2; df -h / | tail -1; "
                               "head -2 /etc/os-release; ps -p 1 -o comm=; cat /etc/resolv.conf | grep -v '^#'; "
                               "for b in git python3 pip3 node npm curl tar unzip systemctl sudo; do "
                               "printf '%s=' $b; command -v $b || echo none; done; "
                               "grep -c avx2 /proc/cpuinfo; ls /usr/local/share/ca-certificates/ 2>&1")
        record("env", None, output=out)
        _, out = await sh(sbx, "env | grep -i -c -E 'deepseek|dashscope|sk-' || true")
        record("no_secret_in_env", out.strip() == "0", matches=out.strip())

        # ---------- 出网 ----------
        cases = {
            "egress_deepseek_injected": ("curl -s -o /dev/null -w '%{http_code}' -m 10 https://api.deepseek.com/models", "200"),
            "egress_public_baidu": ("curl -s -o /dev/null -w '%{http_code}' -m 10 https://www.baidu.com", "200"),
            "egress_metadata_blocked": ("curl -s -o /dev/null -w '%{http_code}' -m 5 http://100.100.100.200/latest/meta-data/ || echo blocked", None),
            "egress_private_blocked": ("curl -s -o /dev/null -w '%{http_code}' -m 5 http://10.0.0.1/ || echo blocked", None),
            "dns_resolve": ("getent hosts api.deepseek.com | head -1", None),
        }
        for name, (cmd, expect) in cases.items():
            _, out = await sh(sbx, cmd)
            ok = (out.strip() == expect) if expect else (("blocked" in out or out.strip() in ("000", "")) if "blocked" in name else bool(out.strip()))
            record(name, ok, output=out[-200:])
        mcp_init = (
            "curl -s -m 15 -o /dev/null -w '%{http_code}' -X POST " + MCP_URL + " -H 'Content-Type: application/json' "
            "-H 'Accept: application/json, text/event-stream' "
            "-d '{\"jsonrpc\":\"2.0\",\"id\":1,\"method\":\"initialize\",\"params\":{\"protocolVersion\":\"2025-03-26\","
            "\"capabilities\":{},\"clientInfo\":{\"name\":\"poc\",\"version\":\"0\"}}}'"
        )
        _, out = await sh(sbx, mcp_init)
        record("egress_bailian_mcp_injected", out.strip() == "200", http_code=out.strip())

        # ---------- 安装并启动 opencode ----------
        t1 = time.monotonic()
        code, out = await sh(sbx, f"set -e; mkdir -p ~/.opencode/bin {WORKDIR} ~/.agent; cd /tmp; "
                                  f"curl -fsSL --retry 3 -m 240 {OPENCODE_TGZ} -o oc.tgz; tar xzf oc.tgz; "
                                  "mv package/bin/opencode ~/.opencode/bin/opencode; chmod 755 ~/.opencode/bin/opencode; "
                                  "rm -rf package oc.tgz; ~/.opencode/bin/opencode --version", timeout=300)
        record("install_opencode", code == 0, seconds=round(time.monotonic() - t1, 1), output=out[-200:])
        if code != 0:
            return 1
        await sbx.files.write(f"{WORKDIR}/opencode.json", json.dumps(opencode_config(), indent=2))
        await sbx.files.write(f"{WORKDIR}/AGENTS.md", "# PoC agent\n\n用中文回答。联网搜索使用 websearch MCP 工具。\n")
        await sbx.files.write("/home/user/.agent/egress.json", json.dumps({"mode": "open", "deny_out": DENY_INTERNAL}))
        await sbx.files.write("/home/user/.agent/run.sh", RUN_SH)
        await sbx.commands.run("bash /home/user/.agent/run.sh", background=True, user="user")
        t2 = time.monotonic()
        healthy = False
        for _ in range(120):
            code, out = await sh(sbx, f"curl -sf -m 2 http://127.0.0.1:{PORT}/global/health")
            if code == 0:
                healthy = True
                break
            await asyncio.sleep(0.5)
        record("opencode_start", healthy, seconds=round(time.monotonic() - t2, 1), health=out[:200])
        if not healthy:
            _, log = await sh(sbx, "tail -30 /home/user/.agent/opencode.log")
            record("opencode_log", None, tail=log)
            return 1

        # ---------- 流量令牌 ----------
        host = sbx.get_host(PORT)
        ip = ingress_ip(host)
        record("ingress", None, ingress_ip=ip)
        base = f"https://{ip or host}"
        ext = {"sni_hostname": host} if ip else {}
        # 直连入口 IP 时不走代理：经代理隧道时 TLS 用 IP 做 server_hostname、忽略 sni_hostname，证书校验会失败
        proxy = None if ip else (os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy") or None)
        async with httpx.AsyncClient(timeout=20, trust_env=False, headers={"Host": host}, proxy=proxy) as c:
            r1 = await with_connect_retry(lambda: c.get(f"{base}/global/health", extensions=ext))
            r2 = await with_connect_retry(
                lambda: c.get(f"{base}/global/health", extensions=ext,
                              headers={"e2b-traffic-access-token": sbx.traffic_access_token})
            )
        record("traffic_token", r1.status_code == 403 and r2.status_code == 200,
               without_token=r1.status_code, with_token=r2.status_code, body=r2.text[:120])
        oc = Opencode(host, sbx.traffic_access_token, ip)

        cfg = await oc.get("/config")
        record("project_config_loaded", cfg.get("model") == "deepseek/deepseek-flash", model=cfg.get("model"),
               share=cfg.get("share"), permission=cfg.get("permission"))
        mcp = await oc.get("/mcp")
        record("mcp_status", None, status=mcp)

        # ---------- 对话 / 工具 ----------
        r = await run_prompt(oc, "请只回答一个数字：1+1 等于几？")
        record("chat_basic", "2" in r["final"] and not r["errors"], **{k: r[k] for k in ("elapsed_s", "first_delta_s", "final", "usage", "event_types", "errors")})
        report["event_samples"] = r["samples"]
        r = await run_prompt(oc, "用 bash 工具执行 `uname -m && python3 --version`，然后告诉我输出结果。")
        record("tool_bash", any(t["tool"] == "bash" and t["status"] == "completed" for t in r["tools"]),
               tools=r["tools"], final=r["final"][:300], elapsed_s=r["elapsed_s"])
        r = await run_prompt(oc, "用 webfetch 工具抓取 https://www.example.com ，告诉我页面标题。")
        record("tool_webfetch", any(t["tool"] == "webfetch" and t["status"] == "completed" for t in r["tools"]),
               tools=r["tools"], final=r["final"][:300], elapsed_s=r["elapsed_s"])
        r = await run_prompt(oc, "使用 websearch MCP 的搜索工具联网搜索“杭州 今天 天气”，用一句话回答。")
        record("tool_websearch_mcp", any("websearch" in (t["tool"] or "") and t["status"] == "completed" for t in r["tools"]),
               tools=r["tools"], final=r["final"][:300], elapsed_s=r["elapsed_s"], errors=r["errors"])

        # ---------- 内存 ----------
        _, out = await sh(sbx, "ps -eo rss,comm | grep -i opencode | awk '{s+=$1} END {print s/1024 \" MB\"}'; free -m | head -2")
        record("opencode_memory", None, output=out)

        # ---------- 实例重载 ----------
        await sbx.files.write(f"{WORKDIR}/opencode.json", json.dumps(opencode_config(mcp_enabled=False), indent=2))
        disposed = await oc.post("/instance/dispose")
        mcp_after = await oc.get("/mcp")
        record("instance_dispose_reload", None, disposed=disposed, mcp_after=mcp_after)
        await sbx.files.write(f"{WORKDIR}/opencode.json", json.dumps(opencode_config(), indent=2))
        await oc.post("/instance/dispose")

        # ---------- update_network ----------
        info = await AsyncSandbox.get_info(sbx.sandbox_id, **opts)
        record("network_echo", None, network=redact_network(getattr(info, "network", None)))
        # deny_out 只接受 IP / CIDR（实测：带域名返回 400），按域名限制只能用白名单模式：拒绝全部 + allow_out
        try:
            await AsyncSandbox.update_network(sbx.sandbox_id, network_config(deepseek_key, mcp_key, ["www.baidu.com"]), **opts)
            record("deny_out_domain", None, accepted=True)
        except Exception as e:  # noqa: BLE001
            record("deny_out_domain", None, accepted=False, error=str(e)[:200])
        allowlist = network_config(deepseek_key, mcp_key)
        allowlist["deny_out"] = ["0.0.0.0/0"]
        t4 = time.monotonic()
        await AsyncSandbox.update_network(sbx.sandbox_id, allowlist, **opts)
        update_s = round(time.monotonic() - t4, 2)
        await asyncio.sleep(2)
        _, out = await sh(sbx, "curl -s -o /dev/null -w '%{http_code}' -m 8 https://www.baidu.com || echo blocked")
        _, out2 = await sh(sbx, "curl -s -o /dev/null -w '%{http_code}' -m 10 https://api.deepseek.com/models")
        blocked = out.strip() != "200"
        await AsyncSandbox.update_network(sbx.sandbox_id, network_config(deepseek_key, mcp_key), **opts)
        await asyncio.sleep(2)
        _, out3 = await sh(sbx, "curl -s -o /dev/null -w '%{http_code}' -m 8 https://www.baidu.com || echo blocked")
        record("update_network_effective", blocked and out2.strip() == "200" and out3.strip() == "200",
               update_call_s=update_s, baidu_in_allowlist_mode=out.strip(), deepseek_in_allowlist_mode=out2.strip(),
               baidu_after_restore=out3.strip())

        # ---------- SSE 空闲保持 ----------
        if not args.skip_idle_sse:
            queue: asyncio.Queue = asyncio.Queue()
            stop = asyncio.Event()
            reader = asyncio.create_task(oc.events(queue, stop))
            t3 = time.monotonic()
            beats = 0
            while time.monotonic() - t3 < 150:
                try:
                    ev = await asyncio.wait_for(queue.get(), 30)
                    beats += ev.get("type") == "server.heartbeat"
                except asyncio.TimeoutError:
                    break
            alive = not reader.done()
            stop.set()
            reader.cancel()
            res = await asyncio.gather(reader, return_exceptions=True)
            record("sse_idle_150s", alive and beats >= 10, heartbeats=beats, held_s=round(time.monotonic() - t3, 1),
                   reader_error=repr(res[0])[:200] if not alive else None)
        return 0
    finally:
        if oc is not None:
            await oc.close()
        report["finished_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        out_path = ROOT / ".data" / "poc-report.json"
        out_path.parent.mkdir(exist_ok=True)
        out_path.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        if not args.keep:
            killed = await AsyncSandbox.kill(sbx.sandbox_id, **opts)
            print(f"sandbox {sbx.sandbox_id} killed: {killed}")
        print(f"report: {out_path}")


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
