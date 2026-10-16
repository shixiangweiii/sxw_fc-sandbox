"""代码执行池沙箱的出网边界探针（会创建真实云沙箱，产生费用；结束即销毁）。

要回答的两个问题，都是 Atlas 集成前必须先知道的事实：

  Q1  按**代码执行池的方式**建沙箱（E2BProvider.create：只传 template / timeout / metadata，
      **不传 network、不传 secure**）时，平台默认拦不拦内网与元数据地址？
  Q2  对这样一个**非 secure** 的沙箱，运行中调用 update_network 还生效吗？
      —— 决定出网策略是在借出时下发，还是必须在建池时就定死。

── v1 的仪器缺陷，记在这里，因为它比结论本身更值得记 ─────────────────────────

v1 拿**元数据地址**当 Q2 的判据：「从可达变为不可达」。实测它默认就是 timeout，
于是加规则前后一模一样 —— 而 v1 的判据写的是 `!= "connected"`，这个条件在
「之前也连不上」时同样成立，summary 因此打出了「接受且生效」。
**判据落在两个假设的重叠区：它测不出它声称在测的东西。**

v2 的改法是换一个**默认确实可达**的目标做前后对照：公网。
公网在默认状态下连得上（v1 实测 TLS 通、拿到 HTTP 响应），所以
「加 deny_out 之后它不通了」是一个干净的二值信号，两种解释分得开。

── 方法：只测「可达性」，不取任何内容 ─────────────────────────────────────────

元数据地址一律只做 **TCP 连接测试**，不发 HTTP 请求、不读任何路径、不打印任何响应体。
「连得上」就足以回答「平台没拦」，取内容对结论没有任何增量。

错误类型按 errno 分类，不同类型含义不同：
  connected    连上了                → **没被拦**
  refused      有主机、明确拒绝      → 包到达了，**没被拦**
  unreachable  路由层说不通          → 被拦或无路由
  timeout      静默丢弃              → 典型的过滤行为，**也可能是根本没有路由**

【定】「timeout」单独一次不构成「平台拦了」的证据。只有在同一个地址上
观察到「默认可达 → 加规则后不可达」的变化，才谈得上规则生效。

运行：
    cd /Users/shixiangweii/run_proj/sxw_fc-sandbox
    set -a; . ./.env; set +a
    .venv/bin/python scripts/probe_code_pool_network.py
"""

import asyncio
import json
import os
import time
from pathlib import Path

import httpx
from e2b import NotFoundException
from e2b.sandbox.commands.command_handle import CommandExitException
from e2b_code_interpreter import AsyncSandbox

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / ".data" / "probe-code-pool-network.json"

# 与 sandbox_pool/agent/policy.py 的 MANDATORY_DENY 一致
MANDATORY_DENY = ["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "169.254.0.0/16", "100.100.100.200/32"]
ALL_TRAFFIC = "0.0.0.0/0"
POOL = "atlas-netprobe"
TIMEOUT_S = 300
INGRESS_PORT = 8080

report: dict = {"started_at": time.strftime("%Y-%m-%d %H:%M:%S"), "checks": {}}


def record(name: str, ok, **detail) -> None:
    report["checks"][name] = {"ok": ok, **detail}
    mark = {True: "PASS", False: "FAIL", None: "INFO"}[ok]
    print(f"[{mark}] {name}: {json.dumps(detail, ensure_ascii=False)[:1000]}")


# 在沙箱里跑的探测脚本。只做连接测试与 HTTP 状态码，不读任何响应体。
PROBE_PY = r'''
import json, socket, subprocess, sys

def tcp(host, port, timeout=5.0):
    """只连接，不发送、不读取。返回错误类别而不是布尔 —— 类别才区分得出「被拦」和「没东西」。"""
    s = socket.socket()
    s.settimeout(timeout)
    try:
        s.connect((host, port))
        return "connected"
    except socket.timeout:
        return "timeout"
    except OSError as e:
        import errno
        name = errno.errorcode.get(e.errno, str(e.errno))
        return {"ECONNREFUSED": "refused", "EHOSTUNREACH": "unreachable",
                "ENETUNREACH": "unreachable", "ETIMEDOUT": "timeout"}.get(name, "other:" + name)
    finally:
        s.close()

def route(host):
    """有没有到这个地址的路由 —— 这是「被过滤」与「根本没有路由」的判别项。

    有路由而连不上 → 包发出去了被丢掉（过滤）；
    无路由        → 网络拓扑上就不通，与平台的屏蔽规则无关。
    """
    try:
        r = subprocess.run(["ip", "route", "get", host], capture_output=True, text=True, timeout=5)
        text = (r.stdout or r.stderr).strip()
        return text.splitlines()[0][:200] if text else "no-output"
    except Exception as e:
        return "error:" + type(e).__name__

def ifaces():
    """沙箱自己在哪个网段 —— 不知道这个，「10.0.0.1 不通」解释不了。"""
    try:
        r = subprocess.run(["ip", "-o", "-4", "addr"], capture_output=True, text=True, timeout=5)
        return [" ".join(l.split()[1:4]) for l in r.stdout.strip().splitlines()]
    except Exception as e:
        return ["error:" + type(e).__name__]

def http_code(url, timeout=8):
    """只取状态码（curl -o /dev/null），不保留响应体。"""
    try:
        r = subprocess.run(["curl", "-s", "-o", "/dev/null", "-w", "%{http_code}", "-m", str(timeout), url],
                           capture_output=True, text=True, timeout=timeout + 5)
        return r.stdout.strip() or "no-response"
    except Exception as e:
        return "error:" + type(e).__name__

def dns(name):
    try:
        return socket.gethostbyname(name)
    except Exception as e:
        return "error:" + type(e).__name__

# 公网对照目标：默认可达，是 Q2 前后对照的干净样本
PUBLIC_IP = sys.argv[1] if len(sys.argv) > 1 else "223.5.5.5"

out = {
    # —— 公网：Q2 的对照组。TCP 层的 connected 比 HTTP 状态码更干净（不受对端 WAF 影响）——
    "public_ip_tcp_443": tcp(PUBLIC_IP, 443),
    "public_https_code": http_code("https://www.aliyun.com"),
    # 白名单相里，这个域名**不在** allow_out 中。裸 TCP 连接测不出按域名的过滤
    # （平台在 80/443 上按 SNI/Host 判，连接先建起来再说），必须真的发一次 TLS 请求
    "other_domain_https_code": http_code("https://www.baidu.com"),
    "dns_resolve_public": dns("www.aliyun.com"),
    # —— 元数据地址：只做 TCP 连接测试，不取任何内容 ——
    "metadata_tcp_80": tcp("100.100.100.200", 80),
    # —— 平台 DNS（在 100.64.0.0/10 内，agent 子系统刻意不整段屏蔽它）——
    "platform_dns_tcp_53": tcp("100.100.2.136", 53),
    # —— 内网网段样本：各取一个常见的网关地址。注意这些地址可能本来就不存在 ——
    "private_10_tcp_22": tcp("10.0.0.1", 22),
    "private_172_tcp_22": tcp("172.16.0.1", 22),
    "private_192_tcp_22": tcp("192.168.0.1", 22),
    "link_local_169_tcp_80": tcp("169.254.169.254", 80),
    # —— 对照：本机地址，说明「connected」长什么样 ——
    "loopback_tcp_22": tcp("127.0.0.1", 22),
    # —— 判别项：有路由而连不上 = 被过滤；无路由 = 拓扑不通 ——
    "route_metadata": route("100.100.100.200"),
    "route_platform_dns": route("100.100.2.136"),
    "route_private_10": route("10.0.0.1"),
    "route_public": route(PUBLIC_IP),
    "own_ifaces": ifaces(),
}
print(json.dumps(out))
'''


async def retry_stale(call):
    """经代理的 SDK 连接空闲后会失效，下一个请求报空的 WriteError（见 e2b_provider._retry_stale）。

    v2 第一次跑就栽在这里：恢复网络那一步直接抛 httpx.WriteError。
    管控面调用是幂等的，重试即可。
    """
    import httpcore
    stale = (httpx.NetworkError, httpx.RemoteProtocolError, httpcore.NetworkError, httpcore.RemoteProtocolError)
    for delay in (0.3, 1.0, 2.0):
        try:
            return await call()
        except stale as e:  # noqa: PERF203
            print(f"    （连接失效，{delay}s 后重试：{type(e).__name__}）")
            await asyncio.sleep(delay)
    return await call()


async def sh(sbx: AsyncSandbox, cmd: str, *, timeout: float = 90, background: bool = False):
    last = None
    for _ in range(10):
        try:
            r = await sbx.commands.run(cmd, user="user", timeout=timeout, background=background)
            if background:
                return 0, ""
            return r.exit_code, (r.stdout + r.stderr).strip()
        except CommandExitException as e:
            return e.exit_code, (e.stdout + e.stderr).strip()
        except Exception as e:  # noqa: BLE001
            last = e
            await asyncio.sleep(1.0)
    raise RuntimeError(f"命令反复失败：{last!r}")


async def run_probe(sbx: AsyncSandbox, public_ip: str, label: str) -> dict:
    code, out = await sh(sbx, f"python3 /home/user/netprobe.py {public_ip}")
    if code != 0:
        raise RuntimeError(f"[{label}] 探测脚本失败：{out[:500]}")
    return json.loads(out.splitlines()[-1])


def diff(before: dict, after: dict) -> dict:
    return {k: {"before": before[k], "after": after.get(k), "changed": before[k] != after.get(k)}
            for k in before}


async def main() -> int:
    opts = {
        "api_key": os.environ["E2B_API_KEY"],
        "api_url": os.environ["E2B_API_URL"],
        "domain": os.environ["E2B_DOMAIN"],
    }
    proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")
    if proxy:
        opts["proxy"] = proxy
    template = os.environ["POOL_TEMPLATE"]
    # 公网对照目标用一个稳定的 IP（公共 DNS 的 443），不依赖域名解析与对端 WAF
    public_ip = os.environ.get("PROBE_PUBLIC_IP", "223.5.5.5")
    report.update({"template": template, "proxy_used": bool(proxy), "public_probe_ip": public_ip})

    sbx = None
    try:
        # ── 1. 完全按代码执行池的方式创建：不传 network、不传 secure ──
        t0 = time.monotonic()
        sbx = await retry_stale(lambda: AsyncSandbox.create(
            template=template,
            timeout=TIMEOUT_S,
            metadata={"pool": POOL, "purpose": "atlas-network-probe"},
            request_timeout=30,
            **opts,
        ))
        record("create_like_code_pool", None, sandbox_id=sbx.sandbox_id,
               elapsed_s=round(time.monotonic() - t0, 2),
               note="只传 template/timeout/metadata，与 E2BProvider.create 一致")
        await sbx.files.write("/home/user/netprobe.py", PROBE_PY)

        info = await retry_stale(lambda: AsyncSandbox.get_info(sbx.sandbox_id, request_timeout=30, **opts))
        net = getattr(info, "network", None)
        record("default_network_echo", None, network=(dict(net) if net else None),
               note="代码池建沙箱时不传 network，这是平台回显的默认值")

        # ── 2. Q1：默认状态下的出网边界 ──
        base = await run_probe(sbx, public_ip, "default")
        record("probe_default", None, **base)

        record("Q1_public_egress_open", base["public_ip_tcp_443"] == "connected",
               tcp=base["public_ip_tcp_443"], http=base["public_https_code"],
               note="默认开放公网出网（TCP 层可达；HTTP 状态码受对端 WAF 影响，不作判据）")
        record("Q1_metadata_reachable", base["metadata_tcp_80"] == "connected",
               result=base["metadata_tcp_80"],
               note="只做 TCP 连接测试、未取任何内容。timeout 既可能是平台拦了，也可能是没有路由")
        record("Q1_private_samples", None,
               **{k: base[k] for k in base if k.startswith(("private_", "link_local_", "platform_dns_"))},
               note="平台 DNS 在 100.64.0.0/10 内且可达，说明内网空间不是整段不通")
        record("Q1_routes", None,
               **{k: base[k] for k in base if k.startswith(("route_", "own_"))},
               note="判别项：有路由而连不上 = 被过滤；无路由 = 拓扑上就不通，与平台屏蔽规则无关")

        # ── 3. Q2：对非 secure 沙箱 update_network，用**默认可达的公网**做对照 ──
        deny_all = {"allow_out": [], "deny_out": [ALL_TRAFFIC], "rules": {}}
        accepted, err = True, None
        t1 = time.monotonic()
        try:
            await retry_stale(lambda: AsyncSandbox.update_network(
                sbx.sandbox_id, deny_all, request_timeout=30, **opts))
        except Exception as e:  # noqa: BLE001
            accepted, err = False, f"{type(e).__name__}: {str(e)[:300]}"
        record("Q2_update_network_accepted", accepted, elapsed_s=round(time.monotonic() - t1, 2),
               error=err, policy_sent=deny_all)

        blocked = restored = None
        if accepted:
            await asyncio.sleep(3)
            info2 = await retry_stale(lambda: AsyncSandbox.get_info(sbx.sandbox_id, request_timeout=30, **opts))
            net2 = getattr(info2, "network", None)
            record("network_echo_after_deny_all", None, network=(dict(net2) if net2 else None))

            blocked = await run_probe(sbx, public_ip, "deny-all")
            record("probe_after_deny_all", None, **blocked)
            record(
                "Q2_update_network_effective",
                base["public_ip_tcp_443"] == "connected" and blocked["public_ip_tcp_443"] != "connected",
                public_before=base["public_ip_tcp_443"], public_after=blocked["public_ip_tcp_443"],
                note="判据：默认可达的公网目标在 deny_out=0.0.0.0/0 之后变为不可达。"
                     "这个目标默认确实连得上，所以两种解释分得开（v1 用元数据地址做不到）",
            )

            record("probe_diff_default_vs_denyall", None, **diff(base, blocked))

            # ── 3b. 白名单模式：这才是 Atlas 要用的形态（默认断网 ＋ 按需放行）──
            #     关键问题：全拒之后 DNS 还活着吗？deny-all 那一相里 DNS 解析已经报 gaierror。
            allowlist = {"allow_out": ["www.aliyun.com"], "deny_out": [ALL_TRAFFIC], "rules": {}}
            try:
                await retry_stale(lambda: AsyncSandbox.update_network(
                    sbx.sandbox_id, allowlist, request_timeout=30, **opts))
                await asyncio.sleep(3)
                al = await run_probe(sbx, public_ip, "allowlist")
                record("Q2_allowlist_mode", None, policy=allowlist,
                       allowed_domain_http=al["public_https_code"], dns=al["dns_resolve_public"],
                       other_public_tcp=al["public_ip_tcp_443"], platform_dns=al["platform_dns_tcp_53"],
                       other_domain_https=al["other_domain_https_code"],
                       note="关键是 other_domain_https：未放行域名的**真实 TLS 请求**通不通。"
                            "裸 TCP 连接测不出按域名的过滤 —— 平台在 80/443 上按 SNI 判，"
                            "连接会先建起来，所以 other_public_tcp=connected 不等于没拦")
            except Exception as e:  # noqa: BLE001
                record("Q2_allowlist_mode", None, error=f"{type(e).__name__}: {str(e)[:200]}")

            # 恢复：借用式集成要求同一个沙箱能改回去（归还前 / 下一次借出）
            t2 = time.monotonic()
            await retry_stale(lambda: AsyncSandbox.update_network(
                sbx.sandbox_id, {"allow_out": [], "deny_out": [], "rules": {}}, request_timeout=30, **opts))
            await asyncio.sleep(3)
            restored = await run_probe(sbx, public_ip, "restored")
            record("Q2_update_network_reversible",
                   restored["public_ip_tcp_443"] == "connected",
                   elapsed_s=round(time.monotonic() - t2, 2),
                   public_after_restore=restored["public_ip_tcp_443"])

        # ── 4. 附带：非 secure 沙箱暴露的端口，公网不带令牌能不能访问 ──
        #    （Atlas 集成的第 4 处缺口：agent 子系统设了 allow_public_traffic=False，代码池没设）
        try:
            if accepted:
                await retry_stale(lambda: AsyncSandbox.update_network(
                    sbx.sandbox_id, {"allow_out": [], "deny_out": [], "rules": {}}, request_timeout=30, **opts))
                await asyncio.sleep(2)
            await sh(sbx, f"nohup python3 -m http.server {INGRESS_PORT} >/tmp/h.log 2>&1 &", background=True)
            await asyncio.sleep(3)
            code, _ = await sh(sbx, f"curl -sf -m 5 -o /dev/null http://127.0.0.1:{INGRESS_PORT}/")
            host = sbx.get_host(INGRESS_PORT)
            token = getattr(sbx, "traffic_access_token", None)
            status = None
            if code == 0:
                async with httpx.AsyncClient(timeout=20, trust_env=False, proxy=proxy, verify=True) as c:
                    for _ in range(4):  # 本机代理的 fake-ip 下到沙箱域名约 1/3 失败，重试
                        try:
                            r = await c.get(f"https://{host}/")
                            status = r.status_code
                            break
                        except Exception as e:  # noqa: BLE001
                            status = f"error:{type(e).__name__}"
                            await asyncio.sleep(1.5)
            record("ingress_public_without_token", status == 200,
                   http_status=status, local_server_ok=(code == 0),
                   has_traffic_token=bool(token),
                   note="代码池建沙箱时没有 allow_public_traffic=False。"
                        "200 = 沙箱里起的服务对公网裸奔（Atlas 集成要补的第 4 处）")
        except Exception as e:  # noqa: BLE001
            record("ingress_public_without_token", None, error=f"{type(e).__name__}: {str(e)[:200]}")

        report["summary"] = {
            "Q1_默认公网": "开放" if base["public_ip_tcp_443"] == "connected" else "不通",
            "Q1_默认元数据地址": ("可达（平台不拦）" if base["metadata_tcp_80"] == "connected"
                                  else f"不可达（{base['metadata_tcp_80']}）—— 拦没拦分不出来，见下"),
            "Q2_update_network": (
                "被拒绝" if not accepted else
                "接受，且实测生效（公网被拦住）" if blocked and blocked["public_ip_tcp_443"] != "connected" else
                "接受但没观察到效果"
            ),
            "Q2_可逆": ("是" if restored and restored["public_ip_tcp_443"] == "connected" else "否/未测"),
        }
        return 0
    finally:
        if sbx is not None:
            try:
                killed = await retry_stale(lambda: AsyncSandbox.kill(sbx.sandbox_id, request_timeout=20, **opts))
                record("cleanup_kill", killed, sandbox_id=sbx.sandbox_id)
            except Exception as e:  # noqa: BLE001
                record("cleanup_kill", False, error=str(e)[:300], sandbox_id=sbx.sandbox_id)
            try:
                await AsyncSandbox.get_info(sbx.sandbox_id, request_timeout=20, **opts)
                record("cleanup_confirmed", False, note="销毁后仍能查到，需要人工复查")
            except NotFoundException:
                record("cleanup_confirmed", True, note="销毁后查不到了")
            except Exception as e:  # noqa: BLE001
                record("cleanup_confirmed", None, error=str(e)[:200])
        OUT.parent.mkdir(parents=True, exist_ok=True)
        OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\n报告写入 {OUT}")
        if "summary" in report:
            print(json.dumps(report["summary"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
