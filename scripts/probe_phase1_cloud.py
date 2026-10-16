"""阶段 1 的真实云端复核（会创建真实云沙箱，产生费用；结束即销毁）。

单元测试全程用 FakeProvider，所以下面这几件事只有真云端答得了：

  P1  `secure=True` 会不会把 **envd 访问**（commands / files）一起挡掉。
      —— 这是改动里最大的风险：关公网直达是为了堵入站的洞，但池子自己
         就是靠 envd 下发命令的。连带还要验**句柄缓存失效后重新 connect**
         的那条路径（agent 子系统用的是 create 时缓存的句柄，没覆盖到重连）。
  P2  入站的洞是不是真的堵上了：沙箱里起个服务，从本机不带令牌访问应当被拒。
  P3  池子按新默认建出来的沙箱，是不是真的断网。
  P4  借用时要域名白名单，下发之后那个域名是不是真的通、别的是不是真的不通。

运行：
    cd /Users/shixiangweii/run_proj/sxw_fc-sandbox
    set -a; . ./.env; set +a
    PYAGENT_TEMPLATE=<模板ID> .venv/bin/python scripts/probe_phase1_cloud.py
"""

import asyncio
import json
import os
import time
from pathlib import Path

import httpx

from sandbox_pool.egress import DENY_ALL, EgressPolicy, policy_network
from sandbox_pool.provider.e2b_provider import E2BProvider

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / ".data" / "probe-phase1-cloud.json"
PORT = 8080

report: dict = {"started_at": time.strftime("%Y-%m-%d %H:%M:%S"), "checks": {}}


def record(name: str, ok, **detail) -> None:
    report["checks"][name] = {"ok": ok, **detail}
    print(f"[{ {True: 'PASS', False: 'FAIL', None: 'INFO'}[ok] }] {name}: "
          f"{json.dumps(detail, ensure_ascii=False)[:600]}")


async def main() -> int:
    template = os.environ.get("PYAGENT_TEMPLATE") or os.environ["POOL_TEMPLATE"]
    proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")
    p = E2BProvider(proxy=proxy)
    sid = None
    try:
        # ── P3：按池子的新默认建（断网 ＋ 关公网直达）──
        t = time.monotonic()
        sid = await p.create(template, {"pool": "atlas-phase1"}, 600,
                             network=policy_network(DENY_ALL), secure=True)
        record("create_offline_secure", None, sandbox_id=sid,
               ms=round((time.monotonic() - t) * 1000))

        # ── P1：secure 沙箱上 envd 还能不能用 ──
        r = await p.run_command(sid, "echo envd-ok && whoami", cwd=None, envs=None,
                                timeout_s=60, sandbox_timeout_s=600, max_output_bytes=65536)
        record("P1_envd_commands_on_secure_sandbox", r.exit_code == 0 and "envd-ok" in r.stdout,
               exit_code=r.exit_code, stdout=r.stdout.strip()[:120],
               note="关公网直达之后池子自己还能不能下发命令")

        await p.write_file(sid, "/home/user/probe.txt", b"hello", sandbox_timeout_s=600)
        back = await p.read_file(sid, "/home/user/probe.txt", sandbox_timeout_s=600)
        record("P1_envd_files_on_secure_sandbox", back == b"hello", read_back=back.decode()[:40])

        # 句柄缓存失效后重新 connect —— 真实服务里借用可能发生在创建很久以后
        p.forget(sid)
        r2 = await p.run_command(sid, "echo reconnect-ok", cwd=None, envs=None,
                                 timeout_s=60, sandbox_timeout_s=600, max_output_bytes=65536)
        record("P1_envd_after_reconnect", r2.exit_code == 0 and "reconnect-ok" in r2.stdout,
               exit_code=r2.exit_code, stdout=r2.stdout.strip()[:120],
               note="丢掉缓存句柄、重新 connect 一个 secure 沙箱")

        # ── P3：真的断网吗 ──
        r3 = await p.run_command(sid, "curl -s -o /dev/null -w '%{http_code}' -m 8 https://pypi.org/simple/ || echo blocked",
                                 cwd=None, envs=None, timeout_s=60, sandbox_timeout_s=600, max_output_bytes=65536)
        offline = r3.stdout.strip() not in ("200", "301", "302")
        record("P3_default_is_offline", offline, probe=r3.stdout.strip()[:40])

        # ── P2：入站的洞堵上了吗 ──
        await p.run_command(sid, f"nohup python3 -m http.server {PORT} >/tmp/h.log 2>&1 & sleep 2; "
                                 f"curl -sf -m 3 -o /dev/null http://127.0.0.1:{PORT}/ && echo local-ok",
                            cwd=None, envs=None, timeout_s=60, sandbox_timeout_s=600, max_output_bytes=65536)
        host = f"{PORT}-{sid}.{os.environ['E2B_DOMAIN']}"
        status = None
        async with httpx.AsyncClient(timeout=20, trust_env=False, proxy=proxy) as c:
            for _ in range(4):
                try:
                    status = (await c.get(f"https://{host}/")).status_code
                    break
                except Exception as e:  # noqa: BLE001
                    status = f"error:{type(e).__name__}"
                    await asyncio.sleep(1.5)
        record("P2_public_ingress_blocked", status != 200, http_status=status,
               note="不带流量令牌从公网访问沙箱端口。200 = 洞还在（改之前实测就是 200）")

        # ── P4：借用级白名单真的生效吗 ──
        allow = EgressPolicy(mode="allowlist", allow_out=("www.aliyun.com",))
        await p.update_network(sid, policy_network(allow))
        await asyncio.sleep(3)
        echo = await p.get_network(sid)
        r4 = await p.run_command(
            sid,
            "echo allowed=$(curl -s -o /dev/null -w '%{http_code}' -m 10 https://www.aliyun.com); "
            "echo other=$(curl -s -o /dev/null -w '%{http_code}' -m 10 https://www.baidu.com)",
            cwd=None, envs=None, timeout_s=90, sandbox_timeout_s=600, max_output_bytes=65536)
        out = r4.stdout
        allowed_ok = "allowed=000" not in out
        other_blocked = "other=000" in out
        record("P4_allowlist_effective", allowed_ok and other_blocked, output=out.strip()[:200],
               echo_allow=(echo or {}).get("allow_out"), echo_deny=(echo or {}).get("deny_out"),
               note="放行域名通、未放行域名 000；回显要与下发一致")

        report["summary"] = {k: v["ok"] for k, v in report["checks"].items() if v["ok"] is not None}
        return 0
    finally:
        if sid:
            try:
                await p.kill(sid)
                record("cleanup_kill", True, sandbox_id=sid)
            except Exception as e:  # noqa: BLE001
                record("cleanup_kill", False, error=str(e)[:200])
        try:
            left = [s.sandbox_id for s in await p.list({"pool": "atlas-phase1"})]
            record("cleanup_remaining", not left, remaining=left)
        except Exception as e:  # noqa: BLE001
            record("cleanup_remaining", None, error=str(e)[:200])
        await p.close()
        OUT.parent.mkdir(parents=True, exist_ok=True)
        OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\n报告写入 {OUT}")
        if "summary" in report:
            print(json.dumps(report["summary"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
