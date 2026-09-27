"""云上验证（PoC）：用现有第二代模板建一个临时沙箱，执行 pi 模板的启动脚本（scripts/build_pi_template.py），
再用网关的 HTTP 客户端与 PiTranslator 逐项验证，最后销毁沙箱。在正式构建模板之前发现问题。

验证项：
- 镜像自带 Node 版本、安装 Node / pi / pi-mcp-adapter 的耗时；
- Node 经平台 CA 通过出网注入的 TLS 检查：pi 用占位符 Key 调 DeepSeek 成功；沙箱与 pi 进程环境里没有真实 Key；
- 桥接进程经平台入口 + 流量令牌访问（不带令牌 403）；流式对话、bash 工具、百炼 WebSearch MCP（直接注册的工具）、
  中止、运行中重载不中止；
- 内存（桥接进程 + 各会话 pi 进程）。

    set -a; . ./.env; . .data/agent-e2e.env; set +a
    python scripts/poc_pi_agent.py [--template <第二代模板 ID>] [--keep]

结果写入 .data/poc-pi-report.json，不含任何密钥。
"""

import argparse
import asyncio
import json
import os
import sys
import time
from pathlib import Path

import httpx
from e2b import AsyncSandbox

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from build_pi_template import PORT, bootstrap_script  # noqa: E402
from pi_bridge_local_check import run_turn  # noqa: E402
from poc_opencode_agent import MCP_URL, bailian_key, network_config, sh  # noqa: E402

from sandbox_pool.agent.engines.base import FilesContext  # noqa: E402
from sandbox_pool.agent.engines.pi import PiEngine  # noqa: E402
from sandbox_pool.agent.opencode import AgentHttpClient  # noqa: E402

WORKDIR = "/home/user/workspace"
report: dict = {"started_at": time.strftime("%Y-%m-%d %H:%M:%S"), "checks": {}}


def record(name: str, ok, **detail) -> None:
    report["checks"][name] = {"ok": ok, **detail}
    mark = {True: "PASS", False: "FAIL", None: "INFO"}[ok]
    print(f"[{mark}] {name}: {json.dumps(detail, ensure_ascii=False)[:700]}", flush=True)


def summarize(turn: dict) -> dict:
    tools = [(d["tool"], d["status"]) for k, d in turn["events"] if k == "tool"]
    return {"text": turn["text"][:200], "tools": tools, "usage": turn["usage"], "error": turn["error"],
            "first_text_s": turn["first_text_s"], "total_s": turn["total_s"], "run_state": turn["run_state"]}


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--template", default=os.environ.get("POOL_TEMPLATE", "xu76gk97q07mgohgw7q3"))
    ap.add_argument("--keep", action="store_true", help="结束后不销毁沙箱（调试用）")
    args = ap.parse_args()

    opts = {}
    proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")
    if proxy:
        opts["proxy"] = proxy
    t0 = time.monotonic()
    sbx = await AsyncSandbox.create(
        template=args.template, timeout=1800, metadata={"pool": "poc-pi"}, secure=True,
        network={**network_config(os.environ["FCSANDBOX_OPENCODE_MODEL_API_KEY"], bailian_key()), "allow_public_traffic": False},
        request_timeout=60, **opts,
    )
    record("create", True, seconds=round(time.monotonic() - t0, 2), sandbox_id=sbx.sandbox_id)
    client = None
    try:
        _, out = await sh(sbx, "id; nproc; free -m | head -2; node -v; npm -v; ls /usr/local/share/ca-certificates/; "
                               "env | grep -i -E 'NODE_EXTRA|SSL_CERT' || true")
        record("env", None, output=out)

        # ---------- 执行模板启动脚本（后台，最后 exec 到守护循环）----------
        await sbx.files.write("/tmp/pi-bootstrap.sh", bootstrap_script(run=True), user="root")
        t1 = time.monotonic()
        await sbx.commands.run("bash /tmp/pi-bootstrap.sh > /tmp/pi-bootstrap.log 2>&1", background=True, user="root")
        host = sbx.get_host(PORT)
        ip = os.environ.get("POOL_AGENT_INGRESS_IP") or None
        client = AgentHttpClient(f"https://{host}", sbx.traffic_access_token, WORKDIR, ingress_ip=ip, timeout_s=30)
        health = None
        next_check = time.monotonic() + 15
        while time.monotonic() - t1 < 600:
            try:
                health = await client.health()
                break
            except Exception:  # noqa: BLE001 - 安装中
                await asyncio.sleep(3)
            if time.monotonic() >= next_check:
                # 启动脚本最后 exec 到守护循环，进程应一直在；提前退出说明安装失败，不必等满
                next_check = time.monotonic() + 15
                _, alive = await sh(sbx, "pgrep -f '[p]i-bootstrap.sh|[r]un.sh' > /dev/null && echo alive || echo exited")
                if "exited" in alive:
                    break
        _, log = await sh(sbx, "tail -5 /tmp/pi-bootstrap.log; ls /opt/node/bin 2>/dev/null | head -3; "
                               "du -sh /opt/pi-agent/node_modules /opt/node 2>/dev/null; "
                               "grep -o '/[^ ]*/node' /opt/pi-agent/run.sh | head -1")
        record("bootstrap", bool(health and health.get("healthy")), seconds=round(time.monotonic() - t1, 1),
               health=health, log=log[-600:])
        if not health:
            return 1

        # 不带令牌访问端口
        async with httpx.AsyncClient(base_url=f"https://{ip or host}", trust_env=False, timeout=15,
                                     headers={"Host": host}) as c:
            r = await c.get("/global/health", extensions={"sni_hostname": host} if ip else {})
        record("traffic_token_required", r.status_code == 403, status=r.status_code)

        # ---------- 网关装配：写 pi.json / mcp.json / AGENTS.md ----------
        engine = PiEngine(template="poc", port=PORT, model="deepseek/deepseek-flash", workdir=WORKDIR)
        files = engine.render_files(FilesContext(
            workdir=WORKDIR, max_life_h=23.5, idle_destroy_after_s=0,
            mcp={"websearch": {"type": "remote", "url": MCP_URL, "enabled": True}},
            egress={"mode": "open"}, instructions="回答尽量简短。",
        ))
        for path, data in files.items():
            await sbx.files.write(path, data, user="user")
        record("config_written", True, files=sorted(files))

        sid = await client.create_session("poc")
        turn = await run_turn(client, sid, "用一句话回答：1+1 等于几？")
        record("deepseek_via_injection", bool(turn["text"]) and turn["error"] is None, **summarize(turn))
        turn = await run_turn(client, sid, "用 bash 执行 `uname -m && node -v`，然后只回复命令输出。")
        record("bash_tool", "x86_64" in turn["text"] and turn["error"] is None, **summarize(turn))
        turn = await run_turn(client, sid, "请联网搜索今天杭州的天气，用一句话回答。")
        tools = [d["tool"] for k, d in turn["events"] if k == "tool"]
        record("bailian_websearch_mcp", any("websearch" in (t or "") for t in tools) and turn["error"] is None,
               **summarize(turn))
        turn = await run_turn(client, sid, "用 bash 执行 `sleep 30; echo done`，完成后告诉我结果。", abort_after=8)
        record("abort", turn["run_state"] == "settled" and turn["total_s"] < 25, **summarize(turn))
        turn = await run_turn(client, sid, "用 bash 执行 `sleep 3; echo reload-ok`，然后只回复命令输出。", reload_after=2)
        record("reload_keeps_running", "reload-ok" in turn["text"] and turn["error"] is None, **summarize(turn))
        sid2 = await client.create_session("poc-2")
        turn = await run_turn(client, sid2, "我们之前聊过什么？如果没有就回答“没有”。")
        record("second_session", turn["error"] is None, **summarize(turn))

        # ---------- 安全与资源 ----------
        _, out = await sh(sbx, "for p in $(pgrep -f 'pi-bridge|cli.js'); do tr '\\0' '\\n' < /proc/$p/environ; done "
                               "| grep -E '^DEEPSEEK_API_KEY=' | sort -u; "
                               "grep -rl -E 'sk-[A-Za-z0-9]{20}' /home/user /opt/pi-agent/run.sh 2>/dev/null | head -3; echo end")
        record("no_real_key_in_sandbox", "injected-by-platform" in out and "sk-" not in out.replace("sk-[", ""), output=out)
        _, out = await sh(sbx, "ps -eo user,rss,cmd --sort=-rss | grep -E '[p]i-bridge|[c]li.js' | cut -c1-160; free -m | head -2")
        rss = sum(int(line.split()[1]) for line in out.splitlines() if line.split() and line.split()[1].isdigit()) // 1024
        record("memory", None, total_rss_mb=rss, output=out)
    finally:
        if client is not None:
            await client.close()
        if not args.keep:
            record("killed", await AsyncSandbox.kill(sbx.sandbox_id, **opts))
        report["finished_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        (ROOT / ".data").mkdir(exist_ok=True)
        (ROOT / ".data" / "poc-pi-report.json").write_text(json.dumps(report, ensure_ascii=False, indent=2))
    failed = [k for k, v in report["checks"].items() if v["ok"] is False]
    print("FAILED:", failed if failed else "none")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
