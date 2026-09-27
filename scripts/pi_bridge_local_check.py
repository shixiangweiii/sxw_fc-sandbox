"""本机真实联调：本机起 pi 桥接进程（真实 pi + 真实模型），用网关的 HTTP 客户端与 PiTranslator 跑一遍关键路径。

在云上构建模板之前发现桥接进程与 pi 真实行为的出入；升级 pi 版本时先跑这个。不访问云沙箱，会消耗少量模型 token。

    npm install --prefix /tmp/pi-local @earendil-works/pi-coding-agent@0.87.1   # 或已有的安装
    DEEPSEEK_API_KEY=... python scripts/pi_bridge_local_check.py --pi /tmp/pi-local/node_modules/@earendil-works/pi-coding-agent/dist/bundle/cli.js

检查项：建会话、流式对话、bash 工具、结果与用量提取、run 状态、会话续聊、中止、运行中重载不中止。
"""

import argparse
import asyncio
import json
import os
import socket
import subprocess
import sys
import tempfile
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from sandbox_pool.agent.engines.pi import PiTranslator, extract_result  # noqa: E402
from sandbox_pool.agent.opencode import AgentHttpClient  # noqa: E402

BRIDGE = ROOT / "sandbox_pool" / "agent" / "pi_bridge" / "pi-bridge.mjs"
results: dict = {}


def record(name: str, ok: bool, **detail) -> None:
    results[name] = {"ok": ok, **detail}
    print(f"[{'PASS' if ok else 'FAIL'}] {name}: {json.dumps(detail, ensure_ascii=False)[:400]}", flush=True)


def free_port() -> int:
    with socket.socket() as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


async def run_turn(client: AgentHttpClient, sid: str, text: str, *, abort_after: float = 0, reload_after: float = 0,
                   timeout: float = 120) -> dict:
    """发一轮提示词，按事件翻译并等 idle；返回对外事件、结果、run 状态。"""
    queue: asyncio.Queue = asyncio.Queue()

    async def reader():
        async for ev in client.events():
            queue.put_nowait(ev)

    task = asyncio.create_task(reader())
    try:
        while (await asyncio.wait_for(queue.get(), 10)).get("type") != "server.connected":
            pass
        t0 = time.monotonic()
        run_id = await client.prompt_async(sid, text, model=None, agent=None)
        tr = PiTranslator(sid)
        out, first_text, acted = [], None, False
        while not tr.idle:
            if time.monotonic() - t0 > timeout:
                raise TimeoutError("run did not settle")
            try:
                ev = await asyncio.wait_for(queue.get(), 1)
            except asyncio.TimeoutError:
                ev = None
            if ev is not None:
                for kind, data in tr.feed(ev):
                    out.append((kind, data))
                    if kind == "text" and first_text is None:
                        first_text = time.monotonic() - t0
            if not acted and abort_after and time.monotonic() - t0 > abort_after:
                acted = True
                await client.abort(sid)
            if not acted and reload_after and time.monotonic() - t0 > reload_after:
                acted = True
                await client.dispose()
        text_out, usage, error = extract_result(await client.messages(sid))
        return {
            "run_id": run_id,
            "events": out,
            "first_text_s": round(first_text, 2) if first_text else None,
            "total_s": round(time.monotonic() - t0, 2),
            "text": text_out,
            "usage": usage,
            "error": error or (tr.errors[-1] if tr.errors else None),
            "run_state": await client.run_state(sid, run_id),
        }
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def main_async(args) -> int:
    work = Path(tempfile.mkdtemp(prefix="pi-bridge-local-"))
    port = free_port()
    (work / "workspace").mkdir()
    (work / "pi.json").write_text(json.dumps({"provider": args.model.split("/")[0], "model": args.model.split("/", 1)[1], "mcp": False}))
    (work / "workspace" / "AGENTS.md").write_text("回答尽量简短。\n")
    env = {
        **os.environ,
        "PI_CMD": json.dumps(["node", args.pi]),
        "PI_BRIDGE_HOST": "127.0.0.1",
        "PI_BRIDGE_PORT": str(port),
        "PI_BRIDGE_WORKDIR": str(work / "workspace"),
        "PI_BRIDGE_SESSION_DIR": str(work / "sessions"),
        "PI_BRIDGE_CONFIG": str(work / "pi.json"),
        "PI_BRIDGE_LOG_DIR": str(work / "logs"),
        "PI_CODING_AGENT_DIR": str(work / "agent"),
        "PI_OFFLINE": "1",
        "PI_SKIP_VERSION_CHECK": "1",
        "PI_TELEMETRY": "0",
    }
    bridge = subprocess.Popen(["node", str(BRIDGE)], env=env, stderr=open(work / "bridge.log", "w"))
    client = AgentHttpClient(f"http://127.0.0.1:{port}", None, str(work / "workspace"))
    try:
        for _ in range(50):
            try:
                health = await client.health()
                break
            except Exception:  # noqa: BLE001
                await asyncio.sleep(0.1)
        record("health", bool(health.get("healthy")), health=health)
        sid = await client.create_session("local check")
        record("create_session", bool(sid), session=sid)

        r = await run_turn(client, sid, "用 bash 执行 `echo hello-bridge`，然后只回复命令输出。")
        tools = [d for k, d in r["events"] if k == "tool"]
        record("tool_turn", "hello-bridge" in r["text"] and any(t["status"] == "completed" for t in tools)
               and r["run_state"] == "settled" and r["error"] is None,
               text=r["text"], tools=[(t["tool"], t["status"]) for t in tools], usage=r["usage"],
               first_text_s=r["first_text_s"], total_s=r["total_s"], run_state=r["run_state"])
        record("usage", r["usage"]["input"] > 0 and r["usage"]["output"] > 0 and r["usage"]["cost"] > 0, usage=r["usage"])

        r = await run_turn(client, sid, "我上一条让你执行的命令是什么？只回复命令本身。")
        record("session_continuity", "echo" in r["text"], text=r["text"])

        r = await run_turn(client, sid, "用 bash 执行 `sleep 30; echo done`，完成后告诉我结果。", abort_after=8)
        record("abort", r["run_state"] == "settled" and r["total_s"] < 25, total_s=r["total_s"], error=r["error"],
               text=r["text"][:100])

        r = await run_turn(client, sid, "用 bash 执行 `sleep 3; echo reload-ok`，然后只回复命令输出。", reload_after=2)
        record("reload_keeps_running", "reload-ok" in r["text"] and r["error"] is None, text=r["text"],
               run_state=r["run_state"])
        record("status_idle", await client.status() == {}, status=await client.status())
    finally:
        await client.close()
        bridge.terminate()
        bridge.wait(timeout=10)
    ok = all(v["ok"] for v in results.values())
    print(f"\n{'ALL PASS' if ok else 'SOME FAILED'}; bridge log: {work / 'bridge.log'}")
    return 0 if ok else 1


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--pi", required=True, help="pi CLI 入口（dist/bundle/cli.js）")
    ap.add_argument("--model", default="deepseek/deepseek-flash")
    return asyncio.run(main_async(ap.parse_args()))


if __name__ == "__main__":
    sys.exit(main())
