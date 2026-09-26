"""agent 子系统端到端场景（真实云沙箱 + 本地多副本集群）。结果写入 .data/e2e-agent-report.json（不含密钥）。

    # 先按 sxw_aicoding/…业务接入使用手册.md 配置环境变量并启动集群（POOL_AGENT_ENABLED=true）
    python scripts/e2e_agent_scenarios.py [--only S1,S2,...] [--replicas 8001,8002]

环境变量：SANDBOX_POOL_API_KEY（调用方 key）、SANDBOX_POOL_ADMIN_KEY（管理员 key，可选）、
POOL_AGENT_INGRESS_IP（可选，用于 S3 直连沙箱端口验证 403）。

场景：
  S1 首次流式对话（记录建沙箱、首字、总耗时）         S7 中止
  S2 工具：bash / webfetch / 百炼联网搜索 MCP          S8 定时任务：立即触发 + 等待自动触发，拉取结果
  S3 安全：沙箱内无 Key、元数据 / 内网不可达、端口 403  S9 空闲销毁后自动新建
  S4 出网策略：API 读取、agent 读文件、运行中切白名单  S10 kill -9 持有任务的副本，任务被接管完成
  S5 会话连续                                          S11 收尾：重置 agent，确认没有残留沙箱
  S6 断线重连（到另一个副本）
"""

import argparse
import asyncio
import json
import os
import signal
import sys
import time
from pathlib import Path
from typing import Optional

import httpx

ROOT = Path(__file__).resolve().parents[1]
USER = f"e2e-{int(time.time())}"
REPORT: dict = {"user": USER, "started_at": time.strftime("%Y-%m-%d %H:%M:%S"), "scenarios": {}}


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


class Api:
    def __init__(self, base: str, key: Optional[str]):
        self.base = base
        self.http = httpx.AsyncClient(base_url=base, timeout=httpx.Timeout(30, read=600), trust_env=False,
                                      headers={"Authorization": f"Bearer {key}"} if key else {})

    async def req(self, method: str, path: str, **kw) -> httpx.Response:
        return await self.http.request(method, path, **kw)

    async def stream_message(self, text: str, *, session_id: Optional[str] = None, stop_after_start: bool = False,
                             on_start=None) -> dict:
        """发消息并读取 SSE，返回事件统计。stop_after_start=True 时收到 start 后立即断开（模拟客户端断线）。"""
        t0 = time.monotonic()
        out = {"events": [], "tools": [], "text": "", "first_text_s": None, "start_s": None}
        body = {"text": text}
        if session_id:
            body["session_id"] = session_id
        async with self.http.stream("POST", f"/v1/agents/{USER}/messages", json=body) as r:
            if r.status_code != 200:
                out["status"] = r.status_code
                out["error"] = (await r.aread()).decode()[:500]
                return out
            kind, data = None, []
            async for line in r.aiter_lines():
                if line.startswith("event:"):
                    kind = line[6:].strip()
                elif line.startswith("data:"):
                    data.append(line[5:].strip())
                elif line == "" and kind:
                    payload = json.loads("\n".join(data))
                    now = round(time.monotonic() - t0, 2)
                    out["events"].append(kind)
                    if kind == "start":
                        out["start_s"] = now
                        out["start"] = payload
                        if on_start:
                            await on_start(payload)
                        if stop_after_start:
                            return out
                    elif kind == "text":
                        out["text"] += payload["delta"]
                        if out["first_text_s"] is None:
                            out["first_text_s"] = now
                    elif kind == "tool":
                        out["tools"].append(payload)
                    elif kind == "done":
                        out["done"] = payload
                    kind, data = None, []
        out["total_s"] = round(time.monotonic() - t0, 2)
        return out

    async def wait_task(self, task_id: str, timeout: float = 300) -> dict:
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            r = await self.req("GET", f"/v1/agents/{USER}/tasks/{task_id}")
            if r.status_code == 200 and r.json()["state"] != "RUNNING":
                return r.json()
            await asyncio.sleep(2)
        raise AssertionError(f"task {task_id} not finished in {timeout}s")


def tool_output(res: dict, tool: str) -> str:
    return "\n".join((t.get("output") or "") for t in res["tools"] if t["tool"] == tool and t["status"] == "completed")


def check(name: str, ok: bool, **detail) -> bool:
    REPORT["scenarios"].setdefault(name, {"checks": []})["checks"].append({"ok": bool(ok), **detail})
    log(f"{'PASS' if ok else 'FAIL'} {name}: {json.dumps(detail, ensure_ascii=False)[:400]}")
    return bool(ok)


async def s1(api: Api, ctx: dict) -> None:
    res = await api.stream_message("请用一句话介绍你自己，不要调用任何工具。")
    ctx["session"] = res.get("start", {}).get("session_id")
    check("S1", res.get("done", {}).get("state") == "SUCCEEDED" and len(res["text"]) > 0,
          start_s=res["start_s"], first_text_s=res["first_text_s"], total_s=res.get("total_s"),
          events=len(res["events"]), usage=res.get("done", {}).get("usage"), text=res["text"][:120])
    res = await api.stream_message("用一句话回答：1+1 等于几？不要调用工具。")
    check("S1", res.get("done", {}).get("state") == "SUCCEEDED", note="warm sandbox", start_s=res["start_s"],
          first_text_s=res["first_text_s"], total_s=res.get("total_s"))


async def s2(api: Api, ctx: dict) -> None:
    res = await api.stream_message("用 bash 工具执行 `python3 -c 'print(6*7)'`，然后只回复输出结果。")
    check("S2", "42" in tool_output(res, "bash"), tool="bash", total_s=res.get("total_s"), answer=res["text"][:80])
    res = await api.stream_message("用 webfetch 工具抓取 https://www.example.com ，告诉我页面标题。")
    check("S2", any(t["tool"] == "webfetch" and t["status"] == "completed" for t in res["tools"])
          and "Example Domain" in res["text"], tool="webfetch", total_s=res.get("total_s"), answer=res["text"][:80])
    res = await api.stream_message("使用 websearch 联网搜索工具搜索“阿里云 函数计算 云沙箱”，用一句话总结搜索结果。")
    names = [t["tool"] for t in res["tools"] if t["status"] == "completed"]
    check("S2", any("websearch" in (n or "") for n in names), tool="websearch MCP", tools=names,
          total_s=res.get("total_s"), answer=res["text"][:120])


async def s3(api: Api, ctx: dict) -> None:
    # 安全检查不经过模型（模型可能拒绝执行「探测密钥」类命令）：测试脚本直接用 SDK 在 agent 的沙箱里执行
    from e2b import AsyncSandbox

    info = (await api.req("GET", f"/v1/agents/{USER}")).json()
    provider_id = next(s["provider_id"] for s in info["sandboxes"] if s["state"] == "ACTIVE")
    sbx = await AsyncSandbox.connect(provider_id, timeout=3600)
    async def run(cmd: str) -> str:
        for attempt in range(6):
            try:
                return (await sbx.commands.run(cmd, timeout=60)).stdout
            except Exception as e:  # noqa: BLE001 - 本机代理 fake-ip 下 envd 新连接偶发失败
                last = repr(e)
                await asyncio.sleep(1 + attempt)
        return last

    # 取回 opencode 进程环境、envd 环境与配置文件，在本地检查真实 Key 是否出现（Key 不发进沙箱、不打印）
    dump = await run("P=$(pgrep -f 'opencode serve' | head -1); tr '\\0' '\\n' < /proc/$P/environ; env; "
                     "cat /home/user/workspace/opencode.json /home/user/.agent/egress.json /home/user/workspace/AGENTS.md")
    secrets = [v for v in (os.environ.get("POOL_AGENT_MODEL_API_KEY"), os.environ.get("BAILIAN_MCP_API_KEY")) if v]
    leaked = any(v in dump or v[-12:] in dump for v in secrets)
    check("S3", bool(secrets) and not leaked and "DEEPSEEK_API_KEY=injected-by-platform" in dump,
          checked_secrets=len(secrets), leaked=leaked, placeholder_present="injected-by-platform" in dump,
          dump_bytes=len(dump))
    out = await run(
        "echo META=$(curl -s -o /dev/null -w '%{http_code}' -m 5 http://100.100.100.200/latest/meta-data/ || echo blocked); "
        "echo PRIV=$(curl -s -o /dev/null -w '%{http_code}' -m 5 http://10.0.0.1/ || echo blocked); "
        "echo PUB=$(curl -s -o /dev/null -w '%{http_code}' -m 8 https://www.baidu.com); "
        "echo MODEL=$(curl -s -o /dev/null -w '%{http_code}' -m 10 https://api.deepseek.com/models)"
    )
    flat = out.replace(" ", "")
    check("S3", "META=000blocked" in flat and "PRIV=000blocked" in flat and "PUB=200" in flat and "MODEL=200" in flat,
          network=out.strip()[:300])
    endpoint = next((s["endpoint"] for s in info["sandboxes"] if s["state"] == "ACTIVE"), None)
    ip = os.environ.get("POOL_AGENT_INGRESS_IP") or None
    host = httpx.URL(endpoint).host
    async with httpx.AsyncClient(base_url=f"https://{ip or host}", trust_env=False, timeout=15, headers={"Host": host}) as c:
        code = None
        for _ in range(5):
            try:
                code = (await c.get("/global/health", extensions={"sni_hostname": host} if ip else {})).status_code
                break
            except httpx.HTTPError:
                await asyncio.sleep(1)
    check("S3", code == 403, direct_access_without_token=code, agent_info_has_token="access_token" in json.dumps(info))


async def s4(api: Api, ctx: dict) -> None:
    eg = (await api.req("GET", f"/v1/agents/{USER}/egress")).json()
    sb = eg["sandboxes"][0] if eg["sandboxes"] else {}
    check("S4", eg["desired"]["mode"] == "open" and sb.get("in_sync") and "sk-" not in json.dumps(eg),
          desired=eg["desired"]["mode"], in_sync=sb.get("in_sync"), platform_rules=(sb.get("platform") or {}).get("rules"))
    res = await api.stream_message("用 bash 工具执行 `cat /home/user/.agent/egress.json`，然后只回复其中 mode 字段的值。")
    check("S4", '"mode": "open"' in tool_output(res, "bash"), agent_reads_file=True)
    t0 = time.monotonic()
    r = await api.req("PUT", f"/v1/agents/{USER}/egress", json={"mode": "allowlist", "allow_out": ["www.example.com"]})
    body = r.json()
    check("S4", r.status_code == 200 and body["sandboxes"][0]["in_sync"], switch_to_allowlist_s=round(time.monotonic() - t0, 2))
    res = await api.stream_message(
        "用 bash 工具原样执行：`echo BAIDU=$(curl -s -o /dev/null -w '%{http_code}' -m 8 https://www.baidu.com || echo blocked); "
        "echo EXAMPLE=$(curl -s -o /dev/null -w '%{http_code}' -m 8 https://www.example.com)`，然后只回复“完成”。"
    )
    out = tool_output(res, "bash")
    check("S4", "BAIDU=000blocked" in out.replace(" ", "") and "EXAMPLE=200" in out, allowlist_effect=out.strip()[:200])
    r = await api.req("PUT", f"/v1/agents/{USER}/egress", json=None)
    res = await api.stream_message(
        "用 bash 工具原样执行：`echo BAIDU=$(curl -s -o /dev/null -w '%{http_code}' -m 8 https://www.baidu.com)`，然后只回复“完成”。"
    )
    check("S4", "BAIDU=200" in tool_output(res, "bash") and r.json()["desired"]["mode"] == "open", restored=True)


async def s5(api: Api, ctx: dict) -> None:
    first = await api.stream_message("请记住这个暗号：蓝色大象。只回复“记住了”，不要调用工具。")
    sid = first.get("start", {}).get("session_id")
    second = await api.stream_message("我刚才告诉你的暗号是什么？只回复暗号本身。", session_id=sid)
    check("S5", "蓝色大象" in second["text"], session_id=sid, answer=second["text"][:60])


async def s6(api: Api, ctx: dict, other: Api) -> None:
    res = await api.stream_message("用 bash 工具执行 `sleep 15 && echo slept`，然后只回复命令输出。", stop_after_start=True)
    task_id = res["start"]["task_id"]
    log(f"S6 disconnected after start, re-attaching task {task_id} on {other.base}")
    t0 = time.monotonic()
    got = {"events": []}
    async with other.http.stream("GET", f"/v1/agents/{USER}/tasks/{task_id}/stream") as r:
        kind = None
        async for line in r.aiter_lines():
            if line.startswith("event:"):
                kind = line[6:].strip()
                got["events"].append(kind)
            elif line.startswith("data:") and kind == "done":
                got["done"] = json.loads(line[5:].strip())
    check("S6", got.get("done", {}).get("state") == "SUCCEEDED" and "slept" in (got["done"].get("result") or ""),
          reattach_replica=other.base, reattach_wait_s=round(time.monotonic() - t0, 1), events=got["events"][:8])


async def s7(api: Api, ctx: dict) -> None:
    holder = {}

    async def on_start(p):
        holder.update(p)

    task = asyncio.create_task(api.stream_message("用 bash 工具执行 `sleep 120`。", on_start=on_start))
    for _ in range(120):
        if holder:
            break
        await asyncio.sleep(0.5)
    await asyncio.sleep(3)
    r = await api.req("POST", f"/v1/agents/{USER}/tasks/{holder['task_id']}/abort")
    res = await asyncio.wait_for(task, 120)
    final = await api.wait_task(holder["task_id"], timeout=60)
    check("S7", r.status_code == 200 and final["state"] == "ABORTED", stream_done=res.get("done", {}).get("state"),
          final=final["state"])


async def s8(api: Api, ctx: dict) -> None:
    r = await api.req("POST", f"/v1/agents/{USER}/schedules",
                      json={"name": "e2e-定时", "every_s": 60, "prompt": "用 bash 工具执行 `date '+%F %T'`，只回复命令输出。"})
    sched = r.json()
    run = (await api.req("POST", f"/v1/agents/{USER}/schedules/{sched['id']}/run")).json()
    first = await api.wait_task(run["task_id"], timeout=180)
    check("S8", first["state"] == "SUCCEEDED" and first["source"] == "schedule", manual_run=first["result"][:60] if first["result"] else None)
    log("S8 waiting for the automatic trigger (every 60s)...")
    deadline = time.monotonic() + 150
    auto = None
    while time.monotonic() < deadline:
        tasks = (await api.req("GET", f"/v1/agents/{USER}/tasks", params={"schedule_id": sched["id"]})).json()
        finished = [t for t in tasks if t["task_id"] != run["task_id"] and t["state"] != "RUNNING"]
        if finished:
            auto = finished[0]
            break
        await asyncio.sleep(5)
    check("S8", auto is not None and auto["state"] == "SUCCEEDED", auto_run=(auto or {}).get("result"),
          fired_after_s=round(auto["created_at"] - sched["created_at"], 1) if auto else None)
    await api.req("DELETE", f"/v1/agents/{USER}/schedules/{sched['id']}")


async def s9(api: Api, ctx: dict) -> None:
    await api.req("PATCH", f"/v1/agents/{USER}/settings", json={"idle_destroy_after_s": 60})
    t0 = time.monotonic()
    gone = False
    while time.monotonic() - t0 < 240:
        info = (await api.req("GET", f"/v1/agents/{USER}")).json()
        if not info["sandboxes"]:
            gone = True
            break
        await asyncio.sleep(5)
    check("S9", gone, destroyed_after_s=round(time.monotonic() - t0, 1))
    res = await api.stream_message("用一句话回答：今天适合写代码吗？不要调用工具。")
    check("S9", res.get("done", {}).get("state") == "SUCCEEDED", recreated_start_s=res["start_s"], total_s=res.get("total_s"))
    await api.req("PATCH", f"/v1/agents/{USER}/settings", json={"idle_destroy_after_s": 0})


async def s10(api: Api, ctx: dict, other: Api, pids: dict) -> None:
    holder = {}

    async def on_start(p):
        holder.update(p)

    port = int(api.base.rsplit(":", 1)[1])
    stream = asyncio.create_task(api.stream_message("用 bash 工具执行 `sleep 20 && echo survived`，然后只回复命令输出。",
                                                    on_start=on_start))
    for _ in range(120):
        if holder:
            break
        await asyncio.sleep(0.5)
    await asyncio.sleep(2)
    os.kill(pids[port], signal.SIGKILL)
    log(f"S10 killed replica :{port} (pid {pids[port]}) holding task {holder['task_id']}")
    await asyncio.gather(stream, return_exceptions=True)
    t0 = time.monotonic()
    final = await other.wait_task(holder["task_id"], timeout=240)
    check("S10", final["state"] == "SUCCEEDED" and "survived" in (final["result"] or ""),
          finished_by_other_replica_after_s=round(time.monotonic() - t0, 1), result=(final["result"] or "")[:60])


async def s11(api: Api, admin: Optional[Api]) -> None:
    r = await api.req("DELETE", f"/v1/agents/{USER}/sandbox")
    check("S11", r.status_code == 200, reset=r.json())
    if admin is not None:
        await asyncio.sleep(3)
        agents = (await admin.req("GET", "/v1/admin/agents")).json()
        mine = [a for a in agents if a["user_id"] == USER]
        check("S11", mine and not mine[0]["sandboxes"], admin_view_sandboxes=len(mine[0]["sandboxes"]) if mine else None,
              token_hidden="access_token" not in json.dumps(agents))


def read_pids() -> dict:
    path = ROOT / ".data" / "cluster.pids"
    out = {}
    if path.exists():
        for line in path.read_text().split("\n"):
            if line.strip():
                port, pid = line.split()
                out[int(port)] = int(pid)
    return out


async def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--only", help="逗号分隔的场景编号，如 S1,S2")
    ap.add_argument("--replicas", default="8001,8002")
    args = ap.parse_args()
    only = set((args.only or "").upper().split(",")) - {""}
    ports = [int(p) for p in args.replicas.split(",")]
    key = os.environ.get("SANDBOX_POOL_API_KEY")
    admin_key = os.environ.get("SANDBOX_POOL_ADMIN_KEY")
    apis = [Api(f"http://127.0.0.1:{p}", key) for p in ports]
    admin = Api(f"http://127.0.0.1:{ports[-1]}", admin_key) if admin_key else None
    a, b = apis[0], apis[-1]
    ctx: dict = {}
    steps = [
        ("S1", lambda: s1(a, ctx)), ("S2", lambda: s2(a, ctx)), ("S3", lambda: s3(a, ctx)), ("S4", lambda: s4(b, ctx)),
        ("S5", lambda: s5(b, ctx)), ("S6", lambda: s6(a, ctx, b)), ("S7", lambda: s7(b, ctx)), ("S8", lambda: s8(a, ctx)),
        ("S9", lambda: s9(b, ctx)), ("S10", lambda: s10(a, ctx, b, read_pids())), ("S11", lambda: s11(b, admin)),
    ]
    try:
        for name, fn in steps:
            if only and name not in only:
                continue
            log(f"===== {name} =====")
            t0 = time.monotonic()
            try:
                await fn()
            except Exception as e:  # noqa: BLE001 - 记录后继续下一个场景
                check(name, False, exception=f"{type(e).__name__}: {e}"[:500])
            REPORT["scenarios"].setdefault(name, {"checks": []})["seconds"] = round(time.monotonic() - t0, 1)
    finally:
        REPORT["finished_at"] = time.strftime("%Y-%m-%d %H:%M:%S")
        if admin is not None:
            try:
                REPORT["agent_stats"] = (await admin.req("GET", "/v1/admin/agents/stats")).json()
            except Exception:  # noqa: BLE001
                pass
        out = ROOT / ".data" / "e2e-agent-report.json"
        out.write_text(json.dumps(REPORT, ensure_ascii=False, indent=2), encoding="utf-8")
        log(f"report: {out}")
        for api in apis + ([admin] if admin else []):
            await api.http.aclose()
    failed = [n for n, s in REPORT["scenarios"].items() if not all(c["ok"] for c in s["checks"])]
    log(f"failed scenarios: {failed or 'none'}")
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
