"""验证研究模板的原生接口、真实模型、工具与会话，退出时回收本次实例。"""

import argparse
import asyncio
import json
import os
import secrets
import sys
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from pi_bridge_local_check import run_turn

from sandbox_pool.agent.engines.opencode_v2 import native_mcp_config
from sandbox_pool.agent.opencode import AgentHttpClient
from sandbox_pool.agent.policy import Injections, build_network, render_opencode_config
from sandbox_pool.egress import parse_policy
from sandbox_pool.provider.e2b_provider import E2BProvider

WORK = "/home/user/workspace"
MCP_URL = "https://dashscope.aliyuncs.com/api/v1/mcps/WebSearch/mcp"
MODEL = "deepseek-flash"


def check(name, ok):
    if not ok:
        raise RuntimeError(f"验证失败：{name}")
    print(f"PASS {name}", flush=True)


async def verify_pi(client):
    sid = await client.create_session("模型与百炼回归")
    for text, tool, marker in [
        ("调用 bash 运行 printf fc-test-42，然后只回复命令输出。", "bash", "fc-test-42"),
        ("上一轮的命令输出是什么？只回答原文。", None, "fc-test-42"),
        ("调用百炼搜索工具搜索阿里云函数计算云沙箱，返回一句话摘要。", "bailian_web_search", None),
    ]:
        result = await run_turn(client, sid, text, timeout=100)
        finished = [e.get("tool", "") for k, e in result["events"] if k == "tool" and e["status"] == "completed"]
        print("tools", finished, "duration", result["total_s"], flush=True)
        check(
            tool or "续聊",
            result["run_state"] == "settled"
            and not result["error"]
            and result["text"]
            and (not marker or marker in result["text"])
            and (not tool or any(tool in n for n in finished)),
        )
    result = await run_turn(client, sid, "请用 bash 执行 sleep 30，再回复。", abort_after=3, timeout=50)
    check("中止", result["run_state"] == "settled" and result["total_s"] < 28 and result["error"])


async def verify_oc(call):
    session = await call(
        "POST",
        "/api/session",
        {
            "title": "模型与百炼回归",
            "location": {"directory": WORK},
            "model": {"providerID": "deepseek", "id": MODEL},
        },
    )
    path = "/api/session/" + session.json()["data"]["id"]
    for prompt, tool, marker in [
        ("务必调用 shell 工具执行 printf fc-test-42，然后只回复命令输出。", "shell", "fc-test-42"),
        ("上一轮的命令输出是什么？只回答原文。", None, "fc-test-42"),
        (
            "调用名含 bailian_web_search 的 MCP 工具搜索阿里云函数计算云沙箱，返回一句话摘要。",
            "bailian_web_search",
            None,
        ),
    ]:
        admitted = (await call("POST", path + "/prompt", {"text": prompt})).json()["data"]
        async with asyncio.timeout(130):
            while True:
                await asyncio.sleep(0.7)
                items = (await call("GET", path + "/message")).json()["data"]
                batch = []
                for item in items:
                    if item["id"] == admitted["id"]:
                        break
                    batch.append(item)
                else:
                    continue
                if batch and batch[0]["type"] == "idle":
                    break
        parts = [p for m in batch if m["type"] == "assistant" for p in m.get("content", [])]
        answer = "".join(p.get("text", "") for p in parts if p["type"] == "text")
        tools = [p for p in parts if "tool" in p["type"]]
        print(
            "message types",
            [m["type"] for m in batch],
            "parts",
            [(p["type"], p.get("name"), p.get("tool")) for p in parts],
            flush=True,
        )
        check(
            tool or "续聊",
            batch[0]["outcome"] == "succeeded"
            and answer
            and (not marker or marker in answer)
            and (not tool or any(tool in p.get("name", "") and not p.get("error") for p in tools)),
        )
    await call("POST", path + "/prompt", {"text": "调用 shell 执行 sleep 40，并等待它结束。"})
    await asyncio.sleep(2)
    check("中止已接受", (await call("POST", path + "/interrupt")).json().get("interrupted"))
    async with asyncio.timeout(30):
        while (await call("GET", path)).json()["data"].get("outcome") != "interrupted":
            await asyncio.sleep(0.5)
    check("中止终态", True)


async def main(args):
    provider = E2BProvider()
    injected = Injections(
        {
            "api.deepseek.com": {"Authorization": "Bearer " + os.environ["FCSANDBOX_OPENCODE_MODEL_API_KEY"]},
            "dashscope.aliyuncs.com": {"Authorization": "Bearer " + os.environ["BAILIAN_MCP_API_KEY"]},
        }
    )
    app = None
    try:
        app = await provider.create_app(
            args.template,
            {"pool": "fc-upgrade-research", "engine": args.engine},
            420,
            port=4096,
            network=build_network(parse_policy({"mode": "open"}), injected),
        )
        print("instance", app.sandbox_id, flush=True)
        engine = args.engine
        model = "deepseek/" + MODEL
        pwd = secrets.token_urlsafe(32)
        mcp = {"websearch": {"type": "remote", "url": MCP_URL}}
        files = (
            {
                WORK + "/opencode.json": json.dumps(
                    native_mcp_config(render_opencode_config(model, mcp))
                ).encode(),
                "/home/user/.agent/server-password": pwd.encode(),
            }
            if engine == "opencode"
            else {
                "/home/user/.agent/pi.json": json.dumps(
                    {"model": MODEL, "provider": "deepseek", "mcp": True}
                ).encode(),
                "/home/user/.pi/agent/mcp.json": json.dumps(
                    {"mcpServers": {"websearch": {"url": MCP_URL, "exposure": "direct"}}}
                ).encode(),
            }
        )
        await provider.write_files(app.sandbox_id, files, sandbox_timeout_s=420)
        host = httpx.URL(app.endpoint).host
        ip = os.environ.get("POOL_AGENT_INGRESS_IP")
        ext = {"sni_hostname": host} if ip else {}
        async with httpx.AsyncClient(
            base_url=f"https://{ip or host}", headers={"Host": host}, trust_env=False, timeout=30
        ) as h:

            async def call(method, path, body=None):
                r = await h.request(
                    method,
                    path,
                    json=body,
                    extensions=ext,
                    headers={"e2b-traffic-access-token": app.access_token},
                    auth=("opencode", pwd) if engine == "opencode" else None,
                )
                r.raise_for_status()
                return r

            route = "/api/info" if engine == "opencode" else "/global/health"
            async with asyncio.timeout(90):
                while True:
                    try:
                        r = await call("GET", route)
                        if r.json().get("version" if engine == "opencode" else "pi_version") == (
                            "2.3.2" if engine == "opencode" else "1.6.0"
                        ):
                            break
                    except (httpx.HTTPError, ValueError):
                        pass
                    await asyncio.sleep(1)
            check("真实版本", True)
            denied = await h.get(route, extensions=ext)
            check("FC 入站认证", denied.status_code in (401, 403))
            if engine == "opencode":
                denied = await h.get(route, headers={"e2b-traffic-access-token": app.access_token}, extensions=ext)
                check("OpenCode 应用认证", denied.status_code == 401)
                await verify_oc(call)
            else:
                client = AgentHttpClient(app.endpoint, app.access_token, WORK, ingress_ip=ip)
                try:
                    await verify_pi(client)
                finally:
                    await client.close()
    finally:
        try:
            if app:
                await provider.kill(app.sandbox_id)
                print("released", app.sandbox_id, flush=True)
        finally:
            await provider.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("engine", choices=["opencode", "pi"])
    parser.add_argument("template")
    asyncio.run(main(parser.parse_args()))
