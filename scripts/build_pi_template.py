"""构建 pi agent 专用的第二代云沙箱模板（OpenAPI CreateTemplate，generation=2）。

模板内容（基础镜像默认为官方 code-interpreter-v1，自带 git / python3 / node / curl，已配置平台 CA）：
- Node：镜像自带的版本低于 22.19（pi 的要求）时，从 npmmirror 的 Node 二进制镜像装到 /opt/node；
- 从 npm 国内镜像安装锁定版本的 @earendil-works/pi-coding-agent 与 pi-mcp-adapter 到 /opt/pi-agent；
- /opt/pi-agent/pi-bridge.mjs（sandbox_pool/agent/pi_bridge/，原文嵌入启动脚本）与守护循环 run.sh，以 user 运行；
- ~/.pi/agent/settings.json：provider 传输固定 SSE（平台出网注入不支持 WebSocket）；pip / npm 用国内镜像。
启动命令在构建期执行，就绪命令探测 /global/health；平台在服务就绪后打快照，基于模板创建的沙箱桥接进程已在运行。
模型、思考级别、MCP 由网关在装配时写入 ~/.agent/pi.json 与 ~/.pi/agent/mcp.json（sandbox_pool/agent/engines/pi.py）。

凭据只从环境变量读取：ALIBABA_CLOUD_ACCESS_KEY_ID / ALIBABA_CLOUD_ACCESS_KEY_SECRET / FCSANDBOX_REGION_ID / FCSANDBOX_TEAM_ID。

    python scripts/build_pi_template.py                  # 构建并等待就绪，输出模板 ID
    python scripts/build_pi_template.py verify <模板ID>   # 用模板建一个沙箱，确认桥接进程已在运行，然后销毁
    python scripts/build_pi_template.py delete <模板ID>   # 删除模板
"""

import argparse
import asyncio
import base64
import gzip
import os
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(Path(__file__).resolve().parent))

from build_opencode_template import new_client, require_env  # noqa: E402

PI_VERSION = "0.87.1"
MCP_ADAPTER_VERSION = "2.37.0"
NODE_VERSION = "24.19.0"
PORT = 4096
BUILD_TIMEOUT_S = 900
BRIDGE = ROOT / "sandbox_pool" / "agent" / "pi_bridge" / "pi-bridge.mjs"

# 构建期以后台进程执行；最后 exec 到守护循环，进程常驻并被打进快照。__RUN__=0 时装好就退出（PoC 用）
BOOTSTRAP = r"""#!/bin/bash
set -euo pipefail
PI_VERSION="__PI_VERSION__"
MCP_VERSION="__MCP_VERSION__"
NODE_VERSION="__NODE_VERSION__"
REG=https://registry.npmmirror.com
SUDO=""
[ "$(id -u)" != "0" ] && SUDO="sudo -n"
# pi 要求 Node >= 22.19
node_ok() {
  command -v node > /dev/null 2>&1 || return 1
  node -e 'const [a,b]=process.versions.node.split(".").map(Number);process.exit(a>22||(a===22&&b>=19)?0:1)'
}
if node_ok; then
  NODE_DIR=$(dirname "$(command -v node)")
else
  if [ ! -x /opt/node/bin/node ]; then
    cd /tmp
    # registry.npmmirror.com/-/binary 会 302 到 CDN，沙箱里经这一跳实测返回 503；直接用 CDN 地址，nodejs.org 兜底
    NODE_TGZ="node-v${NODE_VERSION}-linux-x64.tar.xz"
    for i in 1 2 3 4 5; do
      curl -fsSL -m 300 "https://cdn.npmmirror.com/binaries/node/v${NODE_VERSION}/${NODE_TGZ}" -o node.tar.xz && break
      curl -fsSL -m 300 "https://nodejs.org/dist/v${NODE_VERSION}/${NODE_TGZ}" -o node.tar.xz && break
      sleep 3
    done
    $SUDO mkdir -p /opt/node
    $SUDO tar -xJf node.tar.xz -C /opt/node --strip-components=1
    rm -f node.tar.xz
  fi
  NODE_DIR=/opt/node/bin
fi
# 保留 sbin：最后切换用户要用 /usr/sbin/runuser
export PATH="$NODE_DIR:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"
# pi 与 MCP 适配器：锁版本、国内镜像、不跑安装脚本（pi 官方建议 --ignore-scripts）
$SUDO mkdir -p /opt/pi-agent
$SUDO chown -R "$(id -u):$(id -g)" /opt/pi-agent
cd /opt/pi-agent
[ -f package.json ] || echo '{"name":"pi-agent","private":true}' > package.json
for i in 1 2 3; do
  npm install --no-audit --no-fund --ignore-scripts --registry="$REG" \
    "@earendil-works/pi-coding-agent@${PI_VERSION}" "pi-mcp-adapter@${MCP_VERSION}" && break
  sleep 3
done
test -f /opt/pi-agent/node_modules/@earendil-works/pi-coding-agent/dist/bundle/cli.js
cat > /opt/pi-agent/pi-bridge.mjs <<'PI_BRIDGE_EOF'
__BRIDGE__
PI_BRIDGE_EOF
cat > /opt/pi-agent/run.sh <<RUN
#!/bin/bash
# pi 桥接进程守护循环：退出后 1 秒拉起（由模板启动命令以 user 身份运行，打进快照）
export HOME=/home/user USER=user
export PATH=$NODE_DIR:/usr/local/bin:/usr/bin:/bin
export PI_CMD='["$NODE_DIR/node","/opt/pi-agent/node_modules/@earendil-works/pi-coding-agent/dist/bundle/cli.js"]'
export PI_VERSION=$PI_VERSION PI_MCP_EXTENSION=/opt/pi-agent/node_modules/pi-mcp-adapter PI_BRIDGE_PORT=__PORT__
export PI_OFFLINE=1 PI_SKIP_VERSION_CHECK=1 PI_TELEMETRY=0
# 平台出网注入会做 TLS 检查：Node 要信任平台 CA（官方镜像的 envd 环境带这个变量，这里显式设置，不依赖继承）
if [ -f /usr/local/share/ca-certificates/e2b-ca.crt ]; then export NODE_EXTRA_CA_CERTS=/usr/local/share/ca-certificates/e2b-ca.crt; fi
# 模型 Key 由平台在出网时注入，这里只是占位符
export DEEPSEEK_API_KEY=injected-by-platform
cd /home/user
while true; do
  $NODE_DIR/node /opt/pi-agent/pi-bridge.mjs >> /home/user/.agent/pi-bridge.log 2>&1 || true
  echo "\$(date '+%F %T') pi-bridge exited" >> /home/user/.agent/pi-bridge.log
  sleep 1
done
RUN
chmod 755 /opt/pi-agent/run.sh
$SUDO mkdir -p /home/user/workspace /home/user/.agent/pi-sessions /home/user/.pi/agent /home/user/.config/pip
$SUDO tee /home/user/.config/pip/pip.conf > /dev/null <<'PIP'
[global]
index-url = https://mirrors.aliyun.com/pypi/simple/
trusted-host = mirrors.aliyun.com
PIP
echo "registry=https://registry.npmmirror.com/" | $SUDO tee /home/user/.npmrc > /dev/null
# pi 全局设置：provider 传输固定 SSE（平台出网注入不支持 WebSocket / HTTP Upgrade）；不加载项目级资源
$SUDO tee /home/user/.pi/agent/settings.json > /dev/null <<'SET'
{"transport": "sse", "defaultProjectTrust": "never", "quietStartup": true}
SET
$SUDO chown -R user:user /home/user
if [ "__RUN__" != "1" ]; then
  exit 0
fi
if [ "$(id -u)" = "0" ]; then
  exec runuser -u user -- /opt/pi-agent/run.sh
else
  exec /opt/pi-agent/run.sh
fi
"""
READY = f"curl -sf -m 2 http://127.0.0.1:{PORT}/global/health > /dev/null"
# 平台限制：启动命令最长 16KiB（实测 CreateTemplate 返回 "startCommand is invalid: exceeds 16KiB"）
START_COMMAND_MAX = 16 * 1024


def bootstrap_script(*, run: bool = True, pi_version: str = PI_VERSION, mcp_version: str = MCP_ADAPTER_VERSION,
                     node_version: str = NODE_VERSION) -> str:
    bridge = BRIDGE.read_text().rstrip("\n")
    assert "\nPI_BRIDGE_EOF\n" not in f"\n{bridge}\n", "bridge source must not contain the heredoc delimiter"
    return (
        BOOTSTRAP.replace("__PI_VERSION__", pi_version)
        .replace("__MCP_VERSION__", mcp_version)
        .replace("__NODE_VERSION__", node_version)
        .replace("__PORT__", str(PORT))
        .replace("__BRIDGE__", bridge)
        .replace("__RUN__", "1" if run else "0")
    )


def start_command(**kw) -> str:
    """启动脚本整体 gzip + base64（桥接脚本原文在里面，只编码一层），受平台 16KiB 上限约束。"""
    encoded = base64.b64encode(gzip.compress(bootstrap_script(**kw).encode(), mtime=0)).decode()
    cmd = f"bash -c 'echo {encoded} | base64 -d | gunzip > /tmp/pi-bootstrap.sh && exec bash /tmp/pi-bootstrap.sh'"
    if len(cmd) > START_COMMAND_MAX:
        raise SystemExit(f"start command is {len(cmd)} bytes, exceeds the platform limit {START_COMMAND_MAX}")
    return cmd


def create(args: argparse.Namespace) -> int:
    from alibabacloud_fcsandbox20260509 import models

    client = new_client()
    team_id = require_env("FCSANDBOX_TEAM_ID")
    region = require_env("FCSANDBOX_REGION_ID")
    image = args.image or f"fc-e2b-registry.{region}.cr.aliyuncs.com/runtime/code-interpreter-v1:v0.0.52"
    name = args.name or f"pi-agent-{int(time.time())}"
    cmd = start_command(pi_version=args.pi_version, mcp_version=args.mcp_version)
    print(f"==> 创建第二代模板 name={name} pi={args.pi_version} pi-mcp-adapter={args.mcp_version}")
    print(f"    image={image} cpu={args.cpu} memory={args.memory}MB disk={args.disk}MB start_command={len(cmd)} bytes")
    resp = client.create_template(
        models.CreateTemplateRequest(
            body=models.CreateTemplateInput(
                name=name,
                team_id=team_id,
                runtime_config=models.CreateTemplateRuntimeConfig(
                    cpu=args.cpu,
                    memory_size=args.memory,
                    disk_size=args.disk,
                    sandbox_config=models.CreateTemplateSandboxConfig(
                        image=image, generation=2, start_command=cmd, ready_command=READY,
                    ),
                ),
            )
        )
    )
    template_id = resp.body.template_id
    print(f"    template_id={template_id} request_id={resp.body.request_id}")
    start = time.time()
    while True:
        got = client.get_template(template_id, models.GetTemplateRequest(team_id=team_id))
        state = got.body.status.state
        print(f"    [{time.time() - start:5.0f}s] state={state}")
        if state in ("ready", "error"):
            break
        if time.time() - start > BUILD_TIMEOUT_S:
            print("构建超时")
            return 1
        time.sleep(10)
    if state == "error":
        reason = got.body.status.reason
        print(f"构建失败: {reason.message if reason else got.body.status}")
        return 1
    print(f"模板就绪：POOL_AGENT_PI_TEMPLATE={template_id}")
    return 0


async def verify(template_id: str) -> int:
    """用模板建一个沙箱：桥接进程应已在运行（快照），检查健康、版本、进程用户与内存，然后销毁。"""
    import httpx
    from e2b import AsyncSandbox

    t0 = time.monotonic()
    sbx = await AsyncSandbox.create(template=template_id, timeout=300, metadata={"pool": "template-verify"},
                                    secure=True, network={"allow_public_traffic": False}, request_timeout=60)
    print(f"created {sbx.sandbox_id} in {time.monotonic() - t0:.2f}s")
    try:
        host = sbx.get_host(PORT)
        ip = os.environ.get("POOL_AGENT_INGRESS_IP") or None
        async with httpx.AsyncClient(base_url=f"https://{ip or host}", trust_env=False, timeout=15,
                                     headers={"Host": host, "e2b-traffic-access-token": sbx.traffic_access_token}) as c:
            ext = {"sni_hostname": host} if ip else {}
            for i in range(20):
                try:
                    r = await c.get("/global/health", extensions=ext)
                    print(f"health after {time.monotonic() - t0:.2f}s: {r.status_code} {r.text}")
                    break
                except httpx.HTTPError as e:
                    print(f"health attempt {i}: {e!r}")
                    await asyncio.sleep(1)
            # 第一个会话要起 pi 进程：模板预热过时约 1s，没预热约 11s（实测）
            t1 = time.monotonic()
            for i in range(5):
                try:
                    r = await c.post("/session", json={"title": "verify"}, extensions=ext, timeout=60)
                    print(f"first session in {time.monotonic() - t1:.2f}s: {r.status_code} {r.text}")
                    break
                except httpx.ConnectError as e:
                    print(f"session attempt {i}: {e!r}")
                    await asyncio.sleep(1)
        for i in range(10):
            try:
                r = await sbx.commands.run(
                    "ps -u user -o pid,rss,args | cut -c1-150; free -m | head -2; "
                    "cat ~/.pi/agent/settings.json; ls -la /home/user/workspace /home/user/.agent; "
                    "$(grep -o '/[^ ]*/node' /opt/pi-agent/run.sh | head -1) --version; "
                    "grep '\"version\"' /opt/pi-agent/node_modules/@earendil-works/pi-coding-agent/package.json "
                    "/opt/pi-agent/node_modules/pi-mcp-adapter/package.json",
                    timeout=30,
                )
                print(r.stdout)
                break
            except Exception as e:  # noqa: BLE001 - 刚创建时 envd 可能短暂不可达
                print(f"envd attempt {i}: {e!r}")
                await asyncio.sleep(1)
    finally:
        print("killed:", await AsyncSandbox.kill(sbx.sandbox_id))
    return 0


def delete(template_id: str) -> int:
    from alibabacloud_fcsandbox20260509 import models

    new_client().delete_template(template_id, models.DeleteTemplateRequest(team_id=require_env("FCSANDBOX_TEAM_ID")))
    print(f"已删除模板 {template_id}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="构建 / 验证 / 删除 pi agent 模板")
    sub = parser.add_subparsers(dest="cmd")
    parser.add_argument("--name")
    parser.add_argument("--image", help="基础镜像，默认当前地域官方 code-interpreter-v1:v0.0.52")
    parser.add_argument("--pi-version", default=PI_VERSION)
    parser.add_argument("--mcp-version", default=MCP_ADAPTER_VERSION)
    parser.add_argument("--cpu", type=float, default=2)
    parser.add_argument("--memory", type=int, default=2048, help="内存 MB（pi 每个活动会话进程约 110 MB）")
    parser.add_argument("--disk", type=int, default=15360, help="磁盘 MB（含镜像大小）")
    v = sub.add_parser("verify")
    v.add_argument("template_id")
    d = sub.add_parser("delete")
    d.add_argument("template_id")
    args = parser.parse_args()
    if args.cmd == "verify":
        return asyncio.run(verify(args.template_id))
    if args.cmd == "delete":
        return delete(args.template_id)
    return create(args)


if __name__ == "__main__":
    sys.exit(main())
