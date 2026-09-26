"""构建 opencode agent 专用的第二代云沙箱模板（OpenAPI CreateTemplate，generation=2）。

模板内容（基础镜像默认为官方 code-interpreter-v1，自带 git / python3 / pip / node / npm / curl）：
- 从 npm 国内镜像安装固定版本的 opencode 到 /opt/opencode/bin/opencode；
- /opt/opencode-agent/run.sh：以 user 身份运行的守护循环，opencode serve 退出后 1 秒拉起；
- pip / npm 默认使用国内镜像；工作目录 /home/user/workspace。
启动命令在构建期执行，就绪命令探测 /global/health；平台在服务就绪后打快照，基于模板创建的沙箱 opencode 已在运行。

凭据只从环境变量读取：ALIBABA_CLOUD_ACCESS_KEY_ID / ALIBABA_CLOUD_ACCESS_KEY_SECRET / FCSANDBOX_REGION_ID / FCSANDBOX_TEAM_ID。

    python scripts/build_opencode_template.py                  # 构建并等待就绪，输出模板 ID
    python scripts/build_opencode_template.py verify <模板ID>   # 用模板建一个沙箱，确认 opencode 已在运行，然后销毁
    python scripts/build_opencode_template.py delete <模板ID>   # 删除模板
"""

import argparse
import asyncio
import base64
import os
import sys
import time

OPENCODE_VERSION = "1.18.32"
PORT = 4096
BUILD_TIMEOUT_S = 900

# 构建期以后台进程执行；exec 到守护循环，进程常驻并被打进快照
BOOTSTRAP = r"""#!/bin/bash
set -euo pipefail
V="__VERSION__"
SUDO=""
[ "$(id -u)" != "0" ] && SUDO="sudo -n"
if [ ! -x /opt/opencode/bin/opencode ]; then
  $SUDO mkdir -p /opt/opencode/bin
  cd /tmp
  for i in 1 2 3 4 5; do
    curl -fsSL -m 300 "https://registry.npmmirror.com/opencode-linux-x64/-/opencode-linux-x64-${V}.tgz" -o oc.tgz && break
    sleep 3
  done
  tar xzf oc.tgz
  $SUDO mv package/bin/opencode /opt/opencode/bin/opencode
  $SUDO chmod 755 /opt/opencode/bin/opencode
  rm -rf package oc.tgz
fi
$SUDO ln -sf /opt/opencode/bin/opencode /usr/local/bin/opencode
$SUDO mkdir -p /opt/opencode-agent
$SUDO tee /opt/opencode-agent/run.sh > /dev/null <<'RUN'
#!/bin/bash
# opencode 守护循环：opencode serve 退出后 1 秒拉起（由模板启动命令以 user 身份运行，打进快照）
export HOME=/home/user USER=user
export PATH=/usr/local/bin:/usr/bin:/bin:${PATH:-}
export OPENCODE_DISABLE_AUTOUPDATE=1 OPENCODE_DISABLE_MODELS_FETCH=1 OPENCODE_DISABLE_LSP_DOWNLOAD=1
# 模型 Key 由平台在出网时注入，这里只是占位符
export DEEPSEEK_API_KEY=injected-by-platform
cd /home/user
while true; do
  /opt/opencode/bin/opencode serve --hostname 0.0.0.0 --port __PORT__ >> /home/user/.agent/opencode.log 2>&1 || true
  echo "$(date '+%F %T') opencode exited" >> /home/user/.agent/opencode.log
  sleep 1
done
RUN
$SUDO chmod 755 /opt/opencode-agent/run.sh
mkdir -p /home/user/workspace /home/user/.agent /home/user/.config/pip 2>/dev/null || $SUDO mkdir -p /home/user/workspace /home/user/.agent /home/user/.config/pip
$SUDO tee /home/user/.config/pip/pip.conf > /dev/null <<'PIP'
[global]
index-url = https://mirrors.aliyun.com/pypi/simple/
trusted-host = mirrors.aliyun.com
PIP
echo "registry=https://registry.npmmirror.com/" | $SUDO tee /home/user/.npmrc > /dev/null
$SUDO chown -R user:user /home/user
if [ "$(id -u)" = "0" ]; then
  exec runuser -u user -- /opt/opencode-agent/run.sh
else
  exec /opt/opencode-agent/run.sh
fi
"""
READY = f"curl -sf -m 2 http://127.0.0.1:{PORT}/global/health > /dev/null"


def start_command(version: str) -> str:
    script = BOOTSTRAP.replace("__VERSION__", version).replace("__PORT__", str(PORT))
    encoded = base64.b64encode(script.encode()).decode()
    return f"bash -c 'echo {encoded} | base64 -d > /tmp/opencode-bootstrap.sh && exec bash /tmp/opencode-bootstrap.sh'"


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise SystemExit(f"缺少环境变量: {name}")
    return value


def new_client():
    from alibabacloud_fcsandbox20260509.client import Client
    from alibabacloud_tea_openapi import models as open_api_models

    config = open_api_models.Config(
        access_key_id=require_env("ALIBABA_CLOUD_ACCESS_KEY_ID"),
        access_key_secret=require_env("ALIBABA_CLOUD_ACCESS_KEY_SECRET"),
        security_token=os.environ.get("ALIBABA_CLOUD_SECURITY_TOKEN") or None,
        region_id=require_env("FCSANDBOX_REGION_ID"),
    )
    if os.environ.get("FCSANDBOX_ENDPOINT"):
        config.endpoint = os.environ["FCSANDBOX_ENDPOINT"]
    return Client(config)


def create(args: argparse.Namespace) -> int:
    from alibabacloud_fcsandbox20260509 import models

    client = new_client()
    team_id = require_env("FCSANDBOX_TEAM_ID")
    region = require_env("FCSANDBOX_REGION_ID")
    image = args.image or f"fc-e2b-registry.{region}.cr.aliyuncs.com/runtime/code-interpreter-v1:v0.0.52"
    name = args.name or f"opencode-agent-{int(time.time())}"
    print(f"==> 创建第二代模板 name={name} opencode={args.opencode_version}")
    print(f"    image={image} cpu={args.cpu} memory={args.memory}MB disk={args.disk}MB")
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
                        image=image,
                        generation=2,
                        start_command=start_command(args.opencode_version),
                        ready_command=READY,
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
    print(f"模板就绪：POOL_AGENT_TEMPLATE={template_id}")
    return 0


async def verify(template_id: str) -> int:
    """用模板建一个沙箱：opencode 应已在运行（快照），检查版本、进程用户、环境，然后销毁。"""
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
        for i in range(10):
            try:
                r = await sbx.commands.run(
                    "ps -eo user,rss,cmd | grep -E '[o]pencode serve|[r]un.sh'; cat ~/.config/pip/pip.conf ~/.npmrc; "
                    "ls -la /home/user/workspace; opencode --version", timeout=30)
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
    parser = argparse.ArgumentParser(description="构建 / 验证 / 删除 opencode agent 模板")
    sub = parser.add_subparsers(dest="cmd")
    parser.add_argument("--name")
    parser.add_argument("--image", help="基础镜像，默认当前地域官方 code-interpreter-v1:v0.0.52")
    parser.add_argument("--opencode-version", default=OPENCODE_VERSION)
    parser.add_argument("--cpu", type=float, default=2)
    parser.add_argument("--memory", type=int, default=4096, help="内存 MB（实测 opencode 常驻约 850 MB）")
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
