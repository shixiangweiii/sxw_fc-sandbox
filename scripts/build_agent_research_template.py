"""为 OpenCode 2 / Pi 1 构建独立的研究模板。输出模板 ID，保留现有模板。"""

import argparse
import base64
import gzip
import time
from pathlib import Path

from build_opencode_template import new_client, require_env

OPENCODE = "2.3.2"
PI = "1.6.0"
NODE = "24.19.0"
OC_SHA512 = "c8f1PimJ0dtKcF1sF7I+BSsySOH6HzmELqEfDTduzqBG2OzBH6g1sPwC/R5LhTf6kfG7FeLH5UFr9ZTTlcRdEw=="
ROOT = Path(__file__).resolve().parents[1]

COMMON = """#!/bin/bash
set -euo pipefail
install -d -o user -g user /home/user/workspace /home/user/.agent /home/user/.pi/agent
mkdir -p /opt/fc-agent
cd /opt/fc-agent
"""
OC_INSTALL = """url="@opencode/cli-linux-x64-baseline/-/cli-linux-x64-baseline-OCVERSION.tgz"
for registry in https://registry.npmmirror.com https://registry.npmjs.org; do
  if curl --retry 2 -fsSL --max-time 180 "$registry/$url" -o opencode.tgz &&
     test "$(openssl dgst -sha512 -binary opencode.tgz | openssl base64 -A)" = 'OCHASH'; then
    tar xzf opencode.tgz package/bin/opencode
    install -m755 package/bin/opencode /usr/local/bin/opencode
    rm -rf opencode.tgz package
    break
  fi
done
/usr/local/bin/opencode --version | grep -F 'OCVERSION'
"""
OC_RUN = """umask 077
while [ ! -s /home/user/.agent/server-password ]; do sleep 0.2; done
export OPENCODE_PASSWORD="$(cat /home/user/.agent/server-password)"
export OPENCODE_DISABLE_AUTOUPDATE=1 DEEPSEEK_API_KEY=injected-by-platform
while true; do opencode serve --hostname 0.0.0.0 --port 4096 || true; sleep 1; done
"""
PI_INSTALL = """tgz=node-vNODEVERSION-linux-x64.tar.xz
for base in https://cdn.npmmirror.com/binaries/node/vNODEVERSION https://nodejs.org/dist/vNODEVERSION; do
  if curl --retry 2 -fsSL --max-time 120 "$base/$tgz" -o "$tgz" &&
     curl -fsSL --max-time 30 "$base/SHASUMS256.txt" -o sums &&
     grep "  $tgz$" sums | sha256sum -c -; then break; fi
done
grep "  $tgz$" sums | sha256sum -c -
mkdir -p /opt/fc-agent/node
tar -xJf "$tgz" --strip-components=1 -C /opt/fc-agent/node
export PATH=/opt/fc-agent/node/bin:$PATH
for registry in https://registry.npmmirror.com https://registry.npmjs.org; do
  npm install --ignore-scripts --no-fund --no-audit --registry="$registry" @earendil-works/pi-coding-agent@PIVERSION && break
done
test "$(node -p 'require("./node_modules/@earendil-works/pi-coding-agent/package.json").version')" = PIVERSION
cat > pi-bridge.mjs <<'BRIDGE_EOF'
BRIDGESOURCE
BRIDGE_EOF
echo '{"transport":"sse","defaultProjectTrust":"never","quietStartup":true}' > /home/user/.pi/agent/settings.json
chown user:user /home/user/.pi/agent/settings.json
rm -f "$tgz" sums
"""
PI_RUN = """export PATH=/opt/fc-agent/node/bin:/opt/fc-agent/node_modules/.bin:$PATH
export PI_CMD='["/opt/fc-agent/node/bin/node","/opt/fc-agent/node_modules/@earendil-works/pi-coding-agent/dist/bundle/cli.js"]'
export PI_NATIVE_MCP=1 PI_VERSION=PIVERSION PI_OFFLINE=1 PI_SKIP_VERSION_CHECK=1 PI_TELEMETRY=0
export DEEPSEEK_API_KEY=injected-by-platform
while true; do node /opt/fc-agent/pi-bridge.mjs || true; sleep 1; done
"""


def commands(engine):
    if engine == "opencode":
        install = OC_INSTALL.replace("OCVERSION", OPENCODE).replace("OCHASH", OC_SHA512)
        run = OC_RUN
        ready = "test -f /opt/fc-agent/installed"
    else:
        bridge = (ROOT / "sandbox_pool/agent/pi_bridge/pi-bridge.mjs").read_text()
        install = PI_INSTALL.replace("NODEVERSION", NODE).replace("PIVERSION", PI).replace("BRIDGESOURCE", bridge)
        run = PI_RUN.replace("PIVERSION", PI)
        ready = 'curl -fsS -m 2 http://127.0.0.1:4096/global/health | grep -F \'"pi_version":"1.6.0"\''
    script = (
        COMMON
        + install
        + """cat > /opt/fc-agent/run.sh <<'RUN_EOF'
#!/bin/bash
export HOME=/home/user USER=user
export NODE_EXTRA_CA_CERTS=/usr/local/share/ca-certificates/e2b-ca.crt
cd /home/user/workspace
"""
        + run
        + """RUN_EOF
chmod 755 /opt/fc-agent/run.sh
touch /opt/fc-agent/installed
exec runuser -u user -- /opt/fc-agent/run.sh
"""
    )
    encoded = base64.b64encode(gzip.compress(script.encode(), mtime=0)).decode()
    start = f"bash -c 'echo {encoded} | base64 -d | gunzip >/tmp/setup-agent.sh && exec sudo -n bash /tmp/setup-agent.sh'"
    if len(start) > 16384:
        raise ValueError("启动命令超出二代模板 16 KiB 限制")
    return start, ready


def main():
    from alibabacloud_fcsandbox20260509 import models

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("engine", choices=["opencode", "pi"])
    args = parser.parse_args()
    start, ready = commands(args.engine)
    region, team = require_env("FCSANDBOX_REGION_ID"), require_env("FCSANDBOX_TEAM_ID")
    client = new_client()
    name = f"research-{args.engine}-{int(time.time())}"
    sandbox = models.CreateTemplateSandboxConfig(
        image=f"fc-e2b-registry.{region}.cr.aliyuncs.com/runtime/code-interpreter-v1:v0.0.52",
        generation=2,
        start_command=start,
        ready_command=ready,
    )
    req = models.CreateTemplateInput(
        name=name,
        team_id=team,
        runtime_config=models.CreateTemplateRuntimeConfig(
            cpu=2, memory_size=4096, disk_size=15360, sandbox_config=sandbox
        ),
    )
    result = client.create_template(models.CreateTemplateRequest(body=req))
    tid = result.body.template_id
    print(f"{args.engine} template={tid}", flush=True)
    deadline = time.monotonic() + 1200
    while time.monotonic() < deadline:
        status = client.get_template(tid, models.GetTemplateRequest(team_id=team)).body.status
        print(status.state, flush=True)
        if status.state == "ready":
            return
        if status.state == "error":
            raise RuntimeError(str(status.reason))
        time.sleep(10)
    raise TimeoutError(f"模板构建状态未确认：{tid}")


if __name__ == "__main__":
    main()
