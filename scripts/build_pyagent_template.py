"""构建「预装常用 Python 包」的第二代云沙箱模板（OpenAPI CreateTemplate，generation=2）。

── 它要回答的问题 ─────────────────────────────────────────────────────────

Atlas 走 per-call 借用（一次调用借一次沙箱、归还即销毁）时，装依赖和断网是缠在一起的：

    要装依赖 → 要开网 → 丢掉「副作用只有声明过的那几个文件」这条性质
    要干净   → 不开网 → pip install 根本跑不起来

OSS Volume 当缓存那条路被平台的 500 挡住了（疑似邀测未开通，见
scripts/probe_volume_cache.py 的记录）。剩下的办法是**把依赖烤进模板** ——
那是部署期的静态事实，和「本机装没装 zip」同性质，不污染 transcript，
而且与断网完全相容。

本脚本做模板，scripts/probe_template_deps.py 量代价。

── 做法照抄 build_pi_template.py ──────────────────────────────────────────

start_command 在**构建期**执行，ready_command 探测成功后平台打快照，
装好的东西随快照固化。与 pi 模板的唯一区别是结尾 exec 的不是守护进程，
而是代码解释器本身 —— 第二代运行时 PID 1 是 systemd、不会自动拉起它，
不 exec 的话 run_code 一直返回 500（create_gen2_template.py 踩过）。

运行：
    cd /Users/shixiangweii/run_proj/sxw_fc-sandbox
    set -a; . ./.env; set +a
    .venv/bin/python scripts/build_pyagent_template.py                 # 构建，输出模板 ID
    .venv/bin/python scripts/build_pyagent_template.py delete <模板ID>
"""

import argparse
import base64
import gzip
import os
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))

from build_opencode_template import require_env  # noqa: E402

BUILD_TIMEOUT_S = 1200
CI_PORT = 49999
# 镜像自带 numpy / pandas / matplotlib；这里补的是「办公与抓取类任务」常用、而镜像里没有的
PACKAGES = [
    "requests", "httpx",
    "beautifulsoup4", "lxml",
    "openpyxl", "python-docx", "pypdf",
    "pillow",
    "pyyaml", "tabulate", "python-dateutil",
]
# import 名与包名不同的几个，验证时用
IMPORT_NAMES = {
    "beautifulsoup4": "bs4", "python-docx": "docx", "pyyaml": "yaml",
    "pillow": "PIL", "python-dateutil": "dateutil",
}
START_COMMAND_MAX = 16 * 1024

BOOTSTRAP = r"""#!/bin/bash
set -euo pipefail
export PIP_INDEX_URL=https://mirrors.aliyun.com/pypi/simple/
export PIP_TRUSTED_HOST=mirrors.aliyun.com
export PIP_DISABLE_PIP_VERSION_CHECK=1
# Debian 13 带 PEP 668 保护，pip >= 23 才认 --break-system-packages；老版本不加这个参数
FLAGS="--no-cache-dir"
if python3 -m pip install --help 2>/dev/null | grep -q break-system-packages; then
  FLAGS="$FLAGS --break-system-packages"
fi
python3 -m pip install $FLAGS __PACKAGES__ > /tmp/pip-build.log 2>&1 || {
  echo "pip install failed:"; tail -40 /tmp/pip-build.log; exit 1;
}
python3 -c 'import __IMPORTS__' || { echo "import check failed"; exit 1; }
# 【定】这里原来有一行 `pip list > /opt/preinstalled-packages.txt || true`，已删。
# 实测那个文件在沙箱里**不存在** —— `|| true` 把失败吞掉了，于是脚本声称写了一份
# 清单而盘上没有。没有消费者的声明不如不写（宁可去 `pip list` 现查）。
# 【定】必须 exec 代码解释器：第二代 PID 1 是 systemd，不会自动拉起它，
# 不起的话基于本模板建出来的沙箱 run_code 一律 500（create_gen2_template.py 的记录）。
exec /.fce2b/sandbox-code-interpreter
"""
READY = f"curl -sf -m 2 http://127.0.0.1:{CI_PORT}/health > /dev/null"


def new_client():
    """与 create_gen2_template.py 一致：显式带代理（build_opencode_template 那份没带）。"""
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
    proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")
    if proxy:
        config.https_proxy = proxy
    return Client(config)


def import_list(packages: list[str]) -> str:
    return ", ".join(IMPORT_NAMES.get(p, p.replace("-", "_")) for p in packages)


def bootstrap_script(packages: list[str]) -> str:
    return (BOOTSTRAP
            .replace("__PACKAGES__", " ".join(packages))
            .replace("__IMPORTS__", import_list(packages)))


def start_command(packages: list[str]) -> str:
    """整体 gzip + base64，受平台 16KiB 上限约束（实测超了会报 startCommand is invalid）。"""
    enc = base64.b64encode(gzip.compress(bootstrap_script(packages).encode(), mtime=0)).decode()
    cmd = f"bash -c 'echo {enc} | base64 -d | gunzip > /tmp/py-bootstrap.sh && exec bash /tmp/py-bootstrap.sh'"
    if len(cmd) > START_COMMAND_MAX:
        raise SystemExit(f"start command is {len(cmd)} bytes, exceeds {START_COMMAND_MAX}")
    return cmd


def create(args: argparse.Namespace) -> int:
    from alibabacloud_fcsandbox20260509 import models

    client = new_client()
    team_id = require_env("FCSANDBOX_TEAM_ID")
    region = require_env("FCSANDBOX_REGION_ID")
    image = args.image or f"fc-e2b-registry.{region}.cr.aliyuncs.com/runtime/code-interpreter-v1:v0.0.52"
    name = args.name or f"py-agent-{int(time.time())}"
    cmd = start_command(PACKAGES)
    print(f"==> 创建第二代模板 name={name}")
    print(f"    image={image} cpu={args.cpu} memory={args.memory}MB disk={args.disk}MB")
    print(f"    预装 {len(PACKAGES)} 个包: {' '.join(PACKAGES)}")
    print(f"    start_command={len(cmd)} bytes（上限 {START_COMMAND_MAX}）")
    t0 = time.time()
    resp = client.create_template(models.CreateTemplateRequest(body=models.CreateTemplateInput(
        name=name, team_id=team_id,
        runtime_config=models.CreateTemplateRuntimeConfig(
            cpu=args.cpu, memory_size=args.memory, disk_size=args.disk,
            sandbox_config=models.CreateTemplateSandboxConfig(
                image=image, generation=2, start_command=cmd, ready_command=READY)))))
    template_id = resp.body.template_id
    print(f"    template_id={template_id} request_id={resp.body.request_id}")

    while True:
        got = client.get_template(template_id, models.GetTemplateRequest(team_id=team_id))
        state = got.body.status.state
        print(f"    [{time.time() - t0:5.0f}s] state={state}")
        if state in ("ready", "error"):
            break
        if time.time() - t0 > BUILD_TIMEOUT_S:
            print("构建超时"); return 1
        time.sleep(10)
    if state == "error":
        reason = got.body.status.reason
        print(f"构建失败: {reason.message if reason else got.body.status}")
        return 1
    print(f"\n模板就绪（构建耗时 {time.time() - t0:.0f}s，一次性）：")
    print(f"    PYAGENT_TEMPLATE={template_id}")
    return 0


def delete(template_id: str) -> int:
    from alibabacloud_fcsandbox20260509 import models
    new_client().delete_template(template_id, models.DeleteTemplateRequest(
        team_id=require_env("FCSANDBOX_TEAM_ID")))
    print(f"已删除模板 {template_id}")
    return 0


def main() -> int:
    p = argparse.ArgumentParser(description="构建 / 删除预装常用 Python 包的模板")
    sub = p.add_subparsers(dest="cmd")
    p.add_argument("--name")
    p.add_argument("--image")
    p.add_argument("--cpu", type=float, default=2)
    p.add_argument("--memory", type=int, default=2048)
    p.add_argument("--disk", type=int, default=15360, help="磁盘 MB（含镜像）")
    d = sub.add_parser("delete")
    d.add_argument("template_id")
    args = p.parse_args()
    if args.cmd == "delete":
        return delete(args.template_id)
    return create(args)


if __name__ == "__main__":
    raise SystemExit(main())
