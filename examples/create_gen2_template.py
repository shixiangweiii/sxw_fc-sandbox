"""通过阿里云 OpenAPI 创建第二代运行时（MicroVM）云沙箱模板。

第二代运行时默认支持暂停 / 恢复（sandbox.pause() / Sandbox.connect()）。
E2B SDK 的 Template.build 无法选择运行时代际，只能通过 OpenAPI CreateTemplate 创建，
鉴权使用阿里云 AccessKey（不是 E2B API Key）。

凭据只从环境变量读取，不要写入代码或提交到仓库：
    export ALIBABA_CLOUD_ACCESS_KEY_ID="<ak>"
    export ALIBABA_CLOUD_ACCESS_KEY_SECRET="<sk>"
    # export ALIBABA_CLOUD_SECURITY_TOKEN="<sts-token>"  # 使用 STS 临时凭证时
    export FCSANDBOX_REGION_ID="cn-hangzhou"           # 与 E2B_API_URL 的地域一致
    export FCSANDBOX_TEAM_ID="<team-id>"               # 云沙箱控制台 Team ID，需与 E2B API Key 同一 Team
    # export FCSANDBOX_ENDPOINT="fcsandbox.cn-hangzhou.aliyuncs.com"  # 可选

RAM 权限：fcsandbox:CreateTemplate / GetTemplate / ListTemplates / DeleteTemplate

运行：
    python examples/create_gen2_template.py                  # 创建并等待就绪
    python examples/create_gen2_template.py delete <模板ID>   # 删除模板
"""

import argparse
import os
import sys
import time

from alibabacloud_fcsandbox20260509 import models
from alibabacloud_fcsandbox20260509.client import Client as FCSandboxClient
from alibabacloud_tea_openapi import models as open_api_models

BUILD_TIMEOUT_SECONDS = 900


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"缺少环境变量: {name}")
    return value


def new_client() -> FCSandboxClient:
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
    return FCSandboxClient(config)


def create(args: argparse.Namespace) -> int:
    client = new_client()
    team_id = require_env("FCSANDBOX_TEAM_ID")
    region = require_env("FCSANDBOX_REGION_ID")
    image = args.image or (
        f"fc-e2b-registry.{region}.cr.aliyuncs.com/runtime/code-interpreter-v1:v0.0.52"
    )
    name = args.name or f"code-interpreter-gen2-{int(time.time())}"

    print(f"==> 创建第二代模板 name={name}")
    print(f"    image={image} cpu={args.cpu} memory={args.memory}MB disk={args.disk}MB")
    resp = client.create_template(
        models.CreateTemplateRequest(
            body=models.CreateTemplateInput(
                name=name,
                team_id=team_id,
                runtime_config=models.CreateTemplateRuntimeConfig(
                    cpu=args.cpu,
                    memory_size=args.memory,
                    # 第二代的磁盘大小包含镜像自身占用，实际可写空间 = 磁盘大小 - 镜像大小
                    disk_size=args.disk,
                    sandbox_config=models.CreateTemplateSandboxConfig(
                        image=image,
                        generation=2,
                    ),
                ),
            )
        )
    )
    template_id = resp.body.template_id
    print(f"    template_id={template_id} request_id={resp.body.request_id}")

    print("==> 等待构建完成")
    start = time.time()
    while True:
        got = client.get_template(template_id, models.GetTemplateRequest(team_id=team_id))
        state = got.body.status.state
        print(f"    [{time.time() - start:5.0f}s] state={state}")
        if state in ("ready", "error"):
            break
        if time.time() - start > BUILD_TIMEOUT_SECONDS:
            print("构建超时")
            return 1
        time.sleep(5)

    if state == "error":
        reason = got.body.status.reason
        print(f"构建失败: {reason.message if reason else got.body.status}")
        return 1

    print(f"模板就绪，可用于创建沙箱：SANDBOX_TEMPLATE={template_id}")
    return 0


def delete(args: argparse.Namespace) -> int:
    client = new_client()
    team_id = require_env("FCSANDBOX_TEAM_ID")
    client.delete_template(args.template_id, models.DeleteTemplateRequest(team_id=team_id))
    print(f"已删除模板 {args.template_id}")
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description="创建 / 删除第二代运行时云沙箱模板")
    sub = parser.add_subparsers(dest="cmd")
    parser.add_argument("--name", help="模板名，默认 code-interpreter-gen2-<时间戳>")
    parser.add_argument("--image", help="源镜像，默认当前地域的官方 code-interpreter-v1 镜像")
    parser.add_argument("--cpu", type=float, default=2)
    parser.add_argument("--memory", type=int, default=2048, help="内存 MB")
    parser.add_argument("--disk", type=int, default=15360, help="磁盘 MB（含镜像大小）")
    d = sub.add_parser("delete", help="删除模板")
    d.add_argument("template_id")
    args = parser.parse_args()
    return delete(args) if args.cmd == "delete" else create(args)


if __name__ == "__main__":
    sys.exit(main())
