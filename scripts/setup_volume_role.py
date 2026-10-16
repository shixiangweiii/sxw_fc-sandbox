"""给云沙箱挂载 OSS Volume 准备一个**最小权限**的 RAM 角色（会改账号级权限配置）。

背景：挂载 OSS Volume 时平台报
    400: OSS mount requires Function role
         (set by template executionRoleArn, CreateSandbox metadata fc.sandbox.auth.role,
          or Volume mountConfig.role)
而账号下 29 个 RAM 角色没有一个信任 FC：服务关联角色不能当执行角色传，
PAI 那几个 AccessingOSS 角色信任的是 PAI。所以必须新建一个。

【定】权限只给**一个 bucket**，不用 AliyunOSSFullAccess：
挂载需要的是对象读写，不是对整个 OSS 的管理权。桶级动作也只给只读的那几个
（不含 DeleteBucket / 改 ACL / 改生命周期）。

【定】`UpdateVolumeInput` 只有 status / team_id —— **挂载配置建好改不了**。
所以角色要在建 Volume 时就给，已存在的 Volume 只能删了重建
（删的是那个「指针」，OSS 桶里的对象不动）。

运行：
    cd /Users/shixiangweii/run_proj/sxw_fc-sandbox
    set -a; . ./.env; set +a
    .venv/bin/python scripts/setup_volume_role.py
"""

import json
import os
import time

from alibabacloud_fcsandbox20260509 import models as fcm
from alibabacloud_fcsandbox20260509.client import Client as FCClient
from alibabacloud_tea_openapi import models as om
from alibabacloud_tea_openapi.client import Client as OpenApiClient
from alibabacloud_tea_util import models as um

ROLE_NAME = "AtlasSandboxOSSVolumeRole"
POLICY_NAME = "AtlasSandboxOSSVolumeAccess"
VOLUME_NAME = "atlas-pipcache"
BUCKET_PATH = "/atlas-cache"

# 信任 FC：执行角色的标准委托方。沙箱是 FC 上的一种运行形态，
# 报错里说的 "Function role" 指的就是它。
TRUST_POLICY = {
    "Version": "1",
    "Statement": [
        {"Action": "sts:AssumeRole", "Effect": "Allow",
         "Principal": {"Service": ["fc.aliyuncs.com"]}}
    ],
}


def permission_policy(bucket: str, prefix: str) -> dict:
    """只给这一个桶：桶级只读元信息 ＋ 指定前缀下的对象读写。"""
    key_prefix = prefix.strip("/")
    return {
        "Version": "1",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["oss:GetBucketInfo", "oss:GetBucketLocation",
                           "oss:ListObjects", "oss:ListMultipartUploads"],
                "Resource": f"acs:oss:*:*:{bucket}",
            },
            {
                "Effect": "Allow",
                "Action": ["oss:GetObject", "oss:PutObject", "oss:DeleteObject",
                           "oss:GetObjectMeta", "oss:ListParts", "oss:AbortMultipartUpload"],
                "Resource": f"acs:oss:*:*:{bucket}/{key_prefix}/*",
            },
        ],
    }


def ram_client() -> OpenApiClient:
    """RAM 操作用主账号 AK/SK（.env 的「# 阿里云AK/SK」段）——建角色要 ram:* 权限，
    而 ALIBABA_CLOUD_* 那一对是给 fcsandbox 用的，不保证有。"""
    cfg = om.Config(
        access_key_id=os.environ.get("AccessKeyID") or os.environ["ALIBABA_CLOUD_ACCESS_KEY_ID"],
        access_key_secret=os.environ.get("AccessKeySecret") or os.environ["ALIBABA_CLOUD_ACCESS_KEY_SECRET"],
    )
    cfg.endpoint = "ram.aliyuncs.com"
    if os.environ.get("HTTPS_PROXY"):
        cfg.https_proxy = os.environ["HTTPS_PROXY"]
    return OpenApiClient(cfg)


def ram(client: OpenApiClient, action: str, query: dict) -> dict:
    params = om.Params(action=action, version="2015-05-01", protocol="HTTPS", method="POST",
                       auth_type="AK", style="RPC", pathname="/",
                       req_body_type="formData", body_type="json")
    return client.call_api(params, om.OpenApiRequest(query=query), um.RuntimeOptions())["body"]


def fc_client() -> FCClient:
    cfg = om.Config(
        access_key_id=os.environ["ALIBABA_CLOUD_ACCESS_KEY_ID"],
        access_key_secret=os.environ["ALIBABA_CLOUD_ACCESS_KEY_SECRET"],
        region_id=os.environ["FCSANDBOX_REGION_ID"],
    )
    if os.environ.get("HTTPS_PROXY"):
        cfg.https_proxy = os.environ["HTTPS_PROXY"]
    return FCClient(cfg)


def main() -> int:
    bucket = os.environ["OSS_BUCKET"]
    team = os.environ["FCSANDBOX_TEAM_ID"]
    c = ram_client()

    # ── 1. 权限策略 ──
    doc = json.dumps(permission_policy(bucket, BUCKET_PATH), ensure_ascii=False)
    try:
        ram(c, "CreatePolicy", {"PolicyName": POLICY_NAME, "PolicyDocument": doc,
                                "Description": f"Atlas 云沙箱挂载 OSS Volume（仅 {bucket}{BUCKET_PATH}）"})
        print(f"[OK  ] 建策略 {POLICY_NAME}")
    except Exception as e:  # noqa: BLE001
        msg = str(getattr(e, "message", e))
        if "AlreadyExists" in msg or "EntityAlreadyExists" in msg:
            print(f"[SKIP] 策略 {POLICY_NAME} 已存在")
        else:
            print(f"[FAIL] 建策略：{msg[:300]}")
            return 1
    print("       " + doc)

    # ── 2. 角色 ──
    arn = None
    try:
        r = ram(c, "CreateRole", {"RoleName": ROLE_NAME,
                                  "AssumeRolePolicyDocument": json.dumps(TRUST_POLICY),
                                  "Description": "Atlas 云沙箱挂载 OSS Volume 用；仅信任 FC"})
        arn = r["Role"]["Arn"]
        print(f"[OK  ] 建角色 {ROLE_NAME}  arn={arn}")
    except Exception as e:  # noqa: BLE001
        msg = str(getattr(e, "message", e))
        if "AlreadyExists" in msg or "EntityAlreadyExists" in msg:
            arn = ram(c, "GetRole", {"RoleName": ROLE_NAME})["Role"]["Arn"]
            print(f"[SKIP] 角色 {ROLE_NAME} 已存在  arn={arn}")
        else:
            print(f"[FAIL] 建角色：{msg[:300]}")
            return 1

    # ── 3. 挂策略到角色 ──
    try:
        ram(c, "AttachPolicyToRole", {"PolicyType": "Custom", "PolicyName": POLICY_NAME,
                                      "RoleName": ROLE_NAME})
        print(f"[OK  ] {POLICY_NAME} → {ROLE_NAME}")
    except Exception as e:  # noqa: BLE001
        msg = str(getattr(e, "message", e))
        if "AlreadyAttach" in msg or "EntityAlreadyExists" in msg:
            print("[SKIP] 策略已挂在角色上")
        else:
            print(f"[FAIL] 挂策略：{msg[:300]}")
            return 1

    # ── 4. Volume 带着角色重建（mountConfig 建好改不了）──
    fc = fc_client()
    vols = fc.list_volumes(fcm.ListVolumesRequest(team_id=team)).body.to_map().get("volumes") or []
    for v in [x for x in vols if x.get("volumeName") == VOLUME_NAME]:
        fc.delete_volume(v["volumeID"], fcm.DeleteVolumeRequest(team_id=team))
        print(f"[OK  ] 删掉旧 Volume {v['volumeID']}（OSS 桶里的对象不动）")

    r = fc.create_volume(fcm.CreateVolumeRequest(body=fcm.CreateVolumeInput(
        team_id=team, volume_name=VOLUME_NAME,
        oss_volume_config=fcm.OSSVolumeConfig(
            bucket_name=bucket, bucket_path=BUCKET_PATH,
            endpoint=os.environ["OSS_ENDPOINT"], read_only=False),
        mount_config=fcm.CreateVolumeInputMountConfig(role=arn))))
    v = r.body.to_map()["volume"]
    print(f"[OK  ] 重建 Volume {v['volumeID']}  status={v.get('status')}  role={arn}")
    print(f"       mountConfig 回显: {v.get('mountConfig')}")

    # RAM 角色创建后有传播延迟，挂载立刻试可能还不认
    print("\n角色传播通常要几秒到几十秒。接着跑：")
    print("    .venv/bin/python scripts/probe_volume_cache.py")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
