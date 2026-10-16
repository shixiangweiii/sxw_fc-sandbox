"""OSS Volume 能不能当「断网沙箱的依赖缓存」（会创建真实云资源，产生费用；结束即清理）。

── 这个探针要答的那个问题 ─────────────────────────────────────────────────

Atlas 走 per-call 借用时，装依赖和断网是缠在一起的：

    要装依赖 → 要开网 → 丢掉「副作用只有声明过的那几个文件」这条性质
    要干净   → 不开网 → pip install 根本跑不起来

如果 Volume 能跨沙箱持久，那就有第三条路：

    开网的一次性准备 → 把 wheel 预先下到 Volume
    之后每次调用     → **完全断网** ＋ pip install --no-index --find-links <挂载点>

所以本探针的判据不是「挂载成功」，是**在一台 deny_out=0.0.0.0/0 的沙箱里，
离线把包装上并 import 成功**。挂载成功只是中间步骤。

── 背景：调研笔记上那条「不支持 Volume」是错的 ─────────────────────────────

`docs/fc-agent-sandbox-notes.md` 2.3 节把 Volume 列进「不支持」。实测：
    GET  /volumes → 200 []
    POST /volumes → 400 "E2B CreateVolume API is disabled; use the POP CreateVolume API"
也就是**支持**，只是要走阿里云 OpenAPI 建、再用 E2B SDK 挂。本脚本跑通后要回去改那一节。

运行：
    cd /Users/shixiangweii/run_proj/sxw_fc-sandbox
    set -a; . ./.env; set +a
    .venv/bin/python scripts/probe_volume_cache.py
"""

import asyncio
import json
import os
import time
from pathlib import Path

import httpx
from alibabacloud_fcsandbox20260509 import models as fcm
from alibabacloud_fcsandbox20260509.client import Client as FCClient
from alibabacloud_tea_openapi import models as oapi
from e2b import NotFoundException
from e2b.sandbox.commands.command_handle import CommandExitException
from e2b_code_interpreter import AsyncSandbox

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / ".data" / "probe-volume-cache.json"

VOLUME_NAME = "atlas-pipcache"
BUCKET_PATH = "/atlas-cache"
MOUNT = "/mnt/cache"
WHEELS = f"{MOUNT}/wheels"
# 选一个镜像里没有、且带几个传递依赖的纯 Python 包
PKG = "httpx"
TIMEOUT_S = 600

report: dict = {"started_at": time.strftime("%Y-%m-%d %H:%M:%S"), "checks": {}}


def record(name: str, ok, **detail) -> None:
    report["checks"][name] = {"ok": ok, **detail}
    print(f"[{ {True: 'PASS', False: 'FAIL', None: 'INFO'}[ok] }] {name}: "
          f"{json.dumps(detail, ensure_ascii=False)[:700]}")


def fc_client() -> FCClient:
    cfg = oapi.Config(
        access_key_id=os.environ["ALIBABA_CLOUD_ACCESS_KEY_ID"],
        access_key_secret=os.environ["ALIBABA_CLOUD_ACCESS_KEY_SECRET"],
        region_id=os.environ["FCSANDBOX_REGION_ID"],
    )
    if os.environ.get("HTTPS_PROXY"):
        cfg.https_proxy = os.environ["HTTPS_PROXY"]
    return FCClient(cfg)


async def retry_stale(call):
    """经代理的 SDK 连接空闲后失效，下一个请求报空的 WriteError（见 e2b_provider._retry_stale）。"""
    import httpcore
    stale = (httpx.NetworkError, httpx.RemoteProtocolError, httpcore.NetworkError, httpcore.RemoteProtocolError)
    for delay in (0.3, 1.0, 2.0):
        try:
            return await call()
        except stale as e:
            print(f"    （连接失效，{delay}s 后重试：{type(e).__name__}）")
            await asyncio.sleep(delay)
    return await call()


async def sh(sbx: AsyncSandbox, cmd: str, *, timeout: float = 180, user: str = "user"):
    last = None
    for _ in range(8):
        try:
            r = await sbx.commands.run(cmd, user=user, timeout=timeout)
            return r.exit_code, (r.stdout + r.stderr).strip()
        except CommandExitException as e:
            return e.exit_code, (e.stdout + e.stderr).strip()
        except Exception as e:  # noqa: BLE001
            last = e
            await asyncio.sleep(1.0)
    raise RuntimeError(f"命令反复失败：{last!r}")


async def main() -> int:
    opts = {
        "api_key": os.environ["E2B_API_KEY"],
        "api_url": os.environ["E2B_API_URL"],
        "domain": os.environ["E2B_DOMAIN"],
    }
    if os.environ.get("HTTPS_PROXY"):
        opts["proxy"] = os.environ["HTTPS_PROXY"]
    template = os.environ["POOL_TEMPLATE"]
    team = os.environ["FCSANDBOX_TEAM_ID"]
    fc = fc_client()

    sandboxes: list[str] = []
    volume_id = None
    try:
        # ── 0. 准备 Volume（已存在就复用）──
        vols = fc.list_volumes(fcm.ListVolumesRequest(team_id=team)).body.to_map().get("volumes") or []
        mine = [v for v in vols if v.get("volumeName") == VOLUME_NAME]
        if mine:
            volume_id = mine[0]["volumeID"]
            record("volume_reuse", None, volume_id=volume_id, status=mine[0].get("status"))
        else:
            t = time.monotonic()
            r = fc.create_volume(fcm.CreateVolumeRequest(body=fcm.CreateVolumeInput(
                team_id=team, volume_name=VOLUME_NAME,
                oss_volume_config=fcm.OSSVolumeConfig(
                    bucket_name=os.environ["OSS_BUCKET"], bucket_path=BUCKET_PATH,
                    endpoint=os.environ["OSS_ENDPOINT"], read_only=False))))
            v = r.body.to_map()["volume"]
            volume_id = v["volumeID"]
            record("volume_create", v.get("status") == "AVAILABLE", volume_id=volume_id,
                   status=v.get("status"), storage_class=v.get("storageClass"),
                   elapsed_s=round(time.monotonic() - t, 2), note="OSS 后端，不需要 RAM 角色")

        # ══ 第一台：开网，把 wheel 下到 Volume ══════════════════════════
        t = time.monotonic()
        a = await retry_stale(lambda: AsyncSandbox.create(
            template=template, timeout=TIMEOUT_S,
            metadata={"pool": "atlas-volprobe", "phase": "warm"},
            volume_mounts={MOUNT: VOLUME_NAME}, request_timeout=30, **opts))
        sandboxes.append(a.sandbox_id)
        record("mount_create_sandbox", None, sandbox_id=a.sandbox_id,
               elapsed_s=round(time.monotonic() - t, 2), mount=MOUNT)

        code, out = await sh(a, f"mount | grep -i {MOUNT} || true; echo ---; df -h {MOUNT} 2>&1 | tail -2; "
                                f"echo ---; ls -ld {MOUNT} 2>&1")
        record("mount_visible", MOUNT in out, detail=out[:500])

        # 可写性 ＋ 属主（OSS 挂载常见的坑是 root 属主、普通用户写不进）
        code, out = await sh(a, f"mkdir -p {WHEELS} && echo atlas-marker > {MOUNT}/marker.txt "
                                f"&& cat {MOUNT}/marker.txt && stat -c '%U:%G %a' {MOUNT}/marker.txt")
        record("mount_writable", code == 0 and "atlas-marker" in out, exit_code=code, detail=out[:300])

        # 预下载 wheel（这一步**要开网**，是一次性准备，不是每次调用）
        t = time.monotonic()
        code, out = await sh(a, f"pip download {PKG} -d {WHEELS} --quiet 2>&1 | tail -5; "
                                f"echo EXIT=$?; ls {WHEELS} | head -20; ls {WHEELS} | wc -l", timeout=300)
        dl_s = round(time.monotonic() - t, 2)
        record("pip_download_to_volume", "EXIT=0" in out, elapsed_s=dl_s, detail=out[-500:])

        code, size = await sh(a, f"du -sh {WHEELS} 2>/dev/null | cut -f1")
        record("volume_contents", None, wheels_size=size.strip(), elapsed_download_s=dl_s)

        await retry_stale(lambda: AsyncSandbox.kill(a.sandbox_id, request_timeout=20, **opts))
        sandboxes.remove(a.sandbox_id)
        record("warm_sandbox_destroyed", True, sandbox_id=a.sandbox_id)

        # ══ 第二台：**完全断网**，从 Volume 离线装包 ════════════════════
        t = time.monotonic()
        b = await retry_stale(lambda: AsyncSandbox.create(
            template=template, timeout=TIMEOUT_S,
            metadata={"pool": "atlas-volprobe", "phase": "offline"},
            volume_mounts={MOUNT: VOLUME_NAME},
            network={"allow_out": [], "deny_out": ["0.0.0.0/0"], "rules": {}},
            request_timeout=30, **opts))
        sandboxes.append(b.sandbox_id)
        record("offline_sandbox_create", None, sandbox_id=b.sandbox_id,
               elapsed_s=round(time.monotonic() - t, 2),
               note="建的时候就带 deny_out=0.0.0.0/0")

        # 先证明它**真的断网**，否则下面的「离线安装成功」毫无意义
        code, out = await sh(b, "curl -s -o /dev/null -w '%{http_code}' -m 8 https://pypi.org/simple/ || echo blocked")
        truly_offline = out.strip() not in ("200", "301", "302")
        record("offline_confirmed", truly_offline, pypi_probe=out.strip()[:40],
               note="这一条不过的话，下面那条不算数")

        # 跨沙箱持久性：上一台写的 marker 还在吗
        code, out = await sh(b, f"cat {MOUNT}/marker.txt 2>&1; ls {WHEELS} | wc -l")
        record("volume_persists_across_sandboxes", "atlas-marker" in out, detail=out[:300])

        # 关键判据：断网 ＋ 只用 Volume 里的 wheel 装包
        t = time.monotonic()
        code, out = await sh(b, f"pip install --no-index --find-links {WHEELS} {PKG} 2>&1 | tail -6; echo EXIT=$?",
                             timeout=300)
        inst_s = round(time.monotonic() - t, 2)
        code2, out2 = await sh(b, f"python3 -c 'import {PKG}; print({PKG}.__version__)'")
        record("offline_install_from_volume", truly_offline and code2 == 0,
               elapsed_s=inst_s, import_check=out2.strip()[:120], detail=out[-400:],
               note="断网沙箱里只靠挂载的 wheel 装上并 import 成功 —— per-call ＋ 断网同时成立")

        report["summary"] = {
            "Volume 能不能用": "能（OpenAPI 建、SDK 挂，不要 RAM 角色）",
            "跨沙箱持久": "是" if report["checks"].get("volume_persists_across_sandboxes", {}).get("ok") else "否",
            "断网离线装依赖": "成功" if report["checks"].get("offline_install_from_volume", {}).get("ok") else "失败",
            "一次性预下载耗时_s": dl_s,
            "离线安装耗时_s": inst_s,
        }
        return 0
    finally:
        for sid in list(sandboxes):
            try:
                await retry_stale(lambda s=sid: AsyncSandbox.kill(s, request_timeout=20, **opts))
                record(f"cleanup_kill_{sid[:12]}", True)
            except Exception as e:  # noqa: BLE001
                record(f"cleanup_kill_{sid[:12]}", False, error=str(e)[:200])
        try:
            pg = AsyncSandbox.list(request_timeout=30, **opts)
            left = []
            while pg.has_next:
                left += [i.sandbox_id for i in await pg.next_items()]
            record("cleanup_sandboxes_remaining", len(left) == 0, remaining=left)
        except Exception as e:  # noqa: BLE001
            record("cleanup_sandboxes_remaining", None, error=str(e)[:200])
        # Volume 默认保留：它就是「一次性准备」的产物，删了下次还得重下。
        # 要清掉就设 PROBE_DELETE_VOLUME=1（OSS 里 BUCKET_PATH 下的对象需另行清理）。
        if volume_id and os.environ.get("PROBE_DELETE_VOLUME") == "1":
            try:
                fc.delete_volume(volume_id, fcm.DeleteVolumeRequest(team_id=team))
                record("cleanup_volume_deleted", True, volume_id=volume_id)
            except Exception as e:  # noqa: BLE001
                record("cleanup_volume_deleted", False, error=str(e)[:200])
        else:
            record("cleanup_volume_kept", None, volume_id=volume_id,
                   note="Volume 保留（它是一次性准备的产物）。删除：PROBE_DELETE_VOLUME=1")
        OUT.parent.mkdir(parents=True, exist_ok=True)
        OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\n报告写入 {OUT}")
        if "summary" in report:
            print(json.dumps(report["summary"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
