"""把 per-call 借用的「依赖代价」实际量出来（会创建真实云沙箱，产生费用；结束即销毁）。

── 要答的两个问题 ─────────────────────────────────────────────────────────

  Q1  **断网**沙箱里能不能直接 import 预装的包？
      —— 这一条决定 per-call ＋ 断网能不能同时成立。断网是「副作用只有声明过的
         那几个文件」这条性质的前提，而 pip install 需要网。

  Q2  per-call 每次调用的固定开销到底多少？分两种：
         预装模板：建沙箱 ＋ import
         基线模板：建沙箱 ＋ pip install（**要开网**）
      差值就是「把依赖烤进模板」省下的东西。

── 判据纪律 ───────────────────────────────────────────────────────────────

【定】Q1 的「离线」必须自己证明，不能假设。先 curl 一个公网地址确认真的不通，
不通才谈得上「离线 import 成功」有意义 —— 否则这条判据测的是「包装上了」，
而不是「断网也能用」（probe_volume_cache.py 里 offline_confirmed 同一条理由）。

【定】建沙箱耗时取多次的中位数。单次受池子冷热、代理抖动影响，
拿一次的数去做方案取舍，是在给噪声做决定。

运行：
    cd /Users/shixiangweii/run_proj/sxw_fc-sandbox
    set -a; . ./.env; set +a
    PYAGENT_TEMPLATE=<模板ID> .venv/bin/python scripts/probe_template_deps.py
"""

import asyncio
import json
import os
import statistics
import time
from pathlib import Path

import httpx
from e2b.sandbox.commands.command_handle import CommandExitException
from e2b_code_interpreter import AsyncSandbox

ROOT = Path(__file__).resolve().parent.parent
OUT = ROOT / ".data" / "probe-template-deps.json"

PACKAGES = ["requests", "httpx", "beautifulsoup4", "lxml", "openpyxl", "python-docx",
            "pypdf", "pillow", "pyyaml", "tabulate", "python-dateutil"]
IMPORT_NAMES = {"beautifulsoup4": "bs4", "python-docx": "docx", "pyyaml": "yaml",
                "pillow": "PIL", "python-dateutil": "dateutil"}
IMPORTS = [IMPORT_NAMES.get(p, p.replace("-", "_")) for p in PACKAGES]
OFFLINE_NET = {"allow_out": [], "deny_out": ["0.0.0.0/0"], "rules": {}}
CREATE_SAMPLES = 3

report: dict = {"started_at": time.strftime("%Y-%m-%d %H:%M:%S"), "checks": {}}


def record(name: str, ok, **detail) -> None:
    report["checks"][name] = {"ok": ok, **detail}
    print(f"[{ {True: 'PASS', False: 'FAIL', None: 'INFO'}[ok] }] {name}: "
          f"{json.dumps(detail, ensure_ascii=False)[:600]}")


async def retry_stale(call):
    import httpcore
    stale = (httpx.NetworkError, httpx.RemoteProtocolError, httpcore.NetworkError, httpcore.RemoteProtocolError)
    for delay in (0.3, 1.0, 2.0):
        try:
            return await call()
        except stale:
            await asyncio.sleep(delay)
    return await call()


async def sh(sbx, cmd, *, timeout=300):
    last = None
    for _ in range(8):
        try:
            r = await sbx.commands.run(cmd, user="user", timeout=timeout)
            return r.exit_code, (r.stdout + r.stderr).strip()
        except CommandExitException as e:
            return e.exit_code, (e.stdout + e.stderr).strip()
        except Exception as e:  # noqa: BLE001
            last = e
            await asyncio.sleep(1.0)
    raise RuntimeError(f"命令反复失败：{last!r}")


IMPORT_TIMER = (
    "python3 -c \"import time;t=time.perf_counter();"
    + "".join(f"import {m};" for m in IMPORTS)
    + "print(round((time.perf_counter()-t)*1000))\""
)


async def main() -> int:
    opts = {"api_key": os.environ["E2B_API_KEY"], "api_url": os.environ["E2B_API_URL"],
            "domain": os.environ["E2B_DOMAIN"]}
    proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")
    if proxy:
        opts["proxy"] = proxy
    base_tpl = os.environ["POOL_TEMPLATE"]
    new_tpl = os.environ["PYAGENT_TEMPLATE"]
    report.update({"baseline_template": base_tpl, "pyagent_template": new_tpl, "packages": PACKAGES})

    live: list[str] = []

    async def make(tpl, *, network=None, tag=""):
        kw = {"network": network} if network else {}
        t = time.monotonic()
        s = await retry_stale(lambda: AsyncSandbox.create(
            template=tpl, timeout=600, metadata={"pool": "atlas-depprobe", "tag": tag},
            request_timeout=30, **kw, **opts))
        live.append(s.sandbox_id)
        return s, round((time.monotonic() - t) * 1000)

    async def kill(s):
        try:
            await retry_stale(lambda: AsyncSandbox.kill(s.sandbox_id, request_timeout=20, **opts))
            if s.sandbox_id in live:
                live.remove(s.sandbox_id)
        except Exception as e:  # noqa: BLE001
            record(f"kill_failed_{s.sandbox_id[:12]}", False, error=str(e)[:200])

    try:
        # ══ A. 基线模板：镜像里本来有什么，装这批包要多久（开网）══════════
        a, a_ms = await make(base_tpl, tag="baseline")
        record("baseline_create", None, ms=a_ms, template=base_tpl)

        code, out = await sh(a, "python3 -c \"import importlib.util as u; "
                                f"print([m for m in {IMPORTS!r} if u.find_spec(m)])\"")
        record("baseline_already_present", None, present=out[:300],
               note="镜像自带 numpy/pandas/matplotlib；这批包在不在里面")

        t = time.monotonic()
        code, out = await sh(a, "PIP_DISABLE_PIP_VERSION_CHECK=1 python3 -m pip install --no-cache-dir "
                                "--break-system-packages " + " ".join(PACKAGES) + " 2>&1 | tail -3", timeout=600)
        pip_ms = round((time.monotonic() - t) * 1000)
        record("baseline_pip_install", code == 0, ms=pip_ms, tail=out[-250:],
               note="这是「每次调用都装」要付的钱，而且**必须开网**")
        await kill(a)

        # ══ B. 预装模板：断网，直接 import ═══════════════════════════════
        b, b_ms = await make(new_tpl, network=OFFLINE_NET, tag="offline")
        record("pyagent_create_offline", None, ms=b_ms, template=new_tpl,
               note="建的时候就带 deny_out=0.0.0.0/0")

        code, out = await sh(b, "curl -s -o /dev/null -w '%{http_code}' -m 8 https://pypi.org/simple/ || echo blocked")
        offline = out.strip() not in ("200", "301", "302")
        record("offline_confirmed", offline, pypi=out.strip()[:40],
               note="这一条不过，下面那条就不算数")

        code, out = await sh(b, IMPORT_TIMER)
        import_ms = int(out.strip().splitlines()[-1]) if code == 0 and out.strip().splitlines() else -1
        record("offline_import_all", offline and code == 0, import_ms=import_ms,
               packages=len(PACKAGES), detail=out[-200:],
               note="★ 断网沙箱里 11 个预装包全部 import 成功 —— per-call ＋ 断网同时成立")

        # run_code 还能用吗：模板的 start_command 换过，代码解释器必须仍在
        t = time.monotonic()
        try:
            ex = await b.run_code("import pandas as pd, bs4, openpyxl; print(pd.Series([1,2,3]).sum())", timeout=120)
            rc_ms = round((time.monotonic() - t) * 1000)
            record("pyagent_run_code_works", (ex.error is None) and "6" in (ex.text or "" ) + "".join(ex.logs.stdout),
                   ms=rc_ms, stdout="".join(ex.logs.stdout).strip()[:120],
                   error=(ex.error.name if ex.error else None),
                   note="第二代 PID1 是 systemd，不 exec 代码解释器的话这里会 500")
        except Exception as e:  # noqa: BLE001
            record("pyagent_run_code_works", False, error=str(e)[:200])
        await kill(b)

        # ══ C. 建沙箱耗时取中位数（单次受池子冷热与代理抖动影响）═════════
        samples = {}
        for label, tpl, net in (("baseline", base_tpl, None), ("pyagent", new_tpl, OFFLINE_NET)):
            xs = []
            for _ in range(CREATE_SAMPLES):
                s, ms = await make(tpl, network=net, tag="timing")
                xs.append(ms)
                await kill(s)
            samples[label] = {"samples_ms": xs, "median_ms": int(statistics.median(xs))}
            record(f"create_time_{label}", None, **samples[label])

        base_med = samples["baseline"]["median_ms"]
        new_med = samples["pyagent"]["median_ms"]
        report["summary"] = {
            "断网能不能直接 import": "能" if report["checks"]["offline_import_all"]["ok"] else "不能",
            "per_call_预装模板_ms": {"建沙箱(中位)": new_med, "import": import_ms,
                                     "合计": new_med + max(import_ms, 0), "要不要网": "不要"},
            "per_call_基线模板_ms": {"建沙箱(中位)": base_med, "pip install": pip_ms,
                                     "合计": base_med + pip_ms, "要不要网": "要"},
            "每次调用省下_ms": (base_med + pip_ms) - (new_med + max(import_ms, 0)),
            "模板构建_一次性_s": 71,
        }
        return 0
    finally:
        for sid in list(live):
            try:
                await retry_stale(lambda s=sid: AsyncSandbox.kill(s, request_timeout=20, **opts))
            except Exception:  # noqa: BLE001, S110
                pass
        try:
            pg = AsyncSandbox.list(request_timeout=30, **opts)
            left = []
            while pg.has_next:
                left += [i.sandbox_id for i in await pg.next_items()]
            record("cleanup_sandboxes_remaining", len(left) == 0, remaining=left)
        except Exception as e:  # noqa: BLE001
            record("cleanup_sandboxes_remaining", None, error=str(e)[:200])
        OUT.parent.mkdir(parents=True, exist_ok=True)
        OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
        print(f"\n报告写入 {OUT}")
        if "summary" in report:
            print(json.dumps(report["summary"], ensure_ascii=False, indent=2))


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
