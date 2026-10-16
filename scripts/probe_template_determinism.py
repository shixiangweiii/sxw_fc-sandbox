"""同一个模板产出的沙箱，环境是不是一致的（会创建真实云沙箱，产生费用；结束即销毁）。

── 它为什么存在：一次**我自己的仪器错误**（2026-10-16）─────────────────────

当时看到的现象是「同一个模板，有的沙箱有 openpyxl、有的没有」，于是怀疑平台的
template cache 让环境不确定。**那个怀疑是错的。**

真因是：每个 shell 调用都是新进程，`set -a; . ./.env` 把 `POOL_TEMPLATE` 设成了
**基线模板** `xu76gk97q07mgohgw7q3`，而起池子时那句
`export POOL_TEMPLATE=<预装模板>` 不跨调用。于是「扫描」打在基线模板上
（当然没有那 6 个包），而池子与业务跑的是预装模板（有）。
两边都是对的，只是我以为它们是同一个模板。

【定】记在这里而不是删掉，是因为**它的形态是本仓反复记的那一种**：
判据/探针测的不是它声称在测的东西。这次的变体是「测的不是它声称在测的那个对象」。
这个脚本本身现在是那条结论的机械判据 —— 它把模板 ID 打在每一行输出里。

实测结论（预装模板 wmvj5ac9g2l0qywo9yrm，6 次）：**6/6 一致，六个包每台都在**。

跑：
    cd /Users/shixiangweii/run_proj/sxw_fc-sandbox
    set -a; . ./.env; set +a
    PYTHONPATH=. .venv/bin/python scripts/probe_template_determinism.py [次数]
"""

import asyncio
import json
import os
import sys
import time
from pathlib import Path

from sandbox_pool.egress import DENY_ALL, policy_network
from sandbox_pool.provider.e2b_provider import E2BProvider

OUT = Path(__file__).resolve().parent.parent / ".data" / "probe-template-determinism.json"

# 构建脚本声称预装的 6 个（镜像基线里没有的那些）
CLAIMED = ["openpyxl", "bs4", "lxml", "pypdf", "tabulate", "docx"]
# 基线镜像本来就有的，作为对照：它们应当**每次都在**
BASELINE = ["pandas", "numpy", "scipy", "requests"]

PROBE = """
import importlib.util as u, json, os
print(json.dumps({
    "claimed": [m for m in %r if u.find_spec(m)],
    "baseline": [m for m in %r if u.find_spec(m)],
    "build_marker": os.path.exists("/opt/preinstalled-packages.txt"),
    "bootstrap_marker": os.path.exists("/tmp/py-bootstrap.sh"),
}))
""" % (CLAIMED, BASELINE)


async def probe_once(p: E2BProvider, template: str, i: int) -> dict:
    t = time.monotonic()
    sid = await p.create(template, {"pool": "atlas-determinism", "n": str(i)}, 300,
                         network=policy_network(DENY_ALL), secure=True)
    try:
        # 【定】脚本用 files.write 送进去，不走 `python3 -c "..."` ——
        # 多层引号嵌套在这条链上已经静默吃掉过一次输出，而那种失败看起来像「结果是空的」
        await p.write_file(sid, "/tmp/probe.py", PROBE.encode(), sandbox_timeout_s=300)
        r = await p.run_command(sid, "python3 /tmp/probe.py", cwd=None, envs=None,
                                timeout_s=60, sandbox_timeout_s=300, max_output_bytes=20000)
        got = json.loads(r.stdout.strip().splitlines()[-1]) if r.stdout.strip() else {}
        got["sandbox"] = sid
        got["create_ms"] = round((time.monotonic() - t) * 1000)
        return got
    finally:
        await p.kill(sid)


async def main() -> int:
    n = int(sys.argv[1]) if len(sys.argv) > 1 else 6
    template = os.environ["POOL_TEMPLATE"]
    p = E2BProvider(proxy=os.environ.get("HTTPS_PROXY"))
    rows: list[dict] = []
    try:
        for i in range(n):
            row = await probe_once(p, template, i)
            rows.append(row)
            print(f"  [{i + 1}/{n}] {row['sandbox'][:20]}  "
                  f"预装={row.get('claimed')}  基线={len(row.get('baseline') or [])}/{len(BASELINE)}  "
                  f"构建标记={row.get('build_marker')}")
    finally:
        await p.close()

    have = sum(1 for r in rows if r.get("claimed"))
    report = {
        "template": template,
        "runs": len(rows),
        "有预装包的沙箱数": have,
        "没有的": len(rows) - have,
        "结论": ("一致：每台都有" if have == len(rows)
                 else "一致：每台都没有" if have == 0
                 else f"**不一致** —— {have}/{len(rows)} 台有"),
        "基线包是否每台都齐": all(len(r.get("baseline") or []) == len(BASELINE) for r in rows),
        "rows": rows,
    }
    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
    print()
    print(json.dumps({k: v for k, v in report.items() if k != "rows"}, ensure_ascii=False, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
