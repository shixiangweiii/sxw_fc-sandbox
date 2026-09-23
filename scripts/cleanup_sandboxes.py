"""销毁账号（当前 E2B_API_URL 地域）下的沙箱实例，包括暂停中的，并确认清理干净。

    python scripts/cleanup_sandboxes.py            # 全部沙箱
    python scripts/cleanup_sandboxes.py --pool default   # 只清理某个池（按 metadata.pool）
"""

import argparse
import asyncio
import os
import sys

from e2b import SandboxQuery
from e2b_code_interpreter import AsyncSandbox


async def list_all(opts: dict, metadata: dict | None) -> list:
    paginator = AsyncSandbox.list(query=SandboxQuery(metadata=metadata) if metadata else None, **opts)
    items = []
    while paginator.has_next:
        items.extend(await paginator.next_items())
    return items


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--pool", help="只清理 metadata.pool 等于该值的沙箱")
    args = parser.parse_args()

    opts = {}
    proxy = os.environ.get("HTTPS_PROXY") or os.environ.get("https_proxy")
    if proxy:
        opts["proxy"] = proxy
    metadata = {"pool": args.pool} if args.pool else None

    for attempt in range(5):
        items = await list_all(opts, metadata)
        if not items:
            print("沙箱已全部清理，当前列表为空")
            return 0
        print(f"第 {attempt + 1} 轮：发现 {len(items)} 个沙箱")
        for it in items:
            state = getattr(it.state, "value", it.state)
            ok = await AsyncSandbox.kill(it.sandbox_id, **opts)
            print(f"  kill {it.sandbox_id} state={state} metadata={it.metadata} -> {ok}")
        await asyncio.sleep(2)
    left = await list_all(opts, metadata)
    print(f"仍有 {len(left)} 个沙箱未清理：{[i.sandbox_id for i in left]}")
    return 1


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
