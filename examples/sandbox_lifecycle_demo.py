"""云沙箱（FC Agent Sandbox）生命周期验证 demo：创建 -> 验证 -> 暂停 -> 删除。

凭据只从环境变量读取，不要写入代码或提交到仓库：
    export E2B_API_KEY="<your-api-key>"
    export E2B_API_URL="https://api.<region>.e2b.fc.aliyuncs.com"
    export E2B_DOMAIN="<region>.e2b.fc.aliyuncs.com"

运行：
    pip install -r requirements.txt
    python examples/sandbox_lifecycle_demo.py
"""

import os
import sys
import time
from contextlib import contextmanager

from e2b import NotFoundException
from e2b_code_interpreter import Sandbox

TEMPLATE = os.environ.get("SANDBOX_TEMPLATE", "code-interpreter-v1")
TIMEOUT_SECONDS = 300


def require_env(name: str) -> str:
    value = os.environ.get(name, "").strip()
    if not value:
        raise RuntimeError(f"缺少环境变量: {name}")
    return value


@contextmanager
def step(name: str):
    print(f"==> {name}")
    start = time.perf_counter()
    yield
    print(f"    完成，耗时 {time.perf_counter() - start:.2f}s")


def main() -> int:
    conn = {
        "api_key": require_env("E2B_API_KEY"),
        "api_url": require_env("E2B_API_URL"),
        "domain": require_env("E2B_DOMAIN"),
    }

    sandbox = None
    killed = False
    try:
        with step(f"1. 创建沙箱 (template={TEMPLATE}, timeout={TIMEOUT_SECONDS}s)"):
            sandbox = Sandbox.create(
                template=TEMPLATE,
                timeout=TIMEOUT_SECONDS,
                metadata={"purpose": "lifecycle-demo"},
                **conn,
            )
            print(f"    sandbox_id = {sandbox.sandbox_id}")

        with step("2. 验证沙箱可用（执行命令 + 读写文件）"):
            result = sandbox.commands.run("python3 -c \"print('hello from sandbox')\"")
            print(f"    stdout = {result.stdout.strip()!r}")
            sandbox.files.write("/tmp/demo.txt", "fc agent sandbox")
            print(f"    file   = {sandbox.files.read('/tmp/demo.txt')!r}")
            info = sandbox.get_info()
            print(f"    state  = {info.state}, started_at = {info.started_at}, end_at = {info.end_at}")

        with step("3. 暂停沙箱"):
            paused = sandbox.pause()
            print(f"    pause() 返回 {paused}")
            info = Sandbox.get_info(sandbox.sandbox_id, **conn)
            print(f"    state  = {info.state}")

        with step("4. 删除沙箱"):
            killed = Sandbox.kill(sandbox.sandbox_id, **conn)
            print(f"    kill() 返回 {killed}")

        with step("5. 确认沙箱已删除"):
            try:
                info = Sandbox.get_info(sandbox.sandbox_id, **conn)
                print(f"    [WARN] 删除后仍可查询到沙箱, state = {info.state}")
            except NotFoundException:
                print("    get_info 返回 NotFound，沙箱已删除")

        print("全部流程执行成功")
        return 0
    finally:
        # 任一步骤失败时兜底释放，避免遗留计费资源
        if sandbox is not None and not killed:
            try:
                Sandbox.kill(sandbox.sandbox_id, **conn)
                print(f"已兜底删除沙箱 {sandbox.sandbox_id}")
            except Exception as e:  # noqa: BLE001
                print(f"兜底删除失败，请到控制台手动清理 {sandbox.sandbox_id}: {e}")


if __name__ == "__main__":
    sys.exit(main())
