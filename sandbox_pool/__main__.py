"""python -m sandbox_pool --port 8001：启动一个池副本。配置见 sandbox_pool/config.py。"""

import argparse
import logging

import uvicorn

from sandbox_pool.api.app import create_app
from sandbox_pool.config import PoolConfig


def main() -> None:
    parser = argparse.ArgumentParser(description="沙箱池 HTTP 服务（单个副本）")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--log-level", default="info")
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level.upper(),
        format=f"%(asctime)s %(levelname)s [:{args.port}] %(name)s: %(message)s",
    )
    cfg = PoolConfig.from_env()
    uvicorn.run(create_app(cfg), host=args.host, port=args.port, log_level=args.log_level)


if __name__ == "__main__":
    main()
