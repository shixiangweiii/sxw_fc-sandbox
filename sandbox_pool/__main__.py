"""python -m sandbox_pool --port 8001：启动一个池副本。配置见 sandbox_pool/config.py。"""

import argparse
import ipaddress
import logging

import uvicorn

from sandbox_pool.api.app import create_app
from sandbox_pool.config import PoolConfig


def insecure_bind(host: str, cfg: PoolConfig) -> bool:
    """未配置任何 API Key 却监听非回环地址。"""
    if cfg.auth_enabled:
        return False
    try:
        return not ipaddress.ip_address(host).is_loopback
    except ValueError:
        return host != "localhost"


def main() -> None:
    parser = argparse.ArgumentParser(description="沙箱池 HTTP 服务（单个副本）")
    parser.add_argument("--host", default="127.0.0.1")
    parser.add_argument("--port", type=int, default=8000)
    parser.add_argument("--log-level", default="info")
    parser.add_argument(
        "--allow-no-auth",
        action="store_true",
        help="允许在未配置 POOL_API_KEYS / POOL_ADMIN_KEYS 时监听非回环地址（任何能访问端口的人都能借用和执行代码）",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=args.log_level.upper(),
        format=f"%(asctime)s %(levelname)s [:{args.port}] %(name)s: %(message)s",
    )
    cfg = PoolConfig.from_env()
    if insecure_bind(args.host, cfg) and not args.allow_no_auth:
        parser.error(
            f"refusing to listen on {args.host} without authentication: "
            "set POOL_API_KEYS / POOL_ADMIN_KEYS, or pass --allow-no-auth"
        )
    if not cfg.auth_enabled:
        logging.getLogger(__name__).warning("authentication disabled (no POOL_API_KEYS / POOL_ADMIN_KEYS)")
    uvicorn.run(create_app(cfg), host=args.host, port=args.port, log_level=args.log_level)


if __name__ == "__main__":
    main()
