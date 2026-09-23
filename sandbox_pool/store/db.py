"""数据库引擎。SQLite 专有设置只在这里，业务代码不感知方言。"""

import os

from sqlalchemy import event
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


def create_engine(db_url: str) -> AsyncEngine:
    url = make_url(db_url)
    if not url.get_backend_name().startswith("sqlite"):
        return create_async_engine(db_url, pool_pre_ping=True)

    if url.database and url.database != ":memory:":
        os.makedirs(os.path.dirname(os.path.abspath(url.database)), exist_ok=True)
    # 每个进程只用一个连接：进程内的数据库访问排队串行。实测同一进程内多个连接高并发争抢
    # SQLite 文件锁时会出现长达 busy_timeout 的锁等待；进程之间仍靠文件锁 + busy_timeout 协调。
    # 换 Postgres 时走上面的普通连接池。
    engine = create_async_engine(
        db_url, connect_args={"timeout": 30}, pool_size=1, max_overflow=0, pool_timeout=120
    )

    @event.listens_for(engine.sync_engine, "connect")
    def _on_connect(dbapi_conn, _record):
        # 关闭驱动自带的隐式 BEGIN，由下面的 begin 事件统一发 BEGIN IMMEDIATE
        dbapi_conn.isolation_level = None
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA journal_mode=WAL")
        cur.execute("PRAGMA busy_timeout=30000")
        cur.execute("PRAGMA synchronous=NORMAL")
        cur.close()

    @event.listens_for(engine.sync_engine, "begin")
    def _on_begin(conn):
        # 多进程共享同一个文件时，事务开始即拿写锁，避免读后写升级锁导致的死锁
        conn.exec_driver_sql("BEGIN IMMEDIATE")

    return engine
