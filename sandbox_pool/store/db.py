"""数据库引擎。SQLite 专有设置只在这里，业务代码不感知方言。"""

import os
from typing import Optional

from sqlalchemy import event
from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine


def _sqlite_file(db_url: str) -> Optional[str]:
    """SQLite 文件库返回文件路径；内存库和其他数据库返回 None。"""
    url = make_url(db_url)
    if not url.get_backend_name().startswith("sqlite"):
        return None
    if not url.database or url.database == ":memory:":
        return None
    return url.database


def create_engine(db_url: str) -> AsyncEngine:
    """写引擎。Postgres 等使用普通连接池，读写共用。"""
    url = make_url(db_url)
    if not url.get_backend_name().startswith("sqlite"):
        return create_async_engine(db_url, pool_pre_ping=True)

    path = _sqlite_file(db_url)
    if path:
        os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    # 每个进程只用一个写连接：进程内的写操作排队串行。实测同一进程内多个连接高并发争抢
    # SQLite 写锁时会出现长达 busy_timeout 的锁等待；进程之间仍靠文件锁 + busy_timeout 协调。
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


def create_read_engine(db_url: str) -> Optional[AsyncEngine]:
    """SQLite 文件库的只读连接池：普通 BEGIN，不拿写锁。

    WAL 模式下读事务看到开始时已提交的数据，与写事务互不阻塞，只读查询不再和所有进程的写操作串行。
    其他数据库返回 None，读写共用写引擎的连接池。
    """
    if _sqlite_file(db_url) is None:
        return None
    engine = create_async_engine(
        db_url, connect_args={"timeout": 30}, pool_size=4, max_overflow=0, pool_timeout=60
    )

    @event.listens_for(engine.sync_engine, "connect")
    def _on_connect(dbapi_conn, _record):
        dbapi_conn.isolation_level = None
        cur = dbapi_conn.cursor()
        cur.execute("PRAGMA busy_timeout=30000")
        # 防止只读路径误写
        cur.execute("PRAGMA query_only=ON")
        cur.close()

    @event.listens_for(engine.sync_engine, "begin")
    def _on_begin(conn):
        conn.exec_driver_sql("BEGIN")

    return engine
