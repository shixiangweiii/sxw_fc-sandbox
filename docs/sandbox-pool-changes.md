# 沙箱池服务（sandbox_pool）改动说明

> 实施方案：`docs/sandbox-pool-plan.md`｜设计说明：`docs/sandbox-pool-design.md`｜回归测试与评审：`docs/sandbox-pool-review.md`
> 分支：`claude/inspiring-maxwell-n5z73w`

## 1. 提交记录

| 提交 | 内容 |
| --- | --- |
| `412d59d` | 沙箱池服务主体：存储层（CAS）、分配器（排队、借用、代为执行）、维护循环、HTTP 接口，以及基于 Fake 后端的测试 |
| `56a849f` | 本地多副本启动脚本、端到端场景脚本、沙箱清理脚本、设计文档；补充云沙箱实测结论 |
| 本次提交 | 回归测试与评审报告（`sandbox-pool-review.md`）、本改动说明 |

## 2. 新增 / 修改的文件

| 路径 | 说明 |
| --- | --- |
| `sandbox_pool/config.py` | `PoolConfig`，所有字段都可以用 `POOL_<字段名大写>` 环境变量覆盖 |
| `sandbox_pool/models.py` | 沙箱 / 借用 / 排队状态枚举，`LeaseGrant` 和业务异常 |
| `sandbox_pool/store/schema.py` | 5 张表：`sandboxes`、`leases`、`waiters`、`pool_kv`、`events` |
| `sandbox_pool/store/db.py` | 建库引擎。SQLite 下使用 WAL、`busy_timeout`、`BEGIN IMMEDIATE`，每个进程一个连接 |
| `sandbox_pool/store/repository.py` | 数据访问：CAS 状态变更、占容量、入队、心跳、键值计数、事件 |
| `sandbox_pool/provider/base.py` | `SandboxProvider` 协议 |
| `sandbox_pool/provider/e2b_provider.py` | 阿里云云沙箱（E2B 协议）实现：固定 SDK 版本、显式代理、连接句柄缓存 |
| `sandbox_pool/provider/fake.py` | 内存版 Fake 后端，支持注入延迟和失败 |
| `sandbox_pool/core/lifecycle.py` | 销毁、结束借用、后台任务管理 |
| `sandbox_pool/core/allocator.py` | 借用、排队（先来先服务 / 429 / 504）、续期、归还、代为执行 |
| `sandbox_pool/core/maintainer.py` | 补货与预热、空闲暂停、过期回收、崩溃接管、对账、熔断、优雅停止 |
| `sandbox_pool/core/pool.py` | 组装入口与 `/v1/pool/stats` 统计 |
| `sandbox_pool/api/*.py`、`__main__.py` | FastAPI 路由、错误码映射、进程入口 |
| `tests/*.py`、`pytest.ini` | 20 个测试用例（仓储 3 个、池逻辑 15 个、API 2 个） |
| `scripts/run_local_cluster.sh` | 本地多副本（多进程共享 SQLite）启动、停止、查看状态 |
| `scripts/e2e_scenarios.py` | 针对真实云沙箱的 9 个场景 |
| `scripts/cleanup_sandboxes.py` | 销毁账号下全部沙箱（包括已暂停的），并确认清空 |
| `requirements.txt` / `requirements-dev.txt` | 新增 fastapi、uvicorn、sqlalchemy[asyncio]、aiosqlite；开发依赖 pytest、pytest-asyncio |
| `.gitignore` | 忽略 `.data/`（本地数据库和日志）和缓存目录 |
| `docs/fc-agent-sandbox-notes.md` | 补充列表接口延迟约 1.5s、默认包含已暂停的沙箱、并发操作的耗时等实测结论 |

## 3. 与实施方案相比的差异

| 方案 | 实际 | 原因 |
| --- | --- | --- |
| allocator 和 maintainer 两个模块 | 另外抽出了 `core/lifecycle.py` 和 `core/pool.py` | 两者都要用到销毁和结束借用的逻辑；需要一个统一的组装入口 |
| SQLite 使用默认连接池 | **每个进程只用 1 个连接** | 实测同一进程内多个连接高并发争抢时，会出现长达 `busy_timeout`（30s）的锁等待（见 4.3） |
| 停止时取消后台任务 | **优雅停止**：跑完当前这一轮、等后台操作结束，超时才取消 | 直接取消正在进行数据库操作的协程，会让写事务悬挂，拖住其他副本 |
| 对账只读一次数据库 | 列表查询**前后各读一次**，再加宽限期 | 修复误判「已消失」的竞态（见 4.2） |
| 端到端场景 7 个 | 增加 6b（暂停进行中崩溃）和 8（云端与库一致） | 场景 6 的崩溃在实际运行中覆盖不到接管路径 |

## 4. 开发过程中发现并修复的问题

1. **从暂停中恢复时没有把 `lease_id` 写回沙箱记录**（`repository.finish_resume`）：后台维护会把它当成「没有有效借用的已借出沙箱」误销毁。由单元测试发现并修复。
2. **对账竞态**：原来先列云端、再读数据库，刚创建好的沙箱会被误判为已消失并销毁。改为列表查询前后各读一次库，只处理期间版本号没有变化的记录。
3. **SQLite 同一进程内多个连接的锁等待**：两个副本压测时，某个只读查询拿到写锁后等了整整 30s，偶发导致测试失败。换 aiosqlite 版本、换日志模式都能复现，改成每个进程一个连接后问题消失（20 次压测 0 失败）。
4. **停止时直接取消后台任务**导致事务悬挂：改成优雅停止。
5. **测试本身写得不严谨**：一处入队顺序依赖 sleep 的时序；另一处在归还之后才记录计数，导致同一个借用被归还两次。均已修正。
6. **本地集群脚本记录的 PID 不对**：记录的是启动子 shell 的 PID，`kill -9` 杀不到服务进程，所以最初那次「崩溃」测试其实没有发生。改为 `setsid nohup ... < /dev/null`，直接记录服务进程的 PID，也顺带解决了启动脚本不返回的问题。
7. **云沙箱列表接口的行为**：阿里云的列表有约 1.5s 延迟。一开始误以为是 `limit` 参数导致返回空列表，实测后确认是延迟，并确认列表默认包含已暂停的沙箱。对账的 60s 宽限期可以覆盖这个延迟。

## 5. 测试结果

详见 `docs/sandbox-pool-review.md` 第 2 节：单元测试 3 轮 × 20 个全部通过；端到端测试第二轮 9 个场景全部通过，包括两次真实的 kill -9 崩溃接管；结束后账号下的沙箱已全部销毁，列表接口返回 `[]`。

## 6. 遗留事项

评审发现的 19 个问题**本次没有修改**（按要求只评审），处理优先级见 `docs/sandbox-pool-review.md` 第 5 节。另外：
- 第二代模板 `xu76gk97q07mgohgw7q3` 仍保留在账号里（它是模板，不是沙箱实例）。
- 本地运行数据在 `.data/`（已被 gitignore），包含两轮端到端测试的日志。
