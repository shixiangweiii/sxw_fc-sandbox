# 沙箱池服务（sandbox_pool）设计说明

> 实施方案见 `docs/sandbox-pool-plan.md`，本次改动与验证结果见 `docs/sandbox-pool-changes.md`，云沙箱摸底见 `docs/fc-agent-sandbox-notes.md`。

## 1. 定位

服务端 Agent 执行 skill、Python 脚本时，向沙箱池借用一个隔离的执行环境：
- **一次借用 = 一个任务**：借用期内独占一个沙箱，可以多次调用 `run_code` / `commands` / `files`，解释器状态保持。
- **归还即销毁**：沙箱不在调用方之间复用，没有残留数据泄露的风险；池子异步补一个新的。
- 调用方只通过 HTTP 调用，**不需要 E2B SDK，也不持有 API Key**。

## 2. 架构

```
   Agent 服务 A    Agent 服务 B ...
        │ HTTP：借用 / 续期 / 归还 / run_code / commands / files
        ▼
 ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
 │ pool 副本 1 │ │ pool 副本 2 │ │ pool 副本 3 │   无状态、逻辑完全相同；每个副本都跑后台维护
 └──────┬──────┘ └──────┬──────┘ └──────┬──────┘
        └──────── 共享存储（唯一事实来源）──┘
          本地：SQLite 文件（多进程共享）；生产：Postgres
                        │
        SandboxProvider 抽象 ──► E2BProvider（阿里云云沙箱，E2B 协议）
                             └► FakeProvider（单元测试）
```

| 模块 | 职责 |
| --- | --- |
| `store/` | 表结构与数据访问。所有状态变更都是带条件的 UPDATE（CAS） |
| `provider/` | 沙箱后端抽象；`e2b_provider.py` 处理 SDK 版本固定、显式代理、连接句柄缓存 |
| `core/allocator.py` | 借用、排队、续期、归还；借用期内代为执行 |
| `core/maintainer.py` | 后台维护：补货预热、空闲暂停、过期回收、崩溃接管、对账、熔断 |
| `core/lifecycle.py` | 两者共用的销毁、结束借用、后台任务管理 |
| `api/` | FastAPI 路由与错误码映射 |

## 3. 沙箱状态机

```
            补货（总数 < target）
                 ▼
             CREATING ──► WARMING（执行预热代码）──► READY（运行中，空闲）
                                                      │  ▲
                                   空闲 ≥ idle_pause  │  │
                                                      ▼  │
                                  PAUSING（约 10~16s）──► PAUSED
                                                             │ 被借走
           READY 被借走 ──────────► LEASED ◄── RESUMING（约 2s）◄┘
                                     │ 归还 / 借用过期 / 沙箱失效
                                     ▼
                                 DESTROYING ──► 删除记录 → 触发补货
 过渡态（CREATING / WARMING / PAUSING / RESUMING / DESTROYING）超过 op_deadline → 其他副本接管并销毁
```

- **分配优先级**：READY > PAUSED > 等待（PAUSING / CREATING 中的）> 排队。
- **容量**：所有状态的记录都计入总数，总数不超过 `max_size`，补货目标为 `target_size`。
- **预热**：新沙箱先执行 `warmup_code`（默认 `import numpy, pandas, matplotlib`），再进入 READY。暂停时解释器状态一并保存，恢复后是热的。

## 4. 多副本安全

1. **状态变更一律 CAS**：`UPDATE ... WHERE id=? AND state=? [AND version=? / op_owner=?]`，影响行数为 1 才算成功。多个副本同时抢同一个沙箱时只有一个能成功，不需要选主。
2. **先计数再写的操作要串行化**（占容量、入队）：同一事务内先更新池级锁行（`pool_kv.lock`）。SQLite 下事务本身是 `BEGIN IMMEDIATE`；Postgres 下 UPDATE 会拿行锁，语义一致。
3. **调用云端接口时不持有数据库连接**：先 CAS 进入过渡态并写入 `op_owner` / `op_deadline`，调用完成后再 CAS 到目标状态。执行者崩溃时，其他副本在 `op_deadline` 之后接管，把沙箱转为 DESTROYING，销毁后补货。
4. **排队也在库里**：`waiters` 表的自增 `seq` 决定先来先服务，只有队首才能抢沙箱。持有请求的副本每 200ms 刷新一次心跳；副本崩溃后，心跳超过 5s 未更新的排队记录会被清理，不会堵住队列。
5. **对账**：每 60s 按 `metadata.pool` 列出云端沙箱。
   - 云端有、库里没有的是孤儿，销毁（创建不满 60s 的跳过）。
   - 库里有、云端没有的记录清理掉。只看在列表查询前就存在、且期间版本号没变过的记录，再加 60s 宽限，避免误伤刚创建的沙箱（阿里云的列表接口有约 1.5s 延迟）。
6. **后台任务绝不调用 `connect()`**：它会续期，对已暂停的沙箱还会直接恢复。状态查询一律用 `get_info()` / `list()`。

**SQLite 的特殊处理**：每个进程只用 1 个数据库连接，进程内的访问排队执行。实测同一进程内多个连接高并发争抢 SQLite 文件锁时，会出现长达 `busy_timeout` 的锁等待。换成 Postgres 后使用普通连接池。

## 5. 借用与排队

- `acquire`：队列为空时直接抢；否则入队（队列已满返回 429），轮询到自己是队首时再抢；等待超时返回 504；客户端断开时撤销排队。
- 借用期限默认 10 分钟，可续期，从借出算起最长 60 分钟（`hard_deadline`）。过期后由维护循环强制回收（结束借用并销毁沙箱）。
- 平台侧的超时只作兜底：借出期间为「借用剩余时间 + 60s」（续期时同步）；空闲沙箱为 `idle_pause_after_s + 120s`。

## 6. HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/v1/leases` `{wait_timeout_s?, lease_ttl_s?}` | 借用，阻塞等待。200 / 429 队列满 / 504 等待超时 |
| GET | `/v1/leases/{id}` | 查询借用 |
| POST | `/v1/leases/{id}/renew` `{ttl_s?}` | 续期（不超过 hard_deadline） |
| DELETE | `/v1/leases/{id}` | 归还（沙箱随后销毁） |
| POST | `/v1/leases/{id}/run_code` `{code, language?, timeout_s?}` | 返回 stdout、stderr、text、results（含 png 等）、error |
| POST | `/v1/leases/{id}/commands` `{cmd, cwd?, envs?, timeout_s?}` | 返回 exit_code、stdout、stderr（退出码非 0 不算错误） |
| PUT / GET | `/v1/leases/{id}/files?path=` | 请求体 / 响应体为原始字节 |
| GET | `/v1/pool/stats` | 各状态数量、排队数、分配来源、各操作耗时 p50/p99 |
| GET | `/v1/sandboxes`、`/healthz` | 调试与健康检查 |

借用已结束或已过期时返回 409，借用不存在返回 404，沙箱侧错误返回 502。

## 7. 配置（环境变量 `POOL_<字段名大写>`）

| 配置 | 默认 | 说明 |
| --- | --- | --- |
| `POOL_TEMPLATE` | `code-interpreter-v1` | **需要暂停时必须设为第二代模板**（见 `examples/create_gen2_template.py`） |
| `POOL_DB_URL` | `sqlite+aiosqlite:///./.data/pool.db` | 生产可改为 `postgresql+asyncpg://...` |
| `POOL_MAX_SIZE` / `POOL_TARGET_SIZE` | 5 / 5 | 容量上限 / 补货目标 |
| `POOL_MIN_HOT` | 0 | 保持运行、不暂停的空闲数量 |
| `POOL_IDLE_PAUSE_AFTER_S` | 60 | 空闲多久后暂停 |
| `POOL_QUEUE_MAX` / `POOL_WAIT_TIMEOUT_S` | 10 / 180 | 排队上限 / 最长等待 |
| `POOL_LEASE_TTL_S` / `POOL_LEASE_MAX_S` | 600 / 3600 | 借用期限 / 最长借用时间 |
| `POOL_MAX_AGE_S` | 21600 | 空闲或暂停的沙箱最长寿命 |
| `POOL_OP_TIMEOUT_S` | 120 | 过渡态的截止时间（超时即被接管） |
| `POOL_WARMUP_CODE` | `import numpy, pandas, matplotlib` | 预热代码；设为空字符串则不预热 |
| `POOL_CREATE_FAIL_THRESHOLD` / `POOL_CREATE_COOLDOWN_S` | 3 / 60 | 连续创建失败熔断 |

## 8. 运行

```bash
pip install -r requirements-dev.txt
pytest                                  # 单元测试（FakeProvider，不访问云端）

source /path/to/e2b.env                 # E2B_API_KEY / E2B_API_URL / E2B_DOMAIN
export POOL_TEMPLATE=<第二代模板 ID>
scripts/run_local_cluster.sh start      # 3 个副本：8001~8003，共享 .data/pool.db
python scripts/e2e_scenarios.py         # 端到端场景（真实云沙箱）
scripts/run_local_cluster.sh stop
python scripts/cleanup_sandboxes.py     # 销毁账号下全部沙箱实例并确认清空
```

## 9. 已知限制与后续

- `min_hot > 0` 时，多个副本同时判断可能多暂停一个（每个副本各自计数），影响很小。
- 排队靠轮询（本地 200ms）。生产环境可以换成 Postgres LISTEN/NOTIFY 或 Redis 通知，减少数据库压力。
- 只支持一个模板；多模板需要把容量、队列、补货都按模板分片。
- 平台侧单个沙箱的最长存活时间、暂停后的保留时长还未实测，`max_age_s` 先取 6 小时。
