# 沙箱池服务（sandbox_pool）设计说明

> 实施方案见 `docs/sandbox-pool-plan.md`；首轮改动见 `docs/sandbox-pool-changes.md`；评审见 `docs/sandbox-pool-review.md`；评审问题修复见 `docs/sandbox-pool-fix-plan.md`、`docs/sandbox-pool-fix-changes.md`；云沙箱摸底见 `docs/fc-agent-sandbox-notes.md`。

## 1. 定位

服务端 Agent 执行 skill、Python 脚本时，向沙箱池借用一个隔离的执行环境：
- **一次借用 = 一个任务**：借用期内独占一个沙箱，可以多次调用 `run_code` / `commands` / `files`，解释器状态保持。
- **归还即销毁**：沙箱不在调用方之间复用，不会有残留数据泄露；池子异步补一个新的。
- 调用方只通过 HTTP 调用，**不需要 E2B SDK，也不持有云沙箱的 API Key**。开启鉴权后，调用方用池子分配的 API Key 访问，只能操作自己的借用。

## 2. 架构

```
   Agent 服务 A    Agent 服务 B ...
        │ HTTP（Bearer API Key）：借用 / 续期 / 归还 / run_code / commands / files
        ▼
 ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
 │ pool 副本 1 │ │ pool 副本 2 │ │ pool 副本 3 │   无状态、逻辑完全相同；每个副本都跑后台维护
 └──────┬──────┘ └──────┬──────┘ └──────┬──────┘
        └──────── 共享存储（唯一事实来源）──┘
          本地：SQLite 文件（多进程共享）；生产：Postgres
                        │
        SandboxProvider 抽象 ──► E2BProvider（阿里云云沙箱，E2B 协议）
                             └► FakeProvider（单元测试，模拟平台超时回收和各种失败）
```

| 模块 | 职责 |
| --- | --- |
| `store/` | 表结构、数据访问、轻量迁移。所有状态变更都是带条件的 UPDATE（CAS） |
| `provider/` | 沙箱后端抽象；`e2b_provider.py` 负责固定 SDK 版本、显式代理、请求超时、连接句柄缓存（LRU + 空闲淘汰） |
| `core/allocator.py` | 借用、排队、续期、归还；借用期内代为执行 |
| `core/maintainer.py` | 后台维护：补货预热、空闲暂停、READY 保活、过期回收、崩溃接管、对账、历史清理、排空、熔断 |
| `core/lifecycle.py` | 两者共用的销毁、结束借用、后台任务管理 |
| `api/` | FastAPI 路由、鉴权、错误码映射 |

## 3. 沙箱状态机

```
            补货（总数 < target，未熔断、未排空）
                 ▼
             CREATING ──► WARMING（执行预热代码）──► READY（运行中，空闲；平台超时由保活续期）
                                                      │  ▲
                              空闲 ≥ idle_pause 且    │  │ 接管时平台上仍在运行
                              READY 数 > min_hot      ▼  │
                                  PAUSING（约 10~16s）──► PAUSED
                                                             │ 被借走
           READY 被借走 ──────────► LEASED ◄── RESUMING（约 2s）◄┘
                                     │ 归还（同步销毁）/ 借用过期 / 沙箱失效
                                     ▼
                                 DESTROYING ──► 删除记录 → 触发补货
```

- **分配优先级**：READY > PAUSED > 等待（PAUSING / CREATING 中的）> 排队。
- **容量**：所有状态的记录都计入总数，总数不超过 `max_size`，补货目标为 `target_size`。
- **预热**：新沙箱先执行 `warmup_code`（默认 `import numpy, pandas, matplotlib`），再进入 READY。暂停时解释器状态一并保存，恢复后是热的。
- **过渡态接管**：执行者崩溃后，其他副本在 `op_deadline` 之后接管。
  - 截止时间按操作区分：创建、预热、暂停 120s；恢复 60s；销毁 30s。
  - 暂停、恢复途中崩溃的：先查云端实际状态，已暂停的收回为 PAUSED，仍在运行的收回为 READY，查不到才销毁。
  - 其余情况销毁后补货。

## 4. 多副本安全

1. **状态变更一律 CAS**：`UPDATE ... WHERE id=? AND state=? [AND version=? / op_owner=?]`，影响行数为 1 才算成功。多个副本同时抢同一个沙箱时只有一个能成功，不需要选主。
2. **先计数再写的操作要串行化**：同一事务内先更新池级锁行（`pool_kv.lock`）。包括：
   - 占容量（同时检查熔断和排空）；
   - 入队；
   - 暂停前检查 `min_hot`。

   SQLite 下写事务本身是 `BEGIN IMMEDIATE`；Postgres 下 UPDATE 会拿行锁，语义一致。
3. **调用云端接口时不持有数据库连接**：先 CAS 进入过渡态并写入 `op_owner` / `op_deadline`，调用完成后再 CAS 到目标状态。
4. **排队也在库里**：`waiters` 表的自增 `seq` 决定先来先服务。
   - 排在前面的人数小于可分配沙箱数时，才可以去抢，多个沙箱同时可用时前几个请求可以并行抢。
   - 抢到沙箱与排队记录改为 GRANTED 在同一事务内完成。
   - 心跳由排队期间常驻的后台任务每秒刷新，抢沙箱（包括较慢的恢复）期间不中断。
   - 副本崩溃后，心跳超过 5s 未更新的排队记录会被清理。
5. **对账**：全池每 60s 由一个副本执行（`pool_kv.reconcile_at` 上的时间戳 CAS 决定由谁执行）。按 `metadata.pool` 列出云端沙箱：
   - 云端有、库里没有的是孤儿，销毁（创建不满 60s 的跳过）。
   - 库里有、云端没有的记录清理掉。只看在列表查询前就存在、且期间版本号没变过的记录，再加 60s 宽限，避免误伤刚创建的沙箱（阿里云的列表接口有约 1.5s 延迟）。
6. **READY 保活**：READY 沙箱的平台超时是短的兜底超时（空闲暂停时间 + 120s）。维护循环在剩余时间不足一半时续期，保证 `min_hot` 保留的热沙箱不被平台回收。
   - 保活先用版本号 CAS 占住这一行，调用平台后重读。
   - 若期间被借走，就按借用剩余时间重设平台超时，防止保活的短超时后到平台、覆盖借用方的超时。
7. **后台任务绝不调用 `connect()`**：它会续期，对已暂停的沙箱还会直接恢复。状态查询一律用 `get_info()` / `list()`。
8. **历史清理**：每小时由一个副本执行，删除已结束超过 7 天的排队、借用记录和事件。

**SQLite 的特殊处理**：
- 每个进程只有 **1 个写连接**，事务开始即 `BEGIN IMMEDIATE`。
- 另有**只读连接池**：普通 `BEGIN` + `query_only`，WAL 下读写互不阻塞。
- 正常路径上不执行预期会失败的语句（例如靠主键冲突判断「已存在」）。执行失败的语句会留下未关闭的 sqlite3 游标，可能由主线程的 GC 回收；回收时需要该连接的 SQLite 互斥锁，而此时这个连接可能正在忙等写锁。结果事件循环被卡住，同进程内持有写锁的协程无法提交，形成死锁。

换成 Postgres 后读写都使用普通连接池。

## 5. 借用与排队

- `acquire`：
  - 队列为空时直接抢；否则入队（队列已满返回 429），轮到自己时再抢；等待超时返回 504。
  - 抢的过程中截止时间已到，也照常交付拿到的沙箱。
  - 客户端断开时撤销排队（已拿到的借用随即归还）。
  - `POOL_STRICT_FIFO=true` 时所有请求都先入队，队列上限改为「排队数 < `queue_max` + 可分配沙箱数」。
- 借用期限默认 10 分钟，可续期，从借出算起最长 60 分钟（`hard_deadline`）。
  - 续期先调用平台 `set_timeout`，成功后再写库；平台失败返回 502，借用保持不变。
  - 过期由维护循环回收，条件 CAS（`expires_at <= now`）不会覆盖同时发生的续期。
- 归还：等沙箱销毁完成再返回（约 0.3s），副本随后崩溃也不会占住容量。
- 平台侧的超时只作兜底：借出期间为「借用剩余时间 + 60s」（续期时同步）；空闲 READY 由保活续期。

## 6. HTTP 接口

开启鉴权后，除 `/healthz` 外都需要 `Authorization: Bearer <key>`。

| 方法 | 路径 | 权限 | 说明 |
| --- | --- | --- | --- |
| POST | `/v1/leases` `{wait_timeout_s?, lease_ttl_s?}` | 调用方 | 借用，阻塞等待。200 / 429 队列满 / 503 排空中 / 504 等待超时 |
| GET | `/v1/leases/{id}` | 借用方 | 查询借用 |
| POST | `/v1/leases/{id}/renew` `{ttl_s?}` | 借用方 | 续期（不超过 hard_deadline） |
| DELETE | `/v1/leases/{id}` | 借用方 | 归还（返回时沙箱已销毁） |
| POST | `/v1/leases/{id}/run_code` `{code, language?, timeout_s?}` | 借用方 | 返回 stdout、stderr、text、results（含 png 等）、error |
| POST | `/v1/leases/{id}/commands` `{cmd, cwd?, envs?, timeout_s?}` | 借用方 | 返回 exit_code、stdout、stderr（退出码非 0 不算错误） |
| PUT / GET | `/v1/leases/{id}/files?path=` | 借用方 | 请求体 / 响应体为原始字节；上传超过 `max_upload_bytes` 返回 413 |
| GET | `/v1/pool/stats` | 调用方 | 各状态数量、排队数、分配来源、各操作耗时 p50/p99、是否排空 |
| GET | `/v1/sandboxes` | 管理员 | 调试：沙箱记录（不含 `lease_id`） |
| POST / DELETE | `/v1/admin/drain` | 管理员 | 排空 / 恢复 |
| GET | `/healthz` | 无 | 健康检查 |

「借用方」指创建该借用的调用方，管理员可以操作所有借用。访问别人的借用和借用不存在一样返回 404。

错误码：401 未认证，403 需要管理员，409 借用已结束或已过期，413 上传过大，502 沙箱侧错误。

## 7. 配置（环境变量 `POOL_<字段名大写>`）

| 配置 | 默认 | 说明 |
| --- | --- | --- |
| `POOL_TEMPLATE` | `code-interpreter-v1` | **需要暂停时必须设为第二代模板**（见 `examples/create_gen2_template.py`） |
| `POOL_DB_URL` | `sqlite+aiosqlite:///./.data/pool.db` | 生产可改为 `postgresql+asyncpg://...` |
| `POOL_API_KEYS` / `POOL_ADMIN_KEYS` | 空 | 「名称:key」逗号分隔。都为空时关闭鉴权，此时进程拒绝监听非回环地址（除非 `--allow-no-auth`） |
| `POOL_MAX_SIZE` / `POOL_TARGET_SIZE` | 5 / 5 | 容量上限 / 补货目标 |
| `POOL_MIN_HOT` | 0 | 保持运行、不暂停的空闲数量 |
| `POOL_IDLE_PAUSE_AFTER_S` / `POOL_IDLE_PLATFORM_EXTRA_S` | 60 / 120 | 空闲多久后暂停 / READY 平台超时在此基础上多给的余量 |
| `POOL_QUEUE_MAX` / `POOL_WAIT_TIMEOUT_S` | 10 / 180 | 排队上限 / 最长等待 |
| `POOL_STRICT_FIFO` | false | 所有请求先入队 |
| `POOL_LEASE_TTL_S` / `POOL_LEASE_MAX_S` | 600 / 3600 | 借用期限 / 最长借用时间 |
| `POOL_MAX_AGE_S` | 21600 | 空闲或暂停的沙箱最长寿命 |
| `POOL_OP_TIMEOUT_S` / `POOL_RESUME_TIMEOUT_S` / `POOL_DESTROY_TIMEOUT_S` | 120 / 60 / 30 | 过渡态截止时间（超时即被接管） |
| `POOL_WARMUP_CODE` | `import numpy, pandas, matplotlib` | 预热代码；设为空字符串则不预热 |
| `POOL_CREATE_FAIL_THRESHOLD` / `POOL_CREATE_COOLDOWN_S` | 3 / 60 | 连续创建失败熔断 |
| `POOL_HISTORY_RETENTION_S` / `POOL_CLEANUP_INTERVAL_S` | 604800 / 3600 | 历史记录保留时间 / 清理周期 |
| `POOL_HANDLE_CACHE_MAX` / `POOL_HANDLE_IDLE_TTL_S` | 64 / 600 | 连接句柄缓存上限 / 空闲淘汰时间 |
| `POOL_MAX_UPLOAD_BYTES` | 67108864 | 上传文件大小上限 |

## 8. 运行

```bash
pip install -r requirements-dev.txt
pytest                                  # 单元测试（FakeProvider，不访问云端）

source /path/to/e2b.env                 # E2B_API_KEY / E2B_API_URL / E2B_DOMAIN
export POOL_TEMPLATE=<第二代模板 ID>
export POOL_ADMIN_KEYS="ops:<随机 key>" SANDBOX_POOL_API_KEY=<同一个 key>   # 可选：开启鉴权
scripts/run_local_cluster.sh start      # 3 个副本：8001~8003，共享 .data/pool.db
python scripts/e2e_scenarios.py         # 端到端场景（真实云沙箱），最后一步会排空
scripts/run_local_cluster.sh drain      # 排空：销毁空闲和已暂停的沙箱、停止补货
scripts/run_local_cluster.sh stop
python scripts/cleanup_sandboxes.py     # 兜底：销毁账号下全部沙箱实例并确认清空
```

排空状态记录在库里，重启后仍然有效，需要 `DELETE /v1/admin/drain` 恢复。

## 9. 已知限制与后续

- 排队靠轮询（本地 200ms）。生产环境可以换成 Postgres LISTEN/NOTIFY 或 Redis 通知，减少数据库压力。
- 只支持一个模板；多模板需要把容量、队列、补货都按模板分片。
- 自动加列只处理新增的可空列，生产环境建议用 Alembic 管理表结构。
- 下载文件（`GET files`）仍整体读入内存，大文件可改为流式转发。
- 平台侧单个沙箱的最长存活时间、暂停后的保留时长还未实测，`max_age_s` 先取 6 小时。
