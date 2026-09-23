# 沙箱池服务（sandbox_pool）评审问题修复：改动说明

> 修复方案：`docs/sandbox-pool-fix-plan.md`｜评审报告：`docs/sandbox-pool-review.md`｜设计说明（已按本轮改动更新）：`docs/sandbox-pool-design.md`
> 分支：`claude/inspiring-maxwell-n5z73w`

## 1. 结论

- 评审发现的 **19 个问题（高 2、中 7、低 10）全部修复**，每个问题都有针对性测试。
- 修复过程中又发现并修复了 **2 个新问题**，都记录在第 4 节：
  - 单元测试压测时发现一个同进程死锁；
  - 真实云沙箱端到端测试时发现，经代理的 HTTP/2 连接空闲后会失效。
- 单元测试：47 个用例（原有 20 个 + 新增 27 个），全量连续 13 轮全部通过。关键修复做了反向验证：临时去掉修复后，对应用例都会失败（12/12）。
- 真实云沙箱端到端测试（3 副本、第二代模板、开启鉴权）：全部场景通过，包括两次真实的 kill -9；min_hot 保活单独实测通过。
- 结束后账号下的沙箱已全部销毁（列表接口返回 0 个）；第二代模板 `xu76gk97q07mgohgw7q3` 保留。

## 2. 逐条修复对照

| 编号 | 修复 | 主要改动位置 | 测试（`tests/test_review_fixes.py`） |
| --- | --- | --- | --- |
| H1 | READY 保活：新增 `platform_deadline` 列，剩余时间不足一半时续期；与借出并发时，按借用超时重设平台超时 | `core/maintainer.py` `keepalive_ready` / `_keepalive` | `test_h1_min_hot_sandboxes_survive_platform_idle_timeout`、`test_h1_keepalive_racing_with_claim_restores_lease_timeout` |
| H2 | Bearer 鉴权（调用方 / 管理员两类 key，常量时间比较）；借用绑定调用方（`leases.client_id`，访问他人借用返回 404）；`/v1/sandboxes` 仅管理员且不含 `lease_id`；未配置 key 时拒绝监听非回环地址 | `api/auth.py`（新增）、`api/routes.py`、`__main__.py` | `test_h2_auth_and_lease_binding`、`test_h2_key_parsing_and_public_bind_guard` |
| M1 | 排队心跳改为常驻后台任务；抢到沙箱与排队记录改为 GRANTED 在同一事务内完成，被判超时的也照常交付，不再销毁 | `core/allocator.py` `_Heartbeat`、`store/repository.py` `_grant_waiter` | `test_m1_heartbeat_kept_while_claiming_slow_resume`、`test_m1_grant_after_waiter_deadline_is_still_returned` |
| M2 | 过期回收的 CAS 带 `expires_at <= now` 条件 | `core/lifecycle.py` `end_lease(expired_before=)`、`store/repository.py` `cas_lease` | `test_m2_expiry_does_not_override_concurrent_renew` |
| M3 | 续期先调用平台、成功后再写库；平台失败返回 502，借用不变 | `core/allocator.py` `renew` | `test_m3_renew_platform_failure_keeps_lease_unchanged`、`test_m3_renew_failure_maps_to_502` |
| M4 | 归还时同步销毁；截止时间按操作区分（销毁 30s、恢复 60s）；云端调用设置请求超时 | `core/lifecycle.py`、`core/allocator.py`、`provider/e2b_provider.py` | `test_m4_release_returns_after_sandbox_destroyed`、`test_m4_stuck_destroy_taken_over_after_destroy_timeout` |
| M5 | SQLite 只读连接池（普通 BEGIN + `query_only`）；心跳降频；`expire_waiters` 先读后写 | `store/db.py` `create_read_engine`、`store/repository.py` | `test_m5_reads_not_blocked_by_open_write_transaction` |
| M6 | 句柄缓存 LRU + 空闲淘汰（只丢弃引用，不关共享连接池）；借用结束时 `forget` | `provider/e2b_provider.py` `HandleCache`、`core/allocator.py` `_use` | `test_m6_handle_cache_lru_and_idle_ttl`、`test_m6_ended_lease_forgets_cached_handle` |
| M7 | kv 键初始化时预置；插入冲突时整体重试；创建失败的善后逐步保护，先累加熔断计数再释放名额 | `store/repository.py`、`core/maintainer.py` `_on_create_failed` | `test_m7_kv_keys_prepopulated_and_retry_on_insert_conflict`、`test_m7_create_failure_releases_slot_even_if_counter_fails` |
| L1 | e2e 脚本把仓库根目录加入 `sys.path` | `scripts/e2e_scenarios.py` | 端到端直接用 `python scripts/e2e_scenarios.py` 运行 |
| L2 | 池名从 stats 读取；一致性检查排除空的 `provider_id` | 同上 | 端到端场景 8 |
| L3 | 场景 6 复用 `_pid_of` | 同上 | 端到端场景 6 |
| L4 | 接管 PAUSING / RESUMING 时按云端实际状态收回为 PAUSED / READY，查不到才销毁 | `core/maintainer.py` `_adopt` | `test_l4_stuck_pause_and_resume_adopted_by_actual_state`；端到端场景 6b |
| L5 | 对账每个周期只由一个副本执行（`pool_kv.reconcile_at` 时间戳 CAS） | `store/repository.py` `try_periodic` | `test_l5_periodic_task_has_single_winner_per_interval`、`test_l5_orphan_killed_once_with_two_replicas` |
| L6 | 暂停前在同一事务内（持池级锁行）重新统计 READY | `store/repository.py` `start_pause` | `test_l6_concurrent_pause_respects_min_hot` |
| L7 | 每小时由一个副本分批清理 7 天前已结束的排队、借用记录和事件；新增 `events(pool, ts)` 索引 | `store/repository.py` `purge_history`、`core/maintainer.py` `cleanup` | `test_l7_purge_history_keeps_recent_and_active` |
| L8 | 上传先看 `Content-Length` 再流式计数，超过 64 MiB 返回 413 | `api/routes.py` `_read_body` | `test_l8_upload_size_limit` |
| L9 | 排空：`POST/DELETE /v1/admin/drain`，排空期间不补货、不暂停，空闲 / 暂停的沙箱陆续销毁，借用返回 503；`run_local_cluster.sh drain` | `core/pool.py`、`core/maintainer.py` `drain_idle`、`store/repository.py`（占名额时检查） | `test_l9_drain_destroys_idle_and_blocks_new_leases`、`test_l9_drain_fails_queued_waiters_fast`；端到端场景 9 |
| L10 | `POOL_STRICT_FIFO`：所有请求先入队，队列上限计入可分配沙箱，前 K 个排队请求并行抢 | `core/allocator.py`、`store/repository.py` `enqueue` / `queue_position` | `test_l10_strict_fifo_burst_keeps_queue_semantics` |
| — | 数据库轻量迁移：自动补可空列和索引 | `store/repository.py` `_migrate` | `test_init_schema_adds_new_columns_to_existing_db` |

**反向验证**：逐个去掉以下修复并运行对应用例，12 个用例全部失败，说明这些用例能区分修复前后：
- H1 保活、H1 竞态恢复
- M1 心跳、M1 超时后交付
- M2 过期条件
- M4 同步归还、M4 销毁截止时间
- M5 只读连接
- M7 失败分支
- L4 按状态收回、L5 周期执行权、L6 事务内计数

首轮有两个用例（M1 超时后交付、M4 同步归还）去掉修复后仍然通过，已补强断言：
- M1：断言排队记录最终为 GRANTED；
- M4：Fake 后端增加 kill 延迟，模拟实测约 0.3s 的销毁耗时。

## 3. 行为变化（对调用方可见）

- 开启鉴权（配置 `POOL_API_KEYS` / `POOL_ADMIN_KEYS`）后，除 `/healthz` 外都需要 `Authorization: Bearer <key>`。不配置时行为与之前一致，但只能监听本机地址。
- 借用只能由创建它的调用方操作，其他调用方访问返回 404。
- 归还接口返回时沙箱已销毁（多约 0.3s）。
- 续期失败（平台侧错误）返回 502，借用保持不变，可重试。
- 新增错误码：401、403、413、503。
- `GET /v1/sandboxes` 仅管理员可用，不返回 `lease_id`。
- `/v1/pool/stats` 新增 `pool`、`draining`、`config.ready_platform_timeout_s`、`config.strict_fifo`、`config.auth_enabled`。

## 4. 修复过程中新发现的问题

### 4.1 未关闭的 SQLite 游标被 GC 回收时卡住事件循环（同进程死锁）

- **现象**：新增的启动时预置 kv 键之后，两个池在同一进程内的单元测试约有一半会卡住 30s，然后报 `database is locked`。
- **定位**：
  - 插桩发现，持有写锁的事务在两条语句之间停了 30s；同时间段内只读连接也一起停了。
  - 用看门狗线程采样主线程栈：主线程停在任意一行纯 Python 代码上。
  - 另一个 aiosqlite 线程正在执行 `BEGIN IMMEDIATE`，处于 `busy_timeout` 的忙等中。
  - 单独验证，sqlite3 忙等时会释放 GIL，因此不是 GIL 问题。
- **根因**：
  - 执行失败的语句（这里是预置键时的主键冲突）在 SQLAlchemy 的 aiosqlite 适配层不会关闭游标。
  - 这个游标之后由主线程的循环 GC 回收，回收时调用 `sqlite3_reset`，需要拿该连接的 SQLite 互斥锁。
  - 如果该连接的工作线程正在 `BEGIN IMMEDIATE` 的忙等中，它会一直持有这把互斥锁。
  - 于是主线程（事件循环）被卡住，同进程内持有写锁的协程无法提交，形成死锁，直到 30s 超时。
- **修复**：预置键时先查已有的键，只插入缺失的，正常路径上不再执行预期会失败的语句。修复后该用例 20/20 通过。
- **影响范围**：
  - 生产多进程部署时，持锁方在另一个进程里，不会死锁，但会让事件循环短暂停顿。
  - 首轮开发时遇到的「同一进程多连接出现 busy_timeout 级锁等待」很可能就是同一个原因，当时的规避办法是每进程 1 个写连接。

### 4.2 经 HTTP 代理的 SDK 连接空闲后失效

- **现象**：本轮第一次端到端测试时，空闲 60s 后的暂停、突发借用时的恢复都失败了，异常信息为空。沙箱因此被销毁重建，场景 2 失败。
- **定位**：日志补上异常类型和堆栈后确认是 `httpx.WriteError('')`。SDK 按事件循环共享一个 HTTP/2 连接池，经本地 HTTP 代理出网时，空闲一段时间的连接会被断开，下一个请求在写阶段失败，请求没有到达服务端。
- **为什么首轮没出现**：首轮每个副本每 60s 各自对账（`list`），连接一直保持活跃。L5 改为每个周期只由一个副本对账后，其余副本的连接会空闲几分钟，问题就暴露了。
- **修复**（`provider/e2b_provider.py` 的 `_retry_stale`）：
  - 管控面调用在连接类错误（`NetworkError` / `RemoteProtocolError`）时最多重试两次，间隔 0.3s、1s，包括查询、列表、设置超时、销毁、暂停、连接 / 恢复和恢复后的探活。
  - 同一条 HTTP/2 连接上的并发请求会一起失败，实测有一次需要重试两次才成功。
  - 超时不重试，保证调用耗时不超过过渡态截止时间。
  - 暂停重试失败时，以云端实际状态为准。
  - 创建不是幂等的，只在请求肯定没发出去（`ConnectError` / `WriteError`）时重试。
  - 用户代码执行不重试。
  - 相关失败日志补上异常类型和堆栈。
  - 新增用例 `test_provider_retries_once_on_stale_connection`。

## 5. 测试结果

### 5.1 单元测试（FakeProvider）

全量 47 个用例，连续 13 轮全部通过（每轮约 29s）。另外，两个池同进程的并发用例在死锁修复后单独压测 20 次，0 失败。

### 5.2 端到端（3 副本 + 真实云沙箱，开启鉴权，`POOL_OP_TIMEOUT_S=60`）

本轮共跑了三次，日志在 `.data/round3a` ~ `round3c`、`.data/round3-minhot`（已被 gitignore）：
- 第一次、第二次失败：分别暴露并定位了 4.2 的连接失效问题。
- 第三次（带连接失效重试）全部通过。
- min_hot 保活用最终代码单独跑了一次。

| 场景 | 结果 | 关键数据 |
| --- | --- | --- |
| 0 鉴权 | ✅ | 不带 key、错误 key 都返回 401 |
| 1 预热并在空闲后全部暂停 | ✅ | 5 个全部 PAUSED；调试接口不含 `lease_id` |
| 2 同时 16 个借用请求 | ✅ | 5 个立即拿到（均为恢复，2.3～4.8s），1 个 429，10 个排队 |
| 3 代为执行与隔离 | ✅ | run_code / 跨副本状态保持 / commands 退出码 / 文件读写 / 隔离全部通过 |
| 4 逐个归还，排队请求依次拿到 | ✅ | 从归还到拿到沙箱 p50 3.6s，max 4.3s |
| 5 等待超时 | ✅ | 5.1s 返回 504 |
| 6 归还后立刻 kill -9 | ✅ | **归还返回时沙箱已销毁，被杀副本名下没有残留 DESTROYING 记录**（M4）；首轮这里 5 个容量被占住 70s |
| 6b 暂停途中 kill -9 | ✅ | **3 个卡住的沙箱全部按云端实际状态收回为 PAUSED，没有销毁重建**（L4）；接管后借用走恢复，2.2s |
| 8 云端与库一致 | ✅ | 直接用 `python scripts/...` 运行（L1），池名从 stats 读取（L2） |
| 9 排空 | ✅ | 排空期间借用返回 503；排空后库中无记录、云端该池无沙箱（L9） |
| min-hot 保活（`POOL_MIN_HOT=2`，平台空闲超时 90s） | ✅ | 超过平台超时后再过 152s，2 个热沙箱仍是原沙箱、云端仍在运行，期间续期 13 次；直接借出 0.6s（H1） |
| 收尾 | ✅ | `run_local_cluster.sh drain` 排空后停止集群；`cleanup_sandboxes.py` 与原始列表接口都确认账号下 0 个沙箱 |

第三次运行的耗时统计（`/v1/pool/stats`）：

| 操作 | 次数 | p50 | p99 |
| --- | --- | --- | --- |
| create | 22 | 0.66s | 2.2s |
| warmup | 22 | 2.0s | 2.3s |
| pause（多个同时暂停） | 7 | 14.6s | 15.8s |
| resume | 6 | 2.8s | 4.7s |
| destroy | 22 | 0.37s | 1.2s |

`orphan_killed` 为 4：场景 6 中被 kill -9 的副本在归还后立即开始补货，沙箱已在云端建好但还没记录到库里。这些记录在 `op_deadline` 后被接管销毁，云端的沙箱由对账作为孤儿清理，符合预期。

## 6. 新增 / 修改的文件

| 路径 | 说明 |
| --- | --- |
| `sandbox_pool/api/auth.py` | 新增：鉴权 |
| `sandbox_pool/api/app.py`、`routes.py`、`schemas.py` | 错误码、鉴权依赖、排空接口、上传上限、借用返回 `client_id` |
| `sandbox_pool/__main__.py` | 未配置鉴权时拒绝监听非回环地址 |
| `sandbox_pool/config.py` | 新增配置项（见设计说明第 7 节）；key 字段不出现在 repr 中 |
| `sandbox_pool/models.py` | 新增 `PoolDraining`、`PayloadTooLarge`、`Unauthorized`、`Forbidden` |
| `sandbox_pool/store/db.py` | 只读引擎 |
| `sandbox_pool/store/schema.py` | 新增 `platform_deadline`、`client_id` 列和 `events(pool, ts)` 索引 |
| `sandbox_pool/store/repository.py` | 只读连接、迁移、kv 预置、周期任务 CAS、历史清理、暂停前计数、排队交付、排队位置 |
| `sandbox_pool/core/*.py` | 见第 2 节 |
| `sandbox_pool/provider/e2b_provider.py` | 句柄缓存、请求超时、连接失效重试 |
| `sandbox_pool/provider/fake.py` | 模拟平台超时回收、失败注入、kill 延迟、`set_timeout` 延迟 |
| `scripts/e2e_scenarios.py` | L1～L3、鉴权、6b 新语义、排空场景、min-hot 场景 |
| `scripts/run_local_cluster.sh` | 新增 `drain` |
| `tests/test_review_fixes.py` | 新增 27 个用例 |
| `docs/sandbox-pool-fix-plan.md`、`docs/sandbox-pool-fix-changes.md` | 本轮方案与改动说明 |
| `docs/sandbox-pool-design.md` | 按本轮改动更新 |
