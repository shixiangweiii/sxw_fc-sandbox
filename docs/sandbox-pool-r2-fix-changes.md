# 沙箱池服务第二轮评审：复核结论与修复

- 评审报告：`sxw_aicoding/代码评审/2026-09-23-沙箱池服务第二轮代码评审报告.md`（评审对象 `b4f8789`，28 个问题：高 2、中 12、低 14）。
- 复核方式：逐条对照代码，并阅读已安装的 `e2b==2.31.0`、`e2b-code-interpreter==2.8.1`、uvicorn、FastAPI 源码核实前提；关键结论用 FakeProvider 实验和真实云沙箱实测确认。
- 结论：报告整体扎实，但两个「高」都高估了；有 7 条不成立或描述不准；另外复核时发现一个报告漏掉的真实问题（R2-N1）。
- 修复遵循 CLAUDE.md 的测试约定：每条先写用例、在修复前的代码上确认失败，再修复。用例在 `tests/test_review_r2_fixes.py`。

## 1. 已修复

| 编号 | 复核结论 | 修改 | 测试 |
| --- | --- | --- | --- |
| **R2-N1**（新发现） | **成立，中**。排队请求抢到的 READY 沙箱 `set_timeout` 失败时，借用被结束，但排队记录停在 GRANTED（在抢沙箱的同一事务内写入）；心跳发现记录不是 WAITING 就判定丢失，请求收到 504「waiter expired」。实验：请求要等 20s，0.87s 就 504 了，而补货的沙箱马上就绪 | 失败时把排队记录改回 WAITING；心跳把 GRANTED 也视为有效（只有请求自己会写 GRANTED） | `test_r2_n1_queued_claim_of_unusable_sandbox_keeps_waiting` |
| R2-H1 | **高估，实际中低**。报告说的取消路径在当前部署下基本不会发生：uvicorn 在客户端断开时不取消处理协程（`send` 变成空操作），优雅停机默认无限等待在途请求。借用到期会被回收，报告说「不能自愈」也不对。真正的缺口是：直接抢（不排队）的路径完全不检查请求方是否断开 | 直接抢到的借用也经过 `_hand_over`，交付前请求方已断开就立即归还。响应发出后才断开的情况服务端无法感知，由 R2-L6 的管理接口兜底 | `test_r2_h1_direct_claim_released_when_client_disconnected` |
| R2-H2 | **高估，实际低**。报告给的两副本交错需要保活调用在 CAS 之后超过 timeout/2（默认 90s）才失败，而该调用的请求超时只有 30s，默认配置下不可达。后果也写重了：借出时 `_claim_ready` 会换下一个沙箱，调用方不会收到 502。但改动只有一行且更稳妥 | 失败时把 `platform_deadline` 清空，只带状态条件、不带版本条件，下一轮立即重试 | `test_r2_h2_failed_keepalive_never_leaves_overstated_deadline`（构造了报告里的交错时序） |
| R2-M1 | **成立，低**。机制属实（已确认 `connect(timeout=…)` 会重设平台超时），但要同时满足关闭暂停、且 64 个以上并发创建把句柄挤出缓存才会触发 | `warmup` 增加 `sandbox_timeout_s` 参数，按 READY 的平台超时连接 | `test_r2_m1_warmup_connect_uses_ready_platform_timeout` |
| R2-M2 | **成立，中低**。create / pause 没传 `request_timeout`，回落到 SDK 默认的 60s，文件头注释「每个云端调用都设了请求超时」不属实；照 CLAUDE.md 的 `POOL_OP_TIMEOUT_S=60` 运行时 60 ≮ 60。报告里「create + warmup = 120 > 60」**不对**：进入 WARMING 时截止时间会重新计算，两者不累加 | 显式设置请求超时：创建 30s、预热 45s、暂停 45s；启动时 `check_deadlines` 校验三类截止时间，不满足就拒绝启动 | `test_r2_m2_create_and_pause_have_request_timeouts`、`test_r2_m2_deadlines_checked_against_request_timeouts`、`test_r2_m2_server_refuses_to_start_with_too_short_deadlines` |
| R2-M5 | **成立，且比报告说的更严重**。FastAPI 在解析依赖（包括鉴权）之前就读完并解析 JSON 请求体（`fastapi/routing.py` 先读 body、后 `solve_dependencies`），所以**不带 key 的请求**也能让副本缓冲任意大的请求体 | 新增纯 ASGI 中间件 `api/body_limit.py`：先看 Content-Length，再对分块请求体流式计数，超过 `max_body_bytes`（默认 1 MiB）返回 413；上传文件的接口放行（它先鉴权、再按 `max_upload_bytes` 流式计数）。没有用 Starlette 1.x 自带的中间件，因为 `requirements.txt` 没有固定 Starlette 版本 | `test_r2_m5_request_body_limited_before_auth` |
| R2-M7 | **成立，中低**。已确认 `last_active_at` 只对 READY 有读者，借出中的沙箱归还即销毁、不会回到 READY | 删掉 `_use` 里的写库和 `Store.touch_sandbox` | `test_r2_m7_exec_does_not_write_sandbox_row` |
| R2-M11 | **成立，中**。SDK 在代码执行超时时抛 `TimeoutException`，被 `_guard` 包成 502；调用方按 502 重试会重跑非幂等代码 | provider 按调用方的 `timeout_s` 是否基本用完区分：用完了是执行超时（`ExecutionTimeout`），更早的是连接 / 请求超时、仍返回 502。执行超时返回 200：`run_code` 的 `error.name=TimeoutError`，`commands` 的 `exit_code=-1`、`error` 以 `TimeoutError` 开头；记录 `exec_timeout` 事件 | `test_r2_m11_execution_timeout_is_a_result_not_502`、`test_r2_m11_provider_tells_execution_timeout_from_backend_timeout`；端到端场景 3 在真实 SDK 上验证 |
| R2-M12 | **成立，低**。调用方拿到「销毁未完成」也做不了什么，记录下来便于排查即可 | kill 失败时记录 `destroy_failed` 事件（`/v1/pool/stats` 可见） | `test_r2_m12_failed_destroy_on_release_is_recorded` |
| R2-L6 | **成立**。R2-H1 剩下的情况需要人工兜底 | `GET /v1/sandboxes` 给借出中的沙箱附带借用方和到期时间（仍不返回 `lease_id`：它是借用凭证，第一轮评审 H2 特意去掉的）；新增 `DELETE /v1/sandboxes/{id}`（管理员）：借出中的先结束借用，空闲 / 已暂停的直接销毁，过渡态返回 409 | `test_r2_l6_admin_finds_and_force_releases_stuck_lease`；端到端场景 10 |
| R2-L3 | **成立** | `git rm --cached` 两个 `examples/__pycache__/*.pyc`（`.gitignore` 早已包含 `__pycache__/`） | — |
| R2-M4 | **行为属实，但报告给的两个改法都没有收益**：进行中的暂停无法取消，让请求「抢 PAUSING 的沙箱」同样要等暂停完成再恢复 | 不改代码。文档说明用已有的 `POOL_MIN_HOT>=1` 保留热沙箱 | — |

## 2. 不成立或描述不准（不修）

| 编号 | 复核结论 | 依据 |
| --- | --- | --- |
| R2-M3 | **不成立**：排空时排队的请求会依次快速失败 | 实验：1 个借出 + 5 个排队时排空，5 个请求在 0.18～0.37s 内全部收到 `PoolDraining`（503）。队首被取消后，下一个请求的「前面人数」变成 0，走进检查排空的分支，逐个传递 |
| R2-M10 | **不成立**：大文件不会因 30s 请求超时失败 | SDK 源码（`e2b/sandbox_async/filesystem/filesystem.py` 上传部分）注明请求超时按每次写入生效（httpx 的分阶段超时），不是整个传输的总时长；下载同理 |
| R2-L1 | **不成立**：暂停中的沙箱不受平台超时回收 | 真实云沙箱实测：`timeout=60` 创建后立即暂停，超过 `end_at` 约 50s 仍是 `paused`，恢复后解释器变量还在（见 `fc-agent-sandbox-notes.md` 第 4 节）。说明里「TTL 在暂停期间累计」指的是约 6 小时的最长存活，已由 `max_age_s` 处理 |
| R2-L7 | 设计如此 | 正在恢复的沙箱计入可用数，而恢复它的请求仍是 WAITING、计入排在前面的人数，两者抵消；注释里写明了原因 |
| R2-L13 | 前提不成立 | `AsyncSandbox.create` 只有一次 HTTP 调用，`WriteError` 说明请求没发出去。另外，即使出现重复沙箱，因为 `metadata.pool_row` 对得上，对账也不会把它当孤儿，而是由平台超时（默认 180s）回收 |
| R2-L14 | 描述不准 | 任务结束时 done callback 就把它移出集合，异常在任务被回收时由 asyncio 立即打日志，不是「要等到停机」 |
| R2-L12 第 4 项 | 已有覆盖 | `test_l8_upload_size_limit` 已经测了没有 Content-Length 的分块上传 |
| R2-M8 | 理论上成立，无害 | 只有在 `forget`（暂停、恢复、销毁、借用结束）与多个并发 `_handle` 交错时才会多 connect 一次，而此时沙箱本来就在被回收或恢复 |
| R2-M9 | 潜伏问题 | `kv_set` / `kv_incr` 用到的键都在 `_KV_KEYS` 里预置，INSERT 分支走不到；CLAUDE.md 已要求新增键时加到 `_KV_KEYS` |
| R2-M6 | 需求决策，不是缺陷 | 按调用方的配额留到多租户上线前再做 |
| R2-L2、L4、L5、L8、L9、L10、L11 | 描述属实，暂不处理 | 配置校验、`/healthz` 返回副本 ID、幂等键、Retry-After、池级锁热点、对账读放大、Fake 语义缺失，都是可选改进。注意改 L4 时，端到端场景 6 / 6b 依赖 `/healthz` 返回的 `replica` |

## 3. 接口变化

- `GET /v1/sandboxes`：每条记录新增 `lease` 字段（借出中为 `client_id`、`source`、`created_at`、`expires_at`、`hard_deadline`，否则为 null），仍不含 `lease_id`。
- 新增 `DELETE /v1/sandboxes/{id}`（管理员）：返回 `{id, provider_id, previous_state, lease_ended}`；记录不存在返回 404，过渡态返回 409。
- `run_code` / `commands` 执行超时：由 502 改为 200 + `TimeoutError`。
- 上传文件以外的请求体超过 `POOL_MAX_BODY_BYTES`（默认 1 MiB）返回 413，在鉴权之前检查。
- 启动时 `POOL_OP_TIMEOUT_S` / `POOL_RESUME_TIMEOUT_S` / `POOL_DESTROY_TIMEOUT_S` 必须分别大于 45 / 45 / 20，否则拒绝启动。
- `/v1/pool/stats` 的事件里新增 `exec_timeout`、`destroy_failed`。

## 4. 验证

### 4.1 单元测试

- 60 个用例（原有 47 个 + 本轮 13 个）。本轮每个用例都先在修复前的代码上确认失败，再修复。
- 连跑：顺序 41 轮全部通过（另有 1 轮卡住，见 4.3）；4 个进程并行各 15 轮，59 轮通过。并行时失败的 1 轮是第一轮的 `test_m1_heartbeat_kept_while_claiming_slow_resume`：它把心跳超时设为 0.5s，4 个进程抢 CPU 时一次心跳晚了 0.5s 以上，排队记录被判超时。该用例顺序运行时从未失败，与本轮改动无关（本轮对心跳只是多接受 GRANTED 状态，不影响时序）。

### 4.2 真实云沙箱端到端（cn-hangzhou，第二代模板，3 副本，开启鉴权，`POOL_OP_TIMEOUT_S=60`）

全部场景通过（`scripts/e2e_scenarios.py`，退出码 0），结束后排空、停集群并运行 `cleanup_sandboxes.py`，账号下沙箱为 0。

| 场景 | 结果 |
| --- | --- |
| 0 鉴权 | 不带 / 错误 key 返回 401；2 MiB 的请求体不带 key 直接返回 413（R2-M5） |
| 1 预热并暂停 | 5 个全部 PAUSED（140s，其中一次暂停因连接失效失败、沙箱被重建，见第 5 节） |
| 2 突发 16 个 | 5 个立即拿到（恢复，1.2～3.4s）、10 个排队、1 个 429 |
| 3 代为执行 | 执行、状态保持、隔离、文件都正常。`time.sleep(8)`、`timeout_s=3`：run_code 3.0s 返回 200 + `TimeoutError`，commands 3.1s 返回 200 + `exit_code=-1`；同一借用的下一次 run_code 只用 0.1s（R2-M11） |
| 4 排队 | 10 个排队请求依次拿到，归还→拿到 p50 2.3s、max 5.6s |
| 5 等待超时 | 504（5.2s） |
| 6 归还后 kill -9 | 其余副本 80s 内恢复到 5 个 |
| 6b 暂停途中 kill -9 | 卡住的 1 个 PAUSING 按云端实际状态收回为 PAUSED（70s），没有销毁重建 |
| 10 管理员强制释放 | 管理接口看到借用方和到期时间、不含 `lease_id`；强制释放后借用方访问返回 409，云端沙箱已销毁，5s 内补回（R2-L6） |
| 8 无孤儿 | 库中 5 条与云端 5 个一一对应 |
| 9 排空 | 库中和云端都为 0 |

耗时：创建 p50 0.36s，预热 p50 1.2s，暂停 p50 15.5s，恢复 p50 1.3s，销毁 p50 78ms。

执行超时之后下一次调用很快，说明客户端断开时服务端中断了超时的执行，解释器没有被一直占住（SDK 为执行接口强制使用 HTTP/1.1，断开请求即断开 TCP 连接）。

### 4.3 一次未能复现的卡住

- 第一批顺序连跑的第 5 轮卡住：pytest 进程 CPU 约 97%，3 分钟以上不结束（按 CPU 时间推算，在机器休眠之前就已开始，与休眠无关）。当时的循环只保留了每轮最后一行输出，发 SIGABRT 得到的 faulthandler 栈没有保存下来，无法确定卡在哪个用例。
- 之后开启 faulthandler 超时转储，又跑了顺序 37 轮、并行 60 轮，都没有复现。
- 排查时发现本轮 R2-M11 的用例曾用 `while True: pass` 作为代码参数（按设计不会被执行，注入的超时会先抛出），而 FakeProvider 会真的执行代码：一旦注入没生效，就是一模一样的死循环。已改为万一被执行也会立即报错的代码。没有找到注入会失效的路径，所以不能断定这就是原因；如果再次出现，用 `-o faulthandler_timeout=60` 运行即可拿到卡住时的栈。

## 5. 复核与验证中的新发现（不在本轮修复范围）

- **SDK 空闲连接失效时重试次数可能不够**：端到端预热阶段，一次暂停连续 3 次 `WriteError('')`（重试两次仍失败），沙箱被销毁重建。原因：SDK 的连接池保留最多 20 个空闲连接、300s 才过期（`E2B_KEEPALIVE_EXPIRY`），云端约 60s 就断开空闲连接；启动时并发创建留下了多条连接，空闲后全部失效，3 次尝试各取到一条失效连接。建议让空闲连接在云端断开之前过期（例如 `E2B_KEEPALIVE_EXPIRY=30`），已单独立项。
