# sxw_fc-sandbox · 云沙箱池服务

基于 [阿里云云沙箱（FC Agent Sandbox）](https://help.aliyun.com/zh/functioncompute/product-overview-of-fc-agent-sandbox) 的**带预热、可排队、多副本安全的沙箱池服务**。服务端 Agent 执行 skill、Python 脚本时，通过 HTTP 从池子借用一个隔离的代码执行环境。调用方不需要 E2B SDK，也不持有云沙箱的 API Key。

> 云沙箱兼容 E2B 协议，但并不是 E2B 开源代码，而是阿里在函数计算自有基础设施上重新实现的协议。本项目用 E2B Python SDK 接入，关于兼容性和实测结论见 [`docs/fc-agent-sandbox-notes.md`](docs/fc-agent-sandbox-notes.md)。

## 特性

- **预热池**：启动后补足 5 个沙箱，并预先 `import numpy, pandas, matplotlib`。空闲 60s 后暂停，暂停时连同内存状态一起保存，按需恢复约 2s，恢复后解释器仍是热的。可用 `min_hot` 保留若干个始终运行的热沙箱，并自动续期平台超时。
- **借用模型**：一次借用对应一个任务，借用期内独占沙箱，解释器状态保持。借用期限 10 分钟，可续期，最长 60 分钟。**归还即销毁**，沙箱不在调用方之间复用，随后补一个新的。
- **排队**：池满后排队（上限 10 个，先来先服务），最长等待 180s；队列满返回 429，等待超时返回 504。可选严格先来先服务模式。
- **代为执行**：在借到的沙箱里执行 `run_code`、`commands`、文件读写。
- **多副本高可用**：副本无状态，所有协调都通过数据库条件更新（CAS）完成，不需要选主。副本崩溃后，其他副本会接管它未完成的操作（按云端实际状态收回或销毁）、清理云端孤儿，并定期对账。本地用「多进程 + 共享 SQLite」模拟，生产可以换 Postgres。
- **运维**：Bearer Token 鉴权（调用方 / 管理员两类 key，借用绑定到调用方）、排空接口、创建失败熔断、历史记录自动清理、`/v1/pool/stats` 耗时统计。
- **常驻 opencode agent（agent 子系统，`POOL_AGENT_ENABLED=true`）**：每个用户一个运行在云沙箱里的 opencode agent（DeepSeek 模型、百炼联网搜索 MCP），通过 `/v1/agents/{user_id}/...` 调用：
  - 支持同步流式对话、长任务断线重连、定时任务；
  - 出网策略可配置，业务系统和 agent 都能读到当前策略；
  - 模型与 MCP 的 Key 由平台在出网时注入，不进沙箱；
  - 按 Eco 规则设计：沙箱最长 24h，自动轮换，支持空闲销毁。
  - 接入方式见 [业务接入使用手册](sxw_aicoding/2026-09-25-opencode常驻agent-业务接入使用手册.md)。

## 架构

```
   Agent 服务 A    Agent 服务 B ...
        │ HTTP（Bearer API Key）：借用 / 续期 / 归还 / run_code / commands / files
        ▼
 ┌─────────────┐ ┌─────────────┐ ┌─────────────┐
 │ pool 副本 1 │ │ pool 副本 2 │ │ pool 副本 3 │   无状态，每个副本都跑后台维护
 └──────┬──────┘ └──────┬──────┘ └──────┬──────┘
        └──────── 共享数据库（唯一事实来源）─┘
          本地：SQLite（WAL，多进程共享）；生产：Postgres
                        │
        SandboxProvider ──► E2BProvider（阿里云云沙箱，E2B SDK）
                        └► FakeProvider（单元测试）
```

沙箱状态：`CREATING → WARMING → READY ⇄ PAUSING → PAUSED → RESUMING → LEASED → DESTROYING`。

分配优先级是运行中（READY）> 暂停中（PAUSED，需先恢复）> 排队。状态机、多副本安全和崩溃接管的细节见 [设计说明](docs/sandbox-pool-design.md)。

## 快速开始

### 1. 安装

```bash
python3.11 -m venv .venv && . .venv/bin/activate
pip install -r requirements-dev.txt
```

> 必须使用 `e2b==2.31.0` 和 `e2b-code-interpreter==2.8.1`（已在 `requirements.txt` 中固定）。更新的 SDK 走 `/v2` 接口，云沙箱不支持。

### 2. 准备第二代模板（需要暂停 / 恢复时）

内置模板属于第一代运行时，暂停需要账号开通白名单。第二代运行时（microVM）默认支持暂停和恢复，但只能通过阿里云 OpenAPI 创建，需要 AccessKey 和 `fcsandbox:CreateTemplate` 权限：

```bash
export ALIBABA_CLOUD_ACCESS_KEY_ID=... ALIBABA_CLOUD_ACCESS_KEY_SECRET=...
export FCSANDBOX_REGION_ID=cn-hangzhou FCSANDBOX_TEAM_ID=<team-id>
python examples/create_gen2_template.py            # 输出模板 ID
```

脚本基于官方代码解释器镜像创建模板，并设置好第二代运行时所需的 `start_command` / `ready_command`，否则代码解释器不会启动，`run_code` 会返回 500。

### 3. 启动服务

```bash
export E2B_API_KEY=... E2B_API_URL=https://api.cn-hangzhou.e2b.fc.aliyuncs.com E2B_DOMAIN=cn-hangzhou.e2b.fc.aliyuncs.com
export POOL_TEMPLATE=<第二代模板 ID>
export POOL_API_KEYS="agent-a:<key-a>"  POOL_ADMIN_KEYS="ops:<admin-key>"   # 鉴权（可选）

python -m sandbox_pool --host 127.0.0.1 --port 8000       # 单个副本
# 或本地模拟多副本：3 个进程（8001~8003）共享 .data/pool.db
scripts/run_local_cluster.sh start
```

- 需要通过 HTTP 代理出网时，设置 `HTTPS_PROXY` 即可（SDK 本身不读这个变量，本项目会显式传入）。
- 未配置任何 API Key 时服务不做鉴权，此时只允许监听本机地址。

### 4. 调用

```bash
H="Authorization: Bearer <key-a>"
# 借用（阻塞等待，最长 180s）
LEASE=$(curl -s -X POST localhost:8000/v1/leases -H "$H" -H 'content-type: application/json' -d '{}' | python -c 'import json,sys;print(json.load(sys.stdin)["lease_id"])')

curl -s -X POST localhost:8000/v1/leases/$LEASE/run_code -H "$H" -H 'content-type: application/json' \
     -d '{"code": "import pandas as pd\nx = pd.Series([1,2,3]).sum()\nprint(x)"}'
curl -s -X POST localhost:8000/v1/leases/$LEASE/run_code -H "$H" -H 'content-type: application/json' -d '{"code": "print(x * 2)"}'   # 状态保持
curl -s -X POST localhost:8000/v1/leases/$LEASE/commands -H "$H" -H 'content-type: application/json' -d '{"cmd": "uname -a"}'
curl -s -X PUT  "localhost:8000/v1/leases/$LEASE/files?path=/tmp/in.csv" -H "$H" --data-binary @in.csv
curl -s -X POST localhost:8000/v1/leases/$LEASE/renew -H "$H" -H 'content-type: application/json' -d '{"ttl_s": 1200}'
curl -s -X DELETE localhost:8000/v1/leases/$LEASE -H "$H"          # 归还：沙箱随即销毁
```

## HTTP 接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/v1/leases` `{wait_timeout_s?, lease_ttl_s?}` | 借用。200 / 429 队列满 / 503 排空中 / 504 等待超时 |
| GET | `/v1/leases/{id}` | 查询借用 |
| POST | `/v1/leases/{id}/renew` `{ttl_s?}` | 续期（不超过借出后 60 分钟） |
| DELETE | `/v1/leases/{id}` | 归还（返回时沙箱已销毁） |
| POST | `/v1/leases/{id}/run_code` `{code, language?, timeout_s?}` | 返回 stdout、stderr、text、results（含图片）、error。执行超时返回 200、`error.name=TimeoutError` |
| POST | `/v1/leases/{id}/commands` `{cmd, cwd?, envs?, timeout_s?}` | 返回 exit_code、stdout、stderr（退出码非 0 不视为错误）。执行超时返回 200、`exit_code=-1`、`error` 以 `TimeoutError` 开头 |
| PUT / GET | `/v1/leases/{id}/files?path=` | 上传 / 下载原始字节（上传默认上限 64 MiB） |
| GET | `/v1/pool/stats` | 各状态数量、排队数、分配来源、各操作耗时 p50 / p99 |
| GET | `/v1/sandboxes` | 沙箱列表（管理员）。不含 `lease_id`，借出中的附带借用方和到期时间 |
| DELETE | `/v1/sandboxes/{id}` | 强制销毁沙箱（管理员），借出中的先结束借用；用于处理卡住的借用 |
| POST / DELETE | `/v1/admin/drain` | 排空 / 恢复（管理员） |
| GET | `/healthz` | 健康检查（不鉴权） |

- 借用只能由创建它的调用方操作，访问别人的借用返回 404。
- 其他错误码：401 未认证、403 需要管理员、404 借用或沙箱不存在、409 借用已结束（或沙箱处于过渡态）、413 请求体过大、502 沙箱侧错误。
- 代码或命令执行超时不是 502：代码可能已经执行了一部分，调用方不要自动重试。
- 上传文件以外的请求体不超过 1 MiB（`POOL_MAX_BODY_BYTES`），在鉴权之前检查。

## 配置

所有配置都通过环境变量 `POOL_<字段名大写>` 覆盖，完整列表见 [`sandbox_pool/config.py`](sandbox_pool/config.py) 和 [设计说明第 7 节](docs/sandbox-pool-design.md#7-配置环境变量-pool_字段名大写)。常用的：

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `POOL_TEMPLATE` | `code-interpreter-v1` | 需要暂停时必须用第二代模板 |
| `POOL_DB_URL` | `sqlite+aiosqlite:///./.data/pool.db` | 生产可换成 `postgresql+asyncpg://...` |
| `POOL_API_KEYS` / `POOL_ADMIN_KEYS` | 空 | 「名称:key」，逗号分隔 |
| `POOL_MAX_SIZE` / `POOL_TARGET_SIZE` / `POOL_MIN_HOT` | 5 / 5 / 0 | 容量上限 / 补货目标 / 保持运行的数量（设为 ≥1 可避免空闲后的首个请求等一次完整的暂停 + 恢复，约 16s） |
| `POOL_IDLE_PAUSE_AFTER_S` | 60 | 空闲多久后暂停 |
| `POOL_QUEUE_MAX` / `POOL_WAIT_TIMEOUT_S` | 10 / 180 | 排队上限 / 最长等待 |
| `POOL_LEASE_TTL_S` / `POOL_LEASE_MAX_S` | 600 / 3600 | 借用期限 / 最长借用时间 |
| `POOL_WARMUP_CODE` | `import numpy, pandas, matplotlib` | 预热代码 |
| `POOL_OP_TIMEOUT_S` | 120 | 创建、预热、暂停的截止时间，必须大于 45（否则拒绝启动） |
| `POOL_MAX_UPLOAD_BYTES` / `POOL_MAX_BODY_BYTES` | 64 MiB / 1 MiB | 上传文件 / 其余请求体的大小上限 |

## 测试

```bash
python -m pytest                                   # 单元测试：FakeProvider，不访问云端，约 30s
python -m pytest tests/test_review_fixes.py -k h1  # 单个用例
```

在真实云沙箱上跑端到端测试会产生费用：

```bash
scripts/run_local_cluster.sh start
python scripts/e2e_scenarios.py                    # 预热暂停、突发借用、代为执行、排队、504、副本 kill -9、排空等
scripts/run_local_cluster.sh drain && scripts/run_local_cluster.sh stop
python scripts/cleanup_sandboxes.py                # 兜底：销毁账号下全部沙箱并确认清空
```

最近一轮结果（第二轮评审修复后）：60 个单元测试连续多轮通过；3 副本端到端测试全部场景通过，包括两次真实的 `kill -9` 崩溃接管、执行超时和管理员强制释放。详见 [`docs/sandbox-pool-r2-fix-changes.md`](docs/sandbox-pool-r2-fix-changes.md)。

## 实测数据（cn-hangzhou，第二代模板）

| 操作 | p50 | 说明 |
| --- | --- | --- |
| 创建 | 0.7s | p99 约 2.2s |
| 预热 | 2.0s | `import numpy, pandas, matplotlib` |
| 暂停 | 10～16s | 多个同时暂停时更慢 |
| 恢复 | 2.3～2.8s | 内存状态保留 |
| 销毁 | 0.37s | |
| 从归还到排队请求拿到沙箱 | 3.6s | 包括创建和预热 |

## 目录

```
sandbox_pool/   服务代码：api/（路由、鉴权）、core/（分配、维护、生命周期）、agent/（常驻 opencode agent）、store/（数据库）、provider/（沙箱后端）
tests/          单元测试（FakeProvider、内存版 opencode）
scripts/        本地多副本集群、端到端场景、沙箱清理、opencode 模板构建与云上验证
sxw_aicoding/   agent 子系统的调研、方案、手册与测试报告
examples/       云沙箱摸底：生命周期 demo、第二代模板创建
docs/           调研、方案、设计、评审与改动说明
```

## 文档

| 文档 | 内容 |
| --- | --- |
| [fc-agent-sandbox-notes.md](docs/fc-agent-sandbox-notes.md) | 云沙箱调研与实测结论（SDK 版本、代理、第二代模板、耗时、与 E2B 的关系） |
| [sandbox-pool-plan.md](docs/sandbox-pool-plan.md) | 沙箱池实施方案 |
| [sandbox-pool-design.md](docs/sandbox-pool-design.md) | 设计说明（当前版本） |
| [sandbox-pool-changes.md](docs/sandbox-pool-changes.md) | 首轮实现的改动说明 |
| [sandbox-pool-review.md](docs/sandbox-pool-review.md) | 回归测试与代码评审（19 个问题） |
| [sandbox-pool-fix-plan.md](docs/sandbox-pool-fix-plan.md) / [sandbox-pool-fix-changes.md](docs/sandbox-pool-fix-changes.md) | 评审问题修复方案与改动说明 |
| [sandbox-pool-r2-fix-changes.md](docs/sandbox-pool-r2-fix-changes.md) | 第二轮评审的复核结论与修复说明 |
| [opencode 常驻 agent 实施方案](sxw_aicoding/方案设计/2026-09-25-opencode应用沙箱池-实施方案.md) | agent 子系统的设计（数据模型、状态机、出网策略、流式协议、定时任务）与执行结果 |
| [opencode 调研](sxw_aicoding/技术调研/2026-09-25-opencode云沙箱常驻agent调研.md) / [PoC 验证报告](sxw_aicoding/技术调研/2026-09-25-opencode云沙箱PoC验证报告.md) | 业界方案、平台与 opencode 实测结论 |
| [业务接入使用手册](sxw_aicoding/2026-09-25-opencode常驻agent-业务接入使用手册.md) / [测试报告](sxw_aicoding/2026-09-25-opencode常驻agent-测试报告.md) | agent 子系统的接入、配置、运维、排障与测试结果 |

## 已知限制

- 排队靠轮询数据库（默认 200ms）；规模变大后可以换成 Postgres LISTEN/NOTIFY 或 Redis 通知。
- 只支持单一模板。
- 自动迁移只处理新增的可空列；生产环境建议用 Alembic 管理表结构。
- 下载文件时整个文件会读入内存。
