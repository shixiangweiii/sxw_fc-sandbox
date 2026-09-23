# 沙箱池服务（sandbox_pool）Demo 实施计划

## Context

服务端 Agent 执行 skill、Python 脚本时，需要一个纯粹的工具执行环境。前期已完成阿里云云沙箱摸底（`docs/fc-agent-sandbox-notes.md`）：
- 必须固定 E2B SDK 2.31.0，并显式传代理。
- 第二代模板 `xu76gk97q07mgohgw7q3` 支持暂停/恢复，且保留内存状态。
- 实测耗时：创建约 2s、暂停约 10s、恢复约 1.5s；恢复后 `run_code` 约 0.4s，冷创建后首次约 2s。

本次在仓库 `sandbox_pool/` 下实现一个带预热的沙箱池 HTTP 服务 demo，用于验证摸底，不承接生产流量。架构上要为生产多副本高可用做准备：代码不假设单实例，本地用多进程共享 SQLite 模拟多副本。

**已确认的需求**
- 容量上限 5，借出中/空闲/暂停/创建中都计入；满了就排队。队列最多 10 个，先来先服务，最多等 180s；队列满返回 429，等待超时返回 504。
- 启动时冷创建 5 个并预热；空闲 60s 后暂停；`min_hot` 默认 0。借用时优先给运行中的，其次恢复暂停的，然后独占。
- 一次借用对应一个任务，**归还即销毁**，异步补货。
- 借用期限 10 分钟，可续期，最长 60 分钟；过期强制回收。
- 池子代为执行 `run_code` / `commands` / `files`，调用方不需要 SDK 和 Key。
- 模板用官方代码解释器镜像建的第二代模板，通过 `POOL_TEMPLATE` 配置。
- 技术栈：FastAPI + SQLAlchemy Core（async；本地 aiosqlite，以后换 asyncpg）+ E2B 异步 SDK。
- **代码写完、测试完后，销毁账号下所有沙箱实例。**

## 目录结构

```
sandbox_pool/
  __main__.py            # python -m sandbox_pool：启动 uvicorn
  config.py              # 从 POOL_* 环境变量读取配置的 dataclass
  models.py              # SandboxState / LeaseState / WaiterState 枚举
  store/
    schema.py            # SQLAlchemy Core 表定义
    db.py                # 建 async engine；SQLite 开 WAL、busy_timeout，写事务用 BEGIN IMMEDIATE
    repository.py        # 所有状态变更都是带 version 的条件更新（CAS）；不用任何 SQLite 专有语法
  provider/
    base.py              # SandboxProvider 协议
    e2b_provider.py      # 基于 e2b_code_interpreter.AsyncSandbox；显式 proxy；本副本内缓存连接句柄
    fake.py              # 内存 Fake 实现，可注入延迟和失败，单元测试用
  core/
    allocator.py         # acquire / renew / release / 代为执行时的借用校验
    maintainer.py        # 后台维护循环（每个副本都跑，靠 CAS 保证同一件事只有一个副本做成）
  api/
    app.py               # FastAPI 应用工厂；lifespan 中启动/停止 maintainer
    schemas.py / routes.py
scripts/
  run_local_cluster.sh   # 起 3 个进程（端口 8001~8003），共享 .data/pool.db
  e2e_scenarios.py       # 针对真实云沙箱的场景验证，输出耗时统计
  cleanup_sandboxes.py   # 销毁账号下所有沙箱（含暂停中的），并确认 list 为空
tests/                   # pytest + pytest-asyncio + httpx ASGITransport，全部使用 FakeProvider
docs/sandbox-pool-design.md
```

**复用已有成果**
- `requirements.txt` 中固定的 `e2b==2.31.0`、`e2b-code-interpreter==2.8.1`。
- 显式传 `HTTPS_PROXY` 的做法：`examples/sandbox_lifecycle_demo.py`。
- 第二代模板的创建方式：`examples/create_gen2_template.py`。
- 凭据只从环境变量 / `/root/.config/fc-sandbox/*.env` 读取，不入库。

新增依赖：fastapi、uvicorn、sqlalchemy[asyncio]、aiosqlite；开发依赖 pytest、pytest-asyncio。

## 数据模型

| 表 | 关键字段 |
|---|---|
| `sandboxes` | `id`（池内 uuid）、`provider_id`、`template`、`state`、`version`、`lease_id`、`created_at`、`state_changed_at`、`last_active_at`、`op_owner`、`op_deadline`、`error` |
| `leases` | `id`、`sandbox_row_id`、`state`（ACTIVE/RELEASED/EXPIRED/FAILED）、`created_at`、`expires_at`、`hard_deadline`（= 创建时间 + 60 分钟） |
| `waiters` | `seq`（自增，用于先来先服务）、`state`（WAITING/GRANTED/TIMEOUT/CANCELLED）、`owner_replica`、`deadline`、`heartbeat_at`、`lease_id` |
| `pool_kv` | 共享计数器，例如连续创建失败次数、熔断截止时间 |
| `events` | 状态流转审计：时间、沙箱、from → to、副本、耗时。用于排查和统计 p50/p99 |

**沙箱状态**：`CREATING → WARMING → READY ⇄ (PAUSING → PAUSED → RESUMING) → LEASED → DESTROYING`，任一步骤失败进入 `BROKEN`。

**耗时操作的做法**：先用 CAS 把状态改成过渡态，并写入 `op_owner` 和 `op_deadline`，再调用云端接口，完成后再 CAS 到目标状态。如果执行的副本中途崩溃，其他副本发现 `op_deadline` 已过，就把沙箱转为 DESTROYING，销毁并补货。

## 核心逻辑

**acquire（`core/allocator.py`）**
1. 队列为空时，直接尝试 `try_claim()`，成功即返回。
2. 否则在写事务里判断 WAITING 数量是否小于 10，满了返回 429，没满就入队。
3. 每 200ms 轮询一次：先刷新心跳；如果自己是队首（WAITING 中 `seq` 最小），就执行 `try_claim()`。
4. 超过 deadline：把自己的记录从 WAITING CAS 到 TIMEOUT，返回 504。

**try_claim**
1. 找候选：READY 优先，其次 PAUSED。
2. READY：CAS 到 LEASED，然后 `set_timeout(借用剩余时间 + 60s)`。
3. PAUSED：CAS 到 RESUMING，调用 `connect(timeout=…)` 恢复，再用 `commands.run("true")` 探活，成功后 CAS 到 LEASED。失败则转 DESTROYING，换下一个候选重试。
4. 没有候选（只有 PAUSING / CREATING 中的）：继续等。
5. 给自己的 waiter 记录做 CAS（WAITING → GRANTED）失败时，把刚拿到的借用释放掉。

**release / 过期**：借用 CAS 到 RELEASED 或 EXPIRED，沙箱 CAS 到 DESTROYING → 调用 `kill` → 删除记录 → 触发本副本立即补货。

**renew**：不能超过 `hard_deadline`；续期后同步 `set_timeout`。

**代为执行（run_code / commands / files）**
- 先校验借用有效（ACTIVE 且未过期），并更新 `last_active_at`。
- 复用本副本缓存的 AsyncSandbox 句柄；没有缓存时 `connect(timeout=借用剩余时间 + 60s)`。
- 命令退出码非 0 时正常返回 exit_code，不当作错误。

**maintainer**：每个副本每约 1s（带随机抖动）执行一轮：
1. 清理排队记录：心跳超过 5s 未更新或已过 deadline 的，置为 TIMEOUT。
2. 补货：总数小于 target 时，在容量事务里先插入一条 CREATING 记录占住名额；调用 `create(metadata={pool, row_id})`；进入 WARMING，执行 `run_code("import numpy, pandas, matplotlib")` 预热；然后转为 READY，并 `set_timeout(idle + 120s)` 作为兜底。连续失败 3 次触发熔断，冷却 60s。
3. 空闲暂停：READY 且空闲超过 60s，并且 READY 数量大于 `min_hot` 时，CAS 到 PAUSING → 调用 `pause` → PAUSED。
4. 借用过期回收；超过 `max_age`（默认 6h）的空闲或暂停沙箱回收。
5. 过渡态超时恢复：见上文「耗时操作的做法」。
6. 每 60s 对账一次：用 `list(metadata={pool})` 查云端，销毁数据库里没有对应记录的孤儿沙箱（创建不满 60s 的跳过，避免误伤正在创建的）。

**两条规则**
- **后台任务绝不调用 `connect()`**（它会续期，对暂停的沙箱还会直接恢复），查状态一律用 `get_info()`。
- 所有配置项都可以通过环境变量覆盖。

## HTTP 接口

- `POST /v1/leases {wait_timeout_s?, lease_ttl_s?}`：阻塞等待，最长 180s。返回 `lease_id`、`sandbox_id`、`expires_at`、来源（ready / resumed）、等待耗时。
- `POST /v1/leases/{id}/renew`、`DELETE /v1/leases/{id}`、`GET /v1/leases/{id}`
- `POST /v1/leases/{id}/run_code {code, language?, timeout_s?}` → stdout、stderr、text 结果、png 结果（base64）、error
- `POST /v1/leases/{id}/commands {cmd, cwd?, envs?, timeout_s?}` → exit_code、stdout、stderr
- `PUT /v1/leases/{id}/files?path=`（请求体为原始字节）、`GET /v1/leases/{id}/files?path=`
- `GET /v1/pool/stats`：各状态数量、队列长度、分配来源比例、等待/恢复/暂停耗时 p50/p99。`GET /v1/sandboxes`：调试用。`GET /healthz`

## 实施顺序（每个阶段提交并推送到 `claude/inspiring-maxwell-n5z73w`）

1. `docs/sandbox-pool-design.md`、config、models、store（schema、db、repository）、provider（base、fake）。附带 repository 的 CAS 测试。
2. allocator：acquire、排队、renew、release。测试：先来先服务、429、504、容量永不超过 5、两个 Pool 实例共享同一 SQLite 文件并发抢占不会重复分配。
3. maintainer：补货、预热、空闲暂停、过期回收、崩溃恢复、对账、熔断。
4. API 和代为执行接口，`__main__`、`run_local_cluster.sh`。
5. e2b_provider 接入真实云沙箱；`e2e_scenarios.py`、`cleanup_sandboxes.py`。

## 验证

1. **单元测试**：`pytest tests/`，全部使用 FakeProvider，不花钱。覆盖上面每个阶段列出的场景，以及副本崩溃（op_deadline 过期）后由其他副本恢复。
2. **本地多副本 + 真实云沙箱**：`source` 两个 env 文件后，`POOL_TEMPLATE=xu76gk97q07mgohgw7q3 scripts/run_local_cluster.sh`，然后运行 `e2e_scenarios.py`，依次验证：
   - 启动后补足 5 个，空闲 60s 后全部进入 PAUSED。
   - 同时发 16 个借用请求：5 个立即拿到（从暂停中恢复），10 个排队，1 个收到 429。
   - 在借到的沙箱上跑通 `run_code`、`commands`、文件读写。
   - 归还后排队的请求被依次服务；用小的 `wait_timeout_s` 触发 504。
   - 执行中途 kill 一个副本进程，其余副本继续服务，其持有的过渡态沙箱被回收。
   - 从 `/v1/pool/stats` 输出各项耗时。
3. **收尾**：停掉集群，运行 `scripts/cleanup_sandboxes.py` 销毁账号下全部沙箱实例（含暂停中的），并确认 `list` 为空。第二代模板 `xu76gk97q07mgohgw7q3` 不是实例，默认保留，在总结中说明。
4. 提交前检查：`git diff` 中不能出现 `e2b_`、`LTAI` 这类凭据字样。
