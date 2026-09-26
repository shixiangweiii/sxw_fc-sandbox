# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概况

基于阿里云云沙箱（FC Agent Sandbox，E2B 协议兼容）的**沙箱池服务**（`sandbox_pool/`）：给服务端 Agent 提供借用式的隔离执行环境，调用方只走 HTTP，不持有 SDK 和云端 Key。
- 需求：容量 5、排队 10、最长等待 180s；借用 10 分钟、可续期、最长 60 分钟；**归还即销毁**；空闲 60s 后暂停。
- 部署目标：多副本高可用。本地用「多进程 + 共享 SQLite」模拟，**代码不能写死单实例假设**。

项目文档和代码注释都用中文，新增内容保持一致。设计与决策以 `docs/` 为准：
- `sandbox-pool-design.md`：当前设计，是最权威的总览；
- `sandbox-pool-fix-changes.md`：第一轮评审的修复，含踩坑记录；
- `sandbox-pool-r2-fix-changes.md`：第二轮评审的复核结论（哪些问题不成立、为什么）与修复；
- `fc-agent-sandbox-notes.md`：云沙箱实测结论。

`examples/` 是早期摸底脚本：生命周期 demo、通过 OpenAPI 创建第二代模板。

另有 **agent 子系统**（`sandbox_pool/agent/`，`POOL_AGENT_ENABLED=true` 开启）：每个（调用方，用户）一个运行在云沙箱里的常驻 opencode agent。它的文档在 `sxw_aicoding/`（用户要求过程文档放这里）：
- `方案设计/…实施方案.md`：设计与执行结果；
- `技术调研/…调研.md`、`技术调研/…PoC验证报告.md`：平台与 opencode 的实测结论；
- `…业务接入使用手册.md`、`…测试报告.md`；
- `代码评审/2026-09-26-opencode常驻agent子系统代码评审报告.md`：评审问题（AG-*）、取舍与修复记录。

## 常用命令

Python 3.11，本机虚拟环境在 `/root/.venvs/e2b`（没有就 `pip install -r requirements-dev.txt`）。

```bash
python -m pytest                                   # 全量单元测试（FakeProvider，不访问云端，约 30s）
python -m pytest tests/test_review_fixes.py -k m4  # 按文件 / 关键字跑单个用例
python -m pytest tests/test_pool.py::test_wait_timeout
python -m sandbox_pool --port 8001                 # 启动单个副本（配置全部来自 POOL_* 环境变量）
```

真实云沙箱端到端（会产生费用，结束后必须清理）：

```bash
set -a; . /root/.config/fc-sandbox/e2b.env; set +a    # E2B_API_KEY / E2B_API_URL / E2B_DOMAIN（仓库外，不要入库）
export POOL_TEMPLATE=xu76gk97q07mgohgw7q3 POOL_OP_TIMEOUT_S=60 PYTHON=/root/.venvs/e2b/bin/python
export POOL_ADMIN_KEYS="e2e:<随机>" SANDBOX_POOL_API_KEY=<同一个>   # 可选：开启鉴权
scripts/run_local_cluster.sh start        # 3 个副本 8001~8003，共享 .data/pool.db
python scripts/e2e_scenarios.py           # 全部场景；--only kill-during-pause | min-hot
scripts/run_local_cluster.sh drain        # 排空（销毁空闲 / 暂停的沙箱）
scripts/run_local_cluster.sh stop
python scripts/cleanup_sandboxes.py       # 兜底：销毁账号下全部沙箱并确认列表为空
```

- 排空状态存在库里，重启后仍然生效。重跑前换一个新的 `.data/pool.db`，或调用 `DELETE /v1/admin/drain`。
- 第二代模板 `xu76gk97q07mgohgw7q3` 是模板，不是实例，保留不删。

agent 子系统的端到端测试（本机 macOS 用项目内 `.venv`，Python 3.12）：

```bash
set -a; . ./.env; . .data/agent-e2e.env; set +a   # .env：云沙箱 / DeepSeek Key / POOL_AGENT_TEMPLATE / POOL_AGENT_INGRESS_IP；
                                                  # agent-e2e.env：鉴权 key、MCP 与注入配置（生成方式见测试报告），均不入库
scripts/run_local_cluster.sh start 8001 8002
python scripts/e2e_agent_scenarios.py [--only S1,S4]   # S1–S11；S10 会 kill -9 8001
scripts/run_local_cluster.sh stop && python scripts/cleanup_sandboxes.py
python scripts/build_opencode_template.py [verify <模板ID>]   # 构建 / 验证 opencode 模板（需要 AK/SK 与 FCSANDBOX_TEAM_ID）
python scripts/poc_opencode_agent.py                   # 云上逐项验证平台能力（建临时沙箱，结束销毁）
```

- opencode 模板 `z0tkbiqlztqsma57014d`（cn-hangzhou，2C4G，opencode 1.18.32）同样保留不删。
- 本机开着代理的 fake-ip / TUN 模式：到沙箱子域名的新连接约 1/3 失败。网关访问 opencode 要设 `POOL_AGENT_INGRESS_IP`；envd 调用只能靠重试。

## 架构要点（需要跨文件理解的部分）

**分层**：`api/` → `core/pool.py`（组装入口，一个进程一个 `SandboxPool`）→ `core/allocator.py`（借用、排队、续期、代为执行）+ `core/maintainer.py`（后台维护循环）→ 二者共用 `core/lifecycle.py`（销毁、结束借用）→ `store/`（SQLAlchemy Core）+ `provider/`（`SandboxProvider` 协议：`E2BProvider` 为真实后端，`FakeProvider` 用于测试）。

**多副本协调全靠数据库，不选主**：
- 所有状态变更都是带条件的 UPDATE（CAS），比较 `state` / `version` / `op_owner` / `lease_id`，影响行数为 1 才算成功。
- 「先数再写」的操作先在同一事务里更新池级锁行 `pool_kv.lock` 做串行化，包括：占容量（`reserve_slot`，同时检查熔断和排空）、入队、暂停前检查 `min_hot`（`start_pause`）。
- 耗时的云端调用不持有数据库事务：先 CAS 进入过渡态并写入 `op_owner` / `op_deadline`，调用完成后再 CAS 到目标状态。
  - 执行者崩溃后，其他副本在 `recover_stuck` 中接管。
  - 暂停、恢复途中崩溃的，按云端实际状态收回（`_adopt`）；其余销毁后补货。
  - 截止时间：创建 / 预热 / 暂停用 `op_timeout_s`，恢复用 `resume_timeout_s`，销毁用 `destroy_timeout_s`。
  - `e2b_provider.py` 里各调用都要显式传请求超时（不依赖 SDK 默认的 60s），且小于对应的截止时间；启动时由 `check_deadlines` 校验配置，新增云端调用时要一并纳入。
- 全池周期任务（对账、历史清理）用 `store.try_periodic` 在 `pool_kv` 时间戳上做 CAS，每个周期只有一个副本执行。
- 排队在 `waiters` 表里，`seq` 决定先来先服务：
  - 「前面的人数 < 可分配沙箱数」时才去抢；
  - 心跳是后台任务（`_Heartbeat`）；
  - 抢到沙箱与排队记录改为 GRANTED 在同一事务内完成（`claim_ready` / `finish_resume` 的 `waiter_seq`）。

**沙箱状态**：`CREATING → WARMING → READY ⇄ PAUSING → PAUSED → RESUMING → LEASED → DESTROYING`，任何状态都可以进入 DESTROYING；所有状态都计入容量。
- READY 的平台超时只是短兜底，由 `keepalive_ready` 按 `platform_deadline` 列续期。
- 续期与借出存在竞态：保活调用返回后要重读这一行，若已被借走，按借用超时重设平台超时。

**必须遵守的约束**（都有踩坑记录，见 `docs/sandbox-pool-fix-changes.md` 第 4 节和 `fc-agent-sandbox-notes.md`）：
- **SDK 固定 `e2b==2.31.0`**：新版走 `/v2` 接口，云沙箱返回 405。SDK 不读 `HTTPS_PROXY`，必须显式传 `proxy`。
- **后台维护代码绝不调用 `AsyncSandbox.connect()`**：它会续期，对暂停中的沙箱还会直接恢复。状态只用 `get_info` / `list` 查询。
- **经代理的 SDK 连接空闲后会失效**，报 `httpx.WriteError('')`（信息为空）。
  - 管控面调用都经过 `_retry_stale`；
  - 超时不重试；
  - 创建只在请求确定没发出去时重试；
  - 用户代码执行不重试。
- **SQLite**：
  - 每进程 1 个写连接 + `BEGIN IMMEDIATE`，另有只读连接池（普通 `BEGIN` + `query_only`）；不要随意加大写连接池。
  - 正常路径上不要执行预期会失败的语句（例如靠主键冲突判断「已存在」）。失败留下的游标被主线程 GC 回收时，可能卡住事件循环，同进程内会死锁到 `busy_timeout`。
- 停止时不要直接取消正在做数据库操作的协程（写事务会悬挂），参考 `Maintainer.stop` 和 `_Heartbeat.stop` 的「事件通知 + 等待」写法。
- 不要写「集合非空就 `await gather(*集合)`」的等待循环：Python 3.12 起 `gather` 对已结束的任务直接返回、不让出事件循环，移除任务的回调永远得不到执行，循环空转、外层超时也触发不了（见 `Lifecycle.wait_background`）。
- 云端列表接口有约 1.5s 延迟，且默认包含已暂停的沙箱。对账要在列表查询前后各读一次库，并留宽限期。
- 表结构只做「新增可空列 / 索引」的自动迁移（`store/repository.py` 的 `_migrate`）。新增列必须可空；需要预置的 `pool_kv` 键加到 `_KV_KEYS`。

**agent 子系统**（`agent/service.py` 组装，`maintainer.py` 后台维护，`runner.py` 执行任务，`store/agent_repo.py` 存储）：
- **池与状态**：
  - agent 沙箱与代码执行池共用 `sandboxes` 表，但用独立池名（`agent_pool_name`），不暂停；
  - 状态流转为 CREATING → WARMING（装配：等健康、写 `opencode.json` / `AGENTS.md` / `egress.json`）→ ACTIVE → RETIRING → DESTROYING；
  - 每个 agent 最多一个在建或在服务的沙箱（在池级锁内判断）。
- **任务执行**：
  - 任务由后台 `TaskRunner` 执行，HTTP 响应只读订阅队列；客户端断开不取消 runner，也不在数据库操作中途取消；
  - runner 刷新 `tasks.op_deadline` 作为心跳，过期后其他副本 CAS 接管（`resume=True`）。
- **任务准入**（`AgentStore.create_task`，池级锁内）：同一事务里检查沙箱仍在服务、会话不忙、未超并发上限、沙箱不在重载配置，插入任务并记录沙箱活动时间、把版本号加一。版本号加一使维护循环按旧快照做的空闲销毁或轮换 CAS 失败，不会销毁刚接了新任务的沙箱；`touch_sandbox` 在任务结束时做同样的事。
- **重载配置**（设置变更后的 `POST /instance/dispose`）会中止沙箱里所有运行中的会话，必须与任务准入互斥：`begin_reload` 在池级锁内确认没有运行中任务，再占住 ACTIVE 行的 `op_owner` / `op_deadline`（在服务的沙箱只有这时这两列非空），期间 `create_task` 返回 reloading、请求路径等待；重载整体限时且短于占用时长。
- **出网**：
  - 凭证只经 `network.rules` 注入：模型 Key 与 `POOL_AGENT_INJECT` 只允许管理员配置，调用方无法新增注入域名；
  - 沙箱内只有占位符 `injected-by-platform`；
  - 平台上 `allow_out` 优先于 `deny_out`：放行项不能与强制屏蔽的内网 / 元数据网段重叠，开放模式不接受域名放行项（`parse_policy`）。库里的旧覆盖用 `strict=False` 读取，违规项丢弃而不是报错；
  - 平台实测约束见 `agent/policy.py` 顶部。
- **访问沙箱内 opencode**：
  - 用 `agent/opencode.py`，`trust_env=False`、可选入口 IP 直连（SNI / Host 用沙箱域名）；
  - 建连失败一律重试，非幂等 POST 读失败不重试；
  - 维护循环只用 HTTP 探测 `/global/health`，不调用 `connect()`。
- **接口输出**：`access_token`（流量令牌）与 `lease_id` 一样是凭证，任何接口都不返回（`sandbox_view`、管理员列表都要去掉）。
- **测试**：`provider/fake_opencode.py` 是内存版 opencode，提示词里的 `[sleep:秒]`、`[tool]`、`[error]`、`[ask]`、`[remember]` 等指令模拟不同行为；它与真实行为一致的两点不要去掉：客户端 close 后再调用报错、dispose 取消运行中的会话。`make_agents` 夹具可建多个副本（各自打开 store，模拟多进程）；生产中 `create_app` 让 agent 子系统共用代码执行池的数据库引擎（每进程一个 SQLite 写连接）。`tests/test_agent_review_fixes.py` 按评审编号（AG-*）组织。

**鉴权**（`api/auth.py`）：
- `POOL_API_KEYS` / `POOL_ADMIN_KEYS`，格式「名称:key」，逗号分隔。两者都为空时关闭鉴权，此时进程拒绝监听非回环地址（除非 `--allow-no-auth`）。
- 借用按 `leases.client_id` 绑定调用方；allocator 方法的 `owner` 参数为 None 时表示管理员，不做归属限制。
- 管理接口：`GET /v1/sandboxes`（不含 `lease_id`，借出中的附带借用方和到期时间）、`DELETE /v1/sandboxes/{id}`（强制释放 / 销毁）、`/v1/admin/drain`。`lease_id` 是借用凭证，任何接口都不要返回给非借用方。
- 上传文件以外的请求体由 `api/body_limit.py` 在鉴权之前限制大小（FastAPI 会在执行鉴权依赖之前读完 JSON 请求体）。
- 代为执行超过 `timeout_s` 返回 200 + `TimeoutError`（`ExecutionTimeout`），不是 502：代码可能已执行，不能让调用方当故障重试。

## 测试约定

- `tests/conftest.py` 的 `make_pool` fixture 用 `FAST` 时间参数创建池，可以通过关键字覆盖任意 `PoolConfig` 字段。
  - 多次调用得到多个副本，它们共享同一个 SQLite 文件和同一个 `FakeProvider`（模拟共享的云端）。
  - `run_maintainer=False` 时可以手动调用 `maintainer.replenish(...)` 和 `lifecycle.wait_background()`，精确控制时序。
- `FakeProvider` 可以模拟平台超时回收，并支持失败和延迟注入：`fail_create` / `fail_set_timeout` / `fail_kill` / `fail_resume_ids`、`latency_s` / `kill_latency_s` / `set_timeout_delays`。
- `tests/test_agent_*.py`：agent 子系统。`units` 覆盖纯逻辑（策略、cron、SSE、事件翻译），`service` 覆盖服务与维护循环，`api` 覆盖 HTTP 接口。
- `tests/test_review_fixes.py` 按第一轮评审问题编号（H1、M1…L10）组织，`tests/test_review_r2_fixes.py` 按第二轮编号（R2-*）组织。
  - 修并发问题时要构造出确定性的时序，并确认去掉修复后用例会失败。
  - 修完后全量连跑多轮，排查偶发失败。
