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

另有 **agent 子系统**（`sandbox_pool/agent/`，`POOL_AGENT_ENABLED=true` 开启）：每个（调用方，用户）一个运行在云沙箱里的常驻 agent。引擎有四套，配了模板才启用：`opencode`（1.18.32）、`pi`（0.87.1）、`opencode2`（OpenCode 2.3.2）、`pi1`（Pi 1.6.0）。空闲不会自动暂停；调用方可以显式休眠，下次消息再恢复。本机有操作台 `/console` 和管理页 `/admin`。现行接入说明是业务手册；9 月的方案和测试报告是当时的记录。文档在 `sxw_aicoding/`：
- `2026-09-25-opencode常驻agent-业务接入使用手册.md`：现行接入、配置、运维和排障；
- `方案设计/2026-11-21-OpenCode2与Pi1云沙箱升级验证.md`：研究模板和当天的云端回归；
- `方案设计/…实施方案.md`：当时的设计与执行结果（opencode：`2026-09-25-opencode应用沙箱池-实施方案.md`；pi 与多引擎：`2026-09-26-pi引擎接入-实施方案.md`）；
- `技术调研/…调研.md`、`技术调研/…PoC验证报告.md`：平台、opencode、pi 的实测结论；
- `…测试报告.md`：2026-09-25 至 09-28 的历史结果；
- `代码评审/2026-09-26-opencode常驻agent子系统代码评审报告.md`：评审问题（AG-*）、取舍与修复记录；
- `代码评审/2026-09-26-pi引擎接入代码评审报告.md`：pi 引擎接入的评审（PI-*），含桥接进程的后续项（下次重建模板时处理）；
- `代码评审/2026-09-27-opencode进程池与pi集成-最近三次提交代码评审报告.md`：最近三次提交的评审（R3-*），第六节是二次复核、修复与遗留项；
- `代码评审/2026-09-27-9f265ba修复二次复核与真实云沙箱验证报告.md`：R3 修复的验收（含真实云沙箱探针），第三节是 R3-01 / R3-04 的两个残留，第六节是它们的修复记录。

## 常用命令

优先用项目里的 `.venv`。需要新建时用 Python 3.11：`python3.11 -m venv .venv && . .venv/bin/activate && pip install -r requirements-dev.txt`。下面命令里的 `/root/.venvs/e2b`、固定模板 ID 和 `.env` 路径是当时那台环境的记录，换机器时改成当前解释器、当前模板和仓库外的密钥文件。

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
python scripts/e2e_agent_scenarios.py [--only S1,S4] [--engine pi]   # S1–S12；--engine 还可取 opencode、opencode2、pi1
                                                                      # S10 会 kill -9 8001；S12 切换引擎；S13 做同一引擎的新旧版切换
scripts/run_local_cluster.sh stop && python scripts/cleanup_sandboxes.py --pool <本次池名>
python scripts/build_opencode_template.py [verify <模板ID>]   # 旧版 opencode 1.18.32
python scripts/build_pi_template.py [verify <模板ID>]         # 旧版 pi 0.87.1
python scripts/build_agent_research_template.py opencode      # OpenCode 2.3.2
python scripts/build_agent_research_template.py pi            # Pi 1.6.0
python scripts/verify_agent_research_template.py opencode <模板ID>
python scripts/verify_agent_research_template.py pi <模板ID>
python scripts/poc_opencode_agent.py                   # 旧版 opencode：云上逐项验证，建临时沙箱，结束销毁
python scripts/poc_pi_agent.py                         # 旧版 pi：临时沙箱里跑模板启动脚本并逐项验证，结束销毁
node --test sandbox_pool/agent/pi_bridge/test/bridge.test.mjs   # pi 桥接进程测试（假的 pi，不联网；Node 22+ 不接受目录参数）
DEEPSEEK_API_KEY=... python scripts/pi_bridge_local_check.py --pi <pi 的 dist/bundle/cli.js>   # 本机真实 pi 联调
```

- 旧版模板同样保留不删：opencode `z0tkbiqlztqsma57014d`（cn-hangzhou，2C4G，opencode 1.18.32）、pi `vk1r2o1eln2byetc3lj6`（2C2G，pi 0.87.1 + pi-mcp-adapter 2.37.0）。研究模板是 OpenCode 2.3.2 `towyfj4hujiau0s7fc1z`、Pi 1.6.0 `f8708ktxkiytef0jho48`，构建入口是 `scripts/build_agent_research_template.py` 和 `scripts/verify_agent_research_template.py`。这些 ID 属于当时的账号和地域，换账号后以本次 `GetTemplate` 为准。
- 代码池若要在默认断网下直接使用额外 Python 包，用 `scripts/build_pyagent_template.py` 把依赖烤进第二代模板，再把模板 ID 配给 `POOL_TEMPLATE`。
- 本机开着代理的 fake-ip / TUN 模式时，到沙箱子域名的新连接会失败。网关访问沙箱内引擎要设 `POOL_AGENT_INGRESS_IP`（SNI / Host 仍用沙箱域名）；envd 调用只能靠重试。历史记录里失败比例大约是三分之一，那是当时环境的观察。

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

**沙箱状态**：`CREATING → WARMING → READY ⇄ PAUSING → PAUSED → RESUMING → LEASED → DESTROYING`，任何状态都可以进入 DESTROYING。`max_size` 统计全部未删除记录，`DESTROYING` 仍占云端配额；补货用的 `target_size` 不统计 `DESTROYING`。归还销毁若撞上平台 `409 PAUSING or RESUMING`，卡住的那台只少一个可用名额，不再挡住补货。高频短借用（借一条命令就归还）应设 `POOL_PAUSE_ENABLED=false`。

**代码池出网**：`POOL_EGRESS_DEFAULT_DENY=true` 时，创建就下发 `deny_out=0.0.0.0/0`。借用方要联网必须在 `POST /v1/leases` 里传 `egress`，allocator 下发后读回平台回显，对不上这次借用失败。`POOL_BLOCK_PUBLIC_INGRESS=true` 时创建带 `secure=True`，关掉不带令牌就能访问的公网端口。`run_code` / `commands` 单条流超过 `POOL_MAX_OUTPUT_BYTES`（默认 256 KiB）时截断，并在响应里带上截断前的真实字节数。命令执行超时返回 HTTP 200、`exit_code=-1`，并交回超时前已经产出的输出。
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
  - agent 沙箱与代码执行池共用 `sandboxes` 表，但用独立池名（`agent_pool_name`）。空闲不会自动暂停。`agent_max_sandboxes` 统计该池全部未删除记录，含 `DESTROYING`；
  - 状态流转为 CREATING → WARMING（装配：等健康、写引擎的配置文件与 `egress.json`；`opencode2` 在创建之后还要写实例密码）→ ACTIVE → RETIRING → DESTROYING。调用方显式休眠时走 ACTIVE → PAUSING → PAUSED，再次使用时 PAUSED → RESUMING → ACTIVE。休眠期间维护循环直接返回；
  - `reserve_agent_sandbox` 把「这个 agent 已经有沙箱」限定为 CREATING / WARMING / ACTIVE。`sandbox_for` 会先恢复已有的 PAUSED。轮换时允许旧 RETIRING 与新沙箱并存。
- **引擎**（`agent/engines/`）：
  - `opencode` 与 `opencode2` 跑 `opencode serve`；`pi` 与 `pi1` 跑 `agent/pi_bridge/pi-bridge.mjs`，每个会话一个 `pi --mode rpc` 子进程。网关按沙箱记录的引擎选客户端。`opencode2` 使用 `V2Client`：事件在 `/api/event`，探活是带 Basic Auth 的 `/api/info`，密码由流量令牌经 `server_password()` 推导。其余三套探活仍是 `/global/health`。事件翻译、结果提取、配置文件渲染按引擎区分。
  - 四个模板变量：`POOL_AGENT_TEMPLATE`、`POOL_AGENT_PI_TEMPLATE`、`POOL_AGENT_OPENCODE2_TEMPLATE`、`POOL_AGENT_PI1_TEMPLATE`。`agents.settings.engine` 为空时用 `POOL_AGENT_DEFAULT_ENGINE`，且该默认值必须已启用。沙箱记录存 `sandboxes.engine`（老记录为空，视为 opencode）。接管、重连、健康检查都按沙箱记录找引擎。
  - Pi 1 用原生 MCP，`exposure=direct`，搜索工具名是 `mcp__websearch__bailian_web_search`。旧版 pi 经 adapter 的 `directTools`，工具名是 `websearch_bailian_web_search`。OpenCode 2 的 MCP 设 `codemode=false`，shell 工具名是 `shell`。`pi` 和 `pi1` 拒绝非空 `agent` 参数；两套 opencode 接受。
  - 切换引擎复用轮换：`sandbox_for` 与维护循环把引擎不一致的 ACTIVE 沙箱转为 RETIRING（空闲直接销毁）；带 `session_id` 续聊到旧引擎的沙箱返回 409。
  - 请求拿的 agent 快照可能早于引擎切换：`sandbox_for` 按库里最新设置选沙箱（PI-L1），引擎专属参数（`agent`）在 `_start_task` 按实际沙箱的引擎再校验一次（R3-02）。
  - pi 的 `prompt_async` 返回 `run_id`（存 `tasks.run_id`）：跟进结束后查 run 状态，`lost` / `unknown`（进程或桥接进程重启过）记为 FAILED，不把残缺结果当成功。
  - 用户消息经 `engine.prompt_text()` 发送。`PiEngine` 给以 `/` 开头的文本加前导空格，`PiNativeEngine`（`pi1`）继承这个方法，两套都保留。原问题在 pi 0.87.1 上实测：`/mcp` 等扩展命令不产生运行，响应又没有 `disposition`，桥接进程会把会话一直记为忙（PI-H1）。去掉转义之前要在实际启用的 pi 版本上重新验证。
  - 建会话、发提示词是非幂等 POST，超时不重试：读超时用 `_SLOW_POST_TIMEOUT_S`（150s），必须长于桥接进程处理它们的上限（冷启动、MCP 缓存等待、pi 的 preflight 压缩，约 141s），否则会留下无人跟进的运行（PI-M1）。
  - 桥接进程随模板发布，改它要重建模板，所以只做转发和进程管理；pi 的中止表现为 `stopReason=error`，中止状态以网关自己的标记为准。平台约束（Node 版本、CA、下载源、启动命令 16KiB 上限）见 `fc-agent-sandbox-notes.md`。
- **任务执行**：
  - 任务由后台 `TaskRunner` 执行，HTTP 响应只读订阅队列；客户端断开不取消 runner，也不在数据库操作中途取消；
  - runner 刷新 `tasks.op_deadline` 作为心跳，过期后其他副本 CAS 接管（`resume=True`）；
  - **中止按会话生效**（四套引擎都是按会话中止，没有按单次运行中止的接口）：`abort_task` 只写 `abort_requested`，中止命令只由跟进任务的 runner 下发，不要在别处按会话调用 abort（R3-01）。确认持有任务之后命令仍可能停在路上：旧 runner 停顿、心跳过期被接管、任务结束、同一会话开始下一个任务，迟到的命令就中止了它（验收报告 3.1）。所以：
    - 下发前用 `begin_abort` 在库里占住会话（`tasks.abort_fence_until`，CAS 要求仍持有任务）；`create_task` 对有未到期占用的会话返回 aborting，请求路径等待；
    - 下发用 `wait_for` 限时 `_ABORT_TIMEOUT_S`，短于占用 `_ABORT_HOLD_S`（与重载配置的 60s / 90s 同理）；没送达的隔 `_ABORT_RETRY_S` 重试；
    - 送达且占用是独占的才 `end_abort` 提前释放（正常路径不拖慢下一条消息）；接管方遇到前任未到期的占用，不释放、等它到期；
    - 剩下的只有进程恰好在写出请求的同步路径上被整体冻结超过两者之差；彻底消除要引擎按运行校验中止（pi 桥接进程可在重建模板时加）。
    runner 每秒读一次库（`_DB_POLL_S`）；
  - **跨副本断线重连**（`_follow_remote`）要按轮次划清归属，事件和消息都只按会话区分：快照只在会话正忙（pi 还要 run_id 一致）且最后一条用户消息是本任务那一条时补发，**取消息之后再读一次库**、任务仍 RUNNING 才发（取消息期间会话可能换轮，验收报告 3.2）；订阅建立时、翻译器报告新一轮开始（`round_starts`）时立即读库，本轮结束（idle / agent_settled）后不再转发（R3-04）。「本任务那一条」：opencode 先写用户消息再置忙，比对提示词文本即可；pi 回复受理之后要经过几次 await 才写入用户消息，要按身份绑定——runner 从带同一 runID 的用户消息 `message_end` 记下时间戳（`tasks.prompt_key`）。opencode 不按事件绑定身份：`summarize` 在后台补写旧用户消息、会再发一次 `message.updated`。重连时正在输出的消息错过了开头，以全文为准：opencode 的增量不落盘（快照里没有进行中的文本），缓存的增量不是部件全文的开头就丢弃、按结束时的全文输出；pi 的 `message_update` 不带累积内容，翻译器 `mid_round` 时没看到 `message_start` 的消息等 `message_end` 整条输出（R3-N1）。
- **任务准入**（`AgentStore.create_task`，池级锁内）：同一事务里检查沙箱仍在服务、会话不忙、会话没有未到期的中止占用、未超并发上限、沙箱不在重载配置，插入任务并记录沙箱活动时间、把版本号加一。版本号加一使维护循环按旧快照做的空闲销毁或轮换 CAS 失败，不会销毁刚接了新任务的沙箱；`touch_sandbox` 在任务结束时做同样的事。
- **修改设置**用 `AgentStore.patch_settings`：池级锁内读最新设置、合并、写回并把版本号加一，不要按请求读到的 agent 快照整包覆盖（并发修改不同字段会互相丢失，R3-03）。
- **重载配置**（设置变更后调用该引擎客户端的 `dispose()`）：旧版 opencode 是 `POST /instance/dispose`，会中止沙箱里所有运行中的会话。OpenCode 2 是 `POST /api/location/reload`。pi 桥接进程只重启空闲会话进程。网关对这几套都必须与任务准入互斥：`begin_reload` 在池级锁内确认没有运行中任务，再占住 ACTIVE 行的 `op_owner` / `op_deadline`（在服务的沙箱只有这时这两列非空），期间 `create_task` 返回 reloading、请求路径等待；重载整体限时且短于占用时长。
- **出网**：
  - 凭证只经 `network.rules` 注入：模型 Key 与 `POOL_AGENT_INJECT` 只允许管理员配置，调用方无法新增注入域名；
  - 沙箱内只有占位符 `injected-by-platform`；
  - 平台上 `allow_out` 优先于 `deny_out`：放行项不能与强制屏蔽的内网 / 元数据网段重叠，开放模式不接受域名放行项（`parse_policy`）。库里的旧覆盖用 `strict=False` 读取，违规项丢弃而不是报错；
  - 平台实测约束见 `agent/policy.py` 顶部。
- **访问沙箱内 agent 服务**：
  - 用 `agent/opencode.py`，`trust_env=False`、可选入口 IP 直连（SNI / Host 用沙箱域名）；
  - 建连失败一律重试，非幂等 POST 读失败不重试；
  - 维护循环调用该沙箱客户端的 `health()`。`opencode2` 探测 `/api/info`，其余探测 `/global/health`。`PAUSED` 不探测、不续期。文件列表、上传和下载只连接 `ACTIVE`，避免 `connect()` 把休眠实例唤醒。操作台在沙箱休眠或未启动时不发文件请求。
- **接口输出**：`access_token`（流量令牌）与 `lease_id` 一样是凭证，任何接口都不返回（`sandbox_view`、管理员列表都要去掉）。
- **测试**：`provider/fake_opencode.py` 是内存版 opencode，提示词里的 `[sleep:秒]`、`[tool]`、`[error]`、`[ask]`、`[remember]`、`[partial]`（运行中途先产出一段文本）、`[round]`（回复带上是会话里的第几轮，相同提示词也能分出轮次）等指令模拟不同行为；它与真实行为一致的两点不要去掉：客户端 close 后再调用报错、dispose 取消运行中的会话。`FakeOpencodeV2Client` 只把消息和事件翻成 OpenCode 2 的形状（工具名 `shell`）；`health()` 仍是内存结果，`dispose()` 仍取消运行中会话。真实 `/api/info` 与 `/api/location/reload` 在 `V2Client`。`make_agents` 夹具可建多个副本（各自打开 store，模拟多进程）；生产中 `create_app` 让 agent 子系统共用代码执行池的数据库引擎（每进程一个 SQLite 写连接）。`tests/test_agent_review_fixes.py` 按评审编号（AG-*）组织。`provider/fake_pi.py` 是内存版 pi 桥接进程（另有 `[crash]` 指令），保留的真实语义：重载不中止运行中的会话、进程丢失后 run 为 lost、`restart()` 后旧 run 为 unknown、`/mcp` 等扩展命令不产生运行且会话一直忙（pi 0.87.1）、每条消息 `message_start` → 增量 → `message_end`（带全文）；`FakeProvider.pi_templates` 里的模板建出 pi 沙箱。`tests/test_agent_pi.py` 末尾按评审编号（PI-*）组织；`tests/test_agent_r3_fixes.py` 按 R3-* 组织，并发用例对引擎参数化，用事件门控固定时序。`tests/test_agent_engine_upgrade.py` 覆盖 `opencode2` / `pi1`。

**鉴权**（`api/auth.py`）：
- `POOL_API_KEYS` / `POOL_ADMIN_KEYS`，格式「名称:key」，逗号分隔。两者都为空时关闭鉴权，此时进程拒绝监听非回环地址（除非 `--allow-no-auth`）。
- 借用按 `leases.client_id` 绑定调用方；allocator 方法的 `owner` 参数为 None 时表示管理员，不做归属限制。
- 管理接口：`GET /v1/sandboxes`（不含 `lease_id`，借出中的附带借用方和到期时间）、`DELETE /v1/sandboxes/{id}`（强制释放 / 销毁）、`/v1/admin/drain`（只排空代码池）、`/v1/admin/instances`（云端实例与两个池对齐；暂停只作用于代码池 `READY`，agent 返回 400；删除可销毁指定 agent 沙箱或孤儿）、`/v1/admin/template-caches`（OpenAPI 模板缓存；缺 AK 时 GET 为 `configured: false`，写操作 503）。`lease_id` 和 `access_token` 是凭证，任何接口都不要返回给非借用方或非持有方。页面 `/console`、`/admin` 本身不鉴权。
- 上传文件以外的请求体由 `api/body_limit.py` 在鉴权之前限制大小（FastAPI 会在执行鉴权依赖之前读完 JSON 请求体）。
- 代为执行超过 `timeout_s` 返回 200 + `TimeoutError`（`ExecutionTimeout`），不是 502：代码可能已执行，不能让调用方当故障重试。

## 测试约定

- `tests/conftest.py` 的 `make_pool` fixture 用 `FAST` 时间参数创建池，可以通过关键字覆盖任意 `PoolConfig` 字段。
  - 多次调用得到多个副本，它们共享同一个 SQLite 文件和同一个 `FakeProvider`（模拟共享的云端）。
  - `run_maintainer=False` 时可以手动调用 `maintainer.replenish(...)` 和 `lifecycle.wait_background()`，精确控制时序。
- `FakeProvider` 可以模拟平台超时回收，并支持失败和延迟注入：`fail_create` / `fail_set_timeout` / `fail_kill` / `fail_resume_ids`、`latency_s` / `kill_latency_s` / `set_timeout_delays`。
- `tests/test_agent_*.py`：agent 子系统。`units` 覆盖纯逻辑（策略、cron、SSE、事件翻译），`service` 覆盖服务与维护循环，`api` 覆盖 HTTP 接口，`pi` 覆盖 pi 引擎与引擎切换，`engine_upgrade` 覆盖 OpenCode 2 与 Pi 1。`tests/test_atlas_egress.py` 覆盖借用出网、输出上限和补货不计 `DESTROYING`。`tests/test_admin_instances.py`、`tests/test_admin_template_cache.py` 覆盖实例清单和模板缓存。
- `tests/test_review_fixes.py` 按第一轮评审问题编号（H1、M1…L10）组织，`tests/test_review_r2_fixes.py` 按第二轮编号（R2-*）组织。
  - 修并发问题时要构造出确定性的时序，并确认去掉修复后用例会失败。
  - 修完后全量连跑多轮，排查偶发失败。
