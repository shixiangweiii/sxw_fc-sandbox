# AGENTS.md

本文件适用于整个仓库，供参与开发、排障与评审的编码 agent 使用。依据当前源码及 `docs/`、`sxw_aicoding/` 中的方案和修复记录整理，初次核对日期为 2026-09-27。

## 工作约定

- 项目交流、开发文档和新增代码注释以中文为主；标识符沿用现有英文命名。
- 开始工作先查看 `git status --short`，保留用户已有的暂存、未暂存和未跟踪内容；不顺手清理、回退或提交无关改动。
- 用户要求“先熟悉”“先讨论”“仅评审”“不改代码”时，保持只读；若另外明确要求生成某份文档，只写该文档。不要把评审发现自动扩展成修复任务。
- 先沿相关请求链路阅读代码，再结合方案、复核结论和回归用例判断。说明中区分“当前实现”“历史实测”“本次验证”和“尚未验证”。
- 本文件是**仓库开发指引**。沙箱工作目录中的 `AGENTS.md` 由 `agent/policy.py` 的 `render_agents_md()` 和各引擎的 `render_files()` 动态生成，用于云端 agent 的环境说明；两者不要混淆。

## 项目定位与范围

本项目是基于阿里云 FC Agent Sandbox 的云端 agent 执行服务。阿里云提供 E2B 协议兼容接口，本项目通过 E2B Python SDK 接入，不应假定它与 E2B 开源基础设施完全同构。

技术栈是 Python、FastAPI、asyncio、SQLAlchemy Core 和 aiosqlite；pi 的沙箱内桥接进程使用 Node.js。仓库包含两套共用数据库和 provider、按池名隔离的子系统：

| 子系统 | 入口与核心对象 | 生命周期与职责 |
| --- | --- | --- |
| 代码执行池 | `/v1/leases/*`；`SandboxPool` | 按任务借用独占沙箱，支持代码、命令、文件操作；预热、排队、暂停/恢复；**归还即销毁**，随后补货 |
| 常驻 agent | `/v1/agents/{user_id}/*`；`AgentService` | 按 `(client_id, user_id)` 隔离，`client_id` 是调用方 key 的名称；支持会话、SSE、后台长任务、定时任务和出网策略；通过 `POOL_AGENT_ENABLED=true` 开启 |

- 常驻 agent 支持 opencode 与 pi，按 agent 选择引擎；默认模型配置是 `deepseek/deepseek-flash`，百炼 WebSearch MCP 为可配置能力。
- “常驻”指沙箱在有效期内持续服务。agent 身份、设置、定时任务和任务结果在数据库中保存，沙箱文件与会话没有跨沙箱持久化或迁移保证。
- 常驻 agent 按 Eco 约束设计，不暂停；当前默认 20 小时开始轮换、23.5 小时硬截止、单任务上限 4 小时。实际账号套餐与云端资源状态须另行核实，不能由历史报告推断。
- 当前仓库不包含面向用户的网页界面、IM 接入或挂载卷持久化。Postgres 是数据库替换方向；现有本地多副本验证主要基于共享 SQLite，不等于已验证生产 Postgres 部署。

## 文档阅读顺序

先看 [README.md](README.md) 和现有 [CLAUDE.md](CLAUDE.md)，再按任务阅读下表。文档中的旧主机路径、模板 ID、入口 IP、耗时与测试数量是当时的记录，不直接当作当前环境配置。

| 关注点 | 主要资料 |
| --- | --- |
| 代码执行池设计 | [当前设计](docs/sandbox-pool-design.md)；[初始实施方案](docs/sandbox-pool-plan.md) |
| 代码执行池评审与修复 | [首轮评审](docs/sandbox-pool-review.md)；[首轮修复](docs/sandbox-pool-fix-changes.md)；[第二轮评审](sxw_aicoding/代码评审/2026-09-23-沙箱池服务第二轮代码评审报告.md)；[第二轮复核与修复](docs/sandbox-pool-r2-fix-changes.md) |
| opencode 常驻 agent | [实施方案及执行结果](sxw_aicoding/方案设计/2026-09-25-opencode应用沙箱池-实施方案.md)；[AG 系列评审、取舍与修复](sxw_aicoding/代码评审/2026-09-26-opencode常驻agent子系统代码评审报告.md) |
| pi 与多引擎 | [实施方案及执行结果](sxw_aicoding/方案设计/2026-09-26-pi引擎接入-实施方案.md)；[PI 系列评审、修复与后续项](sxw_aicoding/代码评审/2026-09-26-pi引擎接入代码评审报告.md) |
| 业务接口与运维 | [业务接入使用手册](sxw_aicoding/2026-09-25-opencode常驻agent-业务接入使用手册.md)，已包含 pi 和引擎切换 |
| 平台兼容性和选型依据 | [云沙箱实测笔记](docs/fc-agent-sandbox-notes.md)；[opencode 调研](sxw_aicoding/技术调研/2026-09-25-opencode云沙箱常驻agent调研.md)；[opencode PoC](sxw_aicoding/技术调研/2026-09-25-opencode云沙箱PoC验证报告.md)；[pi 调研](sxw_aicoding/技术调研/2026-09-26-pi-agent沙箱内部署与多引擎接入调研.md) |
| 历史验证结果与复现方法 | [测试报告](sxw_aicoding/2026-09-25-opencode常驻agent-测试报告.md)，同时涵盖两种引擎与评审修复后的回归 |

- 代码执行池的设计总览以 `docs/sandbox-pool-design.md` 为入口；agent 的设计与过程记录以 `sxw_aicoding/` 为入口。
- 读方案时同时读末尾的“与方案的差异”“执行结果”“代码评审”；读问题清单时同时读后续复核和修复记录。不能把已修复、复核不成立或明确暂缓的问题重新当成未处理事实。
- 当前行为通过源码和测试确认，设计意图通过相应方案确认；两者不一致时指出差异，不自行扩张需求。
- 新的开发方案、调研、评审及修复报告放在 `sxw_aicoding/` 的相应分类下，沿用 `YYYY-MM-DD-主题.md` 命名。接口、配置或行为改变时同步相关手册和指引。

## 代码导航

| 路径 | 职责 |
| --- | --- |
| `sandbox_pool/__main__.py`、`config.py` | 命令行入口、启动校验、`POOL_*` 配置及默认值 |
| `sandbox_pool/api/` | FastAPI 装配、Bearer 鉴权、请求体限制、代码池与 agent 路由、HTTP 错误映射 |
| `sandbox_pool/core/pool.py`、`allocator.py` | 代码池组装；借用、排队、续期及代为执行 |
| `sandbox_pool/core/maintainer.py`、`lifecycle.py` | 补货、暂停、保活、接管、对账、排空、销毁与后台操作管理 |
| `sandbox_pool/store/` | 表结构、SQLite 引擎、轻量迁移、池与 agent 的存储及 CAS 操作 |
| `sandbox_pool/provider/` | `SandboxProvider` 协议、真实 `E2BProvider`、`FakeProvider` 与两种引擎的测试替身 |
| `sandbox_pool/agent/service.py`、`runner.py`、`maintainer.py` | agent 请求编排、后台任务执行和订阅、健康检查、轮换、接管及定时调度 |
| `sandbox_pool/agent/engines/` | 引擎能力声明、提示词处理、事件翻译、结果提取和沙箱配置渲染 |
| `sandbox_pool/agent/opencode.py` | 两种引擎共用的 `AgentHttpClient`；文件名保留了历史名称 |
| `sandbox_pool/agent/policy.py`、`cron.py` | 出网规则、凭证注入、配置与环境指引渲染；cron 计算 |
| `sandbox_pool/agent/pi_bridge/` | 沙箱内 HTTP/SSE 到 pi RPC 的桥接进程及 Node 测试 |
| `tests/`、`scripts/`、`examples/` | 本地测试；集群、模板构建、联调、端到端和清理脚本；早期平台摸底示例 |

请求链路：

```text
代码执行：api/routes.py → SandboxPool / Allocator → Lifecycle → Store + SandboxProvider
常驻 agent：api/agent_routes.py → AgentService → TaskRunner + Engine → AgentHttpClient → 沙箱内引擎
后台协调：各 Maintainer → Store / AgentStore 的 CAS → provider 或沙箱内 HTTP 服务
```

## 状态与多副本约束

- 数据库是跨副本协调的事实来源，不选主，不用进程内锁替代跨副本协调。内存 runner、订阅队列与客户端缓存仅属于本副本。
- 沙箱/任务的抢占、完成、接管须使用带 `state`、`version`、`op_owner`、`lease_id` 或截止时间等条件的 CAS，并检查更新结果。
- 容量、入队、暂停前的 `min_hot` 检查、agent 沙箱占位、任务准入等“先判断再写入”操作，在同一事务内先更新池级锁行。所有尚未删除的沙箱状态都计入相应池的容量。
- 云端耗时调用放在数据库事务外：先记录过渡态和 `op_owner` / `op_deadline`，再调用云端，最后 CAS 完成；为调用失败、所有权丢失及执行副本退出保留恢复路径。
- 代码池主要状态为 `CREATING → WARMING → READY → LEASED → DESTROYING`，空闲时可经 `PAUSING → PAUSED`，借用时经 `RESUMING` 恢复。不要把归还路径改成跨调用方复用。
- agent 主要状态为 `CREATING → WARMING → ACTIVE → RETIRING → DESTROYING`；每个 agent 最多一个处于 `CREATING/WARMING/ACTIVE` 的沙箱，轮换时允许旧 `RETIRING` 与新沙箱并存。
- 任务准入必须与设置重载互斥：`AgentStore.create_task()` 和 `begin_reload()` 使用同一池级锁。准入成功时，在同一事务内更新活动时间并增加沙箱版本，防止维护循环按旧快照销毁刚接任务的沙箱。
- 任务心跳、终态写入与过期接管保留所有权条件；接管还要确认心跳仍然过期。接管只继续跟进已经启动的运行，不重新发送提示词。
- 定时任务用 `next_run_at` 的 CAS 竞争触发权；固定间隔从原应触发时刻推进，不因维护循环延迟漂移；错过的历史触发不批量补跑。

### 数据库与停机

- SQLite 使用 WAL；每进程一个写连接、`BEGIN IMMEDIATE`，另设 `query_only` 只读连接池。`api/app.py` 让 agent 与代码池共用数据库引擎，不要再次为 agent 打开独立写池。
- 正常流程不依赖必然失败的 SQL 或唯一键冲突判断“已存在”；已有记录表明，这会经游标回收与 SQLite 锁等待阻塞事件循环。新增 `pool_kv` 键加入 `store/repository.py` 的 `_KV_KEYS`。
- 轻量迁移只补新表、已有表的可空列与索引；新增非空列等变更需要另行设计迁移，不靠删除现有数据库解决。
- 维护循环、心跳和 runner 停机采用“发信号、等待安全退出”，不要直接取消正在访问数据库的协程。应用先停 agent，再由代码池关闭共用 provider 和数据库。
- `Lifecycle.wait_background()` 只等待尚未结束的任务；不要改回“集合非空就反复 `gather`”，这曾在 Python 3.12 下导致停机空转。

## 引擎、任务与流式协议

- opencode 运行 `opencode serve`；pi 通过 `pi-bridge.mjs` 提供同构控制接口，每个会话运行一个 `pi --mode rpc` 子进程。事件翻译和结果判定留在 Python 引擎层，桥接进程保持协议转发与进程管理职责。
- 引擎选择存于 `agents.settings.engine`，未设置时用 `POOL_AGENT_DEFAULT_ENGINE`；沙箱实际引擎存于 `sandboxes.engine`，历史 NULL 记录按 opencode 处理。访问、接管和健康检查已有沙箱时按沙箱记录选择引擎。
- `POOL_AGENT_TEMPLATE` 启用 opencode，`POOL_AGENT_PI_TEMPLATE` 启用 pi。`GET /v1/agent-engines` 返回已启用引擎；默认引擎必须已启用。
- 切换引擎等于轮换沙箱：运行中的旧任务继续跟进，新会话进入新引擎；旧引擎的 `session_id` 续聊返回 409，不迁移会话。创建或退役沙箱前保留对最新 agent 设置的复核（PI-L1）。
- pi 不支持对话接口的 `agent` 参数，传入返回 400；不要伪装成具备 opencode 的 `build/plan` 能力。
- HTTP 响应只消费订阅事件；断连不取消 `TaskRunner`。对外 SSE 使用 `start / text / reasoning / tool / status / done`，结果写入任务表；不要承诺持久化逐事件回放或跨沙箱会话连续性。
- opencode 按会话事件和状态判断结束；pi 使用 `agent_settled`，两者有状态轮询兜底。pi 的 `run_id` 存入任务；运行丢失或桥接进程重启导致 `lost/unknown` 时任务失败，不能把旧文本或残缺结果当成功。
- 中止状态以网关中止标记为准，不能只看引擎消息里的错误；pi 的中止可能表现为 `stopReason=error`。
- 保留 `PiEngine.prompt_text()` 对 `/` 开头文本的转义。当前锁定的 pi 0.87.1 会把 `/mcp` 等当作扩展命令，且响应没有 `disposition`，绕过转义会让桥接进程把会话一直记为忙（PI-H1）。
- 建会话和发提示词是非幂等 POST，读超时不重试。当前 `_SLOW_POST_TIMEOUT_S=150` 秒，要长于桥接进程冷启动、缓存等待与 prompt preflight 的处理上限（PI-M1）。
- 设置变更需要重写配置并调用 `dispose`。opencode 的 dispose 会中止运行中会话，pi 的重载会推迟到会话空闲；网关对两者都先占住无运行中任务的沙箱再重载。
- 修改桥接进程需要更新 `VERSION` 并重建模板，再 `verify` 和更新部署的模板配置；仅改本地 `.mjs` 不会影响已有云模板。桥接进程对“命令已处理”的识别和 prompt RPC 超时改进仍是 PI 评审第五节的后续项，不要标成已经完成。

## 云平台、出网和凭证

- 固定 `e2b==2.31.0`、`e2b-code-interpreter==2.8.1`；仓库实测较新 SDK 的 `/v2` 接口与当前云平台不兼容，升级前需要兼容性验证。
- 需要暂停/恢复的代码池使用第二代模板；agent 使用各引擎专用模板。模板构建脚本固定引擎版本，升级应包含本机联调、新模板构建、verify 与相关端到端验证。
- 所有云端调用显式设置请求超时，并短于所属过渡态截止时间；保持 `check_deadlines()` 的启动校验。创建进入 WARMING 时会重设截止时间，不要把两个阶段误当成同一个计时窗口。
- 后台状态查询使用 `get_info/list`；探测 agent 使用 `/global/health`。不要用 `AsyncSandbox.connect()` 充当只读探活，它会续期并可能恢复暂停的沙箱。
- SDK 代理由 provider 显式传入。agent HTTP 客户端使用 `trust_env=False`；配置 `POOL_AGENT_INGRESS_IP` 时直连该 IP，保留沙箱域名的 SNI/Host，并绕过代理。
- 重试按操作幂等性区分。不要自动重跑用户代码或请求可能已生效的非幂等 POST；代码池执行超时返回 HTTP 200 加 `TimeoutError` 结果，与连接/后端故障的 502 区分。
- 云端列表有可见性延迟；对账保留查询前后数据库快照、版本条件和孤儿宽限期，不把刚创建的实例当孤儿删除。
- 模型/MCP 凭证只由管理员配置，经 `network.rules` 注入；沙箱内使用 `injected-by-platform` 占位符。注入不等于调用授权，沙箱内代码仍可使用注入能力访问目标域名。
- 平台 `allow_out` 优先于 `deny_out`；放行 IP/CIDR 不得覆盖强制屏蔽的内网/元数据地址，开放模式拒绝调用方的域名放行项，`deny_out` 只支持 IP/CIDR。保留旧策略读取的 `strict=False` 过滤行为。
- 不要整段屏蔽 `100.64.0.0/10`，平台 DNS 位于其中；平台策略细节以 `agent/policy.py` 和实测笔记为依据。
- `get_info` 会回显注入值，对外必须脱敏。`access_token` 不出接口；`lease_id` 是借用凭证，不在沙箱管理列表或其他调用方的输出中暴露。
- 不把 `.env`、`.env.*`、`*.env`、`.data/`、真实密钥或令牌加入版本库或输出到报告。`sxw_aicoding/百炼-mcp.txt` 被明确忽略，含明文 Token，不作为可公开引用的资料。
- 未配 API Key 时只用于回环地址开发；不要为排障顺手打开 `--allow-no-auth`。保留鉴权前的普通请求体大小限制及文件上传单独的大小限制。

## 本地开发与验证

优先使用已有项目虚拟环境；README 的环境示例为 Python 3.11，agent 的历史本机验证也使用 Python 3.12。不要照抄旧文档的 `/root/...` 解释器路径。需要新建环境时：

```bash
python3.11 -m venv .venv
. .venv/bin/activate
python -m pip install -r requirements-dev.txt
```

`PoolConfig.from_env()` 读取已导出的 `POOL_<字段名大写>` 环境变量；服务本身不自动读取 `.env`。云平台使用 `E2B_API_KEY / E2B_API_URL / E2B_DOMAIN`。配置默认值和约束同时查看 `config.py`、`check_deadlines()`、`check_agent_config()`。

本地测试入口（FakeProvider、内存引擎或假的 pi，不访问真实云平台和模型；部分测试会监听本地端口）：

```bash
python -m pytest
python -m pytest tests/test_agent_pi.py -k pi_h1
node --test sandbox_pool/agent/pi_bridge/test/bridge.test.mjs
```

Node 测试使用明确的文件路径，不传目录。桥接进程本身零第三方依赖；真实 pi 的 Node 要求及模板内安装版本查看 `scripts/build_pi_template.py`。

| 改动范围 | 相关测试 |
| --- | --- |
| 代码池、存储、HTTP | `test_pool.py`、`test_store.py`、`test_api.py` |
| 代码池评审回归 | `test_review_fixes.py`、`test_review_r2_fixes.py` |
| agent 策略、cron、协议、服务、HTTP | `test_agent_units.py`、`test_agent_service.py`、`test_agent_api.py` |
| AG 系列修复 | `test_agent_review_fixes.py` |
| pi、引擎切换、PI 系列修复 | `test_agent_pi.py`；桥接进程的 `bridge.test.mjs` |

- 测试文件位于 `tests/`，Node 测试位于 `sandbox_pool/agent/pi_bridge/test/`。`tests/conftest.py` 的 `make_pool` / `make_agents` 可构造共享数据库与 FakeProvider 的多个副本；`run_maintainer=False` 可用于控制时序。
- 并发和生命周期修复优先构造确定性的交错与失败注入，回归用例应能暴露修复前的问题；按现有评审编号组织。此类改动需要全量回归并按相关风险检查偶发失败。
- 保留测试替身的重要真实语义：客户端关闭后报错；opencode dispose 取消运行中会话；pi 重载保留运行中会话，进程丢失为 lost、桥接重启为 unknown，并模拟 0.87.1 的扩展命令行为。
- 纯文档改动检查路径、内容和 diff 即可；不为熟悉项目而启动服务或跑真实联调。历史报告中的通过数量不等于本次测试结果，本地 fake 测试也不证明真实云端、代理、模型或 MCP 当前可用。

## 真实运行与清理范围

以下操作会访问云端、模型、MCP，或影响运行进程；按用户当前授权范围执行，不能从“熟悉项目”或“只读评审”推导出运行授权：

| 入口 | 作用与影响 |
| --- | --- |
| `python -m sandbox_pool --host 127.0.0.1 --port 8000` | 启动真实服务；默认代码池会自动补货并创建云沙箱，不能当成无副作用的健康检查 |
| `scripts/run_local_cluster.sh start` | 默认启动 8001–8003 三个副本，共享 `.data/pool.db`；可通过 `PYTHON` 指定解释器 |
| `scripts/e2e_scenarios.py` | 真实代码池场景，包含副本崩溃与排空 |
| `scripts/e2e_agent_scenarios.py --engine pi` | 真实 agent 场景；支持 `--only`、`--replicas`；S10 会 `kill -9` 副本，S12 会切换引擎 |
| `scripts/build_opencode_template.py`、`scripts/build_pi_template.py` | 构建模板；`verify <模板ID>` 也会创建真实沙箱 |
| `scripts/poc_opencode_agent.py`、`scripts/poc_pi_agent.py` | 云上 PoC，创建临时实例并调用真实服务 |
| `scripts/pi_bridge_local_check.py` | 本机真实 pi 联调，会调用模型，与离线 Node 测试不同 |
| `scripts/cleanup_sandboxes.py --pool <池名>` | 按 `metadata.pool` 销毁当前账号、地域中的实例 |

- 真实测试优先使用明确的测试数据库、池名与配置，记录本次创建的资源；只测试 agent 时可以按测试报告设置 `POOL_TARGET_SIZE=0`，避免额外预热代码池。
- 集群的 `drain` 针对代码执行池，排空状态会持久化；重启不会自动解除。需要恢复时使用既有恢复接口，不随意删除数据库。停止网关也不等于立即销毁所有云端 agent 沙箱。
- `cleanup_sandboxes.py` **不带 `--pool` 会销毁当前账号、地域下的全部沙箱，包含暂停实例**。即便带池名，也会影响该池的全部实例；执行前核对目标归属与本次授权范围，不照抄历史报告中的全账号清理命令。
- 测试结束清理本次实例，模板通常保留复用。模板和沙箱实例是不同资源，不因为清理测试而删除现有模板；清理结果以实际查询为准。
- pi 模板的构建约束包括平台 CA、Node 版本、国内下载源及启动命令 16 KiB 上限；修改构建流程前先读实测笔记和脚本，不能仅以本地脚本语法正确作为模板可用的证明。
