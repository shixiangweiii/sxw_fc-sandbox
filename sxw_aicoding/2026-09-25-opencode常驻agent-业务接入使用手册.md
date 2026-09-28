# 常驻 agent（opencode / pi）· 业务接入使用手册

> 更新日期：2026-09-28。按当前源码基线 `4c051fe` 核对，包含 AG、PI、R3 系列及 R3 二次修复后的业务行为。
> 适用范围：`sandbox_pool` 的常驻 agent 子系统，通过 E2B Python SDK 接入阿里云 FC Agent Sandbox。当前构建脚本固定 opencode 1.18.32、pi 0.87.1、pi-mcp-adapter 2.37.0，pi 桥接进程版本为 0.1.2；这些是仓库的构建基线，不代表云端已部署模板的实际版本。
> 本手册中的默认值来自源码；模板 ID、地域、入口 IP、账号配额和实际费用由部署方提供。历史实测与本次文档核对的边界见第 11 节。

业务调用从第 2、3.4、4、5 节开始；部署与运维参阅第 3.1–3.3、6–9 节；引擎选择见第 10 节。

## 1. 这是什么

沙箱池服务按 **`(client_id, user_id)` 提供常驻 agent**，其中 `client_id` 是调用方 API Key 的名称。引擎可选 opencode（默认）或 pi，由接入方按用户选择。agent 运行在阿里云云沙箱里：

- 能读写文件、执行命令；网页读取方式因引擎而异，联网搜索需要配置百炼 WebSearch MCP 等服务。
- 默认使用 `deepseek/deepseek-flash`，模型由管理员配置。

业务系统只需要用 HTTP 调用，不接触云沙箱 SDK，也不持有模型 Key。

```
业务系统 ──HTTP（Bearer API Key）──► 沙箱池服务（可多副本）──► 云沙箱：opencode serve 或 pi 桥接进程（每个用户一个）
                                         │ 状态存库：agent、任务、定时任务、出网策略
                                         │ 后台：轮换、空闲销毁、健康检查、定时任务、任务接管
                                         └ 模型 / 搜索的 Key 由平台在出网时注入，沙箱里只有占位符
```

主要能力：

| 能力 | 说明 |
| --- | --- |
| 同步流式对话 | `POST /v1/agents/{user_id}/messages`，SSE 流式返回文本、思考、工具调用、最终结果 |
| 长时间任务 | 单任务默认最长 4 小时。客户端断开不影响任务，可随时重连或拉取结果 |
| 会话连续 | 带上 `session_id` 继续同一会话；仅限原沙箱仍处于可接任务的 `ACTIVE` 状态 |
| 定时任务 | cron（5 段）或固定间隔。到点自动在 agent 里执行，结果由业务系统拉取 |
| 出网策略 | 默认开放公网，加上规定的内网/元数据屏蔽段。可按用户切到白名单模式；业务系统和 agent 可查询策略，具体边界见 4.4 |
| 多副本接管 | 通过共享数据库协调；其他副本继续跟进已启动的运行，不重新发送提示词；接管边界见第 8 节 |

常驻 agent 与 `/v1/leases/*` 的代码执行池是两个子系统。业务调用 agent 时不需要借用、续租或归还沙箱。当前 agent API 没有文件上传下载、会话列表、历史消息查询、结果 webhook 或 IM 接口；这些能力不能从代码执行池接口推导出来。

## 2. 必须了解的行为约定

1. **agent 身份和沙箱生命周期分开。** 身份、设置、出网覆盖、定时任务和任务结果存数据库；文件、依赖和引擎会话留在沙箱内，没有跨沙箱持久化或迁移保证。任务终态默认保留 30 天，业务需要长期留存时应自行归档。
2. **轮换会停止旧会话接收新任务。** 默认 20 小时开始轮换：空闲沙箱直接销毁，有任务的转为 `RETIRING`，继续跟进已有任务；新任务按需创建新沙箱。新任务要求的运行时间超过剩余寿命时，也可能提前轮换。23.5 小时硬截止会使仍在运行的任务失败并销毁沙箱。agent 沙箱不暂停；这些是服务按 Eco 约束设置的默认值，账号套餐须另行核实。
3. **`session_id` 只在原沙箱可用。** 沙箱已退役、销毁、临近硬截止，或 agent 已切换引擎时，续聊返回 409，需要新建会话。切换引擎不搬迁上下文或文件。
4. **任务状态和最终结果是交付依据。** HTTP 200、收到 `start`、流正常关闭都不等于任务成功。等待 `done` 或查询任务终态；`result` 是本轮最后一条有文本的 assistant 消息，可能不包含前面的说明、工具输出。需要交付的内容应要求 agent 汇总进最终回复，并由业务校验结果是否满足要求。
5. **断线后用 `task_id` 继续查询。** 任务已准入后，断开 SSE 不取消后台执行。保存 `start.task_id`，通过任务详情或 `/stream` 恢复；没有收到 `start` 也可能已创建任务。对话接口没有幂等键，不要因网络超时自动重发同一消息。
6. **会话、agent 和池各有约束。** 同一会话同时只能有一个运行中任务（冲突返回 409）；每个 agent 默认最多 3 个运行中任务（429）。整个 agent 池默认最多 3 个沙箱（503），创建中、退役中、销毁中的记录均占容量，轮换需要留出余量。不同会话共享同一沙箱工作目录，并发修改同一文件须由业务协调。
7. **设置持久化与沙箱应用是异步的。** `PATCH /settings` 返回已保存的设置，维护循环在沙箱空闲时重写配置并重载；返回成功不表示新设置已用于运行。修改引擎则触发沙箱轮换，已有任务继续在旧引擎跟进，新会话使用新引擎。
8. **运行不等待人工交互。** opencode 的权限询问和反问被自动拒绝；pi 扩展的交互由桥接进程自动应答。当前业务 API 没有人工审批或追问答复通道。
9. **首次调用包含准备时间。** 首次消息或定时触发按需建沙箱、装配配置、创建会话，pi 还可能等待进程冷启动、MCP 缓存或上下文压缩。历史秒级耗时不是 SLA；超时配置与接入处理见第 5、6 节。

## 3. 快速开始

### 3.1 前置条件

- 优先使用已有项目虚拟环境。需要新建时按仓库入口使用 Python 3.11；历史 macOS 验证也使用 Python 3.12：

  ```bash
  python3.11 -m venv .venv
  .venv/bin/python -m pip install -r requirements-dev.txt
  ```

- SDK 固定为 `e2b==2.31.0`、`e2b-code-interpreter==2.8.1`，升级前需另做平台兼容性验证。
- 云沙箱：E2B 兼容的 API Key、API URL、Domain（同一地域）。
- 阿里云 AK/SK 与 Team ID：仅构建模板时需要，模板只需构建一次。
- DeepSeek API Key；百炼 WebSearch MCP 的 Key（可选，用于联网搜索）。

服务本身不自动读取 `.env`，只读取已经导出的环境变量。按部署方式注入配置；本地可显式加载自己维护的 `.env`，不要把它、`.data/` 或真实密钥提交到仓库。以下构建、verify、启动和消息调用均属于真实运行入口；构建或 verify 会创建云端资源，消息可能调用模型或 MCP。

### 3.2 构建 opencode 模板（只需一次）

```bash
set -a
. ./.env  # 构建需要 ALIBABA_CLOUD_ACCESS_KEY_ID、ALIBABA_CLOUD_ACCESS_KEY_SECRET、FCSANDBOX_REGION_ID、FCSANDBOX_TEAM_ID
set +a
.venv/bin/python scripts/build_opencode_template.py
# 使用上一步返回、且属于本次账号与地域的模板 ID
.venv/bin/python scripts/build_opencode_template.py verify '<opencode 模板 ID>'
```

- 模板基于官方 code-interpreter 镜像，默认规格 2C4G。
- 模板内置 git / python3 / pip / node / npm / curl，以及固定版本的 opencode 和守护进程；pip / npm 默认使用国内镜像。
- verify 会创建临时沙箱，检查引擎健康并在结束时销毁；它不代替真实模型、MCP 与业务场景验证。

**pi 模板**（使用 pi 引擎时需要，只需构建一次）：

```bash
.venv/bin/python scripts/build_pi_template.py
.venv/bin/python scripts/build_pi_template.py verify '<pi 模板 ID>'
```

- 基于同一类官方镜像，默认规格 2C2G。实际内存需求随并发会话和工具执行变化。
- 镜像的 Node 不满足 22.19+ 时，构建脚本安装固定的 Node 24.19.0；同时安装固定版本的 pi、pi-mcp-adapter、桥接进程与守护进程。
- 两种模板各自复用；历史报告中的模板 ID 不作为当前部署配置。

### 3.3 配置并启动服务

```bash
# 云沙箱：以下以杭州地域为格式示例，替换为本次账号与地域的配置
export E2B_API_KEY='<云沙箱 API Key>'
export E2B_API_URL='https://api.cn-hangzhou.e2b.fc.aliyuncs.com'
export E2B_DOMAIN='cn-hangzhou.e2b.fc.aliyuncs.com'
# 鉴权（名称:key，逗号分隔）
export KEY='<业务调用方随机长串>'
export ADMIN_KEY='<管理员随机长串>'
export POOL_API_KEYS="mybiz:$KEY"
export POOL_ADMIN_KEYS="ops:$ADMIN_KEY"
# agent 子系统
export POOL_AGENT_ENABLED=true
export POOL_AGENT_TEMPLATE='<本次 opencode 模板 ID>'
export POOL_AGENT_MODEL=deepseek/deepseek-flash
export POOL_AGENT_DEFAULT_ENGINE=opencode
export POOL_AGENT_MODEL_API_KEY='<DeepSeek Key>'
# 可选：启用 pi；仅部署 pi 时同时把 DEFAULT_ENGINE 设为 pi
# export POOL_AGENT_PI_TEMPLATE='<本次 pi 模板 ID>'
# 只用 agent、不用代码执行池时，不预热代码沙箱
export POOL_TARGET_SIZE=0
export POOL_AGENT_POOL_NAME=agents
# 可选：按第 9 节核实当前地域的入口 IP 后设置
# export POOL_AGENT_INGRESS_IP='<当前入口 IPv4>'

.venv/bin/python -m sandbox_pool --host 127.0.0.1 --port 8001
# 或本地多副本（共享 SQLite）：
# PYTHON=.venv/bin/python scripts/run_local_cluster.sh start 8001 8002
```

需要百炼联网搜索时，在启动前额外配置：

```bash
export BAILIAN_MCP_API_KEY='<百炼 WebSearch Key>'
export POOL_AGENT_MCP='{"websearch": {"type": "remote", "url": "https://dashscope.aliyuncs.com/api/v1/mcps/WebSearch/mcp"}}'
export POOL_AGENT_INJECT='{"dashscope.aliyuncs.com": {"Authorization": "Bearer ${BAILIAN_MCP_API_KEY}"}}'
```

`POOL_AGENT_INJECT` 外层单引号保留 `${BAILIAN_MCP_API_KEY}`，由服务启动时展开。未配置 MCP 时，默认没有联网搜索服务。

启动时会校验配置，配置有误会直接退出并说明原因。常见原因：两个引擎的模板都没设置、默认引擎没有启用、`POOL_AGENT_INJECT` 引用了未设置的环境变量、时间参数不合理。

以上示例通过 `POOL_TARGET_SIZE=0` 关闭代码执行池预热；它不关闭 `/v1/leases` 接口。agent 本身按需建沙箱，但数据库里已有启用的定时任务时，启动维护循环后也可能自动创建沙箱。需要提供远程入口时，部署方再配置监听地址、TLS 与访问控制。

### 3.4 发第一条消息

```bash
export BASE='http://127.0.0.1:8001'
# KEY 使用部署方提供的业务调用方 Key
curl -sS -N -H "Authorization: Bearer $KEY" -H "Content-Type: application/json" \
  -d '{"text": "用 bash 执行 uname -a，然后告诉我系统架构"}' \
  "$BASE/v1/agents/alice/messages"
```

返回示例（SSE；ID、输出与 usage 为示意值，不是本次实测）：

```
event: start
data: {"task_id": "b136…", "session_id": "ses_f26b…", "sandbox_id": "a6df…"}

event: tool
data: {"tool": "bash", "status": "running", "title": "uname -a", "input": "{\"command\": \"uname -a\"}", "output": null, "error": null}

event: tool
data: {"tool": "bash", "status": "completed", "title": "uname -a", "input": "…", "output": "Linux … x86_64 GNU/Linux\n", "error": null}

event: text
data: {"delta": "系统架构是 x86_64。"}

event: done
data: {"task_id": "b136…", "state": "SUCCEEDED", "result": "系统架构是 x86_64。", "usage": {"input": 312, "output": 15, "reasoning": 0, "cache_read": 7424, "cache_write": 0, "cost": 7.1e-05, "steps": 2}, "error": null, "session_id": "ses_f26b…"}
```

## 4. 接口参考

本节 `/v1/*` 接口都需要 `Authorization: Bearer <key>`（关闭鉴权的本地开发模式除外）；`GET /healthz` 不鉴权。

- **身份隔离按 Key 名称。** `mybiz:key-a` 与 `other:key-b` 下同名的 `user_id` 属于不同 agent；同名 Key 轮换密钥值后仍访问原有数据。不同密钥若配置成同一个名称，也会共享该名称下的 agent。
- **`user_id`** 由业务系统定义，创建时校验长度为 1–128 字符，建议使用稳定且适合 URL 路径的业务 ID。Key 应由业务后端持有，后端负责核对终端用户与 `user_id` 的关系。
- **自动创建身份的入口：** 发消息、修改设置、GET/PUT 出网策略、创建定时任务。仅创建身份不会分配沙箱；首次消息或定时触发才按需分配。其他查询对不存在的 agent 返回 404。
- **管理员身份不自动代入业务身份。** `/v1/admin/agents*` 可跨调用方查看；普通 `/v1/agents/{user_id}/*` 即使带管理员 Key，仍按该 Key 的名称隔离。重置或修改业务 agent 时须使用对应调用方身份。
- **请求体默认上限为 1 MiB**（`POOL_MAX_BODY_BYTES`，鉴权前检查）；字段长度上限与 UTF-8 字节上限同时生效。时间戳统一为 Unix epoch 秒，可带小数。

### 4.1 对话

`POST /v1/agents/{user_id}/messages`

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `text` | string，必填 | 长度 1–200000 字符。请求只支持文本，没有附件或多模态输入字段 |
| `session_id` | string 或 null | 最长 64 字符；继续已有会话，不传则新建会话 |
| `max_duration_s` | number 或 null，60–86400 | 默认 `POOL_AGENT_TASK_MAX_DURATION_S`（14400 秒）；传得更大时截到服务端上限。续聊还会限制到沙箱硬截止前 60 秒；可用时间不足 60 秒时返回 409。超时会发起中止，终态通常为 `TIMEOUT`，不保证在 deadline 当刻已停止 |
| `agent` | string 或 null，最长 64 字符 | opencode 的 agent 名称，如 `build`（默认）、`plan`（规划）。pi 不支持非空 `agent` 参数，传入返回 400；按实际沙箱引擎再次校验，覆盖切换期间的在途请求。网关不验证 opencode 名称是否存在，也不把 `plan` 当作额外的沙箱权限边界 |
| `stream` | bool，默认 true | true：SSE 流式；false：等任务结束后返回任务 JSON（见 4.3） |

**SSE 事件**（`text/event-stream`；响应流建立后，没有事件时默认每 15 秒发送一次 `: keepalive` 注释）：

| event | data | 说明 |
| --- | --- | --- |
| `start` | `{task_id, session_id, sandbox_id}` | 已分配任务和会话；此时提示词可能尚未被引擎受理。`sandbox_id` 是数据库沙箱记录 ID，不是云平台实例 ID。接管/跨副本跟随时可附带 `resumed: true` / `attached: true` |
| `text` | `{delta, snapshot?}` | 回复文本增量；跨副本重连可带 `snapshot: true`。用于过程展示，不承诺拼接后等于最终 `result`，详见 4.3 |
| `reasoning` | `{delta}` | 模型思考过程增量（可忽略） |
| `tool` | `{tool, status, title, input, output, error}` | 状态为 `running` / `completed` / `error`。输入、输出或错误正文超过 2000 字符时截断并附截断说明；对象输入转为 JSON 字符串。pi 的 `title` 为 null；重连可能缺少工具开始事件 |
| `status` | `{type, message, …}` | `retry`：模型限流等原因正在重试（带 `attempt`）；pi 引擎另有 `compaction`（正在压缩上下文）、`ui_request`（扩展请求交互，已自动应答） |
| `done` | `{task_id, state, result, usage, error, session_id}` | 任务结束，流随之关闭 |

任务在库中首先为 `RUNNING`，没有独立的 `QUEUED` 状态。终态包括 `SUCCEEDED`（引擎运行成功）、`FAILED`（模型报错、沙箱失效等）、`ABORTED`（业务请求中止）、`TIMEOUT`（任务运行超时）。失败原因见 `error`。失败或中止的任务也可能有部分 `result` 和 `usage`，不应作为成功交付。

`usage` 包含 `input` / `output` / `reasoning` / `cache_read` / `cache_write` tokens、`cost`（引擎上报的美元成本）和 `steps`（本轮 assistant 消息数，通常对应模型调用步数）。按本轮各 assistant 消息累加，不是整个会话累计；引擎未上报的字段可能为 0，早期失败时 `usage` 可为 null，不能当作云平台或模型账单。

建会话或订阅事件失败时，可能直接出现 `done: FAILED` 而没有 `start`。流已开始后的执行失败通过任务终态报告，HTTP 状态通常仍是 200；无 `done` 就断流时，应查询或重新跟随任务。

### 4.2 agent 信息与设置

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/v1/agents/{user_id}` | 返回 `user_id`、`agent_id`、`created_at`、`settings`、`settings_version`、`engine`、`egress_override`、`sandboxes` 数组、`running_tasks` ID 数组 |
| GET | `/v1/agent-engines` | 返回 `default` 与 `engines` 数组；各项含 `name`、`model`、`default`、`capabilities`，pi 还含 `thinking`。这是网关配置声明，不是云模板版本检测 |
| PATCH | `/v1/agents/{user_id}/settings` | 保存部分设置并返回完整 agent 信息；并发修改不同顶层字段互不覆盖，同一字段以后提交的为准。应用进度见下文 |
| DELETE | `/v1/agents/{user_id}/sandbox` | 重置当前 `ACTIVE` / `RETIRING` 沙箱，运行中任务记为 `FAILED`，原因 `sandbox reset by user`；返回 `{"destroyed": 数量}`。不删除身份、设置、定时任务和已有结果 |

设置字段：

| 字段 | 说明 |
| --- | --- |
| `idle_destroy_after_s` | 非负秒数；0 表示不因空闲销毁，未设置时取 `POOL_AGENT_IDLE_DESTROY_AFTER_S`。只有值大于 0，且最后一次活动来自定时任务时，才取该值与 `POOL_AGENT_SCHEDULE_IDLE_TAIL_S`（默认 600 秒）的较小值收尾；默认值 0 不会因定时任务结束自动销毁 |
| `instructions` | 长期说明，写入工作目录的 `AGENTS.md`，最长 20000 字符；空字符串或 null 清空用户说明 |
| `engine` | agent 使用的引擎：`opencode` / `pi`（必须是已启用的引擎），传 `null` 恢复默认 |
| `mcp` | agent 级 MCP 字典，格式同 opencode（pi 会转换）。每次替换整个 agent 级字典，再与管理员默认 MCP 按名称浅合并，同名配置以 agent 为准；`{}` / null 只清空 agent 覆盖，不清空管理员默认服务。不要在配置中写真实密钥 |

MCP 配置示例（作为 `PATCH /settings` 的 body）：

```json
{
  "instructions": "使用中文回复；需要交付的结论汇总到最终回复。",
  "idle_destroy_after_s": 3600,
  "mcp": {
    "docs": {"type": "remote", "url": "https://mcp.example.com/mcp"},
    "websearch": {"type": "remote", "url": "https://dashscope.aliyuncs.com/api/v1/mcps/WebSearch/mcp", "enabled": false}
  }
}
```

`docs` 是格式示例，需要替换为可用服务。远程服务支持 `headers`，本地服务支持 `{"type":"local","command":["程序","参数"],"environment":{}}`；它们会在沙箱里读取或执行。禁用默认服务时需要提供完整的同名配置和 `enabled:false`，仍须通过类型、URL 或命令校验。凭证注入由管理员配置，添加 MCP 不会自动获得外部服务权限。

**确认设置已应用：** PATCH 成功后查询 agent 信息，核对 `ACTIVE` 沙箱的 `config_version` 与顶层 `settings_version` 一致；引擎变更还需核对 `sandboxes[].engine`。顶层 `engine` 表示当前设置解析出的目标引擎，旧引擎的 `RETIRING` 沙箱可能同时存在。没有沙箱时，在下一次装配中应用。

两种引擎都只在沙箱没有运行中任务时重载。重载占用期间新消息会等待，超过 `POOL_AGENT_WAIT_SANDBOX_S` 返回 504；新设置刚保存而重载尚未开始时，不能假定下一条消息已经使用它。重载通常保留原会话，但切换引擎会换沙箱。

**重置边界：** 当前重置接口只处理 `ACTIVE` / `RETIRING`，不取消 `CREATING` / `WARMING`，也不阻止后续消息或定时任务重新创建实例。需要停用一个用户时，先由业务停止发送消息并停用其定时任务，再核对沙箱状态后重置。

### 4.3 任务

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/v1/agents/{user_id}/tasks` | JSON 数组，按创建时间倒序。筛选参数：`source`（message / schedule）、`schedule_id`、`state`（RUNNING / SUCCEEDED / FAILED / ABORTED / TIMEOUT）、`since`、`limit`（默认 50，1–500） |
| GET | `/v1/agents/{user_id}/tasks/{task_id}` | 详情 |
| GET | `/v1/agents/{user_id}/tasks/{task_id}/stream` | SSE 重连，可访问任意副本；运行中补发可确认归属的本轮文本并继续跟随，已结束通常直接返回 `done`。不支持按事件 ID 续传，具体边界见下文 |
| POST | `/v1/agents/{user_id}/tasks/{task_id}/abort` | 记录中止请求并返回当前任务 JSON；已结束或与结束竞争失败时返回 409。需要继续查询终态，不把 HTTP 200 当作已经停止 |

任务 JSON：

```json
{
  "task_id": "task-example", "state": "SUCCEEDED", "source": "message",
  "session_id": "ses_example", "schedule_id": null, "sandbox_row_id": "sandbox-row-example",
  "prompt": "请回复你好", "result": "你好", "error": null,
  "usage": {"input": 100, "output": 2, "reasoning": 0, "cache_read": 0, "cache_write": 0, "cost": 0, "steps": 1},
  "created_at": 1790351509.2, "started_at": 1790351509.2,
  "finished_at": 1790351524.4, "deadline": 1790365909.2
}
```

`sandbox_row_id` 与 SSE 的 `sandbox_id` 都是数据库记录 ID；云实例 ID 在 agent 信息的 `sandboxes[].provider_id` 中。任务准入时就记录 `started_at` 和 `deadline`，因此任务时限包含随后建会话、发提示词等准备时间；`session_id` 在准备阶段可为 null。内部 `run_id`、心跳及中止标记不属于 `TaskOut`。

**重连与页面展示：**

- 原 runner 仍在所连副本时，会回放它内存中的历史事件（上限 5000 条，连续 text/reasoning 合并），随后接实时事件；这不是持久化日志。
- 跨副本时，只在能确认属于该任务时补发文本快照。快照取本轮最后一条有文本的 assistant 消息，不补齐多步输出的所有历史文本、工具或思考事件。
- 本轮尚未开始、刚结束，或 pi 尚未记录本轮用户消息身份时，可能没有快照。重连时已经开始输出的消息可能等完成后整条补发。
- 取快照与建立订阅之间仍可能漏掉过程文本；连接也可能因接管而结束，需要再次查询或重连。没有 SSE `id` / `Last-Event-ID` 回放协议。
- 重新建立一条 `/stream` 时，应重建本次连接的临时展示缓冲，避免把回放内容再次追加到旧页面文本。收到 `done` 后，用 `done.result` 覆盖最终答案；拿不到 `done` 时以任务详情为准。

**中止延迟：** 持有任务的 runner 负责下发中止，其他副本只写标记，正常情况下约每秒查一次库；冷启动、网络抖动或接管会延长等待。下发失败后默认间隔 5 秒重试。为防止迟到中止影响同一会话后续任务，下发前占用会话 45 秒、下发限时 20 秒；正常送达会提前释放，失败或经历接管时，下一条消息可能等待剩余占用时间。超出准入等待上限返回 504。当前仍有进程整体冻结的极端边界，见第 8 节。

**可靠拉取结果：** `since` 的含义是 `created_at >= since`，不是完成时间或更新时间；列表没有游标、offset 或下一页。不要只用 `state=SUCCEEDED&since=上次轮询时间` 拉增量，这会漏掉先创建、后完成的长任务。先发现所有新任务、按 `task_id` 去重并保存；对 `RUNNING` ID 单独查询到终态。发现窗口适当重叠；一旦返回条数达到 `limit`，就不能据此宣称完整覆盖，高吞吐场景需要业务侧任务登记或扩展接口。

### 4.4 出网策略

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/v1/agents/{user_id}/egress` | 当前策略（见下） |
| PUT | `/v1/agents/{user_id}/egress` | 保存并整体替换该 agent 的覆盖对象，尝试立即下发到 `ACTIVE` / `RETIRING` 沙箱；失败由维护循环重试。body 传 `null` 恢复管理员默认策略 |

GET 返回：

```json
{
  "desired": {"version": "example-version", "mode": "open", "allow_out": ["api.deepseek.com", "dashscope.aliyuncs.com"],
              "deny_out": ["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "169.254.0.0/16", "100.100.100.200/32"],
              "injected_hosts": {"api.deepseek.com": ["Authorization"], "dashscope.aliyuncs.com": ["Authorization"]},
              "notes": "此处省略策略说明文本"},
  "override": null,
  "sandboxes": [{"sandbox_id": "sandbox-row-example", "state": "ACTIVE", "applied_version": "example-version", "in_sync": true,
                 "platform": {"allow_out": ["api.deepseek.com", "dashscope.aliyuncs.com"],
                              "deny_out": ["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "169.254.0.0/16", "100.100.100.200/32"],
                              "allow_public_traffic": false,
                              "rules": {"api.deepseek.com": {"Authorization": "***"}, "dashscope.aliyuncs.com": {"Authorization": "***"}}}}]
}
```

字段的含义：

- `desired`：期望生效的策略。
- `override`：已保存的 agent 覆盖；未提供的字段继承管理员默认策略，不继承上一次 PUT 的覆盖字段。数组是替换关系。
- `sandboxes[].in_sync`：数据库记录的已下发版本与期望版本是否一致，不是对平台每一项规则做比较。
- `platform`：云平台实际回显的配置，凭证已脱敏；读取失败可能为 null 并带 `platform_error`，实例不存在时为 `"not found"`。

PUT 成功不保证每个沙箱都已下发成功。避免并发修改同一个 agent 的出网策略；已有并发交错限制可能使 `in_sync` 与实际规则不一致，应结合 `platform` 核对（AG-L7，见第 11 节评审资料）。

PUT 示例：

```json
{"mode": "allowlist", "allow_out": ["github.com", "*.github.com", "pypi.org", "files.pythonhosted.org"], "deny_out": []}
```

上例只放行所列地址及注入凭证的域名；实际 clone 或依赖下载可能需要补充 CDN、镜像域名。切换到开放模式并添加一个示例屏蔽网段：

```json
{"mode": "open", "allow_out": [], "deny_out": ["203.0.113.0/24"]}
```

策略规则与平台限制：

- `allow_out`、`deny_out` 每个数组最多 200 项。凭证注入域名会自动加入最终 `allow_out`，调用方不能靠自身策略移除这些注入能力。
- 开放模式的 `deny_out` 只支持 IP / CIDR，填域名返回 400。要按域名限制，请用白名单模式。
- 白名单模式最终使用 `deny_out=["0.0.0.0/0"]` 配合放行项，调用方自定义 `deny_out` 不参与最终平台规则；不要用它否定一个已放行域名。
- 仓库平台实测记录：按域名过滤只对 80 / 443 端口生效；平台变化需重新核实。
- 强制屏蔽段为 `10.0.0.0/8`、`172.16.0.0/12`、`192.168.0.0/16`、`169.254.0.0/16`、`100.100.100.200/32`，调用方不能通过 IP/CIDR 放行项覆盖它们。平台上 `allow_out` 优先于 `deny_out`，所以：
  - `allow_out` 里的 IP / CIDR 不能与强制屏蔽网段重叠（包括 `0.0.0.0/0`），IPv4 映射 IPv6 的重叠网段也拒绝，否则返回 400；
  - 开放模式本来就放行全部公网，`allow_out` 只接受 IP / CIDR（用于在自己的 `deny_out` 里开例外），填域名返回 400；
  - 从白名单模式切回开放模式时，要同时清空 `allow_out`：`{"mode": "open", "allow_out": []}`，或者直接传 `null` 恢复默认。
- 白名单模式放行的域名如果被解析到内网地址，是否可达取决于平台按 SNI / Host 匹配的实现，服务无法校验。只放行可信域名。
- 不要整段屏蔽 `100.64.0.0/10`：平台 DNS 位于其中；当前单独屏蔽元数据地址 `100.100.100.200/32`。

agent 在沙箱内读取当前策略：`/home/user/.agent/egress.json`（每次下发后同步更新）；`AGENTS.md` 里也写明了这个文件的位置。

### 4.5 定时任务

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/v1/agents/{user_id}/schedules` | 创建并返回定时任务对象（HTTP 200），不立即执行 |
| GET | `/v1/agents/{user_id}/schedules` | JSON 数组，按创建时间正序 |
| GET / PATCH / DELETE | `/v1/agents/{user_id}/schedules/{id}` | 查看 / 修改 / 删除。PATCH 返回更新后的对象；DELETE 返回 `{"deleted":"定时任务 ID"}`。停用或删除不会中止已经创建的任务 |
| POST | `/v1/agents/{user_id}/schedules/{id}/run` | 手动触发一次，返回 TaskOut（一般为 RUNNING）而不等待执行完成；需要建沙箱时仍会等待就绪。被 `overlap` 跳过时返回 `{"skipped":true}`；准入失败返回 429 / 503 / 504 / 502 |

字段：

| 字段 | 说明 |
| --- | --- |
| `name` | 必填，非空，最长 128 字符 |
| `prompt` | 必填，非空；创建接口最长 200000 字符，每次触发时发送。PATCH 同样建议遵守此上限 |
| `cron` | 5 段 cron（分 时 日 月 周），例如 `0 9 * * 1-5`（工作日 9 点）。与 `every_s` 二选一；不支持秒字段 |
| `every_s` | 固定间隔秒数（≥ 60） |
| `timezone` | cron 的时区，默认 `Asia/Shanghai` |
| `enabled` | 默认 true；返回对象中存储为 1 / 0，停用时 `next_run_at=null` |
| `max_duration_s` | 可选，60 到服务端 `POOL_AGENT_TASK_MAX_DURATION_S`，超出返回 400；与消息接口的截断行为不同。未设置时每次触发使用服务端上限 |
| `overlap` | `skip`（默认）：发现同一定时任务上一次还在运行时跳过；`allow`：允许重叠，仍受 agent 并发与全池容量限制 |

创建示例：

```bash
curl -sS -X POST "$BASE/v1/agents/alice/schedules" \
  -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
  -d '{"name":"日报","cron":"0 9 * * 1-5","timezone":"Asia/Shanghai","prompt":"联网搜索昨天的 AI 行业要闻，最终回复汇总 5 条摘要及来源链接。","max_duration_s":600,"overlap":"skip"}'
```

响应包含 `id`、`agent_id`、`client_id`、`pool`、上述配置字段、`next_run_at`、`last_run_at`、`last_task_id`、`created_at`、`updated_at`。`last_task_id` 是最近成功创建的任务 ID，不代表任务执行成功；`last_run_at` 是自动调度领取触发时写入的时间，不能代替任务完成时间。

**修改与立即触发：** PATCH 只在 `cron` / `every_s` / `timezone` / `enabled` 变化时重算 `next_run_at`，只改名称或提示词不会打乱节奏。切换触发类型须同时清掉另一项，例如 `{"cron":null,"every_s":3600}`。除 `cron`、`every_s` 外，PATCH 的 null 值不用于清空已有字段。手动 `/run` 即使在 `enabled=false` 时也可执行，并且不改变自动触发节奏，也不更新 `last_run_at`。

**实际调度语义：**

- 每次在新会话执行，不使用对话接口的 `agent` 参数。提示词应包含完成任务所需背景，不能依赖上一次沙箱留下的文件。
- 多副本通过 `next_run_at` 的 CAS 领取同一次自动触发，领取时先推进下次时间，再尝试创建任务。领取后崩溃、容量不足或启动失败可能丢失该次执行；不保证每次都执行成功，也不会自动重试补偿。
- 到期迟到不超过 300 秒时，仍尝试执行一次；超过 300 秒只推进下次时间并记 `schedule_missed`，不批量补跑。固定间隔正常从原应触发时间推进，若推后仍落在过去，则从当前时间起算下一次。
- `overlap=skip` 是执行前检查，没有与准入构成一个原子操作；手动触发与自动触发同时发生时仍可能重叠（AG-L9）。要求严格去重的业务须在外部操作上实现幂等。
- 自动触发开不了任务时只记录 `schedule_failed`，可能没有可查询的失败 TaskOut；被跳过记 `schedule_skipped`。结合管理员事件统计和日志判断，不能仅靠任务列表判断调度健康。
- cron 支持数字、`*`、范围、步长与逗号组合；周日为 0 或 7，日与周都非 `*` 时按「或」匹配。

用 `GET /v1/agents/{user_id}/tasks?schedule_id=<id>` 发现执行记录，再按 4.3 的方式跟进 RUNNING 任务；不要把 `since` 当作结果完成时间游标。

### 4.6 管理员接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/v1/admin/agents` | 当前 agent 池中所有调用方的 agent 信息数组，附 `client_id`；沙箱不含访问令牌，`running_tasks` 为任务 ID 数组 |
| GET | `/v1/admin/agents/stats` | `pool`、`replica`、`engines`、`default_engine`、`sandboxes` 状态计数、`running_tasks` 总数、`local_runners`、`events` 与 `latency_ms`（count / p50 / p99 / max） |

`local_runners` 只表示所访问副本；顶层 `template` / `model` 保留 opencode 配置语义，多引擎部署看 `engines`。统计基于尚未清理的事件记录，不是永久累计或账单。

### 4.7 错误码

| 状态码 | 场景 |
| --- | --- |
| 400 | 参数不合法（cron、时区、出网策略、设置） |
| 401 / 403 | 未认证 / 需要管理员 |
| 404 | agent、任务、定时任务不存在，或不属于调用方 |
| 409 | 会话正忙、不可继续、引擎已切换或临近过期；中止的目标任务已结束 |
| 413 | 请求体超过 `POOL_MAX_BODY_BYTES`（默认 1 MiB） |
| 422 | 请求体格式错误（例如 `text` 为空） |
| 429 | 该 agent 运行中的任务数已达上限 |
| 502 | 沙箱启动连续失败，或准入过程中沙箱反复消失（详情见 `detail`） |
| 503 | agent 子系统未开启，或 agent 沙箱总数已达 `POOL_AGENT_MAX_SANDBOXES` |
| 504 | 等待沙箱就绪、设置重载或前一任务中止占用解除超时 |

业务异常通常返回 `{"error":"TaskConflict","detail":"session … is running another task; attach to it or wait"}`。422 为 FastAPI 的字段校验响应（`detail` 数组），不保证有 `error` 字段。执行中的模型、MCP 或沙箱侧错误通常体现在 `done` / TaskOut 的 `FAILED` 和 `error` 中。

重试前区分「明确未准入」与「结果未知」：收到明确的 429/503 等准入拒绝，可按业务策略退避；发送消息或手动 `/run` 后网络断开、读取超时，不能自动重新提交。已有 `task_id` 时查询原任务；没有时先按用户、创建时间窗口、提示词等核对任务记录，无法确认则交由业务处理重复执行风险。服务没有请求幂等键。

## 5. 接入示例（Python）

以下示例只提交一次消息，按 SSE 事件名解析，忽略保活注释；断流且已知任务 ID 时转为查询原任务。`resume()` 可在页面重新连接时单独调用。示例执行时会调用真实网关和模型。

```python
import json
import os
import time
from urllib.parse import quote

import httpx

BASE = os.environ.get("BASE", "http://127.0.0.1:8001")
KEY = os.environ["KEY"]
USER_ID = "alice"
ROOT = f"/v1/agents/{quote(USER_ID, safe='')}"


def sse_events(response):
    """支持多行 data；只在空行结束一个事件时解析 JSON。"""
    response.raise_for_status()
    event, data = None, []
    for line in response.iter_lines():
        if line == "":
            if event and data:
                yield event, json.loads("\n".join(data))
            event, data = None, []
        elif not line.startswith(":"):
            field, _, value = line.partition(":")
            value = value[1:] if value.startswith(" ") else value
            if field == "event":
                event = value
            elif field == "data":
                data.append(value)


def wait_result(client, task_id, wait_s=5 * 3600):
    """本地等待超时只停止查询，不取消服务端任务。"""
    until = time.monotonic() + wait_s
    while time.monotonic() < until:
        response = client.get(f"{ROOT}/tasks/{task_id}", timeout=30)
        response.raise_for_status()
        task = response.json()
        if task["state"] != "RUNNING":
            return task
        time.sleep(2)
    raise TimeoutError(f"任务 {task_id} 仍需继续查询；不要重新提交")


def receive(client, method, path, *, body=None, task_id=None,
            on_start=None, on_text=None):
    options = {"json": body} if body is not None else {}
    try:
        with client.stream(method, path, **options) as response:
            for event, payload in sse_events(response):
                if event == "start":
                    task_id = payload["task_id"]
                    if on_start:
                        on_start(payload)  # 正式业务在此持久化 task_id、session_id
                elif event == "text" and on_text:
                    on_text(payload["delta"])
                elif event == "done":
                    return payload  # 早期失败可能没有 start，仍按 done 处理
    except httpx.TransportError as exc:
        if task_id is None:
            raise RuntimeError("提交结果未知：未取得 task_id，先核对任务列表，不要自动重发") from exc
    if task_id is None:
        raise RuntimeError("流结束但没有 start/done；先核对任务记录，不要自动重发")
    return wait_result(client, task_id)


def chat(client, text, session_id=None, **callbacks):
    body = {"text": text, "stream": True}
    if session_id:
        body["session_id"] = session_id
    return receive(client, "POST", f"{ROOT}/messages", body=body, **callbacks)


def resume(client, task_id, **callbacks):
    # 调用前重建页面的临时文本缓冲；历史内容可能重新发送。
    return receive(client, "GET", f"{ROOT}/tasks/{task_id}/stream",
                   task_id=task_id, **callbacks)


with httpx.Client(
    base_url=BASE,
    headers={"Authorization": f"Bearer {KEY}"},
    timeout=httpx.Timeout(30, read=None),
    trust_env=False,
) as client:
    progress = {}  # 演示用；正式业务应保存到自己的数据库，以便进程退出后恢复
    done = chat(client, "记住：我的项目叫 atlas，然后回复好的。",
                on_start=progress.update)
    if done["state"] != "SUCCEEDED":
        raise RuntimeError(f"任务 {done['task_id']} {done['state']}: {done.get('error')}")
    print(done.get("result"), done.get("usage"))
    # 续聊：chat(client, "我的项目叫什么？", session_id=done["session_id"])
    # 恢复：resume(client, progress["task_id"])
```

接入建议：

- **超时分阶段设置：** SSE 响应建立前可能等待沙箱或配置重载，默认等待参数为 180 秒；此时没有 keepalive。流建立后保活间隔默认 15 秒，反向代理要禁用缓冲并允许长连接。示例的 `read=None` 允许长等待，业务还应设置自己的连接管理与等待上限。
- **接收任务 ID：** `start` 在建会话成功后、发提示词之前发送；pi 建会话或 prompt 的网关读超时为 150 秒。不要用几秒内没有 start/文本就重发的逻辑。
- **短任务：** `stream=false` 等待后返回完整 TaskOut，仍要检查 `state`。连接断开也不等于任务取消。
- **长任务：** 先用流式取得并保存 `task_id`，随后可主动断开并轮询；需要实时展示时再调用 `resume()`。当前没有单独的「提交消息并立即返回任务 JSON」接口。
- **取消：** 调用 `/abort` 后等终态；关闭浏览器或停止本地轮询不执行中止。中止也不会回滚此前已经发生的文件、命令或外部服务副作用。

## 6. 配置参考（环境变量）

以下为 [config.py](../sandbox_pool/config.py) 中的默认值，通过已导出的环境变量覆盖。各副本应使用相同的数据库、池名、鉴权名称、引擎、默认设置和注入配置；不同环境使用不同池名与数据库，agent 池名也应与代码执行池的 `POOL_POOL_NAME` 区分。

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `POOL_AGENT_ENABLED` | false | 开启 agent 子系统 |
| `POOL_AGENT_TEMPLATE` | — | opencode 模板 ID；配了即启用 opencode 引擎（与 pi 模板至少配一个） |
| `POOL_AGENT_PI_TEMPLATE` | — | pi 模板 ID；配了即启用 pi 引擎 |
| `POOL_AGENT_DEFAULT_ENGINE` | `opencode` | 必须已启用。agent 未显式选择或所选引擎已停用时回退到它；更改后维护循环和新请求会按目标引擎轮换沙箱 |
| `POOL_AGENT_PI_MODEL` | `deepseek/deepseek-flash` | pi 引擎的模型（provider/model）；模型 Key 的注入配置与 opencode 共用 |
| `POOL_AGENT_PI_THINKING` | 空 | pi 引擎的思考级别：`off` / `minimal` / `low` / `medium` / `high` / `xhigh` / `max`，空为 pi 默认 |
| `POOL_AGENT_PI_PORT` | 4096 | pi 桥接进程端口（与模板一致） |
| `POOL_AGENT_PORT` | 4096 | opencode 端口（与模板一致） |
| `POOL_AGENT_WORKDIR` | `/home/user/workspace` | 引擎工作目录；变更时须同时核对模板及 pi 桥接进程的工作目录配置，不能只改网关变量 |
| `POOL_AGENT_POOL_NAME` | agents | 池名：库记录和云端元数据 `pool`，用于对账 |
| `POOL_AGENT_MODEL` | `deepseek/deepseek-flash` | opencode 引擎的模型（provider/model） |
| `POOL_AGENT_MODEL_HOST` | `api.deepseek.com` | 模型 Key 注入的域名 |
| `POOL_AGENT_MODEL_API_KEY` | — | 模型 Key：只注入到出网请求，不进沙箱 |
| `POOL_AGENT_INJECT` | 空 | 额外注入 JSON：`{域名: {请求头: 值}}`，支持 `${环境变量}`。连同模型域名最多 10 个精确域名，不接受通配符；每个请求头值 1–2048 字节，只能由管理员配置 |
| `POOL_AGENT_MCP` | 空 | 默认 MCP 配置 JSON（opencode 的 `mcp` 段，不含密钥） |
| `POOL_AGENT_EGRESS` | 开放模式 | 默认出网策略 JSON，例如 `{"mode":"open","allow_out":[],"deny_out":[]}`；mode 可选 `open` / `allowlist` |
| `POOL_AGENT_INGRESS_IP` | 空 | 网关访问两种引擎时可直连当前地域的入口 IP，保留沙箱域名的 SNI/Host，并绕过代理。未配置时可使用显式 `HTTPS_PROXY` / `https_proxy`，客户端 `trust_env=False` |
| `POOL_AGENT_MAX_SANDBOXES` | 3 | agent 沙箱总数上限（包括轮换中的旧沙箱） |
| `POOL_AGENT_MAX_RUNNING_TASKS` | 3 | 每个 agent 同时运行的任务上限 |
| `POOL_AGENT_MAX_LIFE_S` / `POOL_AGENT_ROTATE_AFTER_S` | 84600 / 72000 | 硬截止（23.5h）/ 开始轮换（20h）；启动要求 `0 < ROTATE_AFTER_S < MAX_LIFE_S <= 86400` |
| `POOL_AGENT_IDLE_DESTROY_AFTER_S` | 0 | 未覆盖该设置的 agent 使用的空闲销毁时间，0 表示不因空闲销毁 |
| `POOL_AGENT_SCHEDULE_IDLE_TAIL_S` | 600 | 仅在空闲销毁已开启、最后活动来自 schedule 时，将空闲阈值缩短到两者较小值 |
| `POOL_AGENT_TASK_MAX_DURATION_S` | 14400 | 单任务最长执行时间（必须小于 `MAX_LIFE_S`） |
| `POOL_AGENT_BOOT_TIMEOUT_S` | 180 | 创建、装配各自的过渡态截止时间，进入 WARMING 后重新计时；启动要求 ≥60。超过由维护循环接管销毁 |
| `POOL_AGENT_PLATFORM_TIMEOUT_S` | 7200 | 平台侧兜底 TTL，启动要求 ≥600；剩余不足一半时续期，续期最多到服务硬截止后 60 秒 |
| `POOL_AGENT_HEALTH_INTERVAL_S` / `POOL_AGENT_HEALTH_MAX_FAILURES` | 30 / 3 | `/global/health` 检查间隔 / 连续失败阈值；达到阈值使任务失败并销毁，后续请求按需新建 |
| `POOL_AGENT_WAIT_SANDBOX_S` | 180 | 等待沙箱就绪或任务准入等待重载/中止占用的上限，不是业务请求端到端总超时 |
| `POOL_AGENT_TASK_HEARTBEAT_S` / `POOL_AGENT_TASK_TAKEOVER_S` | 10 / 60 | 刷新心跳间隔 / 每次心跳写入的所有权有效期；启动要求后者大于前者 2 倍 |
| `POOL_AGENT_STREAM_KEEPALIVE_S` | 15 | SSE 保活间隔 |
| `POOL_AGENT_TASK_RETENTION_S` | 2592000 | 按 `finished_at` 清理已结束任务，默认 30 天，不清理 RUNNING |

共用配置中，与 agent 接入有关的还有：

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `POOL_API_KEYS` / `POOL_ADMIN_KEYS` | 空 | 逗号分隔的 `名称:key`；两者都为空时为匿名管理员，仅用于回环地址开发 |
| `POOL_DB_URL` | `sqlite+aiosqlite:///./.data/pool.db` | agent 与代码执行池共用；本地多副本共享同一 SQLite 文件 |
| `POOL_TARGET_SIZE` | 5 | 代码执行池预热目标；仅用 agent 时设置 0 |
| `POOL_MAX_BODY_BYTES` | 1048576 | 普通请求体上限；agent 消息没有独立文件上传入口 |
| `POOL_MAINTAIN_INTERVAL_S` | 1 | 维护循环基础间隔，运行时带随机抖动 |
| `POOL_RECONCILE_INTERVAL_S` / `POOL_ORPHAN_GRACE_S` | 60 / 60 | 对账间隔 / 孤儿实例宽限期 |
| `POOL_CLEANUP_INTERVAL_S` / `POOL_HISTORY_RETENTION_S` | 3600 / 604800 | 历史清理周期 / 事件等历史保留 7 天，任务另用 agent 保留时间 |

启动还会执行共用云端操作超时校验（`check_deadlines()`）。完整配置与约束见 [service.py](../sandbox_pool/agent/service.py)、[E2B provider](../sandbox_pool/provider/e2b_provider.py)。修改环境变量需要重启网关进程；多副本最终须保持一致，避免各自下发不同设置或网络规则。

环境变量中的默认模型、MCP、thinking 等变更不会自动递增已有 agent 的 `settings_version`。重启配置一致的副本后，可用 `PATCH /settings {}` 增加版本并等待空闲重载；或在允许丢失会话时重置沙箱。模板、端口或工作目录变更按模板升级流程处理。

## 7. 安全说明

- **按域名注入凭证：** 模型与 MCP 凭证由管理员交给平台 `network.rules`，模板和生成的模型配置只写占位符 `injected-by-platform`；沙箱端不应保存真实值。不要把密钥写进提示词、`instructions`、MCP headers/environment 或日志。历史凭证隔离实测见第 11 节，本次未重复实测。
- **注入能力由沙箱内代码共享：** 沙箱里的任意代码都可能使用对应域名的注入能力；注入不等于只授权某个工具或模型调用。凭证应只允许需要的外部资源，并设置业务可承受的使用额度。
- **出网范围：** 默认开放模式仅加上源码规定的强制屏蔽地址段，不应当作覆盖所有协议、IPv6 或所有内网情况的通用网络隔离证明。处理敏感数据时按 4.4 配置白名单，并核对平台实际规则及域名解析边界。
- **入口令牌：** 创建时设置 `allow_public_traffic=false`，网关使用保存在数据库中的流量令牌访问沙箱；当前接口过滤 `access_token`、`lease_id`，平台网络回显的注入值也会脱敏。数据库备份仍需要受控保存。
- **租户隔离：** 使用不同的调用方名称和用户 ID 隔离身份；不要让不同终端用户共用一个 `user_id`。同一个 agent 的并发会话共享沙箱文件、进程和网络能力。
- **结果数据：** 提示词、最终结果和部分错误写入数据库；工具事件也可能包含业务内容。业务自行决定可提交的数据和存储、展示、日志范围。

## 8. 运维

### 8.1 观测与多副本

`GET /healthz` 只反映网关基础响应，不验证 agent 引擎、模型、MCP 或账号额度。结合 `/v1/admin/agents` 查看沙箱与任务，结合 `/v1/admin/agents/stats` 查看状态数量、启动耗时、任务终态和 `schedule_failed` / `schedule_missed` 等事件计数。

本地集群日志位于 `.data/replica-<端口>.log`。关注 `taking over task`、`abort failed, will retry`、`apply settings/network ... failed`、`unhealthy`、`vanished` 等信息；引擎日志在沙箱的 `/home/user/.agent/opencode.log`、`/home/user/.agent/pi-bridge.log`、`/home/user/.agent/pi-logs/`，需通过部署方的诊断通道读取，业务 API 不提供这些文件下载。

本地多副本方案是同一主机多进程共享 SQLite（WAL），agent 与代码执行池共用数据库连接管理。Postgres 是替换方向，不应将现有 SQLite 验证当成生产 Postgres 或跨主机部署已经验收。数据库、云账号/地域、池名与配置必须对应，避免另一个环境把实例当孤儿清理。

### 8.2 任务接管与现有限制

正常 SIGTERM 会让 runner 交出任务，由仍运行的副本接管；崩溃时则等待心跳截止过期（默认有效期 60 秒）再接管。接管只订阅和查询已启动的会话，不自动重新发送提示词，避免重复执行外部操作。客户端需容忍断流并按 `task_id` 恢复。

- 沙箱消失、会话尚未建立就丢失负责副本，任务会失败；pi 运行中进程或桥接重启导致 `lost/unknown` 时也按失败处理。
- 仍保留已记录的早期崩溃窗口（AG-L6）：会话 ID 已落库、提示词尚未送达就崩溃，接管可能得到空的 `SUCCEEDED`。业务仍需校验结果，不能只看状态。
- 中止按会话下发，现有会话占用与限时机制覆盖已复现的迟到中止交错；如果进程恰好在写出请求的同步路径整体冻结超过约 25 秒，仍不能从网关彻底排除误伤后续运行。按 `run_id` 校验中止尚未在桥接进程实现。
- 中止或超时发起后，任务仍需等待引擎停止再结算；异常引擎可能一直等到健康检查或沙箱硬截止。它不是外部副作用回滚机制。
- pi 桥接进程识别「命令已处理但没有运行」、prompt RPC 超时完善仍是后续项。当前网关对 `/` 开头文本转义，并将建会话/发提示词的读超时设为 150 秒；没有因此承诺进程可在任意故障点恢复。

### 8.3 停服与资源清理

正常停止网关不会立即销毁所有 agent 沙箱。所有副本停止后，实例按最后一次设置的平台 TTL 到期，默认 TTL 为 7200 秒；实际剩余时间取决于最近续期。期间没有网关负责调度、跟进与落库。

计划结束一个用户的服务时，先停业务请求并禁用其定时任务，再选择等待任务完成或明确中止，最后用对应调用方 Key 调用 `DELETE /v1/agents/{user_id}/sandbox` 并复查。该接口的状态范围见 4.2。代码执行池的 `/v1/admin/drain` 和集群脚本 `drain` 不会排空 agent；代码池的排空标志会持久化，恢复用 `DELETE /v1/admin/drain`，不删除数据库。

只有在已核对云账号、地域、池名和清理归属，且该池全部实例均可销毁时，才使用按池清理：

```bash
.venv/bin/python scripts/cleanup_sandboxes.py --pool '<本次明确要清理的 agent 池名>'
```

`--pool` 会清理该池全部实例；**省略参数会销毁当前账号、地域中的全部沙箱，包括其他池和暂停实例**。共享池不能用来只清理某一次测试。测试使用独立数据库、池名并记录本次资源，结束以实际查询确认清理；模板通常保留复用，不因清理沙箱而删除。

### 8.4 引擎与配置升级

1. opencode 用构建脚本的 `--opencode-version` 指定新版本；pi 先做本机联调，再用 `--pi-version` / `--mcp-version` 构建。`scripts/pi_bridge_local_check.py` 会调用真实模型，属于另行安排的联调。
2. 新模板执行 `verify`，再在独立环境做真实业务场景验证。改 pi 桥接进程须更新 `VERSION` 并重建模板；仅改本地 `.mjs` 不影响云端已有实例。
3. 更新 `POOL_AGENT_TEMPLATE` / `POOL_AGENT_PI_TEMPLATE` 并重启所有网关副本。修改同一引擎的模板 ID 不会立即替换存量沙箱，它们在后续轮换、空闲销毁或重置后才换用新模板。
4. 多副本的引擎默认值、启用集合、凭证注入和默认策略须协调更新，避免新旧副本同时按不同配置维护同一个 agent；需要立即替换时先处理现有任务，再重置对应用户。

模型/MCP 默认配置应用到已有沙箱的方式见第 6 节。实际成本取决于沙箱规格、存活时长、模型与 MCP 用量；请以本次账号账单和报价核算，不沿用历史邀测价格。降低空闲成本可设置 `idle_destroy_after_s`，代价是下次重新装配且原会话与文件不可继续。

## 9. 排障

| 现象 | 原因与处理 |
| --- | --- |
| 首次对话慢，日志有 `ConnectError` / `ProxyError` | 先分清沙箱创建、配置写入、引擎冷启动还是模型响应慢。历史故障包括代理 fake-ip / TUN；核对沙箱域名解析与代理路由，必要时配置当前入口 IP，不把历史耗时或故障比例当诊断结论 |
| `CERTIFICATE_VERIFY_FAILED: IP address mismatch` | 当前网关配置入口 IP 后应直连并保留沙箱域名的 SNI/Host，检查是否运行了旧代码或用了错误入口 IP。pi 到模型的 TLS 错误还应核对模板中平台 CA 与 `NODE_EXTRA_CA_CERTS` |
| 请求返回 502「agent sandbox failed to start 3 times」 | 查看 `detail` 和副本日志。常见原因：模板 ID 错误或不在同一地域、账号配额不足、网络不通 |
| 返回 503「capacity reached」 | 检查所有未删除状态的总容量，尤其 `RETIRING` / `DESTROYING`；引擎切换或轮换可能同时保留旧沙箱。按实际配额留出容量或配置空闲销毁 |
| 续聊返回 409「session … no longer available」 | 会话所在沙箱已被轮换、空闲销毁或重置，请开新会话 |
| 模型报 401 / 鉴权失败 | `POOL_AGENT_MODEL_API_KEY` 错误，或 `POOL_AGENT_MODEL_HOST` 与模型实际域名不一致（注入按精确域名匹配） |
| 联网搜索不可用 | 检查 `POOL_AGENT_MCP`、`POOL_AGENT_INJECT` 与 `BAILIAN_MCP_API_KEY`。打开沙箱内 `/home/user/.agent/egress.json`，确认 `injected_hosts` 里有 `dashscope.aliyuncs.com`；白名单模式下注入域名会自动放行 |
| agent 访问某网站失败 | 结合 `/egress` 的 `desired`、`in_sync` 和 `platform` 核对；白名单可能还需要 CDN/镜像域名，不要盲目扩大放行范围 |
| 定时任务没有执行或没有失败 TaskOut | 检查时区、enabled、next_run_at，以及 `schedule_missed` / `schedule_skipped` / `schedule_failed`；迟到超过 300 秒不执行，准入失败可能没有任务记录 |
| `since` 轮询漏掉完成结果 | `since` 按创建时间筛选；保存并持续跟进 RUNNING ID，不按完成时刻推断游标，见 4.3 |
| 重连没有快照、文本缺段或显示重复 | 过程流不保证完整回放；重连重建临时缓冲，最终以 `done.result` 或任务详情覆盖，见 4.3 |
| `/abort` 返回 RUNNING，下一条消息等待或返回 504 | 中止异步下发，失败重试、接管和会话占用会延迟；继续查询目标任务，检查 `abort failed` 日志。不要重复提交业务消息 |
| pi 任务 `FAILED`，错误包含 `agent process exited` / `lost` / `unknown` | 检查沙箱是否仍 ACTIVE、进程或桥接是否重启及内存情况。只有原沙箱和会话文件仍可用时才可能续聊；先核对已产生的副作用，再决定发新任务，不自动重放原提示词 |
| 切换引擎后续聊返回 409「belongs to engine …」 | 旧会话属于切换前的引擎，请开新会话 |
| `PATCH …/settings` 返回 400「engine … is not enabled」 | 该引擎没有配置模板。先用 `GET /v1/agent-engines` 查看可选的引擎 |
| 设置已返回成功但没有生效 | 比较 `settings_version` 和 ACTIVE 沙箱的 `config_version`，等待运行中任务结束及重载；引擎切换还要检查沙箱引擎和容量 |
| 正常流结束、HTTP 200，但任务失败或没有最终答案 | 读取 `done.state/error` 或 TaskOut；早期失败可能没有 start，空的 SUCCEEDED 也需要业务校验，见 8.2 |

如需核实入口 IPv4，可从可信 DNS 查询本次地域的 API 域名，例如杭州地域的 `dig +short @223.5.5.5 api.cn-hangzhou.e2b.fc.aliyuncs.com`，再由部署方确认路由可达后设置 `POOL_AGENT_INGRESS_IP`。它控制网关到沙箱引擎的入口连接，不是模型出网代理。此处没有固定 IP 值。

## 10. 选择引擎：opencode 与 pi

两种引擎共用同一套接口、任务、定时任务、出网策略和凭证注入，区别只在沙箱里运行的 agent loop：

| | opencode（默认） | pi |
| --- | --- | --- |
| 沙箱里运行 | `opencode serve`（HTTP 服务） | pi 桥接进程 + 每个会话一个 `pi --mode rpc` 进程 |
| 构建脚本默认规格 | 2C4G | 2C2G；不代表生产容量评估 |
| 准备阶段 | 服务预加载工作目录与 MCP | 另有会话进程冷启动与 MCP 元数据缓存等待；默认闲置 600 秒回收会话进程，沙箱内会话文件仍保留。进程回收不等于销毁沙箱 |
| 内置工具 | 读写文件、bash、webfetch 等 | 读写文件、bash；没有网页抓取工具，agent 用 curl |
| 联网搜索（百炼 MCP） | 内置 MCP，工具名 `websearch_bailian_web_search` | 经 pi-mcp-adapter 直接注册，工具名相同 |
| `agent` 参数（build / plan） | 支持 | 不支持，传了返回 400 |
| 以 `/` 开头的消息（如 `/mcp tools`） | 作为普通文本发给模型 | 同左：网关转义后发送，不会执行 pi 的扩展命令 |
| 交互 | 权限询问与反问自动拒绝 | 网关没有人工权限询问通道；扩展交互自动应答（`status` 事件 `ui_request`） |
| 改设置后的重载 | dispose 本身会中断会话，所以网关只在沙箱无运行中任务时调用 | 桥接重载保留运行中进程，但网关同样先等沙箱无运行中任务 |
| 结果与用量 | 统一 result / usage 结构，按引擎消息提取 | 相同对外结构；未上报项为 0，工具 title 为 null；运行丢失判为失败 |
| 上下文过长 | 自动压缩 | 自动压缩（`status` 事件 `compaction`） |

```bash
# 可选的引擎
curl -sS -H "Authorization: Bearer $KEY" "$BASE/v1/agent-engines"
# 把 alice 切到 pi（已配置 pi 模板时；之后不带旧 session_id 的消息使用 pi）
curl -sS -X PATCH -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
     -d '{"engine": "pi"}' "$BASE/v1/agents/alice/settings"
# 恢复默认引擎
curl -sS -X PATCH -H "Authorization: Bearer $KEY" -H 'Content-Type: application/json' \
     -d '{"engine": null}' "$BASE/v1/agents/alice/settings"
```

需要 `build/plan` 参数或内置 webfetch 时，可按这些能力选择 opencode；接入 pi 扩展或使用其会话进程模式时选择 pi。实际延迟、内存和成本应在当前模板、模型、MCP 配置下测量，两种引擎都不能通过「重载能力」推导为运行中立即应用新设置。

切换后旧任务继续跟进，旧会话不能再接新消息；仍可用旧 `task_id` 查询、重连或中止任务。新沙箱按需创建且计入同一个容量上限。管理员停用某个已选引擎时，该 agent 会回退到当前默认引擎；只有 pi 模板的部署必须同时设置 `POOL_AGENT_DEFAULT_ENGINE=pi`。

## 11. 源码依据与验证边界

本次更新核对了 HTTP 路由、请求/响应模型、鉴权、配置、服务编排、runner、维护循环、存储查询、出网策略、两种引擎和模板构建脚本，并对照现有回归用例及修复结论。主要源码入口：

| 业务约定 | 当前源码 |
| --- | --- |
| 接口、字段、鉴权和错误 | [agent_routes.py](../sandbox_pool/api/agent_routes.py)、[schemas.py](../sandbox_pool/api/schemas.py)、[auth.py](../sandbox_pool/api/auth.py)、[app.py](../sandbox_pool/api/app.py) |
| 设置、续聊、重连与手动触发 | [service.py](../sandbox_pool/agent/service.py) |
| 任务执行、中止、结果落库 | [runner.py](../sandbox_pool/agent/runner.py) |
| 轮换、心跳接管、迟到调度与清理 | [maintainer.py](../sandbox_pool/agent/maintainer.py)、[agent_repo.py](../sandbox_pool/store/agent_repo.py) |
| 出网、配置渲染与引擎差异 | [policy.py](../sandbox_pool/agent/policy.py)、[opencode 引擎](../sandbox_pool/agent/engines/opencode.py)、[pi 引擎](../sandbox_pool/agent/engines/pi.py)、[HTTP 客户端](../sandbox_pool/agent/opencode.py) |
| 模板与桥接版本 | [opencode 构建脚本](../scripts/build_opencode_template.py)、[pi 构建脚本](../scripts/build_pi_template.py)、[pi 桥接进程](../sandbox_pool/agent/pi_bridge/pi-bridge.mjs) |

设计与历史验证资料：

- [opencode 实施方案及执行结果](方案设计/2026-09-25-opencode应用沙箱池-实施方案.md)、[pi 实施方案及与方案的差异](方案设计/2026-09-26-pi引擎接入-实施方案.md)。
- [AG 评审、取舍与修复](代码评审/2026-09-26-opencode常驻agent子系统代码评审报告.md)、[PI 修复与桥接后续项](代码评审/2026-09-26-pi引擎接入代码评审报告.md)。
- [R3 评审及修复后的遗留边界](代码评审/2026-09-27-opencode进程池与pi集成-最近三次提交代码评审报告.md)、[R3 二次修复与真实云沙箱验证](代码评审/2026-09-27-9f265ba修复二次复核与真实云沙箱验证报告.md)（以第六节的后续修复结论为准）。
- [测试报告](2026-09-25-opencode常驻agent-测试报告.md)第 9 节记录了 2026-09-28 的本地 210 项回归和两种引擎真实 S1–S12 验证；[平台实测笔记](../docs/fc-agent-sandbox-notes.md)记录兼容性与平台行为。这些都是对应日期、代码和环境的历史结果。

本次离线检查覆盖了 19 个 agent 路由、33 个 agent 配置字段及配置表中的 43 个默认值；26 处本地链接、JSON/请求模型、Shell 和 Python 示例语法检查通过。Python 示例另用 `httpx.MockTransport` 验证了正常 SSE、已知任务断流回查、未知提交不重发、无 start 的失败、GET 重连、HTTP 拒绝、多行 data/CRLF，没有联网。

**本次更新是文档核对，不是新一轮运行验收：** 只修改本手册并做上述离线检查，没有运行全量 pytest、启动服务、构建或 verify 模板，也没有调用云端、模型或 MCP。当前部署的模板版本、实例状态、代理连通性、配额与价格未在本次核实，不能由历史通过数量或文档示例推断当前可用。
