# opencode 常驻 agent · 业务接入使用手册

> 适用版本：sandbox_pool（agent 子系统）+ opencode 1.18.32 + 阿里云云沙箱（cn-hangzhou）。更新日期：2026-09-26（按代码评审修复更新了出网策略校验、设置重载、定时任务接口的说明，见 `sxw_aicoding/代码评审/2026-09-26-opencode常驻agent子系统代码评审报告.md`）。
> 设计与实现见 `sxw_aicoding/方案设计/2026-09-25-opencode应用沙箱池-实施方案.md`；测试结果见 `sxw_aicoding/2026-09-25-opencode常驻agent-测试报告.md`。

## 1. 这是什么

沙箱池服务为**每个用户提供一个常驻的 opencode agent**。agent 运行在阿里云云沙箱里：
- 能读写文件、执行命令、抓取网页、联网搜索（百炼 WebSearch MCP）；
- 使用 DeepSeek 模型（默认 `deepseek/deepseek-flash`）。

业务系统只需要用 HTTP 调用，不接触云沙箱 SDK，也不持有模型 Key。

```
业务系统 ──HTTP（Bearer API Key）──► 沙箱池服务（可多副本）──► 云沙箱：opencode serve（每个用户一个）
                                         │ 状态存库：agent、任务、定时任务、出网策略
                                         │ 后台：轮换、空闲销毁、健康检查、定时任务、任务接管
                                         └ 模型 / 搜索的 Key 由平台在出网时注入，沙箱里只有占位符
```

主要能力：

| 能力 | 说明 |
| --- | --- |
| 同步流式对话 | `POST /v1/agents/{user_id}/messages`，SSE 流式返回文本、思考、工具调用、最终结果 |
| 长时间任务 | 单任务默认最长 4 小时。客户端断开不影响任务，可随时重连或拉取结果 |
| 会话连续 | 带上 `session_id` 继续同一会话，agent 记得上下文 |
| 定时任务 | cron（5 段）或固定间隔。到点自动在 agent 里执行，结果由业务系统拉取 |
| 出网策略 | 默认开放公网，并屏蔽内网与云元数据地址。可按用户切到白名单模式；业务系统和 agent 都能读到当前生效的策略 |
| 高可用 | 多副本无状态，副本崩溃后运行中的任务由其他副本接管 |

## 2. 必须了解的行为约定

1. **沙箱是「会过期的运行体」，agent 身份长期存在**：
   - 按 Eco 计划规则，单个沙箱最长存活 24 小时。服务在 20 小时后、空闲时轮换沙箱，23.5 小时硬截止。
   - 轮换、空闲销毁、手动重置后，沙箱里的文件、安装的依赖、opencode 会话都会清空。
   - agent 的配置（设置、出网策略、定时任务）和任务记录保存在服务的数据库里，不受影响。
2. **会话只能在原沙箱内继续**：沙箱被轮换或销毁后，旧的 `session_id` 返回 409，请开新会话。
3. **结果以最终回复为准**：任务结束时，最终回复、token 用量、状态、错误会写入任务记录，沙箱销毁后仍可查询。agent 生成的文件会随沙箱销毁，需要保留的内容请让 agent 写进最终回复。
4. **无人值守**：agent 的权限询问、反问会被自动拒绝，不会挂起等人。
5. **同一会话同一时间只能跑一个任务**：会话正忙时再发消息返回 409。每个 agent 同时运行的任务数有上限（默认 3），超过返回 429。
6. **首次对话会新建沙箱**：
   - 正常网络下，新建加装配约 2–5 秒；本机开着代理的 fake-ip 时约 10–15 秒，见第 9 节。
   - 沙箱就绪后，首字约 1–2 秒。

## 3. 快速开始

### 3.1 前置条件

- Python 3.10+，安装依赖：`pip install -r requirements-dev.txt`（SDK 固定为 `e2b==2.31.0`）。
- 云沙箱：E2B 兼容的 API Key、API URL、Domain（同一地域）。
- 阿里云 AK/SK 与 Team ID：仅构建模板时需要，模板只需构建一次。
- DeepSeek API Key；百炼 WebSearch MCP 的 Key（可选，用于联网搜索）。

### 3.2 构建 opencode 模板（只需一次）

```bash
set -a; . ./.env; set +a     # ALIBABA_CLOUD_ACCESS_KEY_ID / SECRET、FCSANDBOX_REGION_ID、FCSANDBOX_TEAM_ID
python scripts/build_opencode_template.py            # 约 1.5 分钟，输出 POOL_AGENT_TEMPLATE=<模板ID>
python scripts/build_opencode_template.py verify <模板ID>   # 可选：建一个沙箱确认 opencode 已在运行，然后销毁
```

- 模板基于官方 code-interpreter 镜像，规格 2C4G。
- 模板内置 git / python3 / pip / node / npm / curl，以及固定版本的 opencode 和守护进程；pip / npm 默认使用国内镜像。
- 当前 cn-hangzhou 已构建的模板：`z0tkbiqlztqsma57014d`。

### 3.3 配置并启动服务

```bash
# 云沙箱
export E2B_API_KEY=... E2B_API_URL=https://api.cn-hangzhou.e2b.fc.aliyuncs.com E2B_DOMAIN=cn-hangzhou.e2b.fc.aliyuncs.com
# 鉴权（名称:key，逗号分隔）
export POOL_API_KEYS="mybiz:<随机长串>" POOL_ADMIN_KEYS="ops:<另一个随机长串>"
# agent 子系统
export POOL_AGENT_ENABLED=true
export POOL_AGENT_TEMPLATE=z0tkbiqlztqsma57014d
export POOL_AGENT_MODEL=deepseek/deepseek-flash
export POOL_AGENT_MODEL_API_KEY=<DeepSeek Key>          # 只注入到出网请求，不进沙箱
# 百炼联网搜索 MCP（Key 同样由平台注入）
export BAILIAN_MCP_API_KEY=<百炼 WebSearch Key>
export POOL_AGENT_MCP='{"websearch": {"type": "remote", "url": "https://dashscope.aliyuncs.com/api/v1/mcps/WebSearch/mcp"}}'
export POOL_AGENT_INJECT='{"dashscope.aliyuncs.com": {"Authorization": "Bearer ${BAILIAN_MCP_API_KEY}"}}'
# 只用 agent、不用代码执行池时，不预热代码沙箱
export POOL_TARGET_SIZE=0
# 可选：本机代理为 fake-ip 模式时（见第 9 节）
export POOL_AGENT_INGRESS_IP=47.111.182.73

python -m sandbox_pool --host 0.0.0.0 --port 8001          # 单副本
# 或本地多副本（共享 SQLite）：PYTHON=.venv/bin/python scripts/run_local_cluster.sh start 8001 8002
```

启动时会校验配置，配置有误会直接退出并说明原因。常见原因：未设置模板、`POOL_AGENT_INJECT` 引用了未设置的环境变量、时间参数不合理。

### 3.4 发第一条消息

```bash
curl -N -H "Authorization: Bearer $KEY" -H "Content-Type: application/json" \
  -d '{"text": "用 bash 执行 uname -a，然后告诉我系统架构"}' \
  http://127.0.0.1:8001/v1/agents/alice/messages
```

返回（SSE）：

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

所有接口都需要 `Authorization: Bearer <key>`（关闭鉴权的本地开发模式除外）。

- **agent 的隔离**：agent 由「调用方 key 的名称 + `user_id`」唯一确定。不同 key 下同名的 `user_id` 是不同的 agent，访问别人的 agent 返回 404。
- **`user_id`**：由业务系统自己定义，1–128 个字符。第一次对话时自动创建对应的 agent。

### 4.1 对话

`POST /v1/agents/{user_id}/messages`

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `text` | string，必填 | 发给 agent 的消息 |
| `session_id` | string | 继续已有会话；不传则新建会话 |
| `max_duration_s` | number，60–86400 | 最长执行时间。超过后中止，状态为 `TIMEOUT`。默认同时受 `POOL_AGENT_TASK_MAX_DURATION_S`（4h）限制 |
| `agent` | string | opencode 的 agent（`build` 默认，`plan` 只读规划） |
| `stream` | bool，默认 true | true：SSE 流式；false：等任务结束后返回任务 JSON（见 4.3） |

**SSE 事件**（`text/event-stream`；没有事件时每 15 秒发送一次 `: keepalive` 注释）：

| event | data | 说明 |
| --- | --- | --- |
| `start` | `{task_id, session_id, sandbox_id}` | 任务已开始；保存 `task_id`，用于重连、查询、中止 |
| `text` | `{delta}` | 回复文本增量，按顺序拼接即完整回复 |
| `reasoning` | `{delta}` | 模型思考过程增量（可忽略） |
| `tool` | `{tool, status, title, input, output, error}` | 工具调用状态变化：`running` / `completed` / `error`；输入输出截断到 2000 字符 |
| `status` | `{type: "retry", message, attempt}` | 模型限流等原因正在重试 |
| `done` | `{task_id, state, result, usage, error, session_id}` | 任务结束，流随之关闭 |

`state` 的取值：
- `SUCCEEDED`：成功；
- `FAILED`：模型报错、沙箱失效等，原因见 `error`；
- `ABORTED`：被中止；
- `TIMEOUT`：超过最长执行时间。

`usage` 包含 `input` / `output` / `reasoning` / `cache_read` / `cache_write` tokens、`cost`（美元）、`steps`（模型调用次数）。

### 4.2 agent 信息与设置

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/v1/agents/{user_id}` | 设置、当前沙箱（状态、创建时间、硬截止 `hard_deadline`）、运行中的任务 ID |
| PATCH | `/v1/agents/{user_id}/settings` | 修改设置（只传要改的字段），沙箱空闲时自动应用：重写配置并重载 opencode（通常 1–2 秒）。重载会中断运行中的会话，所以只在没有任务时进行；重载期间到达的新消息会等它完成再开始 |
| DELETE | `/v1/agents/{user_id}/sandbox` | 重置：销毁当前沙箱（运行中任务记为失败），下次对话自动新建 |

设置字段：

| 字段 | 说明 |
| --- | --- |
| `idle_destroy_after_s` | 空闲多少秒后销毁沙箱以节省费用；0 表示不因空闲销毁，默认取 `POOL_AGENT_IDLE_DESTROY_AFTER_S`。由定时任务唤醒的沙箱，结束后按更短的 `POOL_AGENT_SCHEDULE_IDLE_TAIL_S`（默认 600s）收尾 |
| `instructions` | 给 agent 的长期说明，写入工作目录的 `AGENTS.md`，最长 20000 字 |
| `mcp` | 追加的 MCP 服务，格式同 opencode 的 `mcp` 配置：`{"名称": {"type": "remote", "url": "https://…"}}` 或 `{"名称": {"type": "local", "command": ["npx", "-y", "…"]}}`。不要在这里写密钥：沙箱里的 agent 能读到这里的内容，密钥请让管理员通过 `POOL_AGENT_INJECT` 注入 |

### 4.3 任务

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/v1/agents/{user_id}/tasks` | 列表，按创建时间倒序。参数：`source`（message / schedule）、`schedule_id`、`state`、`since`（epoch 秒）、`limit`（≤500） |
| GET | `/v1/agents/{user_id}/tasks/{task_id}` | 详情 |
| GET | `/v1/agents/{user_id}/tasks/{task_id}/stream` | 断线重连（SSE）。任务仍在运行：先补发已有的回复文本，再接实时事件直到 `done`；已结束：直接返回 `done`。可以连任意副本 |
| POST | `/v1/agents/{user_id}/tasks/{task_id}/abort` | 中止运行中的任务（已结束返回 409） |

任务 JSON：

```json
{"task_id": "…", "state": "SUCCEEDED", "source": "message", "session_id": "ses_…", "schedule_id": null,
 "sandbox_row_id": "…", "prompt": "…", "result": "最终回复", "error": null, "usage": {…},
 "created_at": 1790351509.2, "started_at": 1790351509.2, "finished_at": 1790351524.4, "deadline": 1790365909.2}
```

### 4.4 出网策略

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/v1/agents/{user_id}/egress` | 当前策略（见下） |
| PUT | `/v1/agents/{user_id}/egress` | 按用户覆盖策略，立即下发到运行中的沙箱；body 传 `null` 恢复默认 |

GET 返回：

```json
{
  "desired": {"version": "6c1f…", "mode": "open", "allow_out": ["api.deepseek.com", "dashscope.aliyuncs.com"],
              "deny_out": ["10.0.0.0/8", "172.16.0.0/12", "192.168.0.0/16", "169.254.0.0/16", "100.100.100.200/32"],
              "injected_hosts": {"api.deepseek.com": ["Authorization"], "dashscope.aliyuncs.com": ["Authorization"]}},
  "override": null,
  "sandboxes": [{"sandbox_id": "…", "state": "ACTIVE", "applied_version": "6c1f…", "in_sync": true,
                 "platform": {"allow_out": […], "deny_out": […], "allow_public_traffic": false,
                              "rules": {"api.deepseek.com": {"Authorization": "***"}}}}]
}
```

三个字段的含义：
- `desired`：期望生效的策略。
- `sandboxes[].in_sync`：沙箱上已生效的版本与期望是否一致。
- `platform`：云平台实际回显的配置，凭证已脱敏。

PUT 示例：

```json
{"mode": "allowlist", "allow_out": ["*.github.com", "pypi.org", "files.pythonhosted.org"]}   // 白名单：只能访问这些 + 注入凭证的域名
{"deny_out": ["1.2.3.0/24"]}                                                                 // 开放模式下额外屏蔽某些 IP 段
```

平台限制：
- 开放模式的 `deny_out` 只支持 IP / CIDR，填域名返回 400。要按域名限制，请用白名单模式。
- 按域名过滤只对 80 / 443 端口生效。
- 内网段和元数据地址始终屏蔽，无法去掉。平台上 `allow_out` 优先于 `deny_out`，所以：
  - `allow_out` 里的 IP / CIDR 不能与内网段、元数据地址重叠（包括 `0.0.0.0/0`），否则返回 400；
  - 开放模式本来就放行全部公网，`allow_out` 只接受 IP / CIDR（用于在自己的 `deny_out` 里开例外），填域名返回 400；
  - 从白名单模式切回开放模式时，要同时清空 `allow_out`：`{"mode": "open", "allow_out": []}`，或者直接传 `null` 恢复默认。
- 白名单模式放行的域名如果被解析到内网地址，是否可达取决于平台按 SNI / Host 匹配的实现，服务无法校验。只放行可信域名。

agent 在沙箱内读取当前策略：`/home/user/.agent/egress.json`（每次下发后同步更新）；`AGENTS.md` 里也写明了这个文件的位置。

### 4.5 定时任务

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| POST | `/v1/agents/{user_id}/schedules` | 创建 |
| GET | `/v1/agents/{user_id}/schedules` | 列表 |
| GET / PATCH / DELETE | `/v1/agents/{user_id}/schedules/{id}` | 查看 / 修改 / 删除。PATCH 只在 `cron` / `every_s` / `timezone` / `enabled` 变化时重算下次触发时间；只改名称、提示词不会打乱原来的节奏 |
| POST | `/v1/agents/{user_id}/schedules/{id}/run` | 立即触发一次，返回任务。被 `overlap` 跳过时返回 `{"skipped": true}`；开不了任务时按错误码返回（429 并发已满 / 503 容量已满 / 504 等沙箱超时 / 502 建沙箱失败） |

字段：

| 字段 | 说明 |
| --- | --- |
| `name` | 名称 |
| `prompt` | 每次触发时发给 agent 的消息 |
| `cron` | 5 段 cron（分 时 日 月 周），例如 `0 9 * * 1-5`（工作日 9 点）。与 `every_s` 二选一 |
| `every_s` | 固定间隔秒数（≥ 60） |
| `timezone` | cron 的时区，默认 `Asia/Shanghai` |
| `enabled` | 是否启用 |
| `max_duration_s` | 每次最长执行时间 |
| `overlap` | `skip`（默认）：上一次还在运行时跳过本次；`allow`：照常触发 |

触发语义：
- 多副本下每次只触发一次；服务停机期间错过的触发不补跑。
- 每次触发在新会话里执行。沙箱可能已被轮换，所以提示词要写得「自给自足」，例如需要仓库就让 agent 自己 clone。

拉取结果：`GET /v1/agents/{user_id}/tasks?schedule_id=<id>&since=<上次拉取时间>`。

### 4.6 管理员接口

| 方法 | 路径 | 说明 |
| --- | --- | --- |
| GET | `/v1/admin/agents` | 所有 agent、它们的沙箱（不含访问令牌）与运行中任务 |
| GET | `/v1/admin/agents/stats` | agent 池的沙箱状态统计、事件计数、各操作耗时（`agent_create`、`agent_boot`、`task_*` 的 p50 / p99） |

### 4.7 错误码

| 状态码 | 场景 |
| --- | --- |
| 400 | 参数不合法（cron、时区、出网策略、设置） |
| 401 / 403 | 未认证 / 需要管理员 |
| 404 | agent、任务、定时任务不存在，或不属于调用方 |
| 409 | 会话正忙；会话所在沙箱已被轮换或销毁；任务已结束 |
| 422 | 请求体格式错误（例如 `text` 为空） |
| 429 | 该 agent 运行中的任务数已达上限 |
| 502 | 沙箱启动连续失败（详情见 `detail`） |
| 503 | agent 子系统未开启，或 agent 沙箱总数已达 `POOL_AGENT_MAX_SANDBOXES` |
| 504 | 等待沙箱就绪超时 |

错误响应体：`{"error": "TaskConflict", "detail": "session … is running another task; attach to it or wait"}`。

## 5. 接入示例（Python）

```python
import json
import httpx

BASE, KEY, USER = "http://127.0.0.1:8001", "<API Key>", "alice"
client = httpx.Client(base_url=BASE, headers={"Authorization": f"Bearer {KEY}"},
                      timeout=httpx.Timeout(30, read=None), trust_env=False)


def chat(text, session_id=None, on_text=print):
    """流式对话；返回 done 事件（含 task_id、session_id、result、usage）。"""
    body = {"text": text, **({"session_id": session_id} if session_id else {})}
    start = None
    with client.stream("POST", f"/v1/agents/{USER}/messages", json=body) as r:
        r.raise_for_status()
        event, data = None, []
        for line in r.iter_lines():
            if line.startswith("event:"):
                event = line[6:].strip()
            elif line.startswith("data:"):
                data.append(line[5:].strip())
            elif line == "" and event:
                payload = json.loads("\n".join(data))
                if event == "start":
                    start = payload                      # 保存 task_id，断线后用来重连
                elif event == "text":
                    on_text(payload["delta"])
                elif event == "tool":
                    print(f"\n[{payload['tool']} {payload['status']}] {payload.get('title') or ''}")
                elif event == "done":
                    return payload
                event, data = None, []
    # 流意外中断：用 task_id 重连（任意副本）或轮询任务
    return resume(start["task_id"]) if start else None


def resume(task_id):
    with client.stream("GET", f"/v1/agents/{USER}/tasks/{task_id}/stream") as r:
        for line in r.iter_lines():
            if line.startswith("data:") and '"state"' in line:
                return json.loads(line[5:])


done = chat("记住：我的项目叫 atlas。然后回复“好的”。")
done = chat("我的项目叫什么？", session_id=done["session_id"])   # 同一会话继续
print(done["result"], done["usage"])

# 定时任务：工作日 9 点生成日报，结果由业务系统拉取
s = client.post(f"/v1/agents/{USER}/schedules", json={
    "name": "日报", "cron": "0 9 * * 1-5", "prompt": "联网搜索昨天的 AI 行业要闻，整理成 5 条摘要"}).json()
tasks = client.get(f"/v1/agents/{USER}/tasks", params={"schedule_id": s["id"], "state": "SUCCEEDED"}).json()
```

接入建议：
- **超时**：流式读取的超时要足够长（示例里是 `read=None`），长任务可能几分钟没有输出。
- **保活**：服务每 15 秒发送一次 keepalive 注释；中间的网络设备如果有空闲超时，要大于 15 秒。
- **断线处理**：断线后不要重发消息，否则会重复执行。用 `task_id` 重连，或者轮询任务。
- **非流式调用**：`stream=false` 会一直阻塞到任务结束，适合短任务。长任务用流式，或者先发流式、拿到 `task_id` 后断开，再轮询任务。

## 6. 配置参考（环境变量）

agent 子系统的配置都以 `POOL_AGENT_` 开头；其余配置沿用沙箱池，见 `docs/sandbox-pool-design.md` 第 7 节。

| 变量 | 默认 | 说明 |
| --- | --- | --- |
| `POOL_AGENT_ENABLED` | false | 开启 agent 子系统 |
| `POOL_AGENT_TEMPLATE` | — | opencode 模板 ID（必填） |
| `POOL_AGENT_POOL_NAME` | agents | 池名：库记录和云端元数据 `pool`，用于对账 |
| `POOL_AGENT_MODEL` | `deepseek/deepseek-flash` | 模型（provider/model） |
| `POOL_AGENT_MODEL_HOST` | `api.deepseek.com` | 模型 Key 注入的域名 |
| `POOL_AGENT_MODEL_API_KEY` | — | 模型 Key：只注入到出网请求，不进沙箱 |
| `POOL_AGENT_INJECT` | 空 | 额外的凭证注入 JSON：`{域名: {请求头: 值}}`，值支持 `${环境变量}`。每个沙箱最多 10 个域名，只能由管理员配置 |
| `POOL_AGENT_MCP` | 空 | 默认 MCP 配置 JSON（opencode 的 `mcp` 段，不含密钥） |
| `POOL_AGENT_EGRESS` | 开放模式 | 默认出网策略 JSON：`{"mode": "open"/"allowlist", "allow_out": [], "deny_out": []}` |
| `POOL_AGENT_INGRESS_IP` | 空 | 平台入口 IP：本机 DNS 被代理 fake-ip 接管时设置（第 9 节）。设置后网关直连该 IP 访问 opencode，不走 `HTTPS_PROXY`（经代理隧道时 SNI 无法指定，证书校验会失败） |
| `POOL_AGENT_MAX_SANDBOXES` | 3 | agent 沙箱总数上限（包括轮换中的旧沙箱） |
| `POOL_AGENT_MAX_RUNNING_TASKS` | 3 | 每个 agent 同时运行的任务上限 |
| `POOL_AGENT_MAX_LIFE_S` / `POOL_AGENT_ROTATE_AFTER_S` | 84600 / 72000 | 沙箱硬截止（23.5h）/ 开始轮换（20h）。Eco 计划单实例最长 24h |
| `POOL_AGENT_IDLE_DESTROY_AFTER_S` | 0 | 新 agent 的默认空闲销毁时间（0 表示不销毁） |
| `POOL_AGENT_SCHEDULE_IDLE_TAIL_S` | 600 | 定时任务唤醒的沙箱结束后的空闲收尾时间 |
| `POOL_AGENT_TASK_MAX_DURATION_S` | 14400 | 单任务最长执行时间（必须小于 `MAX_LIFE_S`） |
| `POOL_AGENT_BOOT_TIMEOUT_S` | 180 | 建沙箱和装配的截止时间，超过由其他副本接管销毁 |
| `POOL_AGENT_PLATFORM_TIMEOUT_S` | 7200 | 平台侧兜底超时；维护循环自动续期，最长到硬截止。服务整体宕机时，沙箱会在这个时间后自行回收 |
| `POOL_AGENT_HEALTH_INTERVAL_S` / `POOL_AGENT_HEALTH_MAX_FAILURES` | 30 / 3 | 健康检查间隔 / 连续失败多少次后重建沙箱 |
| `POOL_AGENT_WAIT_SANDBOX_S` | 180 | 请求等待沙箱就绪的时长 |
| `POOL_AGENT_TASK_HEARTBEAT_S` / `POOL_AGENT_TASK_TAKEOVER_S` | 10 / 60 | 任务心跳间隔 / 心跳过期多久后由其他副本接管 |
| `POOL_AGENT_STREAM_KEEPALIVE_S` | 15 | SSE 保活间隔 |
| `POOL_AGENT_TASK_RETENTION_S` | 2592000 | 已结束任务的保留时间（30 天） |

## 7. 安全说明

- **凭证不进沙箱**：
  - 模型 Key、MCP Key 由云平台在出网时按域名注入请求头（`network.rules`）。沙箱里只有占位符 `injected-by-platform`。
  - 已实测：沙箱环境变量里没有 Key，agent 执行 `env` 也拿不到。
- **注入不等于授权**：沙箱里的任何代码都能借平台注入的凭证访问对应域名，包括被提示词注入操控的 agent。请给注入的凭证设置最小权限和额度上限：
  - DeepSeek 设消费上限；
  - GitHub 使用细粒度 token，只授权需要的仓库。
- **开放公网的风险**：agent 能访问任意公网地址；如果被提示词注入，可能把沙箱里的数据发出去。
  - 处理敏感数据的用户，建议切到白名单模式（4.4）。
  - 内网和云元数据地址始终屏蔽。
- **沙箱端口不公开**：沙箱以 `allow_public_traffic=false` 创建，直接访问沙箱端口返回 403。访问令牌只保存在服务的数据库里，任何接口都不返回。
- **多用户隔离**：
  - 每个用户一个独立的沙箱（虚拟机级隔离）；
  - agent 按「调用方 key + user_id」隔离；
  - opencode 内部没有多用户隔离，不要让多个终端用户共用同一个 `user_id`。
- **不要把密钥写进 `instructions` 或 `mcp` 设置**：agent 能读到这些内容。

## 8. 运维

- **健康与统计**：
  - `GET /healthz`；
  - `GET /v1/admin/agents/stats`：沙箱状态、`agent_boot` 耗时、任务成功 / 失败 / 超时次数；
  - `GET /v1/admin/agents`：每个 agent 的沙箱和运行中任务。
- **日志**：副本日志中的关键事件：
  - 沙箱：`agent sandbox … ready`、`retiring`、`idle`、`unhealthy`、`vanished`；
  - 任务：`taking over task`；
  - 连接：`retrying in …`（连接类错误重试）。
- **重置单个用户**：`DELETE /v1/agents/{user_id}/sandbox`。
- **停服**：
  - 正常停止（SIGTERM）时，副本会把运行中的任务交给其他副本接管；
  - 全部停止后，沙箱在 `POOL_AGENT_PLATFORM_TIMEOUT_S` 内自行过期；
  - 也可以先逐个重置用户，再执行 `python scripts/cleanup_sandboxes.py --pool agents`。
- **升级 opencode**：
  1. 用 `--opencode-version` 构建新模板；
  2. 先用 `verify` 子命令验证；
  3. 修改 `POOL_AGENT_TEMPLATE` 并滚动重启。
  已有沙箱会在轮换或空闲销毁后换成新模板；需要立即生效时，逐个重置用户即可。不要覆盖正在使用的模板。
- **成本估算**（Eco 邀测价，2C4G 为 0.24 元/小时，不含模型费用）：
  - 一直运行：约 173 元/月/用户；
  - 空闲 1 小时即销毁、每天用约 4 小时：约 36 元/月/用户；
  - 模型按 DeepSeek 实际用量计费，opencode 的系统提示词会命中缓存，缓存读 $0.003/百万 tokens。

## 9. 排障

| 现象 | 原因与处理 |
| --- | --- |
| 首次对话要 10–15 秒，日志里有 `write … failed (ConnectError(''))` | 本机代理开着 fake-ip / TUN，到沙箱子域名的新连接约 1/3 失败，重试后成功。建议在代理工具里把 `*.e2b.fc.aliyuncs.com` 设为直连并加入 fake-ip-filter；同时设置 `POOL_AGENT_INGRESS_IP`（用 `dig +short @223.5.5.5 api.cn-hangzhou.e2b.fc.aliyuncs.com` 查得） |
| 日志里 opencode 请求报 `CERTIFICATE_VERIFY_FAILED: IP address mismatch` | 旧版本在同时设置 `POOL_AGENT_INGRESS_IP` 与 `HTTPS_PROXY` 时，经代理隧道访问入口 IP，SNI 丢失。当前版本设置了入口 IP 就直连、不走代理；仍出现请确认代码已更新 |
| 请求返回 502「agent sandbox failed to start 3 times」 | 查看 `detail` 和副本日志。常见原因：模板 ID 错误或不在同一地域、账号配额不足、网络不通 |
| 返回 503「capacity reached」 | agent 沙箱总数达到 `POOL_AGENT_MAX_SANDBOXES`（包括正在轮换的旧沙箱）。调大上限，或者给不常用的用户设置空闲销毁 |
| 续聊返回 409「session … no longer available」 | 会话所在沙箱已被轮换、空闲销毁或重置，请开新会话 |
| 模型报 401 / 鉴权失败 | `POOL_AGENT_MODEL_API_KEY` 错误，或 `POOL_AGENT_MODEL_HOST` 与模型实际域名不一致（注入按精确域名匹配） |
| 联网搜索不可用 | 检查 `POOL_AGENT_MCP`、`POOL_AGENT_INJECT` 与 `BAILIAN_MCP_API_KEY`。打开沙箱内 `/home/user/.agent/egress.json`，确认 `injected_hosts` 里有 `dashscope.aliyuncs.com`；白名单模式下注入域名会自动放行 |
| agent 访问某网站失败 | 查看 `GET /v1/agents/{user_id}/egress`，确认 `in_sync` 以及是否处于白名单模式 |
| 定时任务没有执行 | 检查 `enabled` 和 `next_run_at`。上一次还在运行且 `overlap=skip` 时会跳过；服务停机期间错过的触发不补跑 |
