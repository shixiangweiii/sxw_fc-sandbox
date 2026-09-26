# opencode 常驻 agent · 测试报告（2026-09-25 ~ 09-26）

> 被测对象：sandbox_pool 的 agent 子系统（见 `sxw_aicoding/方案设计/2026-09-25-opencode应用沙箱池-实施方案.md`）。
> 环境：
> - 本机 macOS，Python 3.12（`.venv`，`e2b==2.31.0`）；
> - 阿里云云沙箱 cn-hangzhou，opencode 模板 `z0tkbiqlztqsma57014d`（第二代运行时，2C4G，opencode 1.18.32）；
> - 模型 `deepseek/deepseek-flash`，联网搜索用百炼 WebSearch MCP。
>
> 云上验证（PoC）报告：`sxw_aicoding/技术调研/2026-09-25-opencode云沙箱PoC验证报告.md`。

## 1. 结论

| 类别 | 结果 |
| --- | --- |
| 单元测试（FakeProvider + 内存版 opencode） | **118 个全部通过**，连跑 3 轮无偶发失败（每轮约 72–76s）；其中原有 60 个回归用例全部通过 |
| 端到端（真实云沙箱，2 副本共享 SQLite，开启鉴权） | 第 2 轮 **S1–S11 全部通过**。第 1 轮除 S3 的一项检查外全部通过：该项依赖模型执行「探测密钥」类命令，模型没有执行；改为测试脚本直接在沙箱内执行后通过 |
| 测试沙箱清理 | 两轮结束后 agent 池 0 个沙箱；`scripts/cleanup_sandboxes.py` 确认账号下沙箱列表为空。opencode 模板保留 |

## 2. 单元测试

新增 58 个用例，按文件划分：

**`tests/test_agent_units.py`（26 个）**：
- 出网策略：内网与元数据地址始终屏蔽；白名单模式；`deny_out` 拒绝域名；按 agent 覆盖；版本号随策略和凭证变化；描述里不含凭证；
- 凭证注入：`${ENV}` 展开，非法配置被拒绝；
- 平台回显脱敏；设置校验与 opencode 配置渲染；
- cron：各类表达式、时区、非法表达式；
- SSE 解码：多行 data、注释行、CRLF；
- 事件翻译：过滤用户消息、类型未知的部件先缓存、只有 updated 的部件补发、工具状态、重试、错误、询问；
- 结果提取：按最后一条用户消息划分。

**`tests/test_agent_service.py`（25 个）**：
- 沙箱与会话：
  - 首次建沙箱与装配（出网规则、占位 Key、三个配置文件）；
  - 后续复用；会话连续与会话忙 409；
  - 两个副本并发首次请求只建一个沙箱；
  - 并发上限 429，容量上限 503。
- 任务生命周期：
  - 客户端断开任务继续；本副本重连、跨副本重连；
  - 中止、超时、模型报错、权限询问自动拒绝；
  - 思考、工具、重试事件。
- 沙箱回收：
  - 空闲销毁后新建；定时任务的短收尾；
  - 轮换：空闲时直接销毁，忙时转 RETIRING、新任务去新沙箱；
  - 硬截止使运行中任务失败；健康检查失败后销毁。
- 配置下发：
  - 出网策略下发与读取；下发失败由维护循环重试；
  - 设置热更新（重写配置并 dispose）。
- 定时任务：参数校验、`overlap=skip`、多副本只触发一次、错过不补跑。
- 任务接管：副本崩溃后接管；正常停止时交接给其他副本。
- 其他：对账（清理孤儿、处理平台侧消失的沙箱）；重置后旧会话 409；启动失败重试与放弃；管理员视图不含访问令牌。

**`tests/test_agent_api.py`（7 个）**：
- SSE / JSON 两种返回；任务查询、重连与中止；
- 409 / 429 / 400 / 404 / 422 / 503 各类错误码；
- 出网策略、设置、定时任务接口；
- 鉴权隔离：不同调用方同名 `user_id` 互不可见；管理员接口与统计。

## 3. 端到端测试

脚本：`scripts/e2e_agent_scenarios.py`，结果 `.data/e2e-agent-report*.json`。两轮都在本机跑 8001、8002 两个副本（`scripts/run_local_cluster.sh`）。

### 3.1 场景结果（第 2 轮，2026-09-25 23:56 – 23:59）

| 场景 | 结果 | 关键数据 |
| --- | --- | --- |
| S1 首次流式对话 | ✅ | 新建沙箱：1.81s 收到 `start`，首字 5.09s，总 5.13s；热沙箱：`start` 0.15s，首字 1.15s，总 1.20s；缓存读 7424 tokens，单次约 $0.00007 |
| S2 工具 | ✅ | bash `print(6*7)` → 42（2.45s）；webfetch example.com → 「Example Domain」（4.14s）；百炼联网搜索 MCP（`websearch_bailian_web_search`，4.05s） |
| S3 安全 | ✅ | 取回 opencode 进程环境、envd 环境与配置文件，两个真实 Key（完整值和末尾片段）都不存在，占位符存在；元数据 / 私网地址被屏蔽；公网与 DeepSeek（平台注入）200；不带令牌直连沙箱端口 403；接口不返回访问令牌 |
| S4 出网策略 | ✅ | API 读到开放模式、`in_sync`、平台规则已脱敏；agent 在沙箱内读取 `egress.json`；运行中切到白名单 0.93s 生效（baidu 被拦截、example.com 200）；恢复默认后 baidu 200 |
| S5 会话连续 | ✅ | 第二轮对话答出上一轮的暗号「蓝色大象」 |
| S6 断线重连 | ✅ | 在 8001 收到 `start` 后断开，到 8002 重连，18.5s 后拿到 `done`（结果含 `slept`） |
| S7 中止 | ✅ | 流中收到 `ABORTED`，任务状态 ABORTED |
| S8 定时任务 | ✅ | 立即触发成功；`every_s=60` 在创建后 61.0s 自动触发成功，结果可拉取 |
| S9 空闲销毁 | ✅ | 设置空闲 60s 后，60.1s 时沙箱被销毁；再次对话自动新建，`start` 1.79s |
| S10 副本崩溃 | ✅ | `kill -9` 持有任务的 8001，任务 22.1s 后由 8002 接管完成（结果 `survived`） |
| S11 收尾 | ✅ | 重置 agent，管理员视图 0 个沙箱，响应中没有访问令牌 |

### 3.2 耗时统计（第 2 轮，`GET /v1/admin/agents/stats`）

| 操作 | 次数 | p50 | p99 / 最大 |
| --- | --- | --- | --- |
| `agent_create`（云端建沙箱） | 2 | 0.42s | 0.44s |
| `agent_boot`（建沙箱 + 健康 + 写配置 + 预加载） | 2 | 1.23s | 1.32s |
| `destroy` | 2 | 0.23s | 0.27s |
| `task_succeeded`（含 S10 的 20s 长任务） | 15 | 2.79s | 23.3s |
| `task_aborted` | 1 | 3.70s | — |

事件计数：
- 沙箱：`agent_boot` 2、`destroy` 2；
- 任务：`task_succeeded` 15、`task_aborted` 1、`task_takeover` 1；
- 配置：`agent_network_applied` 4、`agent_settings_applied` 2、`schedule_fired` 2。

### 3.3 第 1 轮的差异

- **S1 冷启动**：11.58s 收到 `start`（`agent_boot` 11.9s）。原因是本机代理 fake-ip 导致 envd 写配置连续失败 3 次（`ConnectError('')`），重试后才成功。第 2 轮没有触发重试，只要 1.2s。原因分析见 PoC 报告第 2 节，处理建议见手册第 9 节。
- **S3**：第一版让模型执行 `env | grep sk-…` 这类探测命令，模型没有执行、直接回复「完成」，工具输出为空。这属于测试方法问题，已改为由测试脚本直接在沙箱内执行检查。
- 其余场景两轮结果一致。

## 4. 已知问题与限制

1. **本机代理的 fake-ip / TUN 模式会拖慢装配**：
   - 网关访问 opencode 的流量已可通过 `POOL_AGENT_INGRESS_IP` 绕开；
   - envd（SDK 写配置文件）无法绕开，只能靠重试，冷启动会从约 2s 变成 10–15s。
   - 建议在代理工具里把 `*.e2b.fc.aliyuncs.com` 设为直连；服务器部署不受影响。
2. **沙箱最长 24 小时（Eco 规则）**：轮换后会话和文件清空，这是按「不做持久化」的决策得到的结果；agent 的配置、定时任务、任务记录不受影响。
3. **Team 订阅计划存疑**：OpenAPI `ListTeams` 返回的 `plan` 是 `pro`，用户说明为 Eco。方案按 Eco 设计，在 Pro 上同样成立。如果确认是 Pro，可以把 `POOL_AGENT_MAX_LIFE_S` 等参数放宽，或引入暂停 / 恢复。
4. **断线重连只补发文本**：跨副本重连时只补发已有的回复文本，不回放当时的工具事件。同副本重连会完整回放全部事件；连续的文本增量在回放中合并为一条（2026-09-26 起，修复前超过 5000 条后会缺段，见代码评审 AG-M4）。
5. **单任务最长 4 小时**：可调，但必须小于沙箱最长寿命。

## 5. 代码评审修复后的回归（2026-09-26）

评审报告：`sxw_aicoding/代码评审/2026-09-26-opencode常驻agent子系统代码评审报告.md`。修复了 AG-H2、AG-M1～M5、AG-H1（实测后下调为中）、AG-L1～L5、AG-L13、AG-L14。

| 类别 | 结果 |
| --- | --- |
| 回归用例 | 新增 `tests/test_agent_review_fixes.py`（29 个），逐个撤销修复后对应用例均失败 |
| 全量单测 | 147 个。AG-M5 修复前约 1/3 的轮次在清理阶段卡死；修复后连跑 8 轮全部通过（每轮约 84s） |
| 端到端（2 副本，本机带 `HTTPS_PROXY`） | S1–S11 全部通过。第一次运行因 AG-L14（入口 IP 与代理同时设置导致 TLS 失败）卡在装配阶段，修复后通过 |
| 云上补充验证 | `allow_out` 优先于 `deny_out`（公网 IP 同时出现在两边时可达）；元数据地址平台层另有屏蔽；在线服务对 3 种违规出网覆盖返回 400 |
| 收尾 | 账号下沙箱列表为空；两个模板保留 |

端到端关键数据：
- S1：冷启动 `start` 2.36s、首字 5.6s；热沙箱 `start` 0.09s；
- S4：切白名单 0.82s 生效；
- S6：跨副本重连 18.0s 后拿到结果；
- S9：空闲 60.1s 后销毁；
- S10：kill -9 后 22.1s 被接管完成。

## 6. 复现方式

```bash
# 1) 单元测试
.venv/bin/python -m pytest -q

# 2) 端到端：准备 .data/agent-e2e.env（.data/ 已被 gitignore，文件权限 600），内容为以下变量：
#    POOL_DB_URL=sqlite+aiosqlite:///<项目>/.data/agent-e2e.db   POOL_TARGET_SIZE=0   POOL_OP_TIMEOUT_S=60
#    POOL_API_KEYS=e2e:<随机>   POOL_ADMIN_KEYS=ops:<随机>   SANDBOX_POOL_API_KEY=<同 e2e>   SANDBOX_POOL_ADMIN_KEY=<同 ops>
#    POOL_AGENT_ENABLED=true   POOL_AGENT_MODEL_API_KEY="$FCSANDBOX_OPENCODE_MODEL_API_KEY"
#    BAILIAN_MCP_API_KEY=<百炼 WebSearch Key>
#    POOL_AGENT_MCP='{"websearch": {"type": "remote", "url": "https://dashscope.aliyuncs.com/api/v1/mcps/WebSearch/mcp"}}'
#    POOL_AGENT_INJECT='{"dashscope.aliyuncs.com": {"Authorization": "Bearer ${BAILIAN_MCP_API_KEY}"}}'
#    POOL_AGENT_TASK_HEARTBEAT_S=5   POOL_AGENT_TASK_TAKEOVER_S=20   PYTHON=<项目>/.venv/bin/python
set -a; . ./.env; . .data/agent-e2e.env; set +a     # .env 另提供 E2B_*、POOL_AGENT_TEMPLATE、POOL_AGENT_INGRESS_IP
scripts/run_local_cluster.sh start 8001 8002
.venv/bin/python scripts/e2e_agent_scenarios.py --replicas 8001,8002
scripts/run_local_cluster.sh stop
.venv/bin/python scripts/cleanup_sandboxes.py        # 确认账号下没有残留沙箱
```
