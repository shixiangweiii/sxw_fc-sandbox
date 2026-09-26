# opencode 云沙箱常驻 agent 技术调研（2026-09-25）

> 目标：在阿里云云沙箱（FC Agent Sandbox，兼容 E2B 协议）里运行 opencode，让它对外接受业务系统的 HTTP 请求，成为 7×24 工作的 agent（能长时间连续工作、能定时主动执行任务）。
> 本文汇总调研结论；实施方案见 `sxw_aicoding/方案设计/2026-09-25-opencode应用沙箱池-实施方案.md`，云上验证结果见同目录的 PoC 报告。

## 1. 业界类似实现

| 实现 | 做法 | 借鉴点 |
| --- | --- | --- |
| E2B 官方 opencode 模板 | 沙箱内 `opencode serve --hostname 0.0.0.0 --port 4096`，`get_host(4096)` 取地址，轮询 `/global/health` | 与第二代模板的启动 / 就绪命令同构 |
| Modal 官方示例 | 沙箱入口即 `opencode serve`，端口加密，密码放 Secret，`opencode attach` 远程接入 | 远程 TUI 接入 |
| Daytona 指南 / 插件 | `OPENCODE_CONFIG_CONTENT` 注入配置，预览 URL + 官方 SDK | 配置注入 |
| Cloudflare sandbox-sdk | Worker → Durable Object → 容器 :4096 透明代理；配置里放占位 Key，出网拦截替换真 Key | 网关代理、Key 不进沙箱 |
| codecloud（E2B 生产案例） | 私有端口（流量令牌）；沙箱内 relay 订阅 `/event` 外推；`opencode export/import` 迁移会话 | LLM 可能 10–15 分钟不出字；opencode 常用 3–4 GB 内存 |
| Ramp Inspect（Modal + OpenCode） | server-first、镜像定时重建、热池、多端接入、追问排队 | 多渠道、预热 |
| Rivet sandbox-agent | 沙箱内 Rust 守护进程统一多种 agent 的 HTTP API | 只跑 opencode 时是多余的一层 |
| 阿里云 FC OpenClaw 模板 / FunClaw | 同平台常驻 gateway；FunClaw 一人一实例，「节省模式下定时任务不可用」 | 定时器要放在沙箱外 |
| 阿里云 FC Claude Code 模板 | `commands.run` 执行 `claude -p ... --resume`，推荐 2C8G | 一次性调用模式 |

## 2. 云沙箱平台事实

来源：官方文档源码 `github.com/aliyun-fc/fc-docs`（`docs/zh-CN/01.云沙箱/`）与 `e2b==2.31.0` SDK 源码。

- **启动 / 就绪命令**（仅第二代运行时）：构建期后台执行启动命令，就绪命令返回 0 后打快照，基于模板创建的沙箱「创建时服务已在运行」。等待就绪有 5 分钟硬超时。
- **入站鉴权**：`allow_public_traffic` 默认 `True`（拿到 URL 即可访问）。设为 `False` 时必须同时 `secure=True`，请求须带 `e2b-traffic-access-token`，否则 403。令牌只在 create / connect 时返回，要随沙箱 ID 存库。访问地址为 `{port}-{sandbox_id}.{sandbox_domain}`。
- **出网控制**：`deny_out` / `allow_out`（IP、CIDR、域名，`*.` 通配），`allow_out` 优先于 `deny_out`；域名过滤只对 80/443 生效；域名白名单必须配合 `deny_out: ["0.0.0.0/0"]`。
- **出网请求头转换**（`network.rules`）：按精确域名整体注入请求头或替换占位符，真实凭据不进沙箱。
  - 每个沙箱最多 10 个域名，每个值最长 2048 字节；
  - 依赖平台 TLS 检查，沙箱须信任平台 CA（官方镜像 ≥ v0.0.44）；
  - 不支持 WebSocket upgrade；
  - `get_info` 会明文回显注入值。
- **运行中更新网络**：`update_network` 为全量替换。文档矛盾：「E2B 兼容说明」称 Network Config Update 不生效，网络文档称可用，需实测。
- **订阅计划**：

  | 计划 | 不暂停时最长存活 | 深休眠 | 浅休眠 |
  | --- | --- | --- | --- |
  | Eco | 24h | ✗ | ✗ |
  | Std | 24h（暂停后计时清零） | ✓，保留 7 天 | ✗ |
  | Pro | 7 天 | ✓，保留 30 天 | ✓ |

- **SDK 2.31.0** 已支持 `network`、`lifecycle`、`traffic_access_token`、`update_network`，无需升级。
- **Eco 邀测价**：vCPU 0.060 元/时，内存 0.030 元/GiB/时，2C4G 为 0.24 元/时；磁盘 15 GiB 以内免费。

## 3. opencode 事实（v1.18.32 源码，2026-09-21 发布）

- **DeepSeek**：要求 opencode ≥ v1.18.30。
  - 模型写作 `deepseek/deepseek-flash`（models.dev：DeepSeek V4.1 Flash，1M 上下文，支持工具调用）；
  - Key 环境变量 `DEEPSEEK_API_KEY`；
  - npm 包 `@ai-sdk/openai-compatible` 已编译进二进制；
  - 接口地址 `https://api.deepseek.com`。
- **离线运行**：models 注册表有编译期快照，设 `OPENCODE_DISABLE_MODELS_FETCH=1` 可完全离线。
- **信任系统 CA**：二进制编译参数带 `--use-system-ca`。
- **server**：
  - Basic 认证：`OPENCODE_SERVER_PASSWORD`，或 URL 参数 `?auth_token=`；
  - 按 `?directory=` / `x-opencode-directory` 服务多个项目目录；
  - `/event` 每 10 秒发一次心跳；
  - `POST /instance/dispose` 会重新加载项目配置。
- **事件**：
  - `message.part.delta`：`{sessionID, messageID, partID, field, delta}`；
  - `message.part.updated`：`{part}`；
  - `message.updated`：`{sessionID, info}`；
  - `session.status`：`{sessionID, status.type ∈ idle/busy/retry}`；
  - 另有 `session.error`、`permission.asked`、`question.asked`。
- **会话忙时收到新消息**：合并进正在运行的循环（`ensureRunning`），不会报错。
- **步数**：默认不限（`agent.steps ?? Infinity`）。
- **权限**：大多默认 allow，`doom_loop` / `external_directory` 默认 ask。无人值守时 ask 会让会话挂起，必须显式配置。
- **websearch 开关**：用 DeepSeek 等非 opencode 自家 provider 时默认关闭，需 `OPENCODE_ENABLE_EXA=1` 或 `OPENCODE_ENABLE_PARALLEL=1`。
  - Exa 的 Key 放在 URL 参数里，无法注入；
  - Parallel 的 Key 走请求头。
- **会话存储**：存在 `~/.local/share/opencode/opencode.db`（SQLite）；`opencode import` 直接写库。
- **分发**：npm 国内镜像 `registry.npmmirror.com` 上有 `opencode-linux-x64@1.18.32`（约 185 MB），可以直接 curl 下载解压，不依赖 node。

## 4. 用户已确认的决策

1. 每个用户一个常驻 agent；当前规模按 1 个用户（本人生产力工具）估算。
2. 业务系统通过 HTTP 接入，不做 IM 和网页界面；先支持同步流式返回。
3. 「24 小时」指能长时间连续工作、能定时主动执行任务。
4. 出网采用开放公网，只屏蔽内网与元数据地址；需要访问 git、pip/npm 镜像、网页抓取与搜索、MCP。出网策略可以配置；业务系统和沙箱内的 agent 都能读到当前生效的策略。
5. 按 Eco 规则设计（不能暂停，沙箱最长 24 小时）；允许按用户配置「空闲 N 小时销毁、需要时再新建」。
   - 注：OpenAPI `ListTeams` 返回该 Team 的 `plan` 为 `pro`，与用户所述不一致。方案按 Eco 设计，在 Pro 上同样成立。
6. 不做挂载卷持久化，沙箱销毁即数据清除。
7. 定时任务的结果由业务系统拉取。
8. 联网搜索使用百炼 WebSearch MCP（`https://dashscope.aliyuncs.com/api/v1/mcps/WebSearch/mcp`，Bearer 鉴权，见 `sxw_aicoding/百炼-mcp.txt`）。

## 5. 参考

- DeepSeek 接入 opencode：https://api-docs.deepseek.com/zh-cn/quick_start/agent_integrations/opencode
- opencode 文档：https://opencode.ai/docs/server/ 、/cli/ 、/config/ 、/permissions/
- opencode 源码：https://github.com/anomalyco/opencode （tag v1.18.32）
- E2B opencode：https://docs.e2b.dev/agents/opencode ；Modal：https://modal.com/docs/examples/opencode_server
- Cloudflare：https://github.com/cloudflare/sandbox-sdk/tree/main/examples/opencode
- codecloud：https://codecloud.dev/blog/opencode-e2b-sandbox ；Ramp：https://builders.ramp.com/post/why-we-built-our-background-agent
- 阿里云 OpenClaw 模板：https://help.aliyun.com/zh/functioncompute/openclaw-template ；FunClaw：https://help.aliyun.com/zh/functioncompute/special-feature-funclaw
- 云沙箱文档源码：https://github.com/aliyun-fc/fc-docs
