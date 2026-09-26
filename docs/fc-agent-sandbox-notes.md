# 阿里云 云沙箱（FC Agent Sandbox）调研笔记

> 目的：为后续基于云沙箱封装「沙箱池服务」做准备。
> 说明：初版整理自官方文档检索摘要，标记 **[待核实]** 的条目需要对照官方文档或实测确认。第 0 节为 cn-hangzhou 实测结论，优先级高于其余章节。

## 0. 实测结论（cn-hangzhou，2026-09-23）

- **SDK 版本必须固定**：`e2b==2.31.0`、`e2b-code-interpreter==2.8.1`（官方验证版本）。`e2b` 2.51.0 走 `/v2/sandboxes`，云沙箱不支持创建和连接（实测返回 405）。
- **代理**：E2B Python SDK 用自定义 httpx transport，**不读 `HTTPS_PROXY` 环境变量**，需要走 HTTP 代理出网时必须显式传 `proxy=...`。
- **连接失效**：SDK 按事件循环共享一个 HTTP/2 连接池。经 HTTP 代理出网时，空闲约 1 分钟以上的连接会被断开，下一个请求报 `httpx.WriteError('')`（异常信息为空，请求未送达）。长期运行的服务需要对管控面调用做连接类错误重试（沙箱池已处理，见 `docs/sandbox-pool-fix-changes.md` 4.2）。
- **账号内置模板**：只有 `base`、`code-interpreter-v1`（均为 2 vCPU / 2048 MB / 10 GB 磁盘）。browser、AIO、Desktop 等需要从官方镜像自行构建。
- **创建耗时**：SDK 端到端约 2~2.5s。服务端响应头 `x-sandbox-creation-latency-service-ms` 约 785ms，其中 `createfcsession` 约 767ms。
- **默认超时**：创建时不传 timeout 默认 300s；`get_info().end_at` = 创建时间 + timeout。
- **暂停需要开通**：第一代运行时（内置模板）调用 `pause()` 返回 `pauseSession is not enabled for this function`，需账号开通白名单；第二代运行时（microVM）默认支持暂停/恢复，但第二代模板要通过阿里云 OpenAPI `CreateTemplate` 创建（`runtimeConfig.sandboxConfig.generation=2`，需要 AK/SK 和 `fcsandbox:CreateTemplate` 权限，E2B API Key 做不了）。
- **第二代模板实测（已验证可暂停/恢复）**：
  - 创建：`examples/create_gen2_template.py` 通过 OpenAPI 用官方镜像 `code-interpreter-v1:v0.0.52` 建模板（2C/2G/15G），约 43s 就绪。E2B API Key 能直接用这个模板创建沙箱。
  - **坑**：第二代运行时 PID 1 是 systemd，不会自动拉起代码解释器（第一代由 `/.fce2b/entrypoint` 拉起），`run_code` 一直返回 500。需要在模板上设置 `start_command=/.fce2b/sandbox-code-interpreter` 和 `ready_command=curl -sf http://127.0.0.1:49999/health`，脚本已默认设置。
  - 耗时：创建约 1.6~2.2s，首次 `run_code` 约 2s；`pause()` 约 10~10.7s；`connect()` 恢复约 1.1~1.8s，恢复后首次 `run_code` 约 0.4s。
  - 暂停/恢复保留**内存状态**：解释器变量、后台进程（PID 不变）、文件都在。
  - 底层同样是 Dragonball 虚拟机，但没有 kata-containers 层，PID 1 为 `/usr/sbin/init`（systemd），并监听 22（sshd）和 49983（envd）。
- **删除**：`kill()` 返回 True，之后 `get_info` 抛 `NotFoundException`。
- **列表接口（`GET /v2/sandboxes`）**：
  - 支持 `metadata=k%3Dv` 过滤。默认同时返回 running 和 paused。
  - **有约 1～1.5s 延迟**：刚创建的沙箱、刚暂停的状态要过一会儿才出现在列表里。对账逻辑必须加宽限期，不能把「列表里暂时没有」当成「已经消失」。
- **沙箱池实测（3 副本，5 个沙箱同时操作）**：并发暂停时单次约 15～16s（单个约 10s）；并发恢复约 2.2s；创建 p50 约 0.7s、p99 约 2.4s；预热 `import numpy, pandas, matplotlib` p50 约 2.1s。
- **支持地域**：cn-beijing、cn-shanghai、cn-hangzhou、cn-shenzhen、cn-hongkong、ap-southeast-1、us-east-1、us-west-1（另有马来西亚柔佛）。
- **Snapshot**：兼容，但需白名单且仅第二代运行时可用，默认保留 7 天（与下文「不支持」的检索摘要不同，以此为准）。

- **网络（2026-09-25 实测，opencode agent PoC，见 `sxw_aicoding/技术调研/2026-09-25-opencode云沙箱PoC验证报告.md`）**：
  - **入站鉴权**：`allow_public_traffic=False`（须同时 `secure=True`）后，访问 `https://{port}-{sandbox_id}.{domain}` 不带 `e2b-traffic-access-token` 返回 403，带上返回 200。令牌只在 create / connect 时返回。
  - **出网注入**：`network.rules` 的请求头注入对 curl 和 opencode（Bun）都生效，沙箱环境变量里看不到 Key。规则里的域名要同时出现在 `allow_out` 中。
  - **`deny_out`**：只接受 IP / CIDR，带域名返回 400，与文档不符。按域名限制只能用白名单模式（`deny_out=["0.0.0.0/0"]` + `allow_out`），此时 DNS 仍可用。
  - **`update_network`**：运行中修改立即生效（约 0.2s，全量替换，要带上 rules）。`get_info().network` 会明文回显注入值。
  - **DNS**：DNS 服务器是 `100.100.2.136`，在 `100.64.0.0/10` 内。屏蔽元数据只能写 `100.100.100.200/32`，不能整段屏蔽。
  - **本机代理的影响**：本机开着代理的 fake-ip / TUN 模式时，到沙箱域名的连接约 1/3 失败，直连入口 IP 则稳定；httpx 默认读系统代理，对沙箱域名返回 503。
  - **envd 异常类型**：刚创建的沙箱，前几次 envd 调用可能抛 `httpcore.ConnectError`（经 e2b_connect，不是 httpx 异常）。
- **第二代 code-interpreter 镜像环境**：
  - 默认用户 user（sudo 组），x86_64，Debian 13，PID 1 为 systemd；
  - 自带 git / python3 / pip3 / node / npm / curl / tar，没有 unzip。

### 0.1 与 E2B 的关系（实测取证）

结论：**兼容 E2B 协议，实现不是 E2B 开源代码**。阿里在 FC 自有基础设施上重新实现了 E2B 的 API 和沙箱内协议。

| 层 | E2B 开源（e2b-dev/infra） | 云沙箱实测 |
| --- | --- | --- |
| 控制面 REST API | E2B API + orchestrator | 兼容 E2B `/sandboxes` 等 v1 接口，不支持 `/v2`。响应头有 `x-sandboxgw-request-id`、`createfcsession` 耗时、`instanceid: c-…`；报错为 FC 的 `pauseSession is not enabled for this function`，说明一个沙箱就是一个 FC 函数会话 |
| 虚拟化 | Firecracker microVM | 第一代：Dragonball（Kata/RunD）+ Alibaba Cloud Linux 内核 `5.10.134-…kangaroo.al8` + LifseaOS，`systemd.unit=kata-containers.target`；第二代为 microVM |
| 沙箱内守护进程 envd | Go，`github.com/e2b-dev/infra/packages/envd`，依赖 connectrpc 等 | `/.fce2b/envd`，Go 模块路径为 `entrypoint/cmd/envd`，依赖只有 pty/fsnotify/zerolog/x/sys，**不含 e2b-dev 或 connectrpc**；版本号报 `0.5.2` 以通过 SDK 的版本检查 |
| PID 1 | systemd 等 | `/.fce2b/entrypoint`（`entrypoint/cmd/gatewayd`） |
| 代码解释器 | Python + Jupyter 服务 | `/.fce2b/sandbox-code-interpreter`，Go + gin 实现 |

影响：兼容是「重新实现协议」，不是「同一套代码」，所以会有行为差异（`/v2` 接口不支持、暂停需白名单、metrics 为占位值）。要固定 SDK 版本，每次升级 SDK 都要回归；沙箱池应在 E2B SDK 之上加一层自己的抽象，以便对不同后端做差异适配。

## 1. 产品定位

- 云沙箱是函数计算（FC）面向 **AI Agent 与代码执行场景** 的云端隔离运行环境。
- 按需创建 Sandbox，在其中 **执行命令、运行代码、读写文件、启动临时服务**，任务结束后释放。
- 典型场景：
  - Agent 工具执行环境（跑命令、处理文件、调用工具链）
  - 代码解释器（Python / Shell / JS，返回 stdout、stderr、结果、文件产物）
  - 数据分析、自动化脚本、依赖复杂的工具调用、临时 Web 服务
  - 浏览器自动化（CDP / VNC）
- 安全隔离：安全容器（VM 级隔离）+ VPC 租户网络隔离；文档从 **生命周期、文件、网络、控制面** 四个边界描述隔离。

相关但不同的产品（不要混淆）：
- **AgentRun**：FC 上层的一站式 Agentic AI 平台，沙箱只是它的一部分。
- **ACS Agent Sandbox**：容器计算服务里的沙箱，同样兼容 E2B，但属于另一个产品。
- **FC 3.0 Sandbox 函数 / Session**（`PauseSession`/`ResumeSession`、`sandboxIdleTimeoutInSeconds`）：更早的一套基于函数 + 会话的沙箱模型。

## 2. 接入方式：E2B 兼容（推荐）

云沙箱的主接入路径是 **兼容 E2B 协议**，可直接使用 E2B 官方 SDK（Python / TypeScript）和 E2B CLI。

### 2.1 前置

1. 在函数计算控制台创建 **API Key**（只完整展示一次，需放入密钥管理，不要进仓库、日志、前端）。
2. 设置环境变量（**API URL、Domain、模板和 Sandbox 必须属于同一地域**）：

```bash
export E2B_API_KEY=<控制台创建的 API Key>
export E2B_API_URL=https://api.<region>.e2b.fc.aliyuncs.com   # 例：https://api.cn-beijing.e2b.fc.aliyuncs.com
export E2B_DOMAIN=<region>.e2b.fc.aliyuncs.com                # 例：cn-beijing.e2b.fc.aliyuncs.com
```

生产环境建议按 **应用 / 环境 / 工作空间** 隔离 API Key。

### 2.2 基本用法（E2B Python SDK 形态）

```python
from e2b_code_interpreter import Sandbox

sbx = Sandbox.create(template="code-interpreter-v1", timeout=600,
                     metadata={"pool": "default"})
sbx.run_code("x = 1")                  # 代码解释器，跨调用保留上下文
sbx.run_code("print(x + 1)")
sbx.commands.run("ls -la /")           # 执行命令
sbx.files.write("/tmp/a.txt", "hello") # 读写文件（支持文本/二进制/流，支持 metadata）
url = sbx.get_host(8000)               # 获取沙箱内端口的公网域名（预览 / 临时 API）
sbx.set_timeout(600)                   # 续期
sbx.pause()                            # 暂停（对应深休眠）
sbx2 = Sandbox.connect(sbx.sandbox_id) # 连接；若已暂停会自动恢复
Sandbox.list()                         # 查询运行中的沙箱
sbx2.kill()                            # 终止释放
```

### 2.3 兼容范围

| 类别 | 内容 |
| --- | --- |
| 支持 | 创建、连接、查询（list）、暂停、恢复、设置超时、终止；commands、files（含 metadata）、代码执行、`get_host` |
| 受限（仅保持调用兼容，勿做生产依赖） | Sandbox Logs 可能返回空；Network Config Update 可能返回成功但不生效；Metrics 仅 CPU/内存可信，磁盘/页缓存为占位值 |
| 不支持（非 E2B 兼容主路径） | Snapshot、Volume、Team、Access Token 管理 |
| 走 FC Extensions | VPC、OSS 挂载、日志采集、监控告警 |

## 3. 模板（Template）

- 内置模板（平台维护）：`base`、`code-interpreter-v1`（默认；Python、pandas、matplotlib、Jupyter kernel 等，支持 Python/JS 且跨调用保持上下文）、`browser` / `browser-use`（CDP 自动化、截图、VNC）、`desktop`、以及 **AIO 沙箱**（代码执行 + 浏览器一体）。**[待核实：内置模板完整清单与命名]**
- 自定义模板：基于 Dockerfile / 自定义镜像构建，用于特定系统库、运行时版本、企业标准化环境。
- 最佳实践：
  - 生产中 **固定模板名 + 版本 / 别名**，避免 `latest` 类引用带来不可控变化。
  - 运行时、系统依赖、CLI、稳定的业务 SDK **预装进模板**，减少每次启动后的临时安装。

## 4. 生命周期

```
create ──► Running ──pause──► Paused ──connect/resume──► Running
              │                  │
              │ timeout          │ (TTL 到期)
              ▼                  ▼
            killed ◄───kill─── (释放)
```

- **timeout**：创建时设置；到期后释放，或在开启 `auto_pause` 时转入休眠。
- **connect 会重置超时**：使用创建时的 timeout，或 SDK 默认 300 秒。
- **pause/resume**：SDK 的 `pause()` 对应 **深休眠**；保存内存、磁盘、CPU 状态并释放计算资源（快照异步上传 OSS），恢复后文件系统、内存与进程都在，但 **外部网络连接需重建**。
- 恢复后文件与后台进程保持连续。
- 连接前可先 `Sandbox.list()` 确认沙箱仍存活，避免盲连报错。
- 最长存活：FC Session 文档为单实例最长 6 小时，且 **TTL 在暂停期间仍累计、resume 不重置**。**[待核实：是否同样适用于 E2B 兼容的云沙箱]**
- **实测（cn-hangzhou，第二代模板，2026-09-23）：暂停中的沙箱不受 `timeout` 回收**。以 `timeout=60` 创建后立即暂停，等到超过 `end_at` 约 50s 再查询，状态仍为 `paused`（`end_at` 不变），`connect` 恢复后解释器变量还在。所以暂停中的沙箱不需要续期；上面 6 小时最长存活的说法仍未实测（沙箱池按 `max_age_s` 回收空闲和暂停的沙箱）。

## 5. 计费与配额

- Serverless 按需计费，按 **配置规格 × 时长** 计 vCPU / 内存 / 磁盘（/GPU），不是按实际用量。
- 深休眠：不收 vCPU、内存费用，**仍收深休眠磁盘费用**。浅休眠（仅 Pro）仍收内存和磁盘费用。
- 配额（FC 通用，**[待核实：云沙箱是否单独配额]**）：单账号单地域默认实例上限 100（可在配额中心申请提升）；另有地域级 vCPU / 内存总量限额；vCPU:内存 比例 1:1 ~ 1:4。

## 6. FC Extensions（云上扩展）

非 E2B 原生能力，用于接入阿里云体系：
- VPC 网络配置（访问内网资源）
- 创建 Sandbox 时动态挂载 OSS（`volume_mounts` 挂到指定目录；数据不随沙箱释放删除）
- 日志采集（SLS）、监控告警

## 7. 对沙箱池服务的设计启示

| 事实 | 对池设计的影响 |
| --- | --- |
| 创建沙箱有冷启动耗时 | 按模板维度预热（warm pool），维护 min/max 水位 |
| 默认 timeout 较短（300s），connect 会重置 | 空闲池中的沙箱需要定期 `set_timeout` 续期，或借用时统一设置 timeout |
| 存在最长存活上限（疑似 6h，TTL 不因暂停/恢复重置） | 按创建时间做回收，临近上限的沙箱不再分配 |
| 深休眠不收 CPU/内存费用，connect 自动恢复 | 可做「热池（Running）+ 温池（Paused）」两级，权衡恢复延迟与成本 |
| 恢复后外部网络连接需重建 | 池不要缓存沙箱内的长连接状态 |
| 沙箱有状态（文件、进程、解释器上下文） | 默认「一次租用一个沙箱、归还即销毁」；复用必须有可靠的 reset，否则有跨用户数据泄露风险 |
| `metadata` + `list()` 可用 | 用 metadata 标记池归属/租约，服务重启后通过 list 对账、回收孤儿沙箱 |
| 账号/地域实例配额有限 | 池总容量受配额约束，需全局限流与排队 |
| Logs/Metrics/Network Update 为占位 | 健康检查用实际命令探活（如 `commands.run("true")`），不依赖 metrics |
| API Key、Endpoint、模板按地域绑定 | 池按「地域 × 模板」分片 |

## 8. 参考文档

- 产品简介：https://help.aliyun.com/zh/functioncompute/product-overview-of-fc-agent-sandbox
- 通过 SDK 创建第一个云沙箱：https://help.aliyun.com/zh/functioncompute/create-your-first-cloud-sandbox-via-the-sdk
- 通过 SDK 使用云沙箱：https://help.aliyun.com/zh/functioncompute/using-the-cloud-sandbox-via-the-sdk
- 通过 CLI 使用云沙箱：https://help.aliyun.com/zh/functioncompute/using-the-cloud-sandbox-via-the-cli
- E2B 兼容说明：https://help.aliyun.com/zh/functioncompute/e2b-compatibility-explanation
- E2B SDK 兼容 API 清单：https://help.aliyun.com/zh/functioncompute/e2b-sdk-compatible-api-list
- E2B 兼容与迁移：https://www.alibabacloud.com/help/zh/functioncompute/e2b-compatibility-and-migration
- 连接沙箱：https://help.aliyun.com/zh/functioncompute/fc/connect-sandbox
- 休眠与恢复：https://www.alibabacloud.com/help/zh/functioncompute/hibernation-and-recovery
- Sandbox 公网 URL：https://www.alibabacloud.com/help/zh/functioncompute/sandbox-public-url
- 内置模板：https://help.aliyun.com/zh/functioncompute/built-in-templates
- 代码解释器：https://help.aliyun.com/zh/functioncompute/code-interpreter
- browser 模板：https://help.aliyun.com/zh/functioncompute/browser-template
- AIO 沙箱：https://help.aliyun.com/zh/functioncompute/aio-sandbox
- FC Extensions 概览：https://help.aliyun.com/zh/functioncompute/fc-extensions/
- 创建 Sandbox 时配置 OSS 挂载：https://help.aliyun.com/zh/functioncompute/fc/sandbox-supports-instance-level-dynamic-mount-of-oss-test-invitation
- 安全隔离：https://help.aliyun.com/zh/functioncompute/security-isolation
- FC 计费说明：https://help.aliyun.com/zh/functioncompute/billing-overview-of-fc
- FC 配额与限制：https://help.aliyun.com/zh/functioncompute/fc/product-overview/limits-of-usage
