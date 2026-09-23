# 阿里云 云沙箱（FC Agent Sandbox）调研笔记

> 目的：为后续基于云沙箱封装「沙箱池服务」做准备。
> 说明：调研环境无法直连 help.aliyun.com，以下内容整理自官方文档的检索摘要。标记 **[待核实]** 的条目需要对照官方文档或实测确认。

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
