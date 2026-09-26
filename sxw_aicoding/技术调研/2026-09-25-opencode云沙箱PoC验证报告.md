# opencode 云沙箱 PoC 验证报告（2026-09-25，cn-hangzhou）

> 脚本：`scripts/poc_opencode_agent.py`（结果 JSON 写入 `.data/poc-report.json`，不含密钥）。
> 环境：
> - 本机 macOS，`.venv`（Python 3.12，`e2b==2.31.0`）；
> - 沙箱用现有第二代模板 `xu76gk97q07mgohgw7q3`（code-interpreter，2C2G）；
> - opencode v1.18.32 从 npm 国内镜像下载，模型 `deepseek/deepseek-flash`，联网搜索用百炼 WebSearch MCP。
>
> 共跑 6 轮，前 5 轮依次暴露并修正了下文第 2 节的问题，第 6 轮全部通过。每轮结束都已销毁沙箱。

## 1. 结论总览（第 6 轮）

| 验证项 | 结果 | 数据 |
| --- | --- | --- |
| 建沙箱（secure + network + 关闭公开访问） | ✅ | 0.52s；拿到流量令牌 |
| 沙箱环境 | — | user（uid 1000，sudo 组），x86_64，2 核，2 GB；Debian 13；PID 1 为 systemd；DNS `100.100.2.136`；自带 git / python3 / pip3 / node / npm / curl / tar |
| 沙箱环境变量里没有任何 Key | ✅ | 匹配数 0 |
| 出网注入：沙箱内 curl 访问 DeepSeek `/models` | ✅ | 200（未带 Key，由平台注入） |
| 出网注入：百炼 WebSearch MCP `initialize` | ✅ | 200 |
| 公网可达 / DNS | ✅ | baidu 200；`api.deepseek.com` 正常解析 |
| 元数据 `100.100.100.200`、私网 `10.0.0.1` | ✅ 被屏蔽 | 连接失败 |
| 下载安装 opencode（npm 国内镜像，约 185 MB） | ✅ | 4.2s |
| opencode serve 启动到健康 | ✅ | 1.3–4.0s |
| 流量令牌：不带 / 带 | ✅ | 403 / 200 |
| 项目级 `opencode.json`（按 `?directory=` 加载） | ✅ | 模型、share、权限都生效 |
| MCP 连接 | ✅ | websearch：connected |
| 基础对话 | ✅ | 总 2.5s，首字 2.24s；缓存读 7296 tokens，费用约 $0.00006 |
| bash 工具 | ✅ | 2.2s |
| webfetch 工具（开放公网） | ✅ | 2.4s |
| 百炼联网搜索 MCP（工具名 `websearch_bailian_web_search`） | ✅ | 3.0s，返回当天杭州天气 |
| 内存 | — | 4 轮对话后 opencode 约 850 MB，系统已用 900 MB / 2 GB，**模板定为 2C4G** |
| `POST /instance/dispose` 重载项目配置 | ✅ | 改配置后 MCP 状态变为 disabled |
| `get_info` 回显网络配置 | ✅ | 回显 allow_out / deny_out / rules（注入值为明文，必须脱敏） |
| `update_network` 运行中生效 | ✅ | 调用 0.18s；切白名单后 baidu 不通、DeepSeek 仍 200；切回开放后 baidu 200 |
| SSE 空闲保持 150s | ✅ | 收到 15 次心跳，连接未断 |

## 2. 过程中发现的问题与结论

1. **`deny_out` 不支持域名**：`update_network` 返回 `400: denyOut only supports IP / CIDR, domains are not supported`，与文档「支持域名」不符。
   - 开放模式只能按 IP / CIDR 屏蔽；
   - 按域名限制只能用白名单模式：`deny_out=["0.0.0.0/0"]` + `allow_out`。白名单模式下 DNS 仍可用。
2. **DNS 服务器在 `100.64.0.0/10` 内**（`100.100.2.136`），所以只屏蔽元数据地址 `100.100.100.200/32`，不能整段屏蔽。
3. **本机代理的 fake-ip / TUN 模式**：本机 DNS 把所有域名解析成 `198.18.0.x`。
   - 实测：到沙箱端口经假地址访问，30 次失败 10 次；直连真实入口 IP `47.111.182.73`（公共 DNS 解析得到），30 次全部成功。
   - 表现为 `ConnectError`，TLS 握手时被对端关闭（`EndOfStream`）。
   - 对策：网关新增可选配置 `POOL_AGENT_INGRESS_IP`，直连入口 IP，TLS SNI 与 `Host` 仍用沙箱域名。
   - 建连失败一律重试（请求肯定没发出去）。
4. **系统代理对沙箱域名返回 503**：httpx 默认读 macOS 系统代理。网关访问沙箱的客户端改为 `trust_env=False`，只用显式配置的 `HTTPS_PROXY`。
5. **envd 调用抛 `httpcore` 异常**：刚创建的沙箱，前几次 envd 调用可能直接抛 `httpcore.ConnectError`（经 `e2b_connect`，不是 httpx 异常）。
   - 装配阶段调 envd 时要把 httpcore 的连接类异常也算作可重试。
   - 现有 `E2BProvider._retry_stale` 只认 httpx 异常，一并修正。
6. **opencode 事件结构**（实测样本）：
   - `message.part.delta`：`{sessionID, messageID, partID, field: "text", delta}`；
   - `message.part.updated`：`{sessionID, part: {id, messageID, type, text | tool/state}}`。用户消息的 text part 也会出现，要按消息角色过滤；
   - `message.updated`：`{sessionID, info: {id, role, time, tokens, cost, ...}}`，先于对应的 part 事件到达；
   - `session.status`：`{sessionID, status: {type: busy | idle}}`。

## 3. 对实施方案的调整

- 出网策略：开放模式的 `deny_out` 只接受 IP / CIDR，接口层校验并给出明确错误；按域名限制用 `allowlist` 模式。
- 运行中修改出网策略用 `update_network`，已验证可行。
- 模板规格定为 2C4G，基础镜像用 code-interpreter 官方镜像（自带 git / python / node）。
- 网关访问沙箱：`trust_env=False`、可选入口 IP 直连、每个沙箱一个连接池、建连失败重试。
- 事件翻译：记录消息角色，过滤用户消息的 part；未知类型 part 的增量先缓存，等类型确定后再发出。
