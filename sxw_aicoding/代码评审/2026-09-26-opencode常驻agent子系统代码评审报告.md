# 代码评审报告：opencode 常驻 agent 子系统

- **评审日期**：2026-09-26
- **评审对象**：工作树中未提交的 agent 子系统，基线提交 `6d7f203`。
  - 新增：`sandbox_pool/agent/`（service 836 行、runner 486 行、maintainer 296 行、policy 326 行、opencode 212 行、cron 118 行）、`api/agent_routes.py`、`store/agent_repo.py`、`provider/fake_opencode.py`、3 个测试文件、3 个脚本；
  - 修改：`api/app.py`、`config.py`、`models.py`、`provider/*`、`store/schema.py`、`tests/conftest.py`。
- **对照文档**：`方案设计/2026-09-25-opencode应用沙箱池-实施方案.md`、`技术调研/` 下的调研与 PoC 报告、业务接入使用手册、测试报告。
- **评审方式**：
  1. 通读上述代码，以及它依赖的 `core/lifecycle.py`、`store/repository.py`、`store/db.py`、`api/auth.py`；
  2. 对可疑点写一次性探针用例（FakeProvider），构造确定的时序，确认问题能复现；
  3. 核对 opencode 源码（`sst/opencode`，`packages/opencode/src/session/run-state.ts`），确认 `/instance/dispose` 的行为；
  4. 跑全量单测作为基线：**118 个全部通过（73s）**。
- **编号**：本轮问题统一加 **AG** 前缀（H 高 / M 中 / L 低），与前两轮（H/M/L、R2-*）区分。

---

## 一、结论

整体设计沿用了代码执行池的做法：状态存库、CAS、池级锁行串行化「先查再写」、云端调用不持有事务、`op_owner` / `op_deadline` 崩溃接管。runner 与 HTTP 响应解耦，客户端断开不会取消正在写库的协程，思路清楚。**没有发现双建沙箱、超容量、凭证进沙箱或经接口泄露这类致命问题。**

本轮共 **21 个问题：高 1、中 6、低 14**。
- 两个是在修复与验证过程中发现的：AG-M5（修复后多轮回归时发现的既有问题，`core/lifecycle.py`）、AG-L14（云上端到端测试时发现）；
- AG-H1 经云上实测后从「高」下调为「中」。

- **AG-H1（安全，实测后下调为中）**：调用方可以借 `allow_out` 绕过「内网与元数据地址始终屏蔽」。云上实测确认 `allow_out` 优先于 `deny_out`，而策略校验没有拦截与强制屏蔽重叠的放行项，这与方案和手册的承诺（「调用方的覆盖不能去掉这几项」）直接矛盾。元数据地址在平台层另有屏蔽，放行后仍不可达；内网段只在沙箱接入 VPC 后才有可达目标。所以当前部署的实际危害有限，但承诺已经失效。
- **AG-H2（正确性，唯一的「高」）**：设置热更新调用 `POST /instance/dispose`，会取消沙箱里所有运行中的会话。维护循环只在开头判断一次「没有运行中任务」，之后要写文件、再 dispose，这段时间内开始的新任务会被中止。「改完设置马上发消息」就能触发，已构造出确定的复现。
- 其余 5 个「中」都已复现或有明确依据：
  - AG-M1：跨副本中止被记为 FAILED；
  - AG-M2：客户端清理会关掉正在被任务使用的连接，任务可能挂到沙箱硬截止；
  - AG-M3：同一进程开了第二个 SQLite 写连接，违反 `store/db.py` 里记录的实测约束；
  - AG-M4：同副本重连的回放上限会让长任务的文本缺段；
  - AG-M5：`Lifecycle.wait_background` 在 Python 3.12 上可能空转，停机卡死（既有代码，修复后多轮回归时发现）。

### 验证记录（本次实测）

| 验证项 | 结果 |
| --- | --- |
| 全量单测（修复前） | ✅ 118 passed，73.2s |
| 跨副本中止：A 执行任务、B 调用中止 | ❌ 3/3 次终态为 `FAILED`（`The operation was aborted.`），应为 `ABORTED`（AG-M1） |
| 维护循环写配置期间开始新任务，fake 的 dispose 按真实行为取消会话 | ❌ 新任务 `FAILED`（`The operation was aborted.`），结果为空（AG-H2） |
| opencode 源码 `session/run-state.ts` | ✅ 实例状态的 finalizer 对所有 runner 执行 `runner.cancel`，即 dispose 会中止运行中的会话 |
| 维护循环一轮中沙箱转为 ACTIVE、任务开始，本轮末尾清理客户端 | ❌ 任务持有的客户端被关闭（`closed=True`），缓存中也被移除（AG-M2） |
| 已关闭的 `httpx.AsyncClient` 再发请求 | `RuntimeError: Cannot send a request, as the client has been closed.` |
| `parse_policy({"allow_out": ["100.100.100.200"]})` → `build_network` | ❌ 放行项原样下发，与强制屏蔽并存；按平台「allow 优先」规则，元数据地址可达（AG-H1） |
| 优雅停机时本副本是否重新接管刚让出的任务 | 3 次探针均由另一副本接管，窗口很窄，但代码路径存在（AG-L1） |
| 永不触发的 cron（`0 0 31 2 *`）校验耗时 | ⚠️ 约 180ms，期间阻塞事件循环（AG-L5） |
| 仓库卫生：`.env`、`.data/`、`sxw_aicoding/百炼-mcp.txt` | ✅ 均被 `.gitignore` 覆盖，未入库 |
| 云上实测 `allow_out` 与 `deny_out` 的优先级（2 个临时沙箱，测完即销毁） | `223.5.5.5` 只在 `deny_out` 中时不可达（`000`），同时加入 `allow_out` 后可达（`200`）：**放行优先，已确认**。元数据 `100.100.100.200` 放行后仍不可达，平台层另有屏蔽 |
| 修复后连跑全量单测 | ⚠️ 约 1/3 的轮次在某个用例的清理阶段卡死、CPU 100%。定位为 `Lifecycle.wait_background` 在 Python 3.12 上空转（AG-M5，既有问题） |

---

## 二、问题清单总览

| 编号 | 级别 | 位置 | 问题 | 是否复现 |
| --- | --- | --- | --- | --- |
| AG-H1 | 中（原判高） | `agent/policy.py:92-94、178-189` | `allow_out` 可放行内网 / 元数据地址，绕过强制屏蔽 | 代码级复现 + 云上实测放行优先 |
| AG-H2 | 高 | `agent/maintainer.py:180-190`、`agent/service.py:404-412` | 设置热更新的 dispose 会中止刚开始的任务 | ✅ 确定复现 |
| AG-M1 | 中 | `agent/runner.py:398-425` | 中止请求先于 runner 查库时，任务终态为 FAILED | ✅ 3/3 |
| AG-M2 | 中 | `agent/maintainer.py:95、292-296` | 用本轮开始时的快照清理客户端，会关掉正在使用的连接 | ✅ 确定复现 |
| AG-M3 | 中 | `agent/service.py:109`、`api/app.py:82` | agent 子系统另开写引擎，同进程两个 SQLite 写连接 | 依据 `store/db.py` 注释 |
| AG-M4 | 中 | `agent/runner.py:24-25、235-240` | 回放历史上限 5000 条，超出后重连的文本缺段 | 代码推演 |
| AG-M5 | 中 | `core/lifecycle.py:42-44`（既有代码） | `wait_background` 在 Python 3.12 上可能空转，停机卡死、CPU 100% | ✅ 单测中复现 3 次 + 最小脚本确认 |
| AG-L1 | 低 | `agent/service.py:132-148`、`agent/maintainer.py:114-136` | 停机时维护循环可能重新接管刚让出的任务；停机中触发的定时任务 runner 无人等待 | 窗口窄 |
| AG-L2 | 低 | `agent/maintainer.py:118-124` | 接管任务的 CAS 不检查心跳是否仍过期 | 代码推演 |
| AG-L3 | 低 | `agent/service.py:769-793` | 手动触发定时任务失败（容量满、建沙箱失败）也返回 `{"skipped": true}` | 代码推演 |
| AG-L4 | 低 | `agent/service.py:758-759` | PATCH 定时任务的任何字段都会重算 `next_run_at`，固定间隔任务的节奏被重置 | 代码推演 |
| AG-L5 | 低 | `agent/cron.py:13-14` | 永不触发的表达式要搜索 20 万步；注释「约 5 年」不准确 | ✅ 180ms |
| AG-L6 | 低 | `agent/runner.py:300-317、414-425` | 建会话后、发提示词前副本崩溃，接管后任务记为 SUCCEEDED、结果为空 | 代码推演 |
| AG-L7 | 低 | `agent/service.py:414-425` | 两路并发下发出网策略，库里的版本可能与平台实际不一致 | 代码推演 |
| AG-L8 | 低 | `agent/service.py:51、89-94` | 调用方视图用黑名单过滤字段，暴露 `op_owner`（主机名-进程号）等内部字段 | — |
| AG-L9 | 低 | `agent/service.py:779` | `overlap=skip` 的检查不在准入锁内 | 代码推演 |
| AG-L10 | 低 | `agent/runner.py:351-377` | 中止后 opencode 迟迟不回 idle 时，任务一直 RUNNING、占并发名额 | 推测 |
| AG-L11 | 低 | `agent/service.py:258-275` | 没有建沙箱熔断：模板坏掉时每个请求会建 3 次沙箱 | — |
| AG-L12 | 低 | `api/agent_routes.py:117` | `GET /egress` 会自动创建 agent（读接口有副作用） | — |
| AG-L13 | 低 | `provider/fake_opencode.py` | 测试替身缺两项真实语义：关闭后仍可用、dispose 不取消会话，导致 AG-H2 / AG-M2 在单测里看不到 | — |
| AG-L14 | 低 | `agent/opencode.py:113-120` | 同时设置 `POOL_AGENT_INGRESS_IP` 与 `HTTPS_PROXY` 时，访问 opencode 全部 TLS 校验失败 | ✅ 云上端到端测试中复现 |

另有一条**设计层面的风险**（不作为缺陷）：注入了凭证的域名（`api.deepseek.com`、`dashscope.aliyuncs.com`），agent 可以调用这些服务的任意接口，费用算在管理员账号上。被提示词注入的内容也能做到这一点。凭证本身不会泄露（只在出网时加到请求头），但滥用风险存在。建议在手册中写明，并在服务商侧给 Key 设用量上限。

---

## 三、高优先级问题

### AG-H1 `allow_out` 可放行内网 / 元数据地址，绕过强制屏蔽

**现状**：
- `parse_policy` 对 `allow_out` 只校验格式，是域名、IP 或 CIDR 即可（`policy.py:92-94`）。
- `build_network` 在开放模式下把调用方的 `allow_out` 原样下发，同时下发 `MANDATORY_DENY`（`policy.py:180-189`）。

**依据**：调研报告第 26 行记录了平台规则「`allow_out` 优先于 `deny_out`」（E2B 文档语义相同），本次云上实测也确认了（见第一节验证记录）。因此：

```text
PUT /v1/agents/u1/egress  {"allow_out": ["100.100.100.200"]}
→ allow_out=["100.100.100.200"]，deny_out=[…, "100.100.100.200/32"] → 放行优先，元数据地址可达
```

`10.0.0.0/8`、`169.254.169.254`、`0.0.0.0/0` 同理；白名单模式下放行 `192.168.1.0/24` 也能直达内网。

**影响**：
- 方案第 6 节和手册 4.4 节都承诺「内网段和元数据地址始终屏蔽，无法去掉」，代码没有兑现；
- 任何调用方（非管理员）都能去掉这层屏蔽。实测元数据地址在平台层另有屏蔽，内网段在未接入 VPC 时没有可达目标，所以**当前部署的实际危害有限**，据此下调为「中」。但沙箱一旦接入 VPC，就能直接访问内网。

**修复建议**：
- IP / CIDR 形式的 `allow_out` 与 `MANDATORY_DENY` 任一网段重叠时拒绝，两种模式都适用；`0.0.0.0/0` 自然也被拒绝；
- 开放模式下拒绝域名形式的 `allow_out`。开放模式本来就放行全部公网，域名放行项唯一的作用是让某个域名绕过 `deny_out`；
- IPv4 映射的 IPv6 地址（`::ffff:0:0/96`）一并拒绝。
- **残留风险**：白名单模式放行的域名如果解析到内网 IP，是否可达取决于平台按 SNI / Host 匹配的实现，接口层无法校验。写进手册，需要时再上云实测。

### AG-H2 设置热更新的 dispose 会中止刚开始的任务

**现状**：`check_sandbox`（`maintainer.py:146、180-190`）的执行顺序：
1. 在开头读一次 `running`；
2. 依次做健康检查（HTTP，最长 20s）、续期、出网策略下发；
3. `running` 为空时调用 `apply_settings`：先写 3 个文件（envd），再 `POST /instance/dispose`。

步骤 1 和步骤 3 的 dispose 之间可能隔几百毫秒到几十秒。这期间请求路径可以照常准入新任务：`create_task` 只检查任务数，不看沙箱是否在重载。

**依据**：opencode 源码 `session/run-state.ts` 中，实例状态的 finalizer 会对所有会话 runner 执行 `runner.cancel`。探针把 fake 的 dispose 改成同样的行为，在维护循环写配置期间开始新任务：任务终态为 `FAILED`，错误 `The operation was aborted.`，结果为空。

**触发条件很常见**：
- `update_settings` 会立刻唤醒维护循环（`service.py:229`）；
- 业务系统「改完设置马上发消息」，两者几乎同时进行。

**修复建议**：让「重载配置」与「任务准入」互斥，都在池级锁内判断：
- 维护循环重载前，在锁内确认该沙箱没有运行中任务，再占住 `op_owner` / `op_deadline`（ACTIVE 行平时这两列为空）；
- `create_task` 在同一把锁内检查沙箱：不在服务状态返回 gone；正在重载返回 reloading，请求路径短暂等待后重试；
- 准入成功时在同一事务里记录活动并把版本号加一，替代原来单独的 `touch_sandbox`；
- 重载整体加超时，保证 dispose 不会在标记过期之后才发出。

---

## 四、中优先级问题

### AG-M1 中止请求先于 runner 查库时，任务终态为 FAILED

`_follow` 在每轮循环里先处理事件，看到 idle 立即 `break`（`runner.py:400-401、414-415`），`_check_abort` 在它之后执行，而且其他副本写入的 `abort_requested` 每 5 秒才查一次。

以中止请求落在非负责副本为例：
1. B 写入 `abort_requested=1`，再直接调用 opencode 的 abort；
2. 会话很快回到 idle，负责副本 A 的 runner 马上退出循环；
3. 此时 `_abort_reason` 仍为空，终态按消息里的 `MessageAbortedError` 判为 FAILED。

同副本中止也有同样的竞态：`request_abort` 设置的事件要等下一轮 `_check_abort` 才会读到。有负载均衡时，约一半的中止会落在非负责副本上。**3/3 次复现。**

**修复建议**：循环结束后，若 `_abort_reason` 为空，再看一次本地中止事件和库里的 `abort_requested`，有中止请求就判为 ABORTED。

### AG-M2 用本轮开始时的快照清理客户端，会关掉正在使用的连接

`tick()` 在开头取在服务的沙箱快照 `rows`，末尾 `_prune_clients(rows)` 只保留「快照中的行 + 此刻仍在 CREATING / WARMING 的行」（`maintainer.py:88、95、292-296`）。

沙箱如果在这一轮中间从 WARMING 转为 ACTIVE，两个集合都不包含它：
- 它的客户端被关闭并移出缓存；
- 本轮期间已经开始的任务仍持有这个客户端，之后每个请求都会报 `RuntimeError`；
- runner 的状态轮询把错误当作「暂时不可达」，沙箱又仍然存活，于是一直等下去，直到任务超时（默认 4h）或沙箱硬截止；这期间持续占一个并发名额。

单测没有发现这个问题，是因为 `FakeOpencodeClient.close()` 只是设一个标志（AG-L13）。

**修复建议**：清理前重新查询在建、在服务的全部行，再并上本副本 runner 正在使用的沙箱；fake 客户端关闭后调用任何方法都报错。

### AG-M3 同一进程开了第二个 SQLite 写连接

- `AgentService` 默认 `AgentStore.open(cfg.db_url, …)`，会新建一套写引擎和只读引擎（`service.py:109`）；
- `app.py:82` 没有传入代码执行池已有的 store。

结果是开启 agent 子系统后，每个进程有两个写连接。`store/db.py` 的注释记录了实测结论：「同一进程内多个连接高并发争抢 SQLite 写锁时会出现长达 busy_timeout 的锁等待」，CLAUDE.md 也要求「每进程 1 个写连接」。

**修复建议**：`create_app` 用代码执行池的引擎构造 `AgentStore` 传入；`AgentService` 只关闭自己创建的 store。测试夹具里多个副本各自建 store 用于模拟多进程，这个用法不变。

### AG-M4 回放历史上限 5000 条，超出后重连的文本缺段

- `_publish` 在 `history` 满 5000 条后不再追加，但照常推给现有订阅者（`runner.py:235-240`）。
- 文本增量一条一个事件，几千 token 的回复就能超过 5000 条。

长任务中途重连到同一副本时，拿到的是前 5000 条加之后的实时事件，中间一段丢失，客户端拼出的文本会缺段。这与测试报告「同副本重连会完整回放全部事件」的说法不符。

**修复建议**：连续的同类 text / reasoning 增量在历史中合并为一条。合并时生成新对象，不改已经推给订阅者的对象。

### AG-M5 `Lifecycle.wait_background` 在 Python 3.12 上可能空转，停机卡死

**发现经过**：修复后按惯例连跑全量单测，约 1/3 的轮次卡在某个用例的清理阶段，CPU 100%，持续数分钟。
- faulthandler 转储显示主线程停在 `lifecycle.py:44`（`AgentMaintainer.stop` → `wait_for(lc.wait_background())`）；
- 自制探针进一步看到：`_tasks` 里只剩一个**已结束**的 `finish_destroy` 任务，却一直没被移出集合。

**原因**：
- `spawn` 用 `add_done_callback(self._tasks.discard)` 移除结束的任务，而回调只是排进事件循环，要等下一轮才执行；
- Python 3.12 起，`asyncio.gather` 对已结束的 future 直接完成，`await` 一个已完成的 future 不会让出事件循环；
- 任务刚结束、回调还没执行时进入 `while self._tasks: await gather(...)`，循环就再也不让出事件循环：回调永远得不到执行，外层 `wait_for` 的超时也无法触发。

用最小脚本确认：在 3.12.10 上，对一个已结束任务 `gather` 10 万次，期间排队的回调一次都没执行。

**影响**：
- 代码执行池与 agent 子系统的停机都经过这里，副本收到 SIGTERM 后可能卡死，只能 SIGKILL；
- CLAUDE.md 写的是 Python 3.11，但本机 `.venv`（agent 子系统的端到端环境）是 3.12；
- 这个问题早已存在，本次的改动让停机时序更容易踩中它。

**修复**：只等待尚未结束的任务，没有未结束的任务就返回。

---

## 五、低优先级问题

- **AG-L1 停机交接的两个缺口**：
  - `stop()` 先让 runner 让出任务（`op_deadline` 置为当前时间），再停维护循环。这之间本副本的维护循环可能在 `takeover_tasks` 里把任务重新接管，在正在停机的副本上又起一个 runner；
  - 停机过程中维护循环触发的定时任务，也会在「等待 runner」这一步之后再起 runner；
  - 这类 runner 无人等待，store 关闭后才退出。实际后果是交接延迟到心跳过期（60s），或者任务被误判失败。
  - 建议：加停机标志，停机时不再接管、不再触发定时任务；维护循环停下后再让出一次 runner。
- **AG-L2 接管 CAS 不检查心跳是否仍过期**：`cas_task` 只比较 `op_owner`（`maintainer.py:118-124`）。读到「过期」后、CAS 之前，原负责副本刚好刷新了心跳，仍会被抢走。结束时有 CAS 保护，结果不会错，但会重复跟进。建议 CAS 时加上 `op_deadline < now`。
- **AG-L3 手动触发的失败被报成「跳过」**：`fire_schedule` 吞掉 TooManyTasks、AgentUnavailable、WaitTimeout、SandboxOpError 后返回 None，接口因此返回 `{"skipped": true}`。建议手动触发时把这些错误原样抛出（429 / 503 / 504 / 502），自动触发保持记事件。
- **AG-L4 PATCH 定时任务任意字段都重算 `next_run_at`**：改个名字，每天一次的固定间隔任务就被推迟到从现在起算。建议只在 `cron`、`every_s`、`timezone`、`enabled` 变化时重算。
- **AG-L5 cron 搜索上限**：`_MAX_STEPS = 200_000`。像 2 月 31 日这种永不触发的表达式，要跑满 20 万步（约 180ms）才报错，期间阻塞事件循环；注释「约 5 年」并不准确，实际能覆盖数千年。建议按日期设上限（9 年，覆盖 2 月 29 日跨世纪的 8 年间隔）。
- **AG-L6 提示词未送达也记为成功**：原副本在 `cas_task(session_id)` 之后、`prompt_async` 之前崩溃（约 100ms 窗口）。接管后会话不忙、没有 assistant 消息，任务被记为 SUCCEEDED、结果为空。概率很低，可以接受。
- **AG-L7 并发下发出网策略**：两路 `apply_network` 的云端调用和写库顺序可能交错，出现平台是旧策略、库里是新版本的情况，`in_sync` 误报为 true。需要两个并发 PUT，或者 PUT 恰好与维护循环重试交错。`GET /egress` 的 `platform` 字段能看出差异。概率很低，暂不处理。
- **AG-L8 调用方视图用黑名单过滤字段**：`sandbox_view` 只去掉 `access_token`、`lease_id`，`op_owner`（主机名-进程号）、`version` 等内部字段都会返回给业务调用方；以后新增敏感列也容易漏掉。暂不改接口形状，记录在案。
- **AG-L9 `overlap=skip` 的检查不在准入锁内**：手动触发与自动触发同时发生时可能重叠执行。个人规模下可以接受。
- **AG-L10 中止后不回 idle**：如果 opencode 的 abort 没有生效，任务会一直 RUNNING，直到沙箱硬截止。目前没有证据表明 opencode 会出现这种情况，暂不处理。
- **AG-L11 没有建沙箱熔断**：每个请求最多建 3 次沙箱；模板损坏时每个请求都会产生 3 次创建费用。个人规模下可以接受，多用户时再加。
- **AG-L12 读接口有副作用**：`GET /egress` 用的是 `ensure_agent`，读取就会建 agent 记录。影响只是多一条记录，不处理。
- **AG-L13 测试替身缺两项真实语义**：见 AG-H2、AG-M2，随修复一起补上。
- **AG-L14 入口 IP 与 HTTPS_PROXY 同时设置时 TLS 必然失败**：
  - 修复后第一次跑云上端到端测试时，本机 shell 带着 `HTTPS_PROXY`；
  - `OpencodeHttpClient` 既按 `POOL_AGENT_INGRESS_IP` 把地址换成 IP，又经代理隧道连接；
  - httpcore 的隧道连接用目标地址（IP）做 TLS 的 `server_hostname`，忽略 `sni_hostname`（`httpcore/_async/http_proxy.py:312`）；
  - 结果是每个请求都报 `CERTIFICATE_VERIFY_FAILED: IP address mismatch`，装配卡在等待健康检查。
  - 之前的端到端测试环境没有设置 `HTTPS_PROXY`，所以没有暴露。PoC 脚本有同样的问题。
  - 修复：设置了入口 IP 就直连，不走代理（入口 IP 本来就是为绕开本机代理而设）。

---

## 六、确认没有问题的部分

- **凭证**：
  - 模型 Key 与注入值只经 `network.rules` 下发，沙箱内只有占位符；
  - `access_token` 在 `sandbox_view`、管理员列表、代码执行池的 `/v1/sandboxes` 中都被去掉；
  - `get_network` 的回显经 `redact_platform_network` 脱敏；
  - `百炼-mcp.txt` 与 `.env` 均未入库。
- **准入与建沙箱**：
  - `reserve_agent_sandbox`、`create_task`、`ensure_agent` 都在池级锁内先查再写，正常路径上不执行预期会失败的语句；
  - 两个副本并发首次请求只建一个沙箱（已有用例）。
- **空闲销毁与准入的竞态**：`touch_sandbox` 把版本号加一，维护循环按旧快照做的销毁 CAS 会失败；若销毁在前，则 touch 失败并重试。两种顺序都正确。
- **runner 与 HTTP 解耦**：客户端断开只取消读队列的协程；跨副本重连时，读库和订阅事件都放在后台任务里，按事件通知退出。
- **超时与截止时间**：
  - 新增云端调用都显式设置了请求超时；
  - `create_app` 的 30s 小于 `agent_boot_timeout_s`（至少 60s）；
  - `check_agent_config` 校验了 `rotate < max_life <= 86400`、`task_max < max_life`、`takeover > 2 × heartbeat`。
- **池隔离**：
  - agent 池用独立池名（库记录与云端元数据 `pool`）；
  - 代码执行池的对账只列自己池名的沙箱，不会误杀 agent 沙箱；
  - `pool_kv` 的周期任务键按池名区分。

---

## 七、二次反思：哪些值得修

判断标准有三条：
- 会不会在真实使用中出现：个人规模、多副本部署、业务系统按手册接入；
- 后果是否是错误结果、安全承诺失效或资源长期占用；
- 修复的代价与风险是否相称。

| 编号 | 结论 | 理由 |
| --- | --- | --- |
| AG-H1 | **修** | 安全承诺失效，任何调用方都能触发；修复只动校验，风险小 |
| AG-H2 | **修** | 正常使用路径（改设置后发消息）就能触发，任务被静默中止；需要在准入里加互斥，改动集中在 `create_task` 与 `apply_settings` |
| AG-M1 | **修** | 有负载均衡时约一半的中止状态是错的，业务系统依赖这个状态；修复只有几行 |
| AG-M2 | **修** | 概率不高，但后果是任务挂数小时、占名额；修复很小 |
| AG-M3 | **修** | 违反仓库记录的实测约束；修复只改引擎的构造和关闭，行为不变 |
| AG-M4 | **修** | 长任务重连是手册主推的用法；修复局部 |
| AG-M5 | **修** | 停机卡死、只能强杀；修复只有几行 |
| AG-L1 | **修** | 代码路径确实存在，修复只需一个标志位和一次补充等待；手册承诺了「正常停止时交接」 |
| AG-L2 | **修** | 一个条件，降低重复跟进 |
| AG-L3 | **修** | 接口语义错误，业务系统无法区分「跳过」与「失败」；改动小 |
| AG-L4 | **修** | 用户可感知的行为问题；改动小 |
| AG-L5 | **修** | 修复两行，消除认证用户可触发的事件循环阻塞 |
| AG-L6 | 不修 | 约 100ms 的崩溃窗口，要准确判断「提示词是否送达」需要比对会话消息，收益低 |
| AG-L7 | 不修 | 需要并发写同一 agent 的出网策略；大部分交错下维护循环能自愈；`GET /egress` 的平台回显可人工核对 |
| AG-L8 | 不修 | 改动会改变已发布接口的返回字段；字段本身不是凭证。`access_token` 已有测试守护 |
| AG-L9 / L10 / L11 / L12 | 不修 | 个人规模下概率低或影响小；L10 缺乏证据 |
| AG-L13 | 随修复补 | 让 AG-H2、AG-M2 这类问题在单测中可见 |
| AG-L14 | **修** | 配置组合合法、文档也推荐设置入口 IP，一旦同时有代理就完全不可用；修复一行 |
| 注入凭证滥用（设计风险） | 已有文档 | 属于「出网注入」方案的固有取舍；手册第 7 节「注入不等于授权」已写明，并建议给 Key 设最小权限和额度上限 |

修复要求（沿用仓库惯例）：
- 每个修复都配确定时序的回归用例，并确认去掉修复后用例会失败；
- 修完全量连跑多轮，排查偶发失败；
- 同步更新 CLAUDE.md、业务接入使用手册和测试报告中受影响的描述。

---

## 八、修复记录

### 8.1 代码改动

| 编号 | 改动 |
| --- | --- |
| AG-H1 | `agent/policy.py:51、81`：<br>- `allow_out` 中的 IP / CIDR 与强制屏蔽网段重叠（含 `0.0.0.0/0`、IPv4 映射的 IPv6）时拒绝；<br>- 开放模式拒绝域名放行项；合并后再校验，模式来自覆盖、放行项来自默认策略的情况也能拦住；<br>- 新增 `strict=False`：`service.effective_policy`（`service.py:188`）读取库里的旧覆盖时丢弃违规放行项，不让装配和维护循环失败 |
| AG-H2 | `store/agent_repo.py`：<br>- `create_task`（235 行）在池级锁内检查沙箱仍在服务、不在重载，准入成功的同一事务里记录活动时间、把版本号加一，取代原来单独的 `touch_sandbox`；<br>- 新增 `begin_reload` / `end_reload`（290、323 行）。<br>`agent/service.py`：<br>- `apply_settings`（424 行）先占住沙箱再写配置、dispose，整体限时 60s，占用 90s；<br>- `_start_task` 遇到 reloading 时等待，timeout 用 `agent_wait_sandbox_s`。<br>维护循环只在真的应用了设置时记事件 |
| AG-M1 | `agent/runner.py:423`：跟进循环结束后，若尚未判定中止，再看本地中止事件和库里的 `abort_requested`，有就判为 ABORTED |
| AG-M2 | `agent/maintainer.py:300`：`_prune_clients` 重新查询在建、在服务的全部沙箱，并保留本副本 runner 正在用的沙箱，不再用本轮开始时的快照 |
| AG-M3 | `api/app.py:85`：用代码执行池的写引擎和只读引擎构造 `AgentStore` 传给 `AgentService`；`AgentService` 只关闭自己打开的 store（`service.py:115、157`） |
| AG-M4 | `agent/runner.py:239`：回放历史中连续的同类 text / reasoning 增量合并为一条。合并时生成新对象，已推给订阅者的对象不变 |
| AG-M5 | `core/lifecycle.py:42`：`wait_background` 只等待尚未结束的任务，没有就返回 |
| AG-L1 | `agent/service.py:141-167`：`stop()` 先置 `stopping`，再让出 runner、停维护循环，然后再让出一次；`maintainer.py:115、239`：停机中不接管任务、不触发定时任务 |
| AG-L2 | `store/agent_repo.py:363-371`：`cas_task` 新增 `expect_stale_before`，接管时要求心跳仍过期 |
| AG-L3 | `agent/service.py:824`：`fire_schedule(manual=True)` 把开不了任务的错误抛给接口（429 / 503 / 504 / 502）；自动触发仍只记事件 |
| AG-L4 | `agent/service.py:798`：只有 `cron` / `every_s` / `timezone` / `enabled` 变化，或原来没有下次触发时间时才重算 `next_run_at` |
| AG-L5 | `agent/cron.py:15、102`：按日期把搜索限制在 9 年内。`0 0 31 2 *` 的判定从约 180ms 降到约 1.4ms；2096 年之后的 2 月 29 日仍能正确算到 2104 年 |
| AG-L13 | `provider/fake_opencode.py`：客户端 close 后再调用抛 `RuntimeError`（与 httpx 一致）；dispose 取消所有运行中的会话（与 opencode 一致） |
| AG-L14 | `agent/opencode.py:116`：设置了入口 IP 就不走代理；`scripts/poc_opencode_agent.py` 同样处理 |

未修的问题（AG-L6～L12）及理由见第七节。

### 8.2 测试与验证

- **回归用例**：`tests/test_agent_review_fixes.py`，20 个函数、29 个用例，按 AG-* 编号组织。
  - 原有用例中只改了一处：`test_agent_units.py::test_overlay_overrides_only_given_fields` 的放行项从域名改为公网 IP。开放模式不再接受域名放行项，这是本次有意为之的行为变化。
- **去掉修复后用例必须失败**：用 `/tmp/mutate.py` 在临时副本里逐个撤销修复，再跑对应用例：

  | 撤销的修复 | 结果 |
  | --- | --- |
  | AG-H1 | 12 failed |
  | AG-H2 | 3 failed |
  | AG-M1 | 1 failed（跨副本用例；同副本用例在未修复时多数也能通过，符合预期） |
  | AG-M2、AG-M3、AG-M4、AG-M5 | 各 1 failed |
  | AG-L1、AG-L2、AG-L3、AG-L4、AG-L5 | 各 1 failed |

- **全量单测连跑**：
  - AG-M5 修复前，约 1/3 的轮次卡死；
  - 修复后连跑 6 轮，每轮 146 个全部通过（约 84s）；
  - 加上 AG-L14 的用例后再跑 2 轮（结果见 8.4）。
- **云上端到端**（`scripts/e2e_agent_scenarios.py`，2 副本、共享 SQLite、开启鉴权，本机带 `HTTPS_PROXY`）：
  - 第一次运行暴露了 AG-L14，已中止并清理；
  - 修复后 **S1–S11 全部通过**：
    - S1 冷启动 `start` 2.36s，热沙箱 0.09s；
    - S4 切白名单 0.82s 生效；
    - S7 终态 ABORTED；
    - S10 22.1s 被另一副本接管完成。
  - 统计中 `agent_settings_applied` 为 2（S9 修改设置），说明新的重载路径在云上实际执行了；
  - 在线服务上 3 种违规出网覆盖都返回 400。
- **云上验证 `allow_out` 优先级**：2 个临时沙箱，结论见第一节，测完即销毁。

### 8.3 文档同步

- 业务接入使用手册：
  - 出网策略新增放行项约束和切回开放模式的写法；
  - 设置重载的行为；
  - 定时任务 PATCH 与 `/run` 的语义；
  - `POOL_AGENT_INGRESS_IP` 不走代理，排障表新增一条。
- `CLAUDE.md`：
  - 任务准入与重载互斥、出网放行约束；
  - 测试替身必须保留的两点真实语义；
  - 共用数据库引擎；
  - 「集合非空就 `await gather`」的 Python 3.12 陷阱；
  - 本报告的索引。
- 测试报告：追加「评审修复后的回归」一节。

### 8.4 收尾

- 全部测试沙箱已销毁：`scripts/cleanup_sandboxes.py` 确认账号下沙箱列表为空。
- 代码执行模板 `xu76gk97q07mgohgw7q3` 与 opencode 模板 `z0tkbiqlztqsma57014d` 是模板而不是实例，按约定保留。
