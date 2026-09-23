# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## 项目概况

基于阿里云云沙箱（FC Agent Sandbox，E2B 协议兼容）的**沙箱池服务**（`sandbox_pool/`）：给服务端 Agent 提供借用式的隔离执行环境，调用方只走 HTTP，不持有 SDK 和云端 Key。
- 需求：容量 5、排队 10、最长等待 180s；借用 10 分钟、可续期、最长 60 分钟；**归还即销毁**；空闲 60s 后暂停。
- 部署目标：多副本高可用。本地用「多进程 + 共享 SQLite」模拟，**代码不能写死单实例假设**。

项目文档和代码注释都用中文，新增内容保持一致。设计与决策以 `docs/` 为准：
- `sandbox-pool-design.md`：当前设计，是最权威的总览；
- `sandbox-pool-fix-changes.md`：最近一轮修复，含踩坑记录；
- `fc-agent-sandbox-notes.md`：云沙箱实测结论。

`examples/` 是早期摸底脚本：生命周期 demo、通过 OpenAPI 创建第二代模板。

## 常用命令

Python 3.11，本机虚拟环境在 `/root/.venvs/e2b`（没有就 `pip install -r requirements-dev.txt`）。

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

## 架构要点（需要跨文件理解的部分）

**分层**：`api/` → `core/pool.py`（组装入口，一个进程一个 `SandboxPool`）→ `core/allocator.py`（借用、排队、续期、代为执行）+ `core/maintainer.py`（后台维护循环）→ 二者共用 `core/lifecycle.py`（销毁、结束借用）→ `store/`（SQLAlchemy Core）+ `provider/`（`SandboxProvider` 协议：`E2BProvider` 为真实后端，`FakeProvider` 用于测试）。

**多副本协调全靠数据库，不选主**：
- 所有状态变更都是带条件的 UPDATE（CAS），比较 `state` / `version` / `op_owner` / `lease_id`，影响行数为 1 才算成功。
- 「先数再写」的操作先在同一事务里更新池级锁行 `pool_kv.lock` 做串行化，包括：占容量（`reserve_slot`，同时检查熔断和排空）、入队、暂停前检查 `min_hot`（`start_pause`）。
- 耗时的云端调用不持有数据库事务：先 CAS 进入过渡态并写入 `op_owner` / `op_deadline`，调用完成后再 CAS 到目标状态。
  - 执行者崩溃后，其他副本在 `recover_stuck` 中接管。
  - 暂停、恢复途中崩溃的，按云端实际状态收回（`_adopt`）；其余销毁后补货。
  - 截止时间：创建 / 预热 / 暂停用 `op_timeout_s`，恢复用 `resume_timeout_s`，销毁用 `destroy_timeout_s`。
  - `e2b_provider.py` 里各调用的请求超时必须小于对应的截止时间。
- 全池周期任务（对账、历史清理）用 `store.try_periodic` 在 `pool_kv` 时间戳上做 CAS，每个周期只有一个副本执行。
- 排队在 `waiters` 表里，`seq` 决定先来先服务：
  - 「前面的人数 < 可分配沙箱数」时才去抢；
  - 心跳是后台任务（`_Heartbeat`）；
  - 抢到沙箱与排队记录改为 GRANTED 在同一事务内完成（`claim_ready` / `finish_resume` 的 `waiter_seq`）。

**沙箱状态**：`CREATING → WARMING → READY ⇄ PAUSING → PAUSED → RESUMING → LEASED → DESTROYING`，任何状态都可以进入 DESTROYING；所有状态都计入容量。
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
- 云端列表接口有约 1.5s 延迟，且默认包含已暂停的沙箱。对账要在列表查询前后各读一次库，并留宽限期。
- 表结构只做「新增可空列 / 索引」的自动迁移（`store/repository.py` 的 `_migrate`）。新增列必须可空；需要预置的 `pool_kv` 键加到 `_KV_KEYS`。

**鉴权**（`api/auth.py`）：
- `POOL_API_KEYS` / `POOL_ADMIN_KEYS`，格式「名称:key」，逗号分隔。两者都为空时关闭鉴权，此时进程拒绝监听非回环地址（除非 `--allow-no-auth`）。
- 借用按 `leases.client_id` 绑定调用方；allocator 方法的 `owner` 参数为 None 时表示管理员，不做归属限制。
- 管理接口：`/v1/sandboxes`、`/v1/admin/drain`。

## 测试约定

- `tests/conftest.py` 的 `make_pool` fixture 用 `FAST` 时间参数创建池，可以通过关键字覆盖任意 `PoolConfig` 字段。
  - 多次调用得到多个副本，它们共享同一个 SQLite 文件和同一个 `FakeProvider`（模拟共享的云端）。
  - `run_maintainer=False` 时可以手动调用 `maintainer.replenish(...)` 和 `lifecycle.wait_background()`，精确控制时序。
- `FakeProvider` 可以模拟平台超时回收，并支持失败和延迟注入：`fail_create` / `fail_set_timeout` / `fail_kill` / `fail_resume_ids`、`latency_s` / `kill_latency_s` / `set_timeout_delays`。
- `tests/test_review_fixes.py` 按评审问题编号（H1、M1…L10）组织。
  - 修并发问题时要构造出确定性的时序，并确认去掉修复后用例会失败。
  - 修完后全量连跑多轮，排查偶发失败。
