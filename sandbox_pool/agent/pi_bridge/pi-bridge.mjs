#!/usr/bin/env node
// pi 桥接进程：在云沙箱里对网关提供 HTTP / SSE 接口，每个会话起一个 `pi --mode rpc` 子进程。
//
// - 控制接口沿用 opencode serve 的路径（网关用同一个 HTTP 客户端，见 sandbox_pool/agent/opencode.py）；
//   事件与消息是 pi 的原生格式，事件套在 {"type": "pi.event", "properties": {sessionID, runID, event}} 里。
// - 只做协议转发和进程管理，不做事件翻译（翻译在网关的 sandbox_pool/agent/engines/pi.py）：本文件随模板发布，
//   改动要重建模板，所以尽量薄。
// - 零依赖，Node >= 18。设计见 sxw_aicoding/方案设计/2026-09-26-pi引擎接入-实施方案.md 第 4 节。
//
// pi 的 RPC 协议要点（packages/coding-agent/docs/rpc.md）：
// - stdout 严格按 LF 切分，不能用 readline（会把 U+2028 / U+2029 当换行）；必须持续读，否则 pi 被背压卡住；
// - prompt 的成功响应只代表已受理，运行以 agent_settled 结束；abort 等会话空闲后才响应；
// - 关闭 stdin 即有序退出（会中止运行中的会话）。

import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import fs from "node:fs";
import http from "node:http";
import os from "node:os";
import path from "node:path";

const VERSION = "0.1.2";
const env = process.env;
const num = (v, d) => (v !== undefined && v !== "" && Number.isFinite(Number(v)) ? Number(v) : d);

const CONF = {
  host: env.PI_BRIDGE_HOST || "0.0.0.0",
  port: num(env.PI_BRIDGE_PORT, 4096),
  // pi 命令（JSON 数组），测试时替换为假的 pi
  cmd: env.PI_CMD ? JSON.parse(env.PI_CMD) : ["pi"],
  workdir: env.PI_BRIDGE_WORKDIR || "/home/user/workspace",
  sessionDir: env.PI_BRIDGE_SESSION_DIR || "/home/user/.agent/pi-sessions",
  // 网关写入：{provider, model, thinking, mcp}；每次起会话进程时读取
  configFile: env.PI_BRIDGE_CONFIG || "/home/user/.agent/pi.json",
  logDir: env.PI_BRIDGE_LOG_DIR || "/home/user/.agent/pi-logs",
  // pi-mcp-adapter 的入口；pi.json 的 mcp 为 true 时用 -e 加载
  mcpExtension: env.PI_MCP_EXTENSION || "",
  // pi-mcp-adapter 的工具元数据缓存：直接注册的 MCP 工具从这里加载（见 waitMcpCache）
  mcpCacheFile:
    env.PI_MCP_CACHE || path.join(env.PI_CODING_AGENT_DIR || path.join(os.homedir(), ".pi", "agent"), "mcp-cache.json"),
  mcpCacheWaitMs: num(env.PI_BRIDGE_MCP_CACHE_WAIT_S, 10) * 1000,
  piVersion: env.PI_VERSION || "",
  idleMs: num(env.PI_BRIDGE_IDLE_S, 600) * 1000,
  maxProcs: num(env.PI_BRIDGE_MAX_PROCS, 8),
  cmdTimeoutMs: num(env.PI_BRIDGE_CMD_TIMEOUT_S, 60) * 1000,
  abortTimeoutMs: num(env.PI_BRIDGE_ABORT_TIMEOUT_S, 30) * 1000,
  heartbeatMs: num(env.PI_BRIDGE_HEARTBEAT_S, 10) * 1000,
  reapIntervalMs: num(env.PI_BRIDGE_REAP_INTERVAL_S, 15) * 1000,
  killGraceMs: num(env.PI_BRIDGE_KILL_GRACE_S, 10) * 1000,
  // 开始监听前先起一次 pi 预热（见 warmUp），0 表示不预热
  warmupTimeoutMs: num(env.PI_BRIDGE_WARMUP_S, 60) * 1000,
  maxBodyBytes: 8 * 1024 * 1024,
  logMaxBytes: 2 * 1024 * 1024,
  maxRuns: 20000,
};

const SESSION_ID = /^[A-Za-z0-9][A-Za-z0-9_-]{7,63}$/;

class BridgeError extends Error {
  constructor(status, message) {
    super(message);
    this.status = status;
  }
}

function log(...args) {
  process.stderr.write(`[pi-bridge ${new Date().toISOString()}] ${args.join(" ")}\n`);
}

// ---------------- 状态 ----------------

/** @type {Map<string, Session>} */
const sessions = new Map();
/** runID → {sessionId, state: running | settled | lost} */
const runs = new Map();
/** SSE 订阅者 */
const subscribers = new Set();
let requestSeq = 0;

class Session {
  constructor(id, title = "") {
    this.id = id;
    this.title = title;
    this.proc = null;
    this.busy = false;
    this.runId = null;
    this.lastUsed = Date.now();
    // 重载配置时正在运行：结束后重启进程
    this.reloadPending = false;
    // 正在起进程（并发请求共用同一个 Promise）
    this.starting = null;
  }
}

function broadcast(ev) {
  const data = `data: ${JSON.stringify(ev)}\n\n`;
  for (const res of subscribers) {
    res.write(data);
  }
}

function setRun(runId, sessionId, state) {
  runs.set(runId, { sessionId, state });
  if (runs.size > CONF.maxRuns) {
    // Map 按插入顺序迭代：删掉最早的
    const oldest = runs.keys().next().value;
    runs.delete(oldest);
  }
}

function startRun(s, runId) {
  s.busy = true;
  s.runId = runId;
  s.lastUsed = Date.now();
  setRun(runId, s.id, "running");
}

function settle(s, state) {
  if (!s.busy) return;
  const runId = s.runId;
  s.busy = false;
  s.lastUsed = Date.now();
  if (runId) setRun(runId, s.id, state);
  if (s.reloadPending) {
    s.reloadPending = false;
    stopProc(s, "reload after run");
  }
}

// ---------------- 会话进程 ----------------

function readConfig() {
  try {
    return JSON.parse(fs.readFileSync(CONF.configFile, "utf8"));
  } catch (e) {
    if (e.code !== "ENOENT") log("read config failed:", e.message);
    return {};
  }
}

function findSessionFile(id) {
  let names;
  try {
    names = fs.readdirSync(CONF.sessionDir);
  } catch {
    return null;
  }
  const suffix = `_${id}.jsonl`;
  const name = names.find((n) => n.endsWith(suffix));
  return name ? path.join(CONF.sessionDir, name) : null;
}

/** 占名额的进程数：正在有序退出的不算（几秒内就会退出，算进去会多回收一个空闲进程，甚至误报 503）。 */
function aliveProcs() {
  let n = 0;
  for (const s of sessions.values()) if (s.proc && s.proc.alive && !s.proc.stopping) n += 1;
  return n;
}

/** 起新进程前腾位置：超过上限时关掉最久未用的空闲进程；都在忙返回 503。 */
function makeRoom() {
  let n = aliveProcs();
  while (n >= CONF.maxProcs) {
    let victim = null;
    for (const s of sessions.values()) {
      if (s.proc && s.proc.alive && !s.busy && !s.proc.stopping && (!victim || s.lastUsed < victim.lastUsed)) victim = s;
    }
    if (!victim) throw new BridgeError(503, `too many active sessions (${CONF.maxProcs})`);
    stopProc(victim, "evicted");
    n -= 1;
  }
}

function openLog(id) {
  try {
    fs.mkdirSync(CONF.logDir, { recursive: true });
    const file = path.join(CONF.logDir, `pi-${id.slice(0, 8)}.log`);
    try {
      if (fs.statSync(file).size > CONF.logMaxBytes) fs.renameSync(file, `${file}.1`);
    } catch {
      // 文件不存在
    }
    return fs.createWriteStream(file, { flags: "a" });
  } catch (e) {
    log("open log failed:", e.message);
    return null;
  }
}

async function ensureProc(s) {
  // 每次用到会话进程都算一次活动：否则刚重开的进程会因为旧的 lastUsed 被空闲回收
  s.lastUsed = Date.now();
  if (s.proc && s.proc.alive && !s.proc.stopping) return s.proc;
  if (!s.starting) {
    s.starting = startProc(s).finally(() => {
      s.starting = null;
    });
  }
  return s.starting;
}

async function startProc(s) {
  const old = s.proc;
  if (old && old.alive) {
    // 旧进程正在有序退出（重载、回收）：等它退出再起新进程，避免两个进程同时写同一个会话文件
    await Promise.race([old.exited, new Promise((r) => setTimeout(r, CONF.killGraceMs + 1000))]);
  }
  makeRoom();
  const cfg = readConfig();
  const file = findSessionFile(s.id);
  const args = ["--mode", "rpc", "--session-dir", CONF.sessionDir];
  // 会话文件在第一条消息之后才落盘；没有文件时用 --session-id 新建（--session 遇到不存在的 ID 会直接退出）
  if (file) args.push("--session", file);
  else args.push("--session-id", s.id);
  if (cfg.provider) args.push("--provider", String(cfg.provider));
  if (cfg.model) args.push("--model", String(cfg.model));
  if (cfg.thinking) args.push("--thinking", String(cfg.thinking));
  // 只加载显式指定的扩展，忽略项目级 .pi/ 资源，不做自动联网（版本检查、模型目录刷新）
  args.push("-ne");
  if (cfg.mcp && CONF.mcpExtension) args.push("-e", CONF.mcpExtension);
  args.push("-na", "--offline");
  fs.mkdirSync(CONF.sessionDir, { recursive: true });
  const child = spawn(CONF.cmd[0], [...CONF.cmd.slice(1), ...args], {
    cwd: CONF.workdir,
    env: process.env,
    stdio: ["pipe", "pipe", "pipe"],
  });
  const proc = {
    child,
    alive: true,
    stopping: false,
    pending: new Map(),
    stderrTail: "",
    logStream: openLog(s.id),
    exited: null,
  };
  let markExited;
  proc.exited = new Promise((r) => {
    markExited = r;
  });
  s.proc = proc;
  child.stdin.on("error", () => {}); // 进程已退出时写入报 EPIPE，由 exit 处理
  attachStdout(s, proc);
  child.stderr.on("data", (chunk) => {
    proc.stderrTail = (proc.stderrTail + chunk.toString("utf8")).slice(-2000);
    proc.logStream?.write(chunk);
  });
  const onGone = (reason) => {
    if (!proc.alive) return;
    proc.alive = false;
    proc.logStream?.end();
    for (const p of proc.pending.values()) p.reject(new BridgeError(502, `pi process exited (${reason}): ${proc.stderrTail.slice(-500)}`));
    proc.pending.clear();
    markExited();
    // 只有当前进程的退出才影响会话的运行状态（被替换掉的旧进程退出时不动新进程上的运行）
    if (s.proc !== proc) return;
    s.proc = null;
    if (s.busy) {
      const runId = s.runId;
      log(`session ${s.id}: pi exited during run ${runId} (${reason})`);
      settle(s, "lost");
      broadcast({ type: "pi.run_lost", properties: { sessionID: s.id, runID: runId, reason } });
    }
  };
  child.on("exit", (code, signal) => onGone(signal ? `signal ${signal}` : `exit code ${code}`));
  child.on("error", (e) => onGone(`spawn error: ${e.message}`));
  // 确认进程能正常响应（参数错误、模型不存在等会在这里暴露）；没响应就关掉，不把卡住的进程留给后续请求
  try {
    await request(proc, { type: "get_state" });
  } catch (e) {
    stopProc(s, "not responding");
    throw e;
  }
  if (cfg.mcp && CONF.mcpExtension) await waitMcpCache(proc);
  s.lastUsed = Date.now();
  return proc;
}

/**
 * 新沙箱里还没有 MCP 元数据缓存时，适配器先只提供 `mcp` 代理工具，后台拉到元数据后才热加载直接注册的工具；第一条
 * 消息可能赶在这之前，模型就只能走代理（云上端到端测试里出现过，多走几步）。缓存一般随进程启动一起生成（本机约
 * 1.4s），这里等它出现，最多 mcpCacheWaitMs；超时照常继续（代理工具仍可用）。每个沙箱只在第一次起进程时等。
 */
async function waitMcpCache(proc) {
  if (!CONF.mcpCacheWaitMs || fs.existsSync(CONF.mcpCacheFile)) return;
  const t0 = Date.now();
  while (Date.now() - t0 < CONF.mcpCacheWaitMs && proc.alive && !fs.existsSync(CONF.mcpCacheFile)) {
    await new Promise((r) => setTimeout(r, 100));
  }
  // 缓存文件写出后适配器再热加载工具，留一点时间
  if (fs.existsSync(CONF.mcpCacheFile)) await new Promise((r) => setTimeout(r, 200));
  log(`mcp metadata cache ${fs.existsSync(CONF.mcpCacheFile) ? "ready" : "still missing"} after ${Date.now() - t0}ms`);
}

function attachStdout(s, proc) {
  let chunks = [];
  proc.child.stdout.on("data", (chunk) => {
    let start = 0;
    let idx;
    while ((idx = chunk.indexOf(10, start)) !== -1) {
      chunks.push(chunk.subarray(start, idx));
      let line = Buffer.concat(chunks);
      chunks = [];
      start = idx + 1;
      if (line.length && line[line.length - 1] === 13) line = line.subarray(0, line.length - 1);
      if (line.length) onLine(s, proc, line);
    }
    if (start < chunk.length) chunks.push(chunk.subarray(start));
  });
}

function onLine(s, proc, line) {
  let rec;
  try {
    rec = JSON.parse(line.toString("utf8"));
  } catch {
    log(`session ${s.id}: non-JSON stdout line: ${line.toString("utf8").slice(0, 200)}`);
    return;
  }
  if (rec.type === "response") {
    const p = rec.id ? proc.pending.get(rec.id) : null;
    if (p) {
      proc.pending.delete(rec.id);
      p.resolve(rec);
    } else if (!rec.success) {
      log(`session ${s.id}: unmatched error response: ${JSON.stringify(rec).slice(0, 300)}`);
    }
    return;
  }
  if (rec.type === "extension_ui_request") {
    answerUi(s, proc, rec);
    return;
  }
  if (rec.type === "agent_start" && !s.busy) {
    // 不是经本进程受理的运行（例如扩展发起的后续运行）：同样记为忙，结束时 settled
    startRun(s, randomUUID());
  }
  s.lastUsed = Date.now();
  broadcast({ type: "pi.event", properties: { sessionID: s.id, runID: s.runId, event: rec } });
  if (rec.type === "agent_settled") settle(s, "settled");
}

/** 无人值守：对话框类请求一律取消（confirm 回答 false），通知类忽略；都上报给网关。 */
function answerUi(s, proc, rec) {
  const dialog = ["select", "confirm", "input", "editor"].includes(rec.method);
  if (dialog) {
    const reply = rec.method === "confirm" ? { confirmed: false } : { cancelled: true };
    proc.child.stdin.write(`${JSON.stringify({ type: "extension_ui_response", id: rec.id, ...reply })}\n`);
  }
  if (dialog || rec.method === "notify") {
    const summary = [rec.method, rec.title, rec.message].filter(Boolean).join(": ").slice(0, 500);
    broadcast({ type: "pi.ui_request", properties: { sessionID: s.id, runID: s.runId, method: rec.method, summary, answered: dialog } });
  }
}

function request(proc, cmd, timeoutMs = CONF.cmdTimeoutMs) {
  if (!proc.alive) return Promise.reject(new BridgeError(502, "pi process is not running"));
  requestSeq += 1;
  const id = `bridge-${requestSeq}`;
  return new Promise((resolve, reject) => {
    const timer = setTimeout(() => {
      proc.pending.delete(id);
      reject(new BridgeError(504, `pi command ${cmd.type} timed out`));
    }, timeoutMs);
    proc.pending.set(id, {
      resolve: (r) => {
        clearTimeout(timer);
        resolve(r);
      },
      reject: (e) => {
        clearTimeout(timer);
        reject(e);
      },
    });
    proc.child.stdin.write(`${JSON.stringify({ ...cmd, id })}\n`);
  });
}

/** 有序退出：关闭 stdin；超时未退出则 SIGKILL。 */
function stopProc(s, reason) {
  const proc = s.proc;
  if (!proc || !proc.alive || proc.stopping) return;
  proc.stopping = true;
  log(`session ${s.id}: stopping pi (${reason})`);
  proc.child.stdin.end();
  setTimeout(() => {
    if (proc.alive) proc.child.kill("SIGKILL");
  }, CONF.killGraceMs).unref();
}

function getSession(id, { adopt = true } = {}) {
  if (!SESSION_ID.test(id)) throw new BridgeError(404, `session ${id} not found`);
  let s = sessions.get(id);
  if (!s && adopt && findSessionFile(id)) {
    // 桥接进程重启过：会话文件还在，接着用
    s = new Session(id);
    sessions.set(id, s);
  }
  if (!s) throw new BridgeError(404, `session ${id} not found`);
  return s;
}

function textOf(body) {
  const parts = Array.isArray(body?.parts) ? body.parts : [];
  return parts
    .filter((p) => p && p.type === "text" && typeof p.text === "string")
    .map((p) => p.text)
    .join("\n");
}

// ---------------- HTTP ----------------

const routes = [
  ["GET", /^\/global\/health$/, async () => ({
    healthy: true,
    engine: "pi",
    version: VERSION,
    pi_version: CONF.piVersion || null,
    sessions: sessions.size,
    procs: aliveProcs(),
    busy: [...sessions.values()].filter((s) => s.busy).length,
  })],
  ["POST", /^\/session$/, async (m, body) => {
    const id = randomUUID();
    const title = typeof body?.title === "string" ? body.title.slice(0, 200) : "";
    const s = new Session(id, title);
    sessions.set(id, s);
    try {
      const proc = await ensureProc(s);
      if (title) await request(proc, { type: "set_session_name", name: title });
    } catch (e) {
      sessions.delete(id);
      stopProc(s, "create failed");
      throw e;
    }
    return { id, title };
  }],
  ["POST", /^\/session\/([^/]+)\/prompt_async$/, async (m, body) => {
    const s = getSession(m[1]);
    const text = textOf(body);
    if (!text) throw new BridgeError(400, "prompt text is empty");
    if (s.busy) throw new BridgeError(409, `session ${s.id} is busy`);
    const proc = await ensureProc(s);
    if (s.busy) throw new BridgeError(409, `session ${s.id} is busy`);
    const runId = randomUUID();
    // 先标记忙再发命令：运行的事件可能早于 prompt 的响应到达
    startRun(s, runId);
    let resp;
    try {
      resp = await request(proc, { type: "prompt", message: text });
    } catch (e) {
      if (s.runId === runId) settle(s, "lost");
      throw e;
    }
    if (!resp.success) {
      if (s.runId === runId) settle(s, "lost");
      throw new BridgeError(400, resp.error || "prompt rejected");
    }
    if (resp.data?.disposition === "handled" && s.runId === runId) settle(s, "settled");
    return { run_id: runId, disposition: resp.data?.disposition || "started" };
  }],
  ["POST", /^\/session\/([^/]+)\/abort$/, async (m) => {
    const s = getSession(m[1], { adopt: false });
    if (!s.busy || !s.proc || !s.proc.alive) return false;
    const resp = await request(s.proc, { type: "abort" }, CONF.abortTimeoutMs);
    return Boolean(resp.success);
  }],
  ["GET", /^\/session\/status$/, async () => {
    const out = {};
    for (const s of sessions.values()) if (s.busy) out[s.id] = { type: "busy", run_id: s.runId };
    return out;
  }],
  ["GET", /^\/session\/([^/]+)\/run\/([^/]+)$/, async (m) => {
    const run = runs.get(m[2]);
    return { state: run && run.sessionId === m[1] ? run.state : "unknown" };
  }],
  ["GET", /^\/session\/([^/]+)\/message$/, async (m) => {
    const s = getSession(m[1]);
    const proc = await ensureProc(s);
    const resp = await request(proc, { type: "get_messages" });
    if (!resp.success) throw new BridgeError(500, resp.error || "get_messages failed");
    return resp.data?.messages || [];
  }],
  ["POST", /^\/instance\/dispose$/, async () => {
    // 让新配置生效：空闲会话的进程现在重启（下次请求按新配置起），运行中的等结束后重启；不中止运行中的会话
    const reloaded = [];
    const deferred = [];
    for (const s of sessions.values()) {
      if (!s.proc || !s.proc.alive) continue;
      if (s.busy) {
        s.reloadPending = true;
        deferred.push(s.id);
      } else {
        stopProc(s, "reload");
        reloaded.push(s.id);
      }
    }
    return { reloaded, deferred };
  }],
];

function readBody(req) {
  return new Promise((resolve, reject) => {
    const chunks = [];
    let size = 0;
    req.on("data", (c) => {
      size += c.length;
      if (size > CONF.maxBodyBytes) {
        reject(new BridgeError(413, "request body too large"));
        req.destroy();
        return;
      }
      chunks.push(c);
    });
    req.on("end", () => {
      if (!chunks.length) return resolve(null);
      try {
        resolve(JSON.parse(Buffer.concat(chunks).toString("utf8")));
      } catch {
        reject(new BridgeError(400, "invalid JSON body"));
      }
    });
    req.on("error", reject);
  });
}

function sendJson(res, status, value) {
  const body = JSON.stringify(value);
  res.writeHead(status, { "content-type": "application/json", "content-length": Buffer.byteLength(body) });
  res.end(body);
}

function handleEvents(req, res) {
  res.writeHead(200, {
    "content-type": "text/event-stream",
    "cache-control": "no-cache",
    connection: "keep-alive",
    "x-accel-buffering": "no",
  });
  res.write(`data: ${JSON.stringify({ type: "server.connected", properties: { version: VERSION } })}\n\n`);
  subscribers.add(res);
  const hb = setInterval(() => res.write(": ping\n\n"), CONF.heartbeatMs);
  const done = () => {
    clearInterval(hb);
    subscribers.delete(res);
  };
  req.on("close", done);
  res.on("error", done);
}

export function createServer() {
  return http.createServer(async (req, res) => {
    const url = new URL(req.url, "http://bridge");
    try {
      if (req.method === "GET" && url.pathname === "/event") return handleEvents(req, res);
      for (const [method, re, fn] of routes) {
        const m = url.pathname.match(re);
        if (!m) continue;
        if (req.method !== method) continue;
        const body = method === "POST" ? await readBody(req) : null;
        return sendJson(res, 200, await fn(m, body));
      }
      throw new BridgeError(404, `no route for ${req.method} ${url.pathname}`);
    } catch (e) {
      const status = e instanceof BridgeError ? e.status : 500;
      if (status >= 500) log(`${req.method} ${url.pathname} failed: ${e.stack || e}`);
      if (!res.headersSent) sendJson(res, status, { error: String(e.message || e) });
      else res.end();
    }
  });
}

function reapIdle() {
  const now = Date.now();
  for (const s of sessions.values()) {
    if (s.proc && s.proc.alive && !s.busy && now - s.lastUsed > CONF.idleMs) stopProc(s, "idle");
  }
}

/**
 * 预热：起一个一次性的 pi 进程（不建会话），等它响应 get_state 后退出。
 * 云沙箱模板在构建期启动桥接进程、就绪后打快照；快照前没运行过 pi 时，沙箱里第一次起会话进程要约 11s（实测，
 * 之后约 0.7s）。在监听端口（就绪命令探测健康）之前预热，预热结果随快照带进每个沙箱。失败不影响启动。
 */
function warmUp() {
  if (!CONF.warmupTimeoutMs) return Promise.resolve();
  const t0 = Date.now();
  return new Promise((resolve) => {
    let child;
    let finished = false;
    const done = (how) => {
      if (finished) return;
      finished = true;
      clearTimeout(timer);
      log(`warm-up ${how} in ${Date.now() - t0}ms`);
      try {
        child.stdin.end();
      } catch {
        // 已退出
      }
      resolve();
    };
    const timer = setTimeout(() => {
      child?.kill("SIGKILL");
      done("timed out");
    }, CONF.warmupTimeoutMs);
    try {
      child = spawn(CONF.cmd[0], [...CONF.cmd.slice(1), "--mode", "rpc", "--no-session", "-ne", "-na", "--offline"], {
        cwd: fs.existsSync(CONF.workdir) ? CONF.workdir : undefined,
        env: process.env,
        stdio: ["pipe", "pipe", "ignore"],
      });
    } catch (e) {
      done(`failed (${e.message})`);
      return;
    }
    child.stdin.on("error", () => {});
    child.on("error", (e) => done(`failed (${e.message})`));
    // pi 起不来（参数、安装问题）时立即退出：不等超时，照常开始监听，错误在建会话时返回给网关
    child.on("exit", (code, signal) => done(`exited before responding (${signal || `code ${code}`})`));
    child.stdout.on("data", (chunk) => {
      if (chunk.includes('"response"')) done("done");
    });
    child.stdin.write(`${JSON.stringify({ id: "warmup", type: "get_state" })}\n`);
  });
}

export async function start() {
  await warmUp();
  const server = createServer();
  const reaper = setInterval(reapIdle, CONF.reapIntervalMs);
  reaper.unref();
  server.listen(CONF.port, CONF.host, () =>
    log(`listening on ${CONF.host}:${server.address().port} (v${VERSION}, pi ${CONF.piVersion || "?"})`),
  );
  const shutdown = () => {
    for (const s of sessions.values()) stopProc(s, "bridge shutdown");
    server.close();
    setTimeout(() => process.exit(0), 2000).unref();
  };
  process.on("SIGTERM", shutdown);
  process.on("SIGINT", shutdown);
  return server;
}

if (import.meta.url === `file://${process.argv[1]}` || process.argv[1]?.endsWith("pi-bridge.mjs")) {
  start();
}
