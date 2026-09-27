// pi 桥接进程测试：真实起桥接进程（子进程），pi 换成 test/fake-pi.mjs。不访问网络。
//   node --test sandbox_pool/agent/pi_bridge/test/bridge.test.mjs

import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import { after, test } from "node:test";
import { fileURLToPath } from "node:url";

const here = path.dirname(fileURLToPath(import.meta.url));
const BRIDGE = path.join(here, "..", "pi-bridge.mjs");
const FAKE_PI = path.join(here, "fake-pi.mjs");
const bridges = [];

after(() => {
  for (const b of bridges) b.stop();
});

async function startBridge(extraEnv = {}, dir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-bridge-test-"))) {
  const argsLog = path.join(dir, "args.log");
  const env = {
    ...process.env,
    PI_CMD: JSON.stringify([process.execPath, FAKE_PI]),
    PI_BRIDGE_HOST: "127.0.0.1",
    PI_BRIDGE_PORT: "0",
    PI_BRIDGE_WORKDIR: dir,
    PI_BRIDGE_SESSION_DIR: path.join(dir, "sessions"),
    PI_BRIDGE_CONFIG: path.join(dir, "pi.json"),
    PI_BRIDGE_LOG_DIR: path.join(dir, "logs"),
    PI_BRIDGE_HEARTBEAT_S: "0.2",
    PI_BRIDGE_REAP_INTERVAL_S: "0.1",
    PI_BRIDGE_KILL_GRACE_S: "2",
    FAKE_PI_ARGS_LOG: argsLog,
    ...extraEnv,
  };
  fs.writeFileSync(env.PI_BRIDGE_CONFIG, JSON.stringify({ provider: "deepseek", model: "deepseek-flash", mcp: false }));
  const child = spawn(process.execPath, [BRIDGE], { env, stdio: ["ignore", "ignore", "pipe"] });
  let stderr = "";
  const port = await new Promise((resolve, reject) => {
    const t = setTimeout(() => reject(new Error(`bridge did not start: ${stderr}`)), 5000);
    child.stderr.on("data", (c) => {
      stderr += c.toString();
      const m = stderr.match(/listening on [\d.]+:(\d+)/);
      if (m) {
        clearTimeout(t);
        resolve(Number(m[1]));
      }
    });
  });
  const base = `http://127.0.0.1:${port}`;
  const b = {
    dir,
    base,
    argsLog,
    stop: () => child.kill("SIGKILL"),
    async call(method, p, body) {
      const r = await fetch(base + p, {
        method,
        headers: body ? { "content-type": "application/json" } : {},
        body: body ? JSON.stringify(body) : undefined,
      });
      const text = await r.text();
      return { status: r.status, body: text ? JSON.parse(text) : null };
    },
    args() {
      return fs.existsSync(argsLog) ? fs.readFileSync(argsLog, "utf8").split("\n").filter(Boolean).map((l) => JSON.parse(l)) : [];
    },
  };
  bridges.push(b);
  return b;
}

/** 订阅 /event，返回 {events, until(pred), close()}。 */
async function subscribe(b) {
  const ctrl = new AbortController();
  const r = await fetch(`${b.base}/event`, { signal: ctrl.signal });
  const events = [];
  const waiters = [];
  (async () => {
    const decoder = new TextDecoder();
    let buf = "";
    try {
      for await (const chunk of r.body) {
        buf += decoder.decode(chunk, { stream: true });
        let i;
        while ((i = buf.indexOf("\n\n")) >= 0) {
          const block = buf.slice(0, i);
          buf = buf.slice(i + 2);
          const data = block.split("\n").filter((l) => l.startsWith("data: ")).map((l) => l.slice(6)).join("\n");
          if (!data) continue;
          events.push(JSON.parse(data));
          for (const w of [...waiters]) w();
        }
      }
    } catch {
      // 中止
    }
  })();
  return {
    events,
    until(pred, timeoutMs = 5000) {
      return new Promise((resolve, reject) => {
        const check = () => {
          const hit = events.find(pred);
          if (hit) {
            waiters.splice(waiters.indexOf(check), 1);
            clearTimeout(t);
            resolve(hit);
          }
        };
        const t = setTimeout(() => {
          waiters.splice(waiters.indexOf(check), 1);
          reject(new Error(`timeout waiting for event; got ${JSON.stringify(events.map((e) => e.type + ":" + (e.properties?.event?.type || "")))}`));
        }, timeoutMs);
        waiters.push(check);
        check();
      });
    },
    close: () => ctrl.abort(),
  };
}

const prompt = (b, id, text) => b.call("POST", `/session/${id}/prompt_async`, { parts: [{ type: "text", text }] });
const settled = (sid, runId) => (e) =>
  e.type === "pi.event" && e.properties.sessionID === sid && e.properties.event.type === "agent_settled" && (!runId || e.properties.runID === runId);
const textOf = (events, sid) =>
  events
    .filter((e) => e.type === "pi.event" && e.properties.sessionID === sid && e.properties.event.type === "message_update")
    .map((e) => e.properties.event.assistantMessageEvent.delta)
    .join("");
const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

test("health、建会话、对话、事件与运行状态（含 U+2028 分帧）", async () => {
  const b = await startBridge();
  const h = await b.call("GET", "/global/health");
  assert.equal(h.status, 200);
  assert.equal(h.body.healthy, true);
  assert.equal(h.body.engine, "pi");
  const sub = await subscribe(b);
  assert.equal(sub.events.length === 0 || sub.events[0].type === "server.connected", true);
  await sub.until((e) => e.type === "server.connected");
  const s = await b.call("POST", "/session?directory=/x", { title: "t1" });
  assert.equal(s.status, 200);
  const sid = s.body.id;
  // 新会话用 --session-id，模型参数来自 pi.json，只加载显式扩展
  const args = b.args().at(-1);
  assert.deepEqual(args.slice(args.indexOf("--session-id"), args.indexOf("--session-id") + 2), ["--session-id", sid]);
  for (const flag of ["-ne", "-na", "--offline"]) assert.ok(args.includes(flag), flag);
  assert.ok(args.includes("deepseek-flash"));
  const p = await prompt(b, sid, "hello [tool]");
  assert.equal(p.status, 200);
  const runId = p.body.run_id;
  assert.ok(runId);
  await sub.until(settled(sid, runId));
  assert.equal(textOf(sub.events, sid), "echo: hello [tool] end");
  assert.ok(sub.events.some((e) => e.properties?.event?.type === "tool_execution_end"));
  assert.deepEqual((await b.call("GET", `/session/${sid}/run/${runId}`)).body, { state: "settled" });
  assert.deepEqual((await b.call("GET", "/session/status")).body, {});
  const msgs = await b.call("GET", `/session/${sid}/message`);
  assert.deepEqual(msgs.body.map((m) => m.role), ["user", "assistant"]);
  assert.equal((await b.call("GET", `/session/${sid}/run/nope`)).body.state, "unknown");
  sub.close();
});

test("会话忙时 409，中止后回到空闲", async () => {
  const b = await startBridge();
  const sub = await subscribe(b);
  const sid = (await b.call("POST", "/session", {})).body.id;
  const runId = (await prompt(b, sid, "long [sleep:5]")).body.run_id;
  await sub.until((e) => e.properties?.event?.type === "agent_start");
  assert.deepEqual((await b.call("GET", "/session/status")).body, { [sid]: { type: "busy", run_id: runId } });
  assert.equal((await prompt(b, sid, "again")).status, 409);
  const ab = await b.call("POST", `/session/${sid}/abort`);
  assert.equal(ab.body, true);
  await sub.until(settled(sid, runId));
  assert.equal((await b.call("GET", `/session/${sid}/run/${runId}`)).body.state, "settled");
  const last = (await b.call("GET", `/session/${sid}/message`)).body.at(-1);
  assert.equal(last.stopReason, "error");
  // 空闲时中止：没有运行可中止
  assert.equal((await b.call("POST", `/session/${sid}/abort`)).body, false);
  sub.close();
});

test("运行中 pi 进程崩溃：run 记为 lost 并发 pi.run_lost；下次请求按会话文件重开", async () => {
  const b = await startBridge();
  const sub = await subscribe(b);
  const sid = (await b.call("POST", "/session", {})).body.id;
  await prompt(b, sid, "first");
  await sub.until(settled(sid));
  const runId = (await prompt(b, sid, "boom [crash]")).body.run_id;
  const lost = await sub.until((e) => e.type === "pi.run_lost" && e.properties.sessionID === sid);
  assert.equal(lost.properties.runID, runId);
  assert.equal((await b.call("GET", `/session/${sid}/run/${runId}`)).body.state, "lost");
  assert.deepEqual((await b.call("GET", "/session/status")).body, {});
  const r2 = await prompt(b, sid, "[remember]");
  assert.equal(r2.status, 200);
  await sub.until(settled(sid, r2.body.run_id));
  // 重开用 --session <文件>，上下文还在（崩溃那轮的提问已落盘）
  const args = b.args().at(-1);
  assert.ok(args.includes("--session") && !args.includes("--session-id"));
  assert.match(textOf(sub.events, sid), /remembered: boom \[crash\]/);
  sub.close();
});

test("空闲回收进程，之后按会话文件重开并保留上下文", async () => {
  const b = await startBridge({ PI_BRIDGE_IDLE_S: "0.3" });
  const sub = await subscribe(b);
  const sid = (await b.call("POST", "/session", {})).body.id;
  await prompt(b, sid, "remember me");
  await sub.until(settled(sid));
  for (let i = 0; i < 50 && (await b.call("GET", "/global/health")).body.procs > 0; i += 1) await sleep(100);
  assert.equal((await b.call("GET", "/global/health")).body.procs, 0);
  const r = await prompt(b, sid, "[remember]");
  await sub.until(settled(sid, r.body.run_id));
  assert.match(textOf(sub.events, sid), /remembered: remember me/);
  sub.close();
});

test("重载配置：空闲会话立即重启，运行中的会话不受影响、结束后再重启", async () => {
  const b = await startBridge();
  const sub = await subscribe(b);
  const idle = (await b.call("POST", "/session", {})).body.id;
  const busy = (await b.call("POST", "/session", {})).body.id;
  const runId = (await prompt(b, busy, "work [sleep:1]")).body.run_id;
  await sub.until((e) => e.properties?.sessionID === busy && e.properties?.event?.type === "agent_start");
  fs.writeFileSync(path.join(b.dir, "pi.json"), JSON.stringify({ provider: "deepseek", model: "deepseek-v4-pro", mcp: false }));
  const d = await b.call("POST", "/instance/dispose");
  assert.deepEqual(d.body, { reloaded: [idle], deferred: [busy] });
  await sub.until(settled(busy, runId));
  // 运行中的会话正常结束（没有被中止）
  assert.equal((await b.call("GET", `/session/${busy}/run/${runId}`)).body.state, "settled");
  assert.match(textOf(sub.events, busy), /^echo: work/);
  for (let i = 0; i < 50 && (await b.call("GET", "/global/health")).body.procs > 0; i += 1) await sleep(100);
  assert.equal((await b.call("GET", "/global/health")).body.procs, 0);
  // 新进程用新配置
  const r = await prompt(b, idle, "after reload");
  await sub.until(settled(idle, r.body.run_id));
  assert.ok(b.args().at(-1).includes("deepseek-v4-pro"));
  sub.close();
});

test("扩展的对话框请求自动应答（confirm 回答 false）并上报", async () => {
  const b = await startBridge();
  const sub = await subscribe(b);
  const sid = (await b.call("POST", "/session", {})).body.id;
  const r = await prompt(b, sid, "[ui]");
  await sub.until(settled(sid, r.body.run_id));
  const ui = sub.events.find((e) => e.type === "pi.ui_request");
  assert.equal(ui.properties.method, "confirm");
  assert.equal(ui.properties.answered, true);
  assert.match(textOf(sub.events, sid), /"confirmed":false/);
  sub.close();
});

test("进程数达到上限：优先回收空闲进程，全都在忙时返回 503", async () => {
  const b = await startBridge({ PI_BRIDGE_MAX_PROCS: "1" });
  const sub = await subscribe(b);
  const a = (await b.call("POST", "/session", {})).body.id;
  // a 空闲：建 b 时回收 a 的进程
  const s2 = await b.call("POST", "/session", {});
  assert.equal(s2.status, 200);
  const r = await prompt(b, s2.body.id, "busy [sleep:1]");
  await sub.until((e) => e.properties?.sessionID === s2.body.id && e.properties?.event?.type === "agent_start");
  const full = await prompt(b, a, "needs a process");
  assert.equal(full.status, 503);
  await sub.until(settled(s2.body.id, r.body.run_id));
  const ok = await prompt(b, a, "now ok");
  assert.equal(ok.status, 200);
  await sub.until(settled(a, ok.body.run_id));
  sub.close();
});

test("未知会话 404；桥接进程重启后按会话文件接续，旧 run 查询为 unknown", async () => {
  const b1 = await startBridge();
  assert.equal((await prompt(b1, "00000000-0000-4000-8000-000000000000", "x")).status, 404);
  assert.equal((await prompt(b1, "../../etc/passwd", "x")).status, 404);
  const sub1 = await subscribe(b1);
  const sid = (await b1.call("POST", "/session", {})).body.id;
  const runId = (await prompt(b1, sid, "before restart")).body.run_id;
  await sub1.until(settled(sid, runId));
  sub1.close();
  b1.stop();
  const b2 = await startBridge({}, b1.dir);
  assert.equal((await b2.call("GET", `/session/${sid}/run/${runId}`)).body.state, "unknown");
  const sub2 = await subscribe(b2);
  const r = await prompt(b2, sid, "[remember]");
  assert.equal(r.status, 200);
  await sub2.until(settled(sid, r.body.run_id));
  assert.match(textOf(sub2.events, sid), /remembered: before restart/);
  sub2.close();
});

test("启动时先预热一次 pi（不建会话），之后才监听", async () => {
  const b = await startBridge();
  const [first] = b.args();
  assert.ok(first.includes("--no-session") && !first.includes("--session-id"));
  assert.equal((await b.call("GET", "/global/health")).body.procs, 0);
});

test("启用 MCP 时等元数据缓存生成后才受理第一条消息（直接注册的工具可用）", async () => {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "pi-bridge-test-"));
  const cache = path.join(dir, "mcp-cache.json");
  const b = await startBridge({ PI_MCP_EXTENSION: "/opt/fake-adapter", PI_MCP_CACHE: cache, FAKE_PI_MCP_CACHE: cache }, dir);
  fs.writeFileSync(path.join(dir, "pi.json"), JSON.stringify({ provider: "deepseek", model: "deepseek-flash", mcp: true }));
  const sub = await subscribe(b);
  const sid = (await b.call("POST", "/session", {})).body.id;
  assert.ok(b.args().at(-1).includes("/opt/fake-adapter"));
  const r = await prompt(b, sid, "[cache]");
  await sub.until(settled(sid, r.body.run_id));
  assert.match(textOf(sub.events, sid), /cache=true/);
  sub.close();
});

test("pi 启动失败：建会话返回错误并带上 stderr", async () => {
  const b = await startBridge({ FAKE_PI_FAIL_START: "1" });
  const s = await b.call("POST", "/session", {});
  assert.equal(s.status, 502);
  assert.match(s.body.error, /startup failure/);
  assert.equal((await b.call("GET", "/global/health")).body.sessions, 0);
});
