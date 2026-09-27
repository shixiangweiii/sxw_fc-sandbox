#!/usr/bin/env node
// 假的 `pi --mode rpc`：只实现桥接进程用到的命令，事件结构与真实 pi 0.87 一致（本机真实 DeepSeek 实测）。
//
// 提示词指令（可组合）：
//   [sleep:秒]  运行这么久（可被中止）      [tool]   调一次 bash 工具
//   [error]     模型报错（stopReason=error）[crash]  运行中进程直接退出
//   [ui]        发一次 confirm 请求，等应答  [remember] 回复同一会话上一轮的提问
//   [cache]     回复启动时 MCP 元数据缓存是否已存在（带 -e 加载扩展时，FAKE_PI_MCP_CACHE 在 0.3 秒后生成）
// 其余回复 "echo: <提示词>"，文本里带一个 U+2028（验证桥接进程按 LF 分帧）。
// 环境变量：FAKE_PI_ARGS_LOG（把启动参数追加到这个文件）、FAKE_PI_FAIL_START=1（启动即退出）。

import fs from "node:fs";
import path from "node:path";

if (process.env.FAKE_PI_FAIL_START === "1") {
  process.stderr.write("fake pi: startup failure\n");
  process.exit(2);
}
const argv = process.argv.slice(2);
if (process.env.FAKE_PI_ARGS_LOG) fs.appendFileSync(process.env.FAKE_PI_ARGS_LOG, `${JSON.stringify(argv)}\n`);
const opt = (name) => {
  const i = argv.indexOf(name);
  return i >= 0 ? argv[i + 1] : undefined;
};
const sessionDir = opt("--session-dir");
let sessionFile = opt("--session") || null;
const sessionId = sessionFile ? path.basename(sessionFile).split("_").slice(1).join("_").replace(/\.jsonl$/, "") : opt("--session-id");
let messages = [];
if (sessionFile) {
  messages = fs.readFileSync(sessionFile, "utf8").split("\n").filter(Boolean).map((l) => JSON.parse(l));
}
let name = "";
// 模拟 pi-mcp-adapter：加载扩展后稍晚才写出元数据缓存
if (argv.includes("-e") && process.env.FAKE_PI_MCP_CACHE) {
  setTimeout(() => fs.writeFileSync(process.env.FAKE_PI_MCP_CACHE, "{}"), 300);
}
let run = null; // {abort: () => void, done: Promise}
const uiWaiters = new Map();

const out = (rec) => process.stdout.write(`${JSON.stringify(rec)}\n`);
const respond = (cmd, extra) => out({ id: cmd.id, type: "response", command: cmd.type, success: true, ...extra });
const usage = (i, o) => ({ input: i, output: o, cacheRead: 100, cacheWrite: 0, reasoning: 0, totalTokens: i + o + 100,
  cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0.001 } });

function persist(msg) {
  messages.push(msg);
  if (!sessionFile) {
    sessionFile = path.join(sessionDir, `${new Date().toISOString().replace(/[:.]/g, "-")}_${sessionId}.jsonl`);
  }
  fs.appendFileSync(sessionFile, `${JSON.stringify(msg)}\n`);
}

const sleep = (ms, signal) => new Promise((resolve, reject) => {
  const t = setTimeout(resolve, ms);
  signal.addEventListener("abort", () => {
    clearTimeout(t);
    reject(new Error("aborted"));
  });
});

async function doRun(text, signal) {
  out({ type: "agent_start" });
  out({ type: "turn_start" });
  const user = { role: "user", content: text, timestamp: Date.now() };
  persist(user);
  out({ type: "message_end", message: user });
  let reply;
  let stopReason = "stop";
  let errorMessage;
  try {
    if (text.includes("[tool]")) {
      out({ type: "tool_execution_start", toolCallId: "call_1", toolName: "bash", args: { command: "uname -m" } });
      await sleep(20, signal);
      out({ type: "tool_execution_end", toolCallId: "call_1", toolName: "bash", result: { content: [{ type: "text", text: "x86_64\n" }] }, isError: false });
    }
    if (text.includes("[ui]")) {
      const id = `ui-${Date.now()}`;
      const answered = new Promise((r) => uiWaiters.set(id, r));
      out({ type: "extension_ui_request", id, method: "confirm", title: "Allow?", message: "fake confirm" });
      const ans = await answered;
      reply = `ui answered: ${JSON.stringify(ans)}`;
    }
    const m = text.match(/\[sleep:([\d.]+)\]/);
    if (m) await sleep(Number(m[1]) * 1000, signal);
    if (text.includes("[crash]")) process.exit(3);
    if (text.includes("[error]")) {
      stopReason = "error";
      errorMessage = "fake model error";
      reply = "";
    } else if (text.includes("[cache]")) {
      reply = `cache=${fs.existsSync(process.env.FAKE_PI_MCP_CACHE || "/nonexistent")}`;
    } else if (text.includes("[remember]")) {
      const prev = messages.filter((x) => x.role === "user").slice(-2, -1)[0];
      reply = `remembered: ${prev ? prev.content : "(nothing)"}`;
    } else if (!reply) {
      reply = `echo: ${text}\u2028end`;
    }
  } catch {
    stopReason = "error";
    errorMessage = "This operation was aborted";
    reply = "";
  }
  if (reply) {
    for (let i = 0; i < reply.length; i += 5) {
      out({ type: "message_update", usage: usage(0, 0), assistantMessageEvent: { type: "text_delta", contentIndex: 0, delta: reply.slice(i, i + 5) } });
    }
  }
  const assistant = { role: "assistant", content: reply ? [{ type: "text", text: reply }] : [], usage: usage(text.length, reply.length),
    stopReason, ...(errorMessage ? { errorMessage } : {}), timestamp: Date.now() };
  persist(assistant);
  out({ type: "message_end", message: assistant });
  out({ type: "turn_end", message: assistant, toolResults: [] });
  out({ type: "agent_end", messages: [user, assistant], willRetry: false });
  out({ type: "agent_settled" });
}

async function handle(cmd) {
  switch (cmd.type) {
    case "get_state":
      return respond(cmd, { data: { sessionId, sessionFile, sessionName: name, isStreaming: Boolean(run), messageCount: messages.length } });
    case "set_session_name":
      name = cmd.name;
      return respond(cmd);
    case "get_messages":
      return respond(cmd, { data: { messages } });
    case "prompt": {
      if (run) return out({ id: cmd.id, type: "response", command: "prompt", success: false, error: "Agent is already processing" });
      const ctrl = new AbortController();
      respond(cmd, { data: { disposition: "started" } });
      run = { ctrl, done: doRun(cmd.message, ctrl.signal).finally(() => { run = null; }) };
      return undefined;
    }
    case "abort": {
      if (run) {
        run.ctrl.abort();
        await run.done;
      }
      return respond(cmd);
    }
    default:
      return out({ id: cmd.id, type: "response", command: cmd.type, success: false, error: `unknown command ${cmd.type}` });
  }
}

let buf = "";
process.stdin.on("data", (chunk) => {
  buf += chunk.toString("utf8");
  let i;
  while ((i = buf.indexOf("\n")) >= 0) {
    const line = buf.slice(0, i);
    buf = buf.slice(i + 1);
    if (!line.trim()) continue;
    const rec = JSON.parse(line);
    if (rec.type === "extension_ui_response") {
      uiWaiters.get(rec.id)?.(rec);
      uiWaiters.delete(rec.id);
    } else {
      handle(rec);
    }
  }
});
process.stdin.on("end", () => process.exit(0));
