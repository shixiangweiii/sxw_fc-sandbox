#!/usr/bin/env bash
# 启动本机常驻 agent 服务。操作台：http://127.0.0.1:8000/console（调用方 Key 留空）。
# 加载仓库根目录的 .env。POOL_TARGET_SIZE=0，不预热代码执行池。
# 页面上点「启动沙箱」会创建真实云沙箱并计费；测完点「停止沙箱」。Ctrl-C 只停进程。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

set -a
# shellcheck disable=SC1091
. "$ROOT/.env"
set +a

export POOL_AGENT_ENABLED=true
export POOL_AGENT_MODEL_API_KEY="${FCSANDBOX_OPENCODE_MODEL_API_KEY:-}"
export POOL_AGENT_MODEL="${POOL_AGENT_MODEL:-deepseek/deepseek-flash}"
export POOL_TARGET_SIZE=0
export POOL_AGENT_OPENCODE2_TEMPLATE="${POOL_AGENT_OPENCODE2_TEMPLATE:-towyfj4hujiau0s7fc1z}"
export POOL_AGENT_PI1_TEMPLATE="${POOL_AGENT_PI1_TEMPLATE:-f8708ktxkiytef0jho48}"

if [[ -z "${POOL_AGENT_MODEL_API_KEY}" ]]; then
  echo "FCSANDBOX_OPENCODE_MODEL_API_KEY 为空，无法注入模型 Key" >&2
  exit 1
fi

PYTHON="${PYTHON:-$ROOT/.venv/bin/python}"
PORT="${PORT:-8000}"
echo "操作台: http://127.0.0.1:${PORT}/console"
exec "$PYTHON" -m sandbox_pool --host 127.0.0.1 --port "$PORT"
