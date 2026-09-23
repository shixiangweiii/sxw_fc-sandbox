#!/usr/bin/env bash
# 本地模拟多副本：多个进程共享同一个 SQLite 文件。
#   scripts/run_local_cluster.sh start [端口...]   默认 8001 8002 8003
#   scripts/run_local_cluster.sh stop
#   scripts/run_local_cluster.sh status
# 需要先导出 E2B_API_KEY / E2B_API_URL / E2B_DOMAIN 与 POOL_TEMPLATE 等配置。
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
DATA="$ROOT/.data"
PIDS="$DATA/cluster.pids"
PYTHON="${PYTHON:-python3}"
export POOL_DB_URL="${POOL_DB_URL:-sqlite+aiosqlite:///$DATA/pool.db}"

cmd="${1:-start}"
shift || true

case "$cmd" in
  start)
    mkdir -p "$DATA"
    ports=("$@")
    [ ${#ports[@]} -eq 0 ] && ports=(8001 8002 8003)
    : > "$PIDS"
    cd "$ROOT"
    for port in "${ports[@]}"; do
      # 不经子 shell 直接后台启动，$! 即服务进程本身的 PID（setsid / nohup 都会 exec）
      setsid nohup "$PYTHON" -m sandbox_pool --port "$port" < /dev/null > "$DATA/replica-$port.log" 2>&1 &
      echo "$port $!" >> "$PIDS"
    done
    sleep 1
    for port in "${ports[@]}"; do
      for _ in $(seq 1 50); do
        curl -sf "http://127.0.0.1:$port/healthz" > /dev/null && break
        sleep 0.2
      done
      echo "replica :$port -> $(curl -sf "http://127.0.0.1:$port/healthz" || echo 'NOT READY')"
    done
    ;;
  stop)
    [ -f "$PIDS" ] || { echo "no cluster"; exit 0; }
    while read -r port pid; do
      kill "$pid" 2>/dev/null && echo "stopping :$port (pid $pid)" || true
    done < "$PIDS"
    # 等待优雅停止（维护循环跑完当前一轮、后台操作结束）
    for _ in $(seq 1 60); do
      alive=0
      while read -r _port pid; do kill -0 "$pid" 2>/dev/null && alive=1; done < "$PIDS"
      [ "$alive" = 0 ] && break
      sleep 1
    done
    rm -f "$PIDS"
    ;;
  status)
    [ -f "$PIDS" ] || { echo "no cluster"; exit 0; }
    while read -r port pid; do
      if kill -0 "$pid" 2>/dev/null; then echo ":$port pid=$pid alive"; else echo ":$port pid=$pid dead"; fi
    done < "$PIDS"
    ;;
  *)
    echo "usage: $0 start|stop|status [ports...]" >&2
    exit 1
    ;;
esac
