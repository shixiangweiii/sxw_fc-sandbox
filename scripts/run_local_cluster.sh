#!/usr/bin/env bash
# 本地模拟多副本：多个进程共享同一个 SQLite 文件。
#   scripts/run_local_cluster.sh start [端口...]   默认 8001 8002 8003
#   scripts/run_local_cluster.sh stop
#   scripts/run_local_cluster.sh status
#   scripts/run_local_cluster.sh drain [端口]      排空：销毁空闲和已暂停的沙箱、停止补货（停集群前执行，避免沙箱留在云端）
# 需要先导出 E2B_API_KEY / E2B_API_URL / E2B_DOMAIN 与 POOL_TEMPLATE 等配置。
# 开启鉴权（POOL_API_KEYS / POOL_ADMIN_KEYS）时，drain 用 SANDBOX_POOL_API_KEY（管理员 key）调用管理接口。
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
  drain)
    port="${1:-8001}"
    auth=()
    [ -n "${SANDBOX_POOL_API_KEY:-}" ] && auth=(-H "Authorization: Bearer $SANDBOX_POOL_API_KEY")
    curl -sSf -X POST ${auth[@]+"${auth[@]}"} "http://127.0.0.1:$port/v1/admin/drain" || { echo "drain request failed" >&2; exit 1; }
    echo
    # 等空闲和已暂停的沙箱销毁完；借出中的沙箱在归还或过期后销毁
    for _ in $(seq 1 60); do
      left=$(curl -sSf ${auth[@]+"${auth[@]}"} "http://127.0.0.1:$port/v1/pool/stats" \
        | "$PYTHON" -c 'import json,sys; s=json.load(sys.stdin)["sandboxes"]; print(s.get("total",0)-s.get("LEASED",0))')
      [ "$left" = "0" ] && break
      sleep 2
    done
    curl -sSf ${auth[@]+"${auth[@]}"} "http://127.0.0.1:$port/v1/pool/stats" \
      | "$PYTHON" -c 'import json,sys; s=json.load(sys.stdin); print("draining=%s sandboxes=%s" % (s["draining"], s["sandboxes"]))'
    ;;
  *)
    echo "usage: $0 start|stop|status|drain [ports...]" >&2
    exit 1
    ;;
esac
