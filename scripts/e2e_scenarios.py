"""针对本地多副本集群 + 真实云沙箱的端到端场景验证。

前置：scripts/run_local_cluster.sh start（默认 3 个副本 8001~8003）。
    python scripts/e2e_scenarios.py [--urls http://127.0.0.1:8001,...] [--kill-port 8003]

场景：
  1. 预热：池子补足 5 个并在空闲后全部暂停
  2. 突发：同时发 16 个借用请求 → 5 个立即拿到、10 个排队、1 个 429
  3. 执行：在借到的沙箱上跑 run_code / commands / files，并验证沙箱之间互相隔离
  4. 排队：逐个归还，排队请求依次拿到新沙箱
  5. 504：池满且无人归还时，等待超时返回 504
  6. 副本崩溃：通过某副本归还后立刻 kill -9 该副本，其余副本接管并把池子恢复到 5 个
  7. 输出 /v1/pool/stats 统计
"""

import argparse
import asyncio
import os
import signal
import statistics
import sys
import time
from pathlib import Path

import httpx

ROOT = Path(__file__).resolve().parent.parent
TARGET = 5


def log(msg: str) -> None:
    print(f"[{time.strftime('%H:%M:%S')}] {msg}", flush=True)


class Cluster:
    def __init__(self, urls: list[str]):
        self.urls = urls
        self.client = httpx.AsyncClient(timeout=httpx.Timeout(240.0))
        self._i = 0

    def next_url(self) -> str:
        self._i += 1
        return self.urls[self._i % len(self.urls)]

    async def stats(self, url: str | None = None) -> dict:
        r = await self.client.get(f"{url or self.urls[0]}/v1/pool/stats")
        r.raise_for_status()
        return r.json()

    async def acquire(self, url: str | None = None, **body) -> tuple[int, dict, float, str]:
        url = url or self.next_url()
        t0 = time.perf_counter()
        r = await self.client.post(f"{url}/v1/leases", json=body)
        return r.status_code, r.json(), time.perf_counter() - t0, url

    async def release(self, lease_id: str, url: str | None = None) -> dict:
        r = await self.client.delete(f"{url or self.next_url()}/v1/leases/{lease_id}")
        r.raise_for_status()
        return r.json()


async def wait_for(pred, timeout: float, interval: float = 2.0, what: str = ""):
    deadline = time.monotonic() + timeout
    while True:
        value = await pred()
        if value:
            return value
        if time.monotonic() > deadline:
            raise AssertionError(f"timeout waiting for {what}")
        await asyncio.sleep(interval)


def check(cond: bool, msg: str) -> None:
    if not cond:
        raise AssertionError(msg)
    log(f"  ✓ {msg}")


async def scenario_warm(c: Cluster) -> None:
    log("场景 1：预热 5 个并在空闲后全部暂停")
    t0 = time.monotonic()

    async def all_paused():
        s = await c.stats()
        sb = s["sandboxes"]
        log(f"  sandboxes={sb}")
        return sb.get("PAUSED", 0) == TARGET and sb.get("total") == TARGET

    await wait_for(all_paused, timeout=300, interval=5, what="5 paused sandboxes")
    check(True, f"5 个沙箱全部进入 PAUSED（{time.monotonic() - t0:.0f}s）")


async def scenario_burst(c: Cluster) -> tuple[list[dict], list[asyncio.Task]]:
    log("场景 2：同时发 16 个借用请求")
    tasks = [asyncio.create_task(c.acquire(wait_timeout_s=180)) for _ in range(16)]
    done: list = []
    # 立即返回的：5 个成功 + 1 个 429
    deadline = time.monotonic() + 60
    while time.monotonic() < deadline:
        done = [t for t in tasks if t.done()]
        if len(done) >= 6:
            break
        await asyncio.sleep(0.2)
    results = [t.result() for t in done]
    ok = [r for r in results if r[0] == 200]
    full = [r for r in results if r[0] == 429]
    check(len(ok) == TARGET, f"5 个请求立即拿到沙箱（来源：{sorted(r[1]['source'] for r in ok)}，"
          f"耗时 {[round(r[2], 2) for r in ok]}s）")
    check(len(full) == 1, "1 个请求因队列已满返回 429")
    s = await c.stats()
    check(s["waiting"] == 10, "10 个请求在排队")
    pending = [t for t in tasks if not t.done()]
    return [r[1] for r in ok], pending


async def scenario_exec(c: Cluster, leases: list[dict]) -> None:
    log("场景 3：代为执行 run_code / commands / files，并验证隔离")
    for i, lease in enumerate(leases):
        base = f"{c.next_url()}/v1/leases/{lease['lease_id']}"
        t0 = time.perf_counter()
        r = await c.client.post(f"{base}/run_code", json={"code": f"import pandas as pd\nx = {i}\nprint(pd.Series([1,2,3]).sum() + x)"})
        r.raise_for_status()
        out = r.json()
        check(out["stdout"].strip() == str(6 + i) and out["error"] is None,
              f"lease#{i} run_code 输出 {out['stdout'].strip()}（{time.perf_counter() - t0:.2f}s）")
        r = await c.client.post(f"{c.next_url()}/v1/leases/{lease['lease_id']}/run_code", json={"code": "print(x)"})
        check(r.json()["stdout"].strip() == str(i), f"lease#{i} 解释器状态在同一借用内保持（跨副本调用）")
        r = await c.client.post(f"{base}/commands", json={"cmd": "ls /tmp/marker-* 2>/dev/null | wc -l; exit 7"})
        out = r.json()
        check(out["exit_code"] == 7 and out["stdout"].strip() == "0", f"lease#{i} commands 返回退出码 7，且看不到其他借用的文件")
        payload = os.urandom(64)
        r = await c.client.put(f"{base}/files", params={"path": f"/tmp/marker-{i}"}, content=payload)
        r.raise_for_status()
        r = await c.client.get(f"{base}/files", params={"path": f"/tmp/marker-{i}"})
        check(r.content == payload, f"lease#{i} 文件写入/读取 64 字节一致")


async def scenario_queue(c: Cluster, leases: list[dict], pending: list[asyncio.Task]) -> list[dict]:
    log("场景 4：逐个归还，排队请求依次拿到沙箱")
    held = list(leases)
    served: list[dict] = []
    waits: list[float] = []
    for n in range(10):
        lease = held.pop(0)
        t0 = time.perf_counter()
        await c.release(lease["lease_id"])
        done_before = sum(1 for t in pending if t.done())
        while sum(1 for t in pending if t.done()) == done_before:
            await asyncio.sleep(0.1)
            if time.perf_counter() - t0 > 120:
                raise AssertionError("queued request not served in 120s")
        finished = [t for t in pending if t.done() and t.result()[1]["lease_id"] not in {s["lease_id"] for s in served}]
        for t in finished:
            code, body, _, _ = t.result()
            assert code == 200, (code, body)
            served.append(body)
            held.append(body)
        waits.append(time.perf_counter() - t0)
        log(f"  归还第 {n + 1} 个后 {waits[-1]:.1f}s 内有排队请求拿到沙箱（source={served[-1]['source']}）")
    check(len(served) == 10, f"10 个排队请求全部拿到沙箱，归还→拿到 p50={statistics.median(waits):.1f}s max={max(waits):.1f}s")
    s = await c.stats()
    check(s["waiting"] == 0 and s["active_leases"] == TARGET, "队列清空，5 个借用进行中")
    return held


async def scenario_timeout(c: Cluster) -> None:
    log("场景 5：池满且无人归还时等待超时")
    code, body, dt, _ = await c.acquire(wait_timeout_s=5)
    check(code == 504, f"返回 504（{dt:.1f}s）：{body.get('detail')}")


async def scenario_kill_replica(c: Cluster, held: list[dict], kill_port: int) -> None:
    log(f"场景 6：通过 :{kill_port} 归还全部借用后立刻 kill -9 该副本")
    victim = next(u for u in c.urls if u.endswith(f":{kill_port}"))
    survivors = [u for u in c.urls if u != victim]
    pid = None
    for line in (ROOT / ".data" / "cluster.pids").read_text().split("\n"):
        if line.startswith(f"{kill_port} "):
            pid = int(line.split()[1])
    assert pid, "victim pid not found"
    await asyncio.gather(*[c.release(l["lease_id"], url=victim) for l in held])
    os.kill(pid, signal.SIGKILL)
    log(f"  已 kill -9 pid={pid}")
    c.urls = survivors

    async def healed():
        s = await c.stats()
        sb = s["sandboxes"]
        log(f"  sandboxes={sb}")
        return sb.get("total") == TARGET and all(k in ("READY", "PAUSED", "total") for k in sb)

    t0 = time.monotonic()
    await wait_for(healed, timeout=400, interval=10, what="pool healed")
    check(True, f"其余副本接管，池子恢复到 5 个稳定状态（{time.monotonic() - t0:.0f}s）")
    code, body, dt, _ = await c.acquire(wait_timeout_s=60)
    check(code == 200, f"崩溃后仍可借用（{dt:.1f}s，source={body.get('source')}）")
    await c.release(body["lease_id"])


def _pid_of(port: int) -> int:
    for line in (ROOT / ".data" / "cluster.pids").read_text().split("\n"):
        if line.startswith(f"{port} "):
            return int(line.split()[1])
    raise AssertionError(f"pid of :{port} not found")


async def scenario_kill_during_pause(c: Cluster) -> None:
    log("场景 6b：某副本正在暂停沙箱（过渡态 PAUSING）时 kill -9，其余副本在 op_deadline 后接管")
    replicas = {}
    for u in c.urls:
        replicas[(await c.client.get(f"{u}/healthz")).json()["replica"]] = u

    async def pausing_owner():
        rows = (await c.client.get(f"{c.urls[0]}/v1/sandboxes")).json()
        owners = {r["op_owner"] for r in rows if r["state"] == "PAUSING" and r["op_owner"] in replicas}
        return next(iter(owners), None)

    owner = await wait_for(pausing_owner, timeout=300, interval=0.5, what="a replica pausing sandboxes")
    victim = replicas[owner]
    port = int(victim.rsplit(":", 1)[1])
    rows = (await c.client.get(f"{c.urls[0]}/v1/sandboxes")).json()
    stuck = [r["id"] for r in rows if r["op_owner"] == owner]
    os.kill(_pid_of(port), signal.SIGKILL)
    log(f"  已 kill -9 :{port}，它名下有 {len(stuck)} 个过渡态沙箱")
    c.urls = [u for u in c.urls if u != victim]

    async def healed():
        rows = (await c.client.get(f"{c.urls[0]}/v1/sandboxes")).json()
        states = sorted(r["state"] for r in rows)
        log(f"  states={states}")
        return len(rows) == TARGET and not ({r["id"] for r in rows} & set(stuck)) and set(states) <= {"READY", "PAUSED"}

    t0 = time.monotonic()
    await wait_for(healed, timeout=400, interval=10, what="takeover of stuck rows")
    check(True, f"卡在过渡态的沙箱被接管销毁并补齐到 5 个（{time.monotonic() - t0:.0f}s）")
    code, body, dt, _ = await c.acquire(wait_timeout_s=60)
    check(code == 200, f"接管后仍可借用（{dt:.1f}s，source={body.get('source')}）")
    await c.release(body["lease_id"])


async def scenario_no_orphans(c: Cluster) -> None:
    log("场景 8：云端沙箱与池记录一致（无孤儿）")
    from sandbox_pool.provider.e2b_provider import E2BProvider

    provider = E2BProvider()

    async def consistent():
        rows = (await c.client.get(f"{c.urls[0]}/v1/sandboxes")).json()
        items = await provider.list({"pool": "default"})
        ids_db = {r["provider_id"] for r in rows}
        ids_cloud = {i.sandbox_id for i in items}
        log(f"  db={len(ids_db)} cloud={len(ids_cloud)}")
        return ids_db == ids_cloud

    await wait_for(consistent, timeout=180, interval=10, what="db/cloud consistency")
    check(True, "库中记录与云端沙箱一一对应")


async def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--urls", default="http://127.0.0.1:8001,http://127.0.0.1:8002,http://127.0.0.1:8003")
    parser.add_argument("--kill-port", type=int, default=8003)
    parser.add_argument("--only", help="只跑指定场景：kill-during-pause")
    args = parser.parse_args()
    c = Cluster(args.urls.split(","))
    try:
        if args.only == "kill-during-pause":
            await scenario_kill_during_pause(c)
            await scenario_no_orphans(c)
            return 0
        await scenario_warm(c)
        leases, pending = await scenario_burst(c)
        await scenario_exec(c, leases)
        held = await scenario_queue(c, leases, pending)
        await scenario_timeout(c)
        await scenario_kill_replica(c, held, args.kill_port)
        await scenario_kill_during_pause(c)
        await scenario_no_orphans(c)
        stats = await c.stats()
        log("场景 7：统计")
        log(f"  lease_sources={stats['lease_sources']}")
        log(f"  events={stats['events']}")
        for kind, v in stats["latency_ms"].items():
            log(f"  {kind:<16} count={v['count']:<4} p50={v['p50']}ms p99={v['p99']}ms max={v['max']}ms")
        log("全部场景通过")
        return 0
    finally:
        await c.client.aclose()


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
