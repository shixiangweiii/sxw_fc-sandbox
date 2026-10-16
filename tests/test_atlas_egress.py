"""阶段 1（为 Atlas 集成加的）：借用级出网策略、入站关闭、输出上限、超时保留部分输出。

【定】每条判据都配一个**反向**。本仓已经栽过四次「判据落在守卫重叠区」，
形态都一样：一个恒真的实现也能让判据全绿。这里的配对分别排除：

    「恒传 deny-all」        ← A2 用 egress_default_deny=False 让它必须不传
    「从不核对回显」        ← B2 用 echo_wrong_network 让它必须拒绝
    「一律拒绝交付」        ← B1 要求正常情况下必须交付成功
    「恒报 truncated」      ← C2 要求没超限时必须为 False
    「超时一律空输出」      ← D1 要求交回已收到的那段

真实云端复核见 scripts/probe_code_pool_network.py（出网边界、入站裸奔）。
"""

import json

import pytest

from sandbox_pool.egress import DENY_ALL, EgressPolicy, policy_network
from sandbox_pool.models import SandboxOpError
from tests.conftest import wait_until

# ---------------- A. 默认断网 ＋ 关公开访问 ----------------


async def test_a1_pool_sandboxes_are_created_offline_and_without_public_ingress(make_pool, provider):
    """躺在池子里等人借的沙箱：建的时候就断网，且公网直达关掉。"""
    await make_pool(target_size=1, max_size=1)
    await wait_until(lambda: len(provider.created_with) >= 1)
    got = provider.created_with[0]
    assert got["secure"] is True, "必须 secure=True —— 平台要求它才认 allow_public_traffic=False"
    assert got["network"] == policy_network(DENY_ALL)
    assert got["network"]["deny_out"] == ["0.0.0.0/0"]
    assert got["network"]["allow_out"] == []


async def test_a2_create_without_network_when_default_deny_is_off(make_pool, provider):
    """A1 的配对：关掉默认断网时**不得**再传 network。

    少了这一条，一个「无论配置怎样都传 deny-all」的实现照样能让 A1 绿。
    """
    await make_pool(target_size=1, max_size=1, egress_default_deny=False, block_public_ingress=False)
    await wait_until(lambda: len(provider.created_with) >= 1)
    got = provider.created_with[0]
    assert got["network"] is None
    assert got["secure"] is False


# ---------------- B. 借用级策略 ＋ 读回核对（第二道闸门）----------------


async def test_b1_lease_applies_allowlist_and_records_platform_echo(make_pool, provider):
    """借用方要域名白名单：下发、回显核对通过、两份都记进借用记录。"""
    pool = await make_pool(target_size=1, max_size=1)
    await wait_until(lambda: sum(1 for v in provider.sandboxes.values() if v["state"] == "running") >= 1)
    grant = await pool.allocator.acquire(egress=EgressPolicy(mode="allowlist", allow_out=("pypi.org",)))
    row = await pool.allocator.get_lease(grant.lease_id)

    desired = json.loads(row["egress"])
    assert desired["allow_out"] == ["pypi.org"]
    assert desired["deny_out"] == ["0.0.0.0/0"], "白名单模式下 deny 一律 0.0.0.0/0"
    platform = json.loads(row["egress_platform"])
    assert set(platform["allow_out"]) == {"pypi.org"}
    # 交付成功本身就是 B2 的配对：少了它，「一律拒绝」的实现也能让 B2 绿
    assert grant.lease_id


async def test_b2_mismatching_platform_echo_fails_the_lease(make_pool, provider):
    """平台回显与下发的不一致 → **不把沙箱交出去**。

    这是第二道闸门。借用方拿「断网」当安全边界用，而撑着它的只有
    「池子说它下发了」这一句话 —— update_network 返回成功只说明请求被接受了。
    """
    pool = await make_pool(target_size=1, max_size=1)
    await wait_until(lambda: sum(1 for v in provider.sandboxes.values() if v["state"] == "running") >= 1)
    # 平台谎称「公网全放行」，而我们要的是断网
    provider.echo_wrong_network = {"allow_out": ["0.0.0.0/0"], "deny_out": []}
    with pytest.raises(SandboxOpError) as e:
        await pool.allocator.acquire(egress=DENY_ALL)
    assert "egress" in str(e.value).lower()
    # 借用必须已经结束，不能留一个「拿不到但占着容量」的 ACTIVE 记录
    leases = await pool.store.list_leases(["ACTIVE"])
    assert leases == []
    assert (await pool.stats())["events"].get("egress_mismatch") == 1


async def test_b3_unreadable_echo_is_not_a_pass(make_pool, provider):
    """读不到回显 → 算不匹配。「我无法核对」不等于「通过」。"""
    pool = await make_pool(target_size=1, max_size=1)
    await wait_until(lambda: sum(1 for v in provider.sandboxes.values() if v["state"] == "running") >= 1)
    provider.echo_wrong_network = {}
    with pytest.raises(SandboxOpError):
        await pool.allocator.acquire(egress=DENY_ALL)


async def test_b4_illegal_policy_is_rejected_before_any_sandbox_is_touched(make_pool, provider):
    """非法放行项（与强制屏蔽段重叠）→ 解析期就拒，不占用任何资源。"""
    from sandbox_pool.egress import parse_policy
    from sandbox_pool.models import InvalidRequest

    await make_pool(target_size=1, max_size=1)
    with pytest.raises(InvalidRequest):
        parse_policy({"mode": "allowlist", "allow_out": ["10.0.0.0/8"]}, DENY_ALL)
    # 开放模式下域名放行项无意义，也要拒
    with pytest.raises(InvalidRequest):
        parse_policy({"mode": "open", "allow_out": ["pypi.org"]}, DENY_ALL)


# ---------------- C. 输出上限 ----------------


async def test_c1_oversized_output_is_truncated_and_says_so(make_pool, provider):
    """超过上限：截断，但**如实报告**真实字节数。"""
    pool = await make_pool(target_size=1, max_size=1, max_output_bytes=100)
    await wait_until(lambda: sum(1 for v in provider.sandboxes.values() if v["state"] == "running") >= 1)
    grant = await pool.allocator.acquire()
    provider.next_output_bytes = 5000
    r = await pool.allocator.run_command(grant.lease_id, "big", cwd=None, envs=None, timeout_s=5)
    assert r.truncated is True
    assert len(r.stdout.encode()) <= 100
    assert r.stdout_bytes == 5000, "真实字节数必须是截断前的那个数"


async def test_c2_small_output_is_not_marked_truncated(make_pool, provider):
    """C1 的配对：没超限时 truncated 必须是 False，否则「恒报截断」也能绿。"""
    pool = await make_pool(target_size=1, max_size=1, max_output_bytes=100)
    await wait_until(lambda: sum(1 for v in provider.sandboxes.values() if v["state"] == "running") >= 1)
    grant = await pool.allocator.acquire()
    r = await pool.allocator.run_command(grant.lease_id, "hi", cwd=None, envs=None, timeout_s=5)
    assert r.truncated is False
    assert r.stdout_bytes == len(r.stdout.encode())


# ---------------- D. 超时保留已产出的输出 ----------------


async def test_d1_timeout_returns_the_output_produced_so_far(make_pool, provider):
    """超时也要交回已经收到的那段 —— 原因通常就写在那些行里。

    扔掉它的后果：「超时」与「什么都没发生」在调用方那里不可区分。
    """
    pool = await make_pool(target_size=1, max_size=1)
    await wait_until(lambda: sum(1 for v in provider.sandboxes.values() if v["state"] == "running") >= 1)
    grant = await pool.allocator.acquire()
    provider.next_timeout_partial = "step 1 ok\nstep 2 ok\ncompiling...\n"
    r = await pool.allocator.run_command(grant.lease_id, "slow", cwd=None, envs=None, timeout_s=1)
    assert r.exit_code == -1
    assert r.error.startswith("TimeoutError")
    assert "compiling" in r.stdout, "超时前已经打印出来的行必须交回去"
    assert (await pool.stats())["events"].get("exec_timeout") == 1
