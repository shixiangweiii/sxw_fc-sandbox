"""内存版沙箱后端，仅用于测试。多个池实例可共享同一个 FakeProvider，模拟共享的云端。

模拟平台超时：运行中的沙箱超过 create / set_timeout / resume 设置的超时即被回收（之后访问报 SandboxNotFound），
暂停中的沙箱不计时。
"""

import asyncio
import contextlib
import io
import json
import time
import uuid
from typing import Optional

from sandbox_pool.provider.base import (
    AppSandbox,
    CodeResult,
    CommandResult,
    ExecutionTimeout,
    ProviderSandbox,
    SandboxNotFound,
    cap_text,
)
from sandbox_pool.provider.fake_opencode import FakeOpencodeClient, FakeOpencodeServer
from sandbox_pool.provider.fake_pi import FakePiClient, FakePiServer


class FakeProvider:
    def __init__(self, *, latency_s: float = 0.0, pause_latency_s: Optional[float] = None):
        self.latency_s = latency_s
        self.pause_latency_s = latency_s if pause_latency_s is None else pause_latency_s
        self.kill_latency_s = 0.0
        self.sandboxes: dict[str, dict] = {}
        self.calls: list[tuple[str, str]] = []
        # 失败注入：剩余失败次数 / 指定沙箱失败
        self.fail_create = 0
        self.fail_resume_ids: set[str] = set()
        self.fail_warmup = 0
        self.fail_set_timeout = 0
        self.fail_kill = 0
        # 依次作用于之后的 set_timeout 调用：先等待再生效，用于构造调用到达平台的先后顺序
        self.set_timeout_delays: list[float] = []
        # 之后的 run_code / run_command 按剩余次数抛 ExecutionTimeout（模拟用户代码执行超时）
        self.exec_timeouts = 0
        # agent 子系统：失败注入与调用记录
        self.fail_create_app = 0
        self.fail_update_network = 0
        # 每次 create 实际收到的 {network, secure}
        self.created_with: list[dict] = []
        # 注入超大输出：下一次 run_command 产出这么多字节（判据用，验截断与真实字节数）
        self.next_output_bytes = 0
        # 注入「超时但已经产出了一段」：下一次 run_command 先产出这段再超时
        self.next_timeout_partial: str = ""
        # update_network 之后回显要不要故意写错（判据：Atlas 读回核对那一道）
        self.echo_wrong_network: Optional[dict] = None
        self.network_updates: list[tuple[str, dict]] = []
        # 这些模板建出的 app 沙箱里跑 pi 桥接进程，其余跑 opencode
        self.pi_templates: set[str] = {"tpl-pi"}

    def _sweep(self) -> None:
        now = time.time()
        for sid in [s for s, sb in self.sandboxes.items() if sb["state"] == "running" and sb["deadline"] <= now]:
            self._drop(sid)
            self.calls.append(("expired", sid))

    def _drop(self, sandbox_id: str) -> bool:
        sb = self.sandboxes.pop(sandbox_id, None)
        for kind in ("opencode", "pi"):
            if sb is not None and sb.get(kind) is not None:
                sb[kind].shutdown()
        return sb is not None

    def _get(self, sandbox_id: str) -> dict:
        self._sweep()
        sb = self.sandboxes.get(sandbox_id)
        if sb is None:
            raise SandboxNotFound(sandbox_id)
        return sb

    def _running(self, sandbox_id: str) -> dict:
        sb = self._get(sandbox_id)
        if sb["state"] != "running":
            raise RuntimeError(f"sandbox {sandbox_id} is {sb['state']}")
        return sb

    async def _sleep(self, s: float) -> None:
        if s:
            await asyncio.sleep(s)

    def alive_count(self) -> int:
        self._sweep()
        return len(self.sandboxes)

    async def create(self, template: str, metadata: dict[str, str], timeout_s: float,
                     *, network=None, secure: bool = False) -> str:
        self.calls.append(("create", template))
        # 记下建沙箱时实际传进来的出网 / 入站设置：判据要能区分
        # 「池子说它默认断网」和「池子真的传了断网规则」
        self.created_with.append({"network": dict(network) if network else None, "secure": secure})
        await self._sleep(self.latency_s)
        if self.fail_create > 0:
            self.fail_create -= 1
            raise RuntimeError("injected create failure")
        sid = f"fake-{uuid.uuid4().hex[:12]}"
        self.sandboxes[sid] = dict(
            state="running",
            network=({**network, "allow_public_traffic": False} if secure else dict(network or {})) or None,
            secure=secure,
            metadata=dict(metadata),
            started_at=time.time(),
            ns={},
            files={},
            timeout=timeout_s,
            deadline=time.time() + timeout_s,
        )
        return sid

    async def warmup(self, sandbox_id: str, code: str, *, sandbox_timeout_s: float) -> None:
        self.calls.append(("warmup", sandbox_id))
        if self.fail_warmup > 0:
            self.fail_warmup -= 1
            raise RuntimeError("injected warmup failure")
        self._running(sandbox_id)

    async def pause(self, sandbox_id: str) -> None:
        self.calls.append(("pause", sandbox_id))
        await self._sleep(self.pause_latency_s)
        self._running(sandbox_id)["state"] = "paused"

    async def resume(self, sandbox_id: str, timeout_s: float) -> None:
        self.calls.append(("resume", sandbox_id))
        await self._sleep(self.latency_s)
        sb = self._get(sandbox_id)
        if sandbox_id in self.fail_resume_ids:
            raise RuntimeError("injected resume failure")
        sb["state"] = "running"
        sb["timeout"] = timeout_s
        sb["deadline"] = time.time() + timeout_s

    async def set_timeout(self, sandbox_id: str, timeout_s: float) -> None:
        self.calls.append(("set_timeout", sandbox_id))
        if self.set_timeout_delays:
            await asyncio.sleep(self.set_timeout_delays.pop(0))
        if self.fail_set_timeout > 0:
            self.fail_set_timeout -= 1
            raise RuntimeError("injected set_timeout failure")
        sb = self._running(sandbox_id)
        sb["timeout"] = timeout_s
        sb["deadline"] = time.time() + timeout_s

    async def kill(self, sandbox_id: str) -> bool:
        self.calls.append(("kill", sandbox_id))
        await self._sleep(self.kill_latency_s)
        if self.fail_kill > 0:
            self.fail_kill -= 1
            raise RuntimeError("injected kill failure")
        self._sweep()
        return self._drop(sandbox_id)

    async def get_state(self, sandbox_id: str) -> Optional[str]:
        self._sweep()
        sb = self.sandboxes.get(sandbox_id)
        return sb["state"] if sb else None

    async def list(self, metadata: dict[str, str]) -> list[ProviderSandbox]:
        self.calls.append(("list", ""))
        self._sweep()
        out = []
        for sid, sb in self.sandboxes.items():
            if all(sb["metadata"].get(k) == v for k, v in metadata.items()):
                out.append(ProviderSandbox(sid, sb["state"], dict(sb["metadata"]), sb["started_at"]))
        return out

    def _maybe_exec_timeout(self, timeout_s: float) -> None:
        if self.exec_timeouts > 0:
            self.exec_timeouts -= 1
            raise ExecutionTimeout(f"execution exceeded {timeout_s:g}s")

    async def run_code(self, sandbox_id, code, *, language, timeout_s, sandbox_timeout_s,
                       max_output_bytes: int = 256 * 1024) -> CodeResult:
        sb = self._running(sandbox_id)
        self._maybe_exec_timeout(timeout_s)
        out = io.StringIO()
        error = None
        try:
            with contextlib.redirect_stdout(out):
                exec(code, sb["ns"])  # noqa: S102 - 仅测试
        except Exception as e:  # noqa: BLE001
            error = {"name": type(e).__name__, "value": str(e), "traceback": ""}
        text, total, cut = cap_text(out.getvalue(), max_output_bytes)
        return CodeResult(stdout=text, stderr="", text=None, results=[], error=error,
                          truncated=cut, stdout_bytes=total, stderr_bytes=0)

    async def run_command(self, sandbox_id, cmd, *, cwd, envs, timeout_s, sandbox_timeout_s,
                          max_output_bytes: int = 256 * 1024) -> CommandResult:
        self._running(sandbox_id)
        # 真驱动走流式回调，超时时能交回已经收到的那一段；假驱动用这个旋钮模拟同一件事
        if self.next_timeout_partial:
            partial, self.next_timeout_partial = self.next_timeout_partial, ""
            text, total, cut = cap_text(partial, max_output_bytes)
            raise ExecutionTimeout(f"command exceeded {timeout_s:g}s", stdout=text,
                                   truncated=cut, stdout_bytes=total)
        self._maybe_exec_timeout(timeout_s)
        if self.next_output_bytes:
            n, self.next_output_bytes = self.next_output_bytes, 0
            text, total, cut = cap_text("x" * n, max_output_bytes)
            return CommandResult(exit_code=0, stdout=text, stderr="",
                                 truncated=cut, stdout_bytes=total, stderr_bytes=0)
        if cmd.startswith("exit "):
            return CommandResult(exit_code=int(cmd.split()[1]), stdout="", stderr="failed", stderr_bytes=6)
        out = f"ran: {cmd}\n"
        return CommandResult(exit_code=0, stdout=out, stderr="", stdout_bytes=len(out.encode()))

    async def write_file(self, sandbox_id, path, data, *, sandbox_timeout_s) -> None:
        self._running(sandbox_id)["files"][path] = bytes(data)

    async def read_file(self, sandbox_id, path, *, sandbox_timeout_s) -> bytes:
        files = self._running(sandbox_id)["files"]
        if path not in files:
            raise FileNotFoundError(path)
        return files[path]

    def forget(self, sandbox_id: str) -> None:
        self.calls.append(("forget", sandbox_id))

    async def close(self) -> None:
        pass

    # ---------- agent 子系统 ----------

    async def create_app(self, template, metadata, timeout_s, *, port, network) -> AppSandbox:
        self.calls.append(("create_app", template))
        await self._sleep(self.latency_s)
        if self.fail_create_app > 0:
            self.fail_create_app -= 1
            raise RuntimeError("injected create_app failure")
        sid = await self.create(template, metadata, timeout_s)
        self.calls.pop()  # create_app 已记录，不重复记 create
        sb = self.sandboxes[sid]
        sb["network"] = json.loads(json.dumps(network))
        sb["access_token"] = uuid.uuid4().hex
        if template in self.pi_templates:
            sb["pi"] = FakePiServer(sb["files"])
        else:
            sb["opencode"] = FakeOpencodeServer(sb["files"])
        return AppSandbox(sid, f"https://{port}-{sid}.fake.local", sb["access_token"])

    async def update_network(self, sandbox_id: str, network: dict) -> None:
        self.calls.append(("update_network", sandbox_id))
        if self.fail_update_network > 0:
            self.fail_update_network -= 1
            raise RuntimeError("injected update_network failure")
        self._running(sandbox_id)["network"] = json.loads(json.dumps(network))
        self.network_updates.append((sandbox_id, network))

    async def get_network(self, sandbox_id: str):
        # echo_wrong_network：判据用。它模拟「平台没按我说的做，却返回了成功」——
        # 没有这个旋钮，「从不核对回显」的实现与正确实现不可区分
        if self.echo_wrong_network is not None:
            return dict(self.echo_wrong_network)
        sb = self._get(sandbox_id)
        return {**(sb.get("network") or {}), "allow_public_traffic": False}

    async def write_files(self, sandbox_id: str, files: dict, *, sandbox_timeout_s: float) -> None:
        sb = self._running(sandbox_id)
        for path, data in files.items():
            sb["files"][path] = bytes(data)

    def app_client(self, endpoint, access_token, directory, *, ingress_ip):
        # endpoint 形如 https://4096-<sandbox_id>.fake.local
        sandbox_id = endpoint.split("://", 1)[1].split(".", 1)[0].split("-", 1)[1]
        sb = self.sandboxes.get(sandbox_id) or {}
        if sb.get("pi") is not None:
            return FakePiClient(self, sandbox_id, access_token, directory)
        return FakeOpencodeClient(self, sandbox_id, access_token, directory)

    def opencode(self, sandbox_id: str) -> FakeOpencodeServer:
        return self.sandboxes[sandbox_id]["opencode"]

    def pi(self, sandbox_id: str) -> FakePiServer:
        return self.sandboxes[sandbox_id]["pi"]
