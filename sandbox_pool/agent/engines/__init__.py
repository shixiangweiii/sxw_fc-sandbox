"""agent 引擎：opencode（engines/opencode.py）与 pi（engines/pi.py）。

- 引擎按 agent 选择（设置项 engine，默认 POOL_AGENT_DEFAULT_ENGINE）；每个引擎一个模板，配了模板即启用。
- 沙箱记录上存引擎名（sandboxes.engine，老记录为空，视为 opencode）；接管、断线重连、健康检查都按沙箱记录找引擎。
"""

from sandbox_pool.agent.engines.base import Engine, FilesContext
from sandbox_pool.agent.engines.opencode import OpencodeEngine
from sandbox_pool.agent.engines.pi import THINKING_LEVELS, PiEngine

ENGINE_NAMES = ("opencode", "pi")
# 老的沙箱记录没有 engine 列的值
LEGACY_ENGINE = "opencode"

__all__ = ["ENGINE_NAMES", "LEGACY_ENGINE", "THINKING_LEVELS", "Engine", "FilesContext", "build_engines"]


def build_engines(cfg) -> dict[str, Engine]:
    """全部引擎（含未启用的：老沙箱记录可能属于已停用的引擎，仍要能访问、销毁它）。"""
    return {
        "opencode": OpencodeEngine(
            template=cfg.agent_template, port=cfg.agent_port, model=cfg.agent_model, workdir=cfg.agent_workdir
        ),
        "pi": PiEngine(
            template=cfg.agent_pi_template,
            port=cfg.agent_pi_port,
            model=cfg.agent_pi_model,
            workdir=cfg.agent_workdir,
            thinking=cfg.agent_pi_thinking,
        ),
    }
