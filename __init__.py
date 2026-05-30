"""chief-tools — dynamic spawn / monitor / terminate of project-chief sub-agents.

A "chief" is a Hermes worker process that owns a high-level project. Spawned
on-demand by main:manager (or another chief) for complex, long-running tasks
that should NOT pollute the orchestrator's conversation context.

Exposes 8 tools:
- chief_spawn          create board + initial chief-manager task
- chief_status         aggregate one chief's board into a summary
- chief_list           list active chiefs
- chief_terminate      terminate a chief (cascade by default)
- chief_answer_question Гермес→chief reply for chief→Гермес questions
- tg_send              chief→user TG notification
- tg_ask               chief→user TG question (awaits operator reply)
- tg_ask_status        check whether the operator answered a tg_ask

Replaces in-fork ``tools/chief_tools.py`` + ``chief_*`` block in
``TOOLSETS['kanban']['tools']`` so future upstream merges of toolsets.py
stay conflict-free.
"""

from __future__ import annotations

import logging
from typing import Any

logger = logging.getLogger(__name__)


_TOOL_NAMES = (
    "chief_spawn", "chief_status", "chief_list", "chief_terminate",
    "chief_answer_question",
    "tg_send", "tg_ask", "tg_ask_status",
)


def register(ctx: Any) -> None:
    """Entry point — load chief_tools (which self-registers handlers) and
    extend TOOLSETS['kanban']['tools'] so the names surface for the
    orchestrator toolset.
    """
    try:
        from . import chief_tools  # noqa: F401 — module is the side-effect
    except Exception as exc:
        logger.error("chief-tools: failed to load chief_tools module: %s", exc)
        return

    try:
        import toolsets

        kanban_ts = toolsets.TOOLSETS.get("kanban") or {}
        kanban_tools = kanban_ts.get("tools")
        if isinstance(kanban_tools, list):
            added = 0
            for name in _TOOL_NAMES:
                if name not in kanban_tools:
                    kanban_tools.append(name)
                    added += 1
            logger.info(
                "chief-tools: registered %d tools (%d added to TOOLSETS['kanban'])",
                len(_TOOL_NAMES), added,
            )
        else:
            logger.warning(
                "chief-tools: TOOLSETS['kanban']['tools'] not a list — "
                "tools registered but won't surface in kanban toolset"
            )
    except Exception as exc:
        logger.warning(
            "chief-tools: tools registered but TOOLSETS['kanban'] extension "
            "failed (%s)", exc,
        )
