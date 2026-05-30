"""Chief tools — dynamic spawn / monitor / terminate of project-chief sub-agents.

A "chief" is a Hermes worker process that owns a high-level project. Spawned
on-demand by main:manager (or another chief) for complex, long-running tasks
that should NOT pollute the orchestrator's conversation context.

Architecture (see ``plans/dynamic-chief-spawn-and-lifecycle.md`` in the repo
root for the full design):

* Each chief = one kanban board + one initial ready task with
  ``assignee="chief-manager"``.
* The kanban dispatcher (per-board tick, already in gateway) sees the ready
  task and spawns a worker process. That worker loads the ``chief-manager``
  skill and operates the project: decomposes into sub-tasks on its OWN
  board, comments progress on the initial task, completes when done.
* Main:manager monitors via ``chief_status`` (aggregates board state into a
  compact summary) and decides lifetime via ``chief_terminate``.

Lifecycle policy is fixed at spawn time and stored in ``board.json`` via
``meta_extra``:

* ``cascade`` (default) — terminating a chief recursively terminates every
  sub-chief that lists it as ``parent_chief_id``. Safe, predictable, no
  orphans. Implemented in this MVP.
* ``independent`` — sub-chiefs survive parent death (re-parented to user
  via a system comment). Planned for Phase 2; not yet implemented.

POC scope (Phase 0+1 of the plan):
* ``chief_spawn`` — create board + initial task with chief metadata.
* ``chief_status`` — aggregate one chief's board into a summary.
* ``chief_list`` — list all live chiefs across boards.
* ``chief_terminate`` — cascade-only for MVP; ``independent`` raises NYI.
"""
from __future__ import annotations

import logging
import os
import re
import signal
import time
import uuid
from typing import Any, Optional

from tools.registry import registry, tool_error

logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Gating: chief tools are available wherever kanban orchestrator tools are.
# Workers DO get them too (a chief is itself a worker that may spawn
# under-chiefs), so the env-var check used by kanban_list/etc is too strict.
# We gate on "kanban toolset enabled in profile" — same as orchestrator mode
# but without excluding workers.
# ---------------------------------------------------------------------------

def _profile_has_kanban_toolset() -> bool:
    # Why: check_fn is global (no platform context), but kanban PM tools may
    # be opted in either at the top-level (legacy CLI sessions) or via a
    # platform_toolsets composite like `hermes-telegram-pm` that
    # `includes: [kanban]`. Walk both so a platform-scoped opt-in unlocks
    # mc_*/chief_* tools at registry-filter time. Schema visibility per
    # session is still gated by resolve_toolset(enabled_toolsets).
    try:
        from hermes_cli.config import load_config
        cfg = load_config()
        toolsets = cfg.get("toolsets", []) or []
        if "kanban" in toolsets:
            return True
        platform_toolsets = cfg.get("platform_toolsets", {}) or {}
        try:
            from toolsets import TOOLSETS
        except Exception:
            TOOLSETS = {}
        stack: list[str] = []
        for entries in platform_toolsets.values():
            if isinstance(entries, list):
                stack.extend(str(e) for e in entries)
        seen: set[str] = set()
        while stack:
            name = stack.pop()
            if name in seen:
                continue
            seen.add(name)
            if name == "kanban":
                return True
            ts = TOOLSETS.get(name) if isinstance(TOOLSETS, dict) else None
            if isinstance(ts, dict):
                stack.extend(ts.get("includes") or [])
        return False
    except Exception:
        return False


def _profile_dir_exists(name: str) -> bool:
    """Cheap on-disk check that ~/.hermes/profiles/<name>/config.yaml exists.
    Used by chief_spawn(profile=…) to fail fast on typos before the
    dispatcher silently buckets the task as `skipped_nonspawnable`.
    """
    if not name or any(c in name for c in ("/", "\\", "..", "\x00")):
        return False
    hermes_home = os.environ.get("HERMES_HOME") or os.path.expanduser("~/.hermes")
    profile_cfg = os.path.join(hermes_home, "profiles", name, "config.yaml")
    return os.path.isfile(profile_cfg)


def _check_chief_mode() -> bool:
    """Chief tools available to:
      1. Dispatcher-spawned chief workers (HERMES_KANBAN_TASK set + we are
         the chief-manager assignee — they can spawn under-chiefs).
      2. Orchestrator profiles with kanban toolset enabled (main:manager).
    """
    if os.environ.get("HERMES_KANBAN_TASK"):
        return True
    return _profile_has_kanban_toolset()


def _check_chief_worker_only() -> bool:
    """tg_send / tg_ask / tg_ask_status are worker-side only — they require
    a chief board context. Hermes-main has its own TG channel; it doesn't
    need (and shouldn't have) these. Gating here is intentionally narrow:
    a dispatcher-spawned task (HERMES_KANBAN_TASK set) on a chief board.
    Root-vs-under-chief enforcement happens inside the handlers
    (_is_root_chief) so the operator gets a clear error rather than a
    missing tool.
    """
    if not os.environ.get("HERMES_KANBAN_TASK"):
        return False
    board = os.environ.get("HERMES_KANBAN_BOARD")
    if not board:
        return False
    try:
        return _read_chief_meta(board) is not None
    except Exception:
        return False


# ---------------------------------------------------------------------------
# Constants & helpers
# ---------------------------------------------------------------------------

CHIEF_BOARD_KIND = "chief"
CHIEF_ASSIGNEE = "chief-manager"
DEFAULT_LIFETIME = "ephemeral"
DEFAULT_TERMINATE_POLICY = "cascade"
DEFAULT_MAX_RUNTIME_MIN = 120
MAX_NEST_DEPTH = 3  # parent → child → grandchild; further is rejected

# Anti-spam guard for chief→operator messaging. Must match bridge enum.
CHIEF_SEND_INTENTS = {
    "milestone", "delivery_complete", "unblocked", "escalation_resolved",
}
CHIEF_ASK_INTENTS = {
    "blocker_clarify", "scope_check", "credential_needed", "decision_required",
}
CHIEF_MSG_MIN_LEN = 30

# Followup supervisor script. Lives in $HERMES_HOME/scripts/. Hermes can
# wire it up to a `cronjob: create` so the assistant keeps the spawned
# chief in view without hand-waving timers. We do NOT auto-create the
# cron — that's the assistant's call; we only provide the script.
CHIEF_SUPERVISOR_SCRIPT = "chief_followup.py"
CHIEF_SUPERVISOR_DEFAULT_SCHEDULE = "every 1m"


_SLUG_RE = re.compile(r"[^a-z0-9]+")


def _slugify(name: str) -> str:
    """Lower-snake with single hyphens, max 32 chars. Empty string never returned."""
    cleaned = _SLUG_RE.sub("-", name.lower()).strip("-")
    if not cleaned:
        cleaned = "chief"
    return cleaned[:32]


def _new_chief_id(name: str) -> str:
    """``chief-<slug>-<ulid-tail>``. ULID tail keeps it sortable + unique."""
    suffix = uuid.uuid4().hex[-6:]
    return f"chief-{_slugify(name)}-{suffix}"


def _import_kanban_db():
    """Lazy import so tool module loads cleanly in non-kanban contexts."""
    from hermes_cli import kanban_db
    return kanban_db


def _read_chief_meta(chief_id: str) -> Optional[dict]:
    """Return board metadata IFF it's a chief board; else None."""
    kb = _import_kanban_db()
    try:
        meta = kb.read_board_metadata(chief_id)
    except Exception:
        return None
    if meta.get("kind") != CHIEF_BOARD_KIND:
        return None
    return meta


def _check_recursion_depth(parent_chief_id: Optional[str]) -> Optional[str]:
    """Return error message if spawning under this parent would exceed depth."""
    if not parent_chief_id:
        return None
    depth = 0
    cur = parent_chief_id
    while cur and depth < MAX_NEST_DEPTH + 1:
        meta = _read_chief_meta(cur)
        if not meta:
            break
        cur = meta.get("parent_chief_id")
        depth += 1
    if depth >= MAX_NEST_DEPTH:
        return (
            f"chief recursion depth would be {depth + 1}, exceeds limit "
            f"{MAX_NEST_DEPTH}. Decompose the work flatter — spawn a peer "
            f"chief under the top-level main:manager instead of nesting."
        )
    return None


def _list_chief_boards(include_archived: bool = False) -> list[dict]:
    """Enumerate boards with ``kind=chief`` metadata."""
    kb = _import_kanban_db()
    out = []
    for b in kb.list_boards(include_archived=include_archived):
        meta = kb.read_board_metadata(b["slug"])
        if meta.get("kind") != CHIEF_BOARD_KIND:
            continue
        out.append(meta)
    return out


def _find_initial_task(chief_id: str):
    """Initial task = oldest task with assignee=chief-manager on this board."""
    kb = _import_kanban_db()
    try:
        conn = kb.connect(board=chief_id)
    except FileNotFoundError:
        return None
    try:
        row = conn.execute(
            "SELECT * FROM tasks WHERE assignee = ? "
            "ORDER BY created_at ASC LIMIT 1",
            (CHIEF_ASSIGNEE,),
        ).fetchone()
        return dict(row) if row else None
    finally:
        conn.close()


def _board_task_counts(chief_id: str) -> dict[str, int]:
    """Status → count map for the chief's board."""
    kb = _import_kanban_db()
    try:
        conn = kb.connect(board=chief_id)
    except FileNotFoundError:
        return {}
    try:
        rows = conn.execute(
            "SELECT status, COUNT(*) AS n FROM tasks GROUP BY status"
        ).fetchall()
        return {r["status"]: int(r["n"]) for r in rows}
    finally:
        conn.close()


def _derive_stage(initial_task: Optional[dict], events: list) -> str:
    """Cheap textual progress signal for Main."""
    if not initial_task:
        return "no-initial-task"
    status = initial_task.get("status", "unknown")
    if status == "done":
        return "completed"
    if status == "ready":
        return "queued"  # dispatcher hasn't claimed yet
    if status == "running":
        # Prefer latest non-system commit/comment kind
        for ev in events:
            if ev.get("kind") in ("commented", "comment", "heartbeat"):
                return f"running:{ev.get('kind')}"
        return "running"
    return status


# ---------------------------------------------------------------------------
# chief→operator messaging helpers (Phase 2 — tg_send / tg_ask)
# ---------------------------------------------------------------------------

def _is_root_chief() -> tuple[bool, Optional[str]]:
    """True iff the current worker is a *root* chief (depth=1, no parent).
    Returns (ok, reason_if_not). Under-chiefs (parent_chief_id != None) and
    non-chief workers are denied access to tg_send / tg_ask — they must
    surface through kanban_comment → parent's chief_status pipeline.
    """
    board = os.environ.get("HERMES_KANBAN_BOARD")
    task_id = os.environ.get("HERMES_KANBAN_TASK")
    if not board or not task_id:
        return False, "tg_send/tg_ask are only available to chief workers"
    meta = _read_chief_meta(board)
    if not meta:
        return False, (
            "current board is not a chief board; this tool is only "
            "available to chief-manager workers"
        )
    if meta.get("parent_chief_id"):
        return False, (
            "under-chiefs cannot push to the operator directly. Write a "
            "kanban_comment on your initial task — the parent chief will "
            "see it via chief_status and decide whether to escalate."
        )
    return True, None


def _resolve_operator_chat_id() -> Optional[int]:
    """Read operator chat_id from env (dispatcher propagates from board
    metadata at spawn time). Returns None if unset."""
    raw = (os.environ.get("HERMES_OPERATOR_CHAT_ID") or "").strip()
    if not raw:
        return None
    try:
        return int(raw)
    except ValueError:
        return None


def _hitl_bridge_url() -> str:
    return (
        os.environ.get("HERMES_HITL_BRIDGE_URL")
        or "http://hermes-hitl:8889"
    ).rstrip("/")


def _post_hitl(path: str, body: dict) -> tuple[int, dict]:
    """Thin httpx POST helper. Returns (status_code, parsed_or_text)."""
    import httpx
    url = f"{_hitl_bridge_url()}{path}"
    try:
        with httpx.Client(timeout=15.0) as client:
            resp = client.post(url, json=body)
    except httpx.HTTPError as e:
        return 0, {"error": "bridge_unreachable", "detail": str(e)[:200]}
    try:
        return resp.status_code, resp.json()
    except ValueError:
        return resp.status_code, {"raw": resp.text[:500]}


def _get_hitl(path: str) -> tuple[int, dict]:
    import httpx
    url = f"{_hitl_bridge_url()}{path}"
    try:
        with httpx.Client(timeout=15.0) as client:
            resp = client.get(url)
    except httpx.HTTPError as e:
        return 0, {"error": "bridge_unreachable", "detail": str(e)[:200]}
    try:
        return resp.status_code, resp.json()
    except ValueError:
        return resp.status_code, {"raw": resp.text[:500]}


def _handle_tg_send(args: dict, **kw) -> str:
    """Direct fire-and-forget push to the operator's TG chat. Rate-limited
    by the bridge; root-chief only; intent-gated; min length 30."""
    ok, why = _is_root_chief()
    if not ok:
        return tool_error(f"tg_send: {why}")
    text = (args.get("text") or "").strip()
    intent = (args.get("intent") or "").strip()
    if intent not in CHIEF_SEND_INTENTS:
        return tool_error(
            f"tg_send: intent must be one of {sorted(CHIEF_SEND_INTENTS)}, "
            f"got {intent!r}. If none fits — this probably belongs in a "
            f"kanban_comment, not a TG push."
        )
    if len(text) < CHIEF_MSG_MIN_LEN:
        return tool_error(
            f"tg_send: text must be ≥{CHIEF_MSG_MIN_LEN} chars. Be "
            f"specific — operator's attention is finite, every push must "
            f"earn its place."
        )
    chat_id = _resolve_operator_chat_id()
    if chat_id is None:
        return tool_error(
            "tg_send: HERMES_OPERATOR_CHAT_ID is not set on this worker. "
            "Either the dispatcher didn't propagate it, or chief_spawn "
            "was called without operator_chat_id. Write kanban_comment "
            "instead — orchestrator will see it via chief_status."
        )
    body = {
        "chief_id": os.environ.get("HERMES_KANBAN_BOARD"),
        "agent_id": os.environ.get("HERMES_PROFILE"),
        "task_id": os.environ.get("HERMES_KANBAN_TASK"),
        "chat_id": chat_id,
        "intent": intent,
        "text": text,
    }
    status, payload = _post_hitl("/chief/send", body)
    if status == 200 and payload.get("ok"):
        return _json_ok({
            "delivered": True,
            "req_id": payload.get("req_id"),
            "intent": intent,
        })
    if status == 429:
        return tool_error(
            "tg_send: rate-limited by bridge — "
            f"{payload.get('hint') or payload}"
        )
    return tool_error(
        f"tg_send: bridge HTTP {status}: {payload}"
    )


def _handle_tg_ask(args: dict, **kw) -> str:
    """HITL clarification request — chief asks, operator answers free-text.
    Non-blocking: returns req_id; chief polls tg_ask_status(req_id).
    """
    ok, why = _is_root_chief()
    if not ok:
        return tool_error(f"tg_ask: {why}")
    question = (args.get("question") or "").strip()
    intent = (args.get("intent") or "").strip()
    if intent not in CHIEF_ASK_INTENTS:
        return tool_error(
            f"tg_ask: intent must be one of {sorted(CHIEF_ASK_INTENTS)}, "
            f"got {intent!r}."
        )
    if len(question) < CHIEF_MSG_MIN_LEN:
        return tool_error(
            f"tg_ask: question must be ≥{CHIEF_MSG_MIN_LEN} chars. "
            f"State the specific decision / piece of info you need."
        )
    chat_id = _resolve_operator_chat_id()
    if chat_id is None:
        return tool_error(
            "tg_ask: HERMES_OPERATOR_CHAT_ID is not set on this worker."
        )
    body = {
        "chief_id": os.environ.get("HERMES_KANBAN_BOARD"),
        "agent_id": os.environ.get("HERMES_PROFILE"),
        "task_id": os.environ.get("HERMES_KANBAN_TASK"),
        "chat_id": chat_id,
        "intent": intent,
        "question": question,
        "context": (args.get("context") or None),
        "options": args.get("options"),
        "timeout_sec": args.get("timeout_sec"),
    }
    status, payload = _post_hitl("/chief/ask", body)
    if status == 200 and payload.get("ok"):
        return _json_ok({
            "req_id": payload.get("req_id"),
            "expires_at": payload.get("expires_at"),
            "timeout_sec": payload.get("timeout_sec"),
            "hint": (
                "Poll with tg_ask_status(req_id) every 30-60s. Don't "
                "block the project waiting — keep working on independent "
                "sub-tasks. Operator may take minutes to answer."
            ),
        })
    if status == 429:
        return tool_error(
            "tg_ask: rate-limited by bridge — "
            f"{payload.get('hint') or payload}"
        )
    return tool_error(f"tg_ask: bridge HTTP {status}: {payload}")


def _handle_chief_answer_question(args: dict, **kw) -> str:
    """Operator-side tool (Hermes-main): deliver a free-text answer or
    decline to a pending chief_ask. Called when the operator types
    `/answer <req_id> <text>` or `/decline <req_id>` in TG. Forwards to
    hitl-bridge /chief/answer; bridge stores the answer and the chief
    picks it up via tg_ask_status.
    """
    req_id = (args.get("req_id") or "").strip()
    if not req_id:
        return tool_error("chief_answer_question: 'req_id' is required")
    decision = (args.get("decision") or "answered").strip()
    if decision not in ("answered", "declined"):
        return tool_error(
            "chief_answer_question: decision must be 'answered' or 'declined'"
        )
    answer = (args.get("answer") or "").strip() if decision == "answered" else None
    if decision == "answered" and not answer:
        return tool_error(
            "chief_answer_question: 'answer' is required when decision="
            "'answered'"
        )
    body = {"req_id": req_id, "decision": decision, "answer": answer}
    status, payload = _post_hitl("/chief/answer", body)
    if status == 200 and payload.get("ok"):
        return _json_ok({
            "delivered": True,
            "req_id": req_id,
            "decision": decision,
        })
    if status == 404:
        return tool_error(
            f"chief_answer_question: unknown req_id {req_id!r}. The "
            f"question may have expired or never existed."
        )
    if status == 409:
        return tool_error(
            f"chief_answer_question: req_id {req_id!r} already resolved "
            f"({payload.get('decision') or payload})"
        )
    return tool_error(
        f"chief_answer_question: bridge HTTP {status}: {payload}"
    )


def _handle_tg_ask_status(args: dict, **kw) -> str:
    """Poll the bridge for a chief_ask answer."""
    ok, why = _is_root_chief()
    if not ok:
        return tool_error(f"tg_ask_status: {why}")
    req_id = (args.get("req_id") or "").strip()
    if not req_id:
        return tool_error("tg_ask_status: 'req_id' is required")
    status, payload = _get_hitl(f"/chief/ask/{req_id}")
    if status == 200 and payload.get("ok"):
        return _json_ok({
            "status": payload.get("status"),
            "decision": payload.get("decision"),
            "answer": payload.get("answer"),
            "decided_at": payload.get("decided_at"),
            "expires_at": payload.get("expires_at"),
        })
    if status == 404:
        return tool_error(f"tg_ask_status: unknown req_id {req_id!r}")
    return tool_error(f"tg_ask_status: bridge HTTP {status}: {payload}")


# ---------------------------------------------------------------------------
# Handlers
# ---------------------------------------------------------------------------

def _handle_chief_spawn(args: dict, **kw) -> str:
    """Create a new chief board + initial ready task. Returns chief_id."""
    name = (args.get("name") or "").strip()
    brief = (args.get("brief") or "").strip()
    if not name:
        return tool_error("chief_spawn: 'name' is required")
    if not brief:
        return tool_error("chief_spawn: 'brief' is required (the task description)")
    lifetime = (args.get("lifetime") or DEFAULT_LIFETIME).lower()
    if lifetime not in ("ephemeral", "permanent"):
        return tool_error(
            f"chief_spawn: lifetime must be 'ephemeral' or 'permanent', got {lifetime!r}"
        )
    terminate_policy = (args.get("terminate_policy") or DEFAULT_TERMINATE_POLICY).lower()
    if terminate_policy not in ("cascade", "independent"):
        return tool_error(
            f"chief_spawn: terminate_policy must be 'cascade' or 'independent', got "
            f"{terminate_policy!r}"
        )
    try:
        max_runtime_min = int(args.get("max_runtime_min", DEFAULT_MAX_RUNTIME_MIN))
    except (TypeError, ValueError):
        return tool_error("chief_spawn: max_runtime_min must be an integer")
    if max_runtime_min < 1:
        return tool_error("chief_spawn: max_runtime_min must be >= 1")

    parent_chief_id = args.get("parent_chief_id")
    if parent_chief_id and not _read_chief_meta(parent_chief_id):
        return tool_error(
            f"chief_spawn: parent_chief_id {parent_chief_id!r} is not a known "
            f"chief board"
        )
    err = _check_recursion_depth(parent_chief_id)
    if err:
        return tool_error(err)

    # operator_chat_id — propagated to worker env so tg_send / tg_ask know
    # where to deliver. Resolution order: explicit arg → spawning chief's
    # own metadata (cascade for nested) → spawner env. Stored in board
    # metadata so dispatcher can read it at worker spawn time without
    # re-resolving.
    operator_chat_id: Optional[int] = None
    raw_cid = args.get("operator_chat_id")
    if raw_cid not in (None, ""):
        try:
            operator_chat_id = int(raw_cid)
        except (TypeError, ValueError):
            return tool_error(
                "chief_spawn: operator_chat_id must be an integer "
                "(Telegram chat id)"
            )
    if operator_chat_id is None and parent_chief_id:
        parent_meta = _read_chief_meta(parent_chief_id) or {}
        inherited = parent_meta.get("operator_chat_id")
        if inherited is not None:
            try:
                operator_chat_id = int(inherited)
            except (TypeError, ValueError):
                pass
    if operator_chat_id is None:
        env_cid = (os.environ.get("HERMES_OPERATOR_CHAT_ID") or "").strip()
        if env_cid:
            try:
                operator_chat_id = int(env_cid)
            except ValueError:
                operator_chat_id = None

    # Optional profile override — lets the operator route a project to a
    # specialised chief (e.g. `mc-pm-chief` which drives Mission Control)
    # instead of the default `chief-manager`. The profile must exist on
    # disk; the dispatcher uses task.assignee verbatim as the profile name
    # when spawning the worker, so a typo here = a quiet "skipped_nonspawnable"
    # outcome. Validate up-front.
    profile_override = (args.get("profile") or "").strip()
    if profile_override:
        if not _profile_dir_exists(profile_override):
            return tool_error(
                f"chief_spawn: profile {profile_override!r} not found in "
                f"~/.hermes/profiles/. Available chief-shaped profiles must "
                f"have the `kanban` toolset enabled."
            )
        chief_assignee = profile_override
    else:
        chief_assignee = CHIEF_ASSIGNEE

    kb = _import_kanban_db()
    chief_id = _new_chief_id(name)
    now = int(time.time())
    chief_meta = {
        "kind": CHIEF_BOARD_KIND,
        "lifetime": lifetime,
        "terminate_policy": terminate_policy,
        "max_runtime_min": max_runtime_min,
        "parent_chief_id": parent_chief_id,
        "spawned_at": now,
        "spawned_by_task": os.environ.get("HERMES_KANBAN_TASK"),
        "operator_chat_id": operator_chat_id,
    }

    try:
        kb.create_board(
            chief_id,
            name=f"{name} chief",
            description=brief[:200],
            meta_extra=chief_meta,
        )
    except ValueError as e:
        return tool_error(f"chief_spawn: failed to create board: {e}")

    # Create initial task assigned to chief-manager. Dispatcher will spawn
    # the chief worker on next tick.
    #
    # IMPORTANT: pass `db_path` explicitly (not `board=`) so the new
    # chief board's path is used regardless of whether the caller is a
    # worker process. ``kanban_db_path(board=...)`` honours
    # ``HERMES_KANBAN_DB`` env first (dispatchers pin workers to their
    # claimed board via that env), which means a worker invoking
    # chief_spawn(name='X') would otherwise land the initial task on
    # ITS OWN board instead of board 'X'. Constructing the path locally
    # bypasses that override — this is the one place where ignoring the
    # env pin is intentional.
    try:
        chief_db_path = kb.board_dir(chief_id) / "kanban.db"
        conn = kb.connect(db_path=chief_db_path)
        try:
            task_id = "t_" + uuid.uuid4().hex[:8]
            title = brief.splitlines()[0][:80] if brief else f"{name} brief"
            conn.execute(
                "INSERT INTO tasks (id, title, body, assignee, status, "
                "created_at, priority, max_runtime_seconds) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    task_id, title, brief, chief_assignee,
                    "ready", now, 0, max_runtime_min * 60,
                ),
            )
            conn.commit()
        finally:
            conn.close()
    except Exception as e:
        logger.exception("chief_spawn: failed to create initial task")
        return tool_error(f"chief_spawn: failed to create initial task: {e}")

    return _json_ok({
        "chief_id": chief_id,
        "board": chief_id,
        "initial_task": task_id,
        "lifetime": lifetime,
        "terminate_policy": terminate_policy,
        "parent_chief_id": parent_chief_id,
        "operator_chat_id": operator_chat_id,
    })


def _handle_chief_status(args: dict, **kw) -> str:
    """Aggregate one chief's board state into a Main-friendly summary."""
    chief_id = (args.get("chief_id") or "").strip()
    if not chief_id:
        return tool_error("chief_status: 'chief_id' is required")

    meta = _read_chief_meta(chief_id)
    if not meta:
        return tool_error(
            f"chief_status: {chief_id!r} is not a known chief board (or board "
            f"was archived/deleted)"
        )

    counts = _board_task_counts(chief_id)
    initial = _find_initial_task(chief_id)

    kb = _import_kanban_db()
    events: list = []
    last_comment = None
    try:
        conn = kb.connect(board=chief_id)
        try:
            rows = conn.execute(
                "SELECT * FROM task_events ORDER BY created_at DESC LIMIT 20"
            ).fetchall()
            events = [dict(r) for r in rows]
        finally:
            conn.close()
    except FileNotFoundError:
        pass
    except Exception as e:
        logger.warning("chief_status: events fetch failed: %s", e)

    if initial:
        for ev in events:
            if ev.get("kind") == "commented" and ev.get("task_id") == initial["id"]:
                last_comment = ev.get("payload")
                break

    open_count = (
        counts.get("ready", 0) + counts.get("running", 0)
        + counts.get("blocked", 0) + counts.get("triage", 0)
        + counts.get("todo", 0)
    )
    alive = initial is not None and initial.get("status") in (
        "ready", "running", "blocked", "todo", "triage"
    )

    return _json_ok({
        "chief_id": chief_id,
        "lifetime": meta.get("lifetime"),
        "terminate_policy": meta.get("terminate_policy"),
        "parent_chief_id": meta.get("parent_chief_id"),
        "alive": alive,
        "stage": _derive_stage(initial, events),
        "initial_task": initial["id"] if initial else None,
        "initial_status": initial.get("status") if initial else None,
        "subtasks_total": sum(counts.values()),
        "subtasks_open": open_count,
        "subtasks_done": counts.get("done", 0),
        "by_status": counts,
        "last_comment": last_comment,
        "last_event_at": events[0]["created_at"] if events else None,
        "runtime_min": (
            (int(time.time()) - int(meta.get("spawned_at", 0))) // 60
            if meta.get("spawned_at") else None
        ),
    })


def _handle_chief_list(args: dict, **kw) -> str:
    """List every active chief. Returns compact summaries."""
    include_archived = bool(args.get("include_archived", False))
    chiefs = _list_chief_boards(include_archived=include_archived)
    out = []
    for meta in chiefs:
        cid = meta["slug"]
        counts = _board_task_counts(cid)
        initial = _find_initial_task(cid)
        out.append({
            "chief_id": cid,
            "name": meta.get("name"),
            "lifetime": meta.get("lifetime"),
            "terminate_policy": meta.get("terminate_policy"),
            "parent_chief_id": meta.get("parent_chief_id"),
            "alive": bool(initial and initial.get("status") in (
                "ready", "running", "blocked", "todo", "triage"
            )),
            "initial_status": initial.get("status") if initial else None,
            "subtasks_open": (
                counts.get("ready", 0) + counts.get("running", 0)
                + counts.get("blocked", 0) + counts.get("triage", 0)
                + counts.get("todo", 0)
            ),
            "subtasks_done": counts.get("done", 0),
            "spawned_at": meta.get("spawned_at"),
        })
    return _json_ok({"chiefs": out, "count": len(out)})


def _terminate_cascade(chief_id: str, force: bool, _visited=None) -> dict:
    """Recursive cascade. Returns summary dict for the JSON response."""
    if _visited is None:
        _visited = set()
    if chief_id in _visited:
        return {"chief_id": chief_id, "terminated": False, "reason": "already visited"}
    _visited.add(chief_id)

    kb = _import_kanban_db()
    cascaded: list[dict] = []
    # Walk children first so a worker never sees its parent gone before
    # itself.
    for meta in _list_chief_boards():
        if meta.get("parent_chief_id") == chief_id:
            cascaded.append(_terminate_cascade(meta["slug"], force, _visited))

    killed_workers = 0
    if force:
        try:
            conn = kb.connect(board=chief_id)
            try:
                rows = conn.execute(
                    "SELECT worker_pid FROM tasks "
                    "WHERE status = 'running' AND worker_pid IS NOT NULL"
                ).fetchall()
            finally:
                conn.close()
            for r in rows:
                pid = r["worker_pid"]
                if not pid:
                    continue
                try:
                    os.kill(int(pid), signal.SIGTERM)
                    killed_workers += 1
                except (ProcessLookupError, PermissionError, ValueError):
                    pass
        except FileNotFoundError:
            pass

    # Archive the board so dispatcher stops ticking it. With our connect()
    # no-resurrect fix (kanban_db.py FileNotFoundError gate), stale callers
    # passing the gone slug won't recreate it.
    try:
        kb.remove_board(chief_id, archive=True)
    except ValueError as e:
        # default board is protected; chiefs are never named "default"
        return {"chief_id": chief_id, "terminated": False, "error": str(e)}

    return {
        "chief_id": chief_id,
        "terminated": True,
        "force": force,
        "killed_workers": killed_workers,
        "cascaded": cascaded,
    }


def _handle_chief_terminate(args: dict, **kw) -> str:
    """Terminate a chief. Policy selected at spawn determines behaviour."""
    chief_id = (args.get("chief_id") or "").strip()
    if not chief_id:
        return tool_error("chief_terminate: 'chief_id' is required")
    force = bool(args.get("force", False))

    meta = _read_chief_meta(chief_id)
    if not meta:
        return tool_error(
            f"chief_terminate: {chief_id!r} is not a known chief board"
        )

    policy = meta.get("terminate_policy", DEFAULT_TERMINATE_POLICY)
    if policy == "cascade":
        result = _terminate_cascade(chief_id, force=force)
        return _json_ok(result)
    elif policy == "independent":
        # Phase 2 — to be implemented. Keep the error structured so Main can
        # detect this and fall back to cascade if it really needs cleanup.
        return tool_error(
            "chief_terminate: terminate_policy='independent' is not yet "
            "implemented (Phase 2). Re-spawn the chief with "
            "terminate_policy='cascade' or call chief_terminate on each "
            "sub-chief manually before terminating this one."
        )
    else:
        return tool_error(
            f"chief_terminate: unknown terminate_policy {policy!r}"
        )


# ---------------------------------------------------------------------------
# Output helpers
# ---------------------------------------------------------------------------

def _json_ok(payload: dict) -> str:
    """Stringify success payloads consistently with kanban_tools convention."""
    import json
    return json.dumps({"ok": True, **payload}, ensure_ascii=False, default=str)


# ---------------------------------------------------------------------------
# Schemas
# ---------------------------------------------------------------------------

CHIEF_SPAWN_SCHEMA = {
    "name": "chief_spawn",
    "description": (
        "Spawn a project-chief sub-agent for a complex task you don't want "
        "to operate yourself. Creates a new isolated kanban board + initial "
        "ready task assigned to 'chief-manager'. The dispatcher's per-board "
        "tick will spawn a worker process that loads the chief-manager skill "
        "and owns the project end-to-end. Returns chief_id you can pass to "
        "chief_status / chief_terminate. Use this for long-running work "
        "(>5 min, multiple stages, parallel sub-tasks); for small one-shots "
        "use delegate_task instead."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "name": {
                "type": "string",
                "description": (
                    "Short slug-friendly project name, e.g. 'yt-indexer' "
                    "or 'tax-report'. Becomes part of the chief_id."
                ),
            },
            "brief": {
                "type": "string",
                "description": (
                    "Full task brief for the chief. This is the body of the "
                    "initial ready task — the chief will read it as 'your "
                    "assignment from upstream'. Include scope, acceptance "
                    "criteria, constraints, and any inputs / paths. Be "
                    "specific: the chief operates with no shared "
                    "conversation context with you."
                ),
            },
            "lifetime": {
                "type": "string",
                "enum": ["ephemeral", "permanent"],
                "description": (
                    "ephemeral (default): chief auto-completes once its "
                    "initial task is done. permanent: chief stays alive "
                    "and pulls additional tasks from its board until "
                    "explicitly terminated."
                ),
            },
            "terminate_policy": {
                "type": "string",
                "enum": ["cascade", "independent"],
                "description": (
                    "How termination propagates to sub-chiefs spawned BY "
                    "this chief. cascade (default): terminating recurses "
                    "into descendants. independent: descendants survive "
                    "(NYI, Phase 2)."
                ),
            },
            "max_runtime_min": {
                "type": "integer",
                "description": (
                    "Hard ceiling on the initial task's runtime in minutes. "
                    "Default 120. Dispatcher will block the task with a "
                    "timeout reason if exceeded."
                ),
            },
            "parent_chief_id": {
                "type": "string",
                "description": (
                    "Internal: set automatically when one chief spawns "
                    "another. main:manager should usually omit this."
                ),
            },
            "operator_chat_id": {
                "type": "integer",
                "description": (
                    "Telegram chat_id of the operator who owns the "
                    "project. Stored on the chief's board metadata and "
                    "propagated to workers as HERMES_OPERATOR_CHAT_ID — "
                    "needed for tg_send / tg_ask. main:manager should "
                    "pass the current operator's chat_id (resolvable "
                    "from the active platform session). Sub-chiefs "
                    "inherit from parent if omitted."
                ),
            },
        },
        "required": ["name", "brief"],
    },
}

CHIEF_STATUS_SCHEMA = {
    "name": "chief_status",
    "description": (
        "Get a compact progress summary for one chief. Includes stage, "
        "subtask counts (open/done), last comment on the initial task, "
        "runtime so far. Poll periodically (~every few minutes) to keep "
        "your overview fresh without diving into the chief's board."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "chief_id": {
                "type": "string",
                "description": "The chief_id returned by chief_spawn.",
            },
        },
        "required": ["chief_id"],
    },
}

CHIEF_LIST_SCHEMA = {
    "name": "chief_list",
    "description": (
        "List every chief board (kind=chief in board metadata) on this "
        "host. Includes compact alive flag, subtask counts, parent chief "
        "link. Use for an overview before deciding which chiefs to check "
        "in detail (chief_status) or terminate (chief_terminate)."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "include_archived": {
                "type": "boolean",
                "description": (
                    "Include archived (terminated) chief boards. Defaults "
                    "to false — usually you only want live ones."
                ),
            },
        },
        "required": [],
    },
}

TG_SEND_SCHEMA = {
    "name": "tg_send",
    "description": (
        "Push a short message directly to the operator's Telegram chat. "
        "Fire-and-forget — there is NO operator response. Reserved for "
        "material moments: milestones reached, deliverables ready, an "
        "earlier escalation resolved, work unblocked. NOT for status "
        "updates (use kanban_comment — orchestrator polls via "
        "chief_status), NOT for trivia, NOT for thanks. Rate-limited "
        "(default 2/hour, 6/day per chief). Available only to root "
        "chiefs — under-chiefs must surface through their parent."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "text": {
                "type": "string",
                "description": (
                    "The message body (≥30 chars). Be specific and "
                    "self-contained — operator may not have the chief's "
                    "context loaded."
                ),
            },
            "intent": {
                "type": "string",
                "enum": sorted(CHIEF_SEND_INTENTS),
                "description": (
                    "Why this push is justified. milestone: significant "
                    "phase done. delivery_complete: deliverable shipped. "
                    "unblocked: previously-stuck work resumed. "
                    "escalation_resolved: earlier escalation closed."
                ),
            },
        },
        "required": ["text", "intent"],
    },
}

TG_ASK_SCHEMA = {
    "name": "tg_ask",
    "description": (
        "Ask the operator a clarifying question via Telegram. Non-blocking: "
        "returns a req_id; poll tg_ask_status(req_id) for the operator's "
        "free-text answer (or 'expired' / 'declined' status). Keep "
        "working on independent sub-tasks while waiting. Rate-limited "
        "(default 3/hour, 8/day per chief). Available only to root chiefs."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "question": {
                "type": "string",
                "description": (
                    "The specific question (≥30 chars). Frame it so the "
                    "answer maps to a concrete next action."
                ),
            },
            "intent": {
                "type": "string",
                "enum": sorted(CHIEF_ASK_INTENTS),
                "description": (
                    "Why you need to interrupt. blocker_clarify: cannot "
                    "proceed without input. scope_check: need go/no-go "
                    "before irreversible work. credential_needed: "
                    "secret/access missing. decision_required: plan "
                    "branch only operator can choose."
                ),
            },
            "context": {
                "type": "string",
                "description": (
                    "Optional short context (≤800 chars) so operator "
                    "doesn't need to ask back 'what?'. Skip if the "
                    "question is self-explanatory."
                ),
            },
            "options": {
                "type": "array",
                "items": {"type": "string"},
                "description": (
                    "Optional list of choices, surfaced as a hint to the "
                    "operator. Operator can still answer free-text."
                ),
            },
            "timeout_sec": {
                "type": "integer",
                "description": (
                    "How long to wait for an answer before the request "
                    "expires. Default 600 (10 min), max 1800 (30 min)."
                ),
            },
        },
        "required": ["question", "intent"],
    },
}

CHIEF_ANSWER_QUESTION_SCHEMA = {
    "name": "chief_answer_question",
    "description": (
        "Deliver the operator's free-text answer (or decline) to a "
        "pending chief tg_ask. Call this when the operator replies in "
        "TG with `/answer <req_id> <text>` or `/decline <req_id>` to a "
        "chief's question. The chief polls tg_ask_status to pick up the "
        "answer. Use req_id from the original ❓ Chief asks message — "
        "you can also list outstanding asks via chief_pending."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "req_id": {
                "type": "string",
                "description": "req_id of the chief's outstanding question.",
            },
            "decision": {
                "type": "string",
                "enum": ["answered", "declined"],
                "description": (
                    "'answered' delivers `answer` text to the chief; "
                    "'declined' tells the chief the operator chose not "
                    "to answer."
                ),
            },
            "answer": {
                "type": "string",
                "description": (
                    "The operator's free-text answer. Required when "
                    "decision='answered'. Omit for decline."
                ),
            },
        },
        "required": ["req_id"],
    },
}

TG_ASK_STATUS_SCHEMA = {
    "name": "tg_ask_status",
    "description": (
        "Poll for the operator's answer to a prior tg_ask. Returns "
        "{status: 'pending'|'resolved'|'expired', decision?, answer?}. "
        "Call periodically (every 30-60s) while waiting — never spin in "
        "a tight loop."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "req_id": {
                "type": "string",
                "description": "req_id returned by tg_ask.",
            },
        },
        "required": ["req_id"],
    },
}


CHIEF_TERMINATE_SCHEMA = {
    "name": "chief_terminate",
    "description": (
        "Terminate a chief and (per its terminate_policy) any sub-chiefs "
        "it spawned. Archives the board so the dispatcher stops ticking "
        "it. Use when the chief reports completion (alive=false in "
        "chief_status) and is no longer needed, or when you decide the "
        "work should stop."
    ),
    "parameters": {
        "type": "object",
        "properties": {
            "chief_id": {
                "type": "string",
                "description": "The chief_id to terminate.",
            },
            "force": {
                "type": "boolean",
                "description": (
                    "If true, SIGTERM any running worker processes on this "
                    "chief's board immediately. If false (default), "
                    "workers finish their current step gracefully then exit "
                    "on next heartbeat (the board is already archived, so "
                    "they detect the stop signal)."
                ),
            },
        },
        "required": ["chief_id"],
    },
}


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------

registry.register(
    name="chief_spawn",
    toolset="kanban",
    schema=CHIEF_SPAWN_SCHEMA,
    handler=_handle_chief_spawn,
    check_fn=_check_chief_mode,
    emoji="👑",
)

registry.register(
    name="chief_status",
    toolset="kanban",
    schema=CHIEF_STATUS_SCHEMA,
    handler=_handle_chief_status,
    check_fn=_check_chief_mode,
    emoji="📊",
)

registry.register(
    name="chief_list",
    toolset="kanban",
    schema=CHIEF_LIST_SCHEMA,
    handler=_handle_chief_list,
    check_fn=_check_chief_mode,
    emoji="📊",
)

registry.register(
    name="chief_terminate",
    toolset="kanban",
    schema=CHIEF_TERMINATE_SCHEMA,
    handler=_handle_chief_terminate,
    check_fn=_check_chief_mode,
    emoji="🛑",
)

registry.register(
    name="tg_send",
    toolset="kanban",
    schema=TG_SEND_SCHEMA,
    handler=_handle_tg_send,
    check_fn=_check_chief_worker_only,
    emoji="📣",
)

registry.register(
    name="tg_ask",
    toolset="kanban",
    schema=TG_ASK_SCHEMA,
    handler=_handle_tg_ask,
    check_fn=_check_chief_worker_only,
    emoji="❓",
)

registry.register(
    name="tg_ask_status",
    toolset="kanban",
    schema=TG_ASK_STATUS_SCHEMA,
    handler=_handle_tg_ask_status,
    check_fn=_check_chief_worker_only,
    emoji="🔁",
)

registry.register(
    name="chief_answer_question",
    toolset="kanban",
    schema=CHIEF_ANSWER_QUESTION_SCHEMA,
    handler=_handle_chief_answer_question,
    # Operator-side: available to orchestrators (Hermes-main), NOT to
    # chief workers (they should use tg_ask, not pretend to answer
    # themselves). Same gate as the other chief_* orchestrator tools.
    check_fn=_check_chief_mode,
    emoji="↩️",
)
