# hermes-plugin-chief-tools

External Hermes plugin — dynamic chief sub-agent lifecycle.

## About

A "chief" is a Hermes worker process that owns a high-level project. Spawned
on-demand by main:manager (or another chief) for complex, long-running
tasks that should NOT pollute the orchestrator's conversation context.

```
main:manager
    │ chief_spawn(brief="build me X")
    ▼
chief-A (own kanban board)
    ├─ kanban_create — sub-tasks for workers
    ├─ chief_spawn — sub-chief for parallel sub-project
    └─ tg_ask — escalate question to operator
```

The kanban dispatcher (per-board tick, already in upstream gateway) sees
the chief's initial `ready` task and spawns the worker process that
loads the `chief-manager` skill.

## Tools

| Tool | Purpose |
|---|---|
| `chief_spawn` | Create board + initial chief-manager task with `chief` metadata |
| `chief_status` | Aggregate one chief's board into a compact summary |
| `chief_list` | List active chiefs (filter by parent, status) |
| `chief_terminate` | Terminate a chief (cascade default → sub-chiefs die too) |
| `chief_answer_question` | Operator → chief: reply to a chief's tg_ask |
| `tg_send` | Chief → operator notification (no reply expected) |
| `tg_ask` | Chief → operator question (awaits answer; pauses chief flow) |
| `tg_ask_status` | Chief polls "did the operator answer my tg_ask?" |

## Lifecycle policy

Stored at spawn time in `board.json` via `meta_extra.lifecycle_policy`:

- `cascade` (default, implemented) — terminating a chief recursively
  terminates every sub-chief that lists it as `parent_chief_id`.
- `independent` (Phase 2, planned) — sub-chiefs survive parent death.

## Activate

```yaml
# config.yaml
plugins:
  enabled:
    - chief-tools
```

## Mounting

```yaml
# docker-compose.hermes-core.yml
volumes:
  - ./sources/hermes-external-plugins/chief-tools:/opt/data/plugins/chief-tools:ro
```
