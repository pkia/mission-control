# Mission Control — agent integration

Mission Control (port **8788**) is the phone dashboard for Hermes agent work.
It is a **view + API layer**; the board of record is Hermes' own durable kanban
(`~/.hermes/kanban.db`, same DB the `hermes kanban` CLI and the gateway
dispatcher use). Nothing here re-implements orchestration.

## Golden rules for agents

1. When you pick up multi-step work you'll delegate or run long: create a card.
2. `mc start` when you begin, `mc activity` when you switch sub-task, `mc review`
   when done, `mc done` after verification passes, `mc block` + reason when stuck.
3. Never hand-edit `~/.hermes/kanban.db` or `telemetry.db` — always go through
   the HTTP API / `mc`.
4. Update existing cards; do not create duplicates (use `mc create --depends-on`
   for parent/child, the API updates in place on status changes).

## Quick reference

```bash
cd ~/apps/mission-control
./mc create "Ship X" --desc "..." --priority high
./mc start  <task-id> --agent researcher
./mc activity <task-id> "Reviewing research sources"
./mc progress <task-id> 65
./mc review <task-id> --summary "Implementation + tests green"
./mc done   <task-id> --result "merged as abc123"
./mc block  <task-id> "waiting on API quota reset"
./mc agent  dev-1 --name Developer --role implementer --task <task-id> --activity "writing scoring module"
./mc board   # summary line
```

Task IDs are printed by `mc create` (also visible on the dashboard cards).

## HTTP API (writes need `Authorization: Bearer $MC_TOKEN` from ~/.hermes/.env or app .env)

- `GET  /health` — liveness
- `GET  /api/snapshot` — full board JSON (tasks/agents/feed/summary)
- `GET  /api/task?id=<tid>` — task detail + comments + events
- `GET  /events` — SSE stream of snapshots (what the dashboard uses)
- `POST /api/tasks` `{title, description, assignee, priority, depends_on[], repo_url, log_url, agent_role}`
- `POST /api/task/<id>/claim` `{agent}` — ready→running (agent starts)
- `POST /api/task/<id>/heartbeat` `{agent, note, activity, progress}` — liveness + current activity
- `POST /api/task/<id>/meta` `{activity, progress, repo_url, log_url, agent_role, est_seconds}`
- `POST /api/task/<id>/review` `{summary}` — work submitted for verification
- `POST /api/task/<id>/complete` `{result, summary}` — verified → done
- `POST /api/task/<id>/block` `{reason, kind?}`
- `POST /api/task/<id>/unblock` / `retry` — back to ready
- `POST /api/task/<id>/assign` `{assignee}`
- `POST /api/task/<id>/comment` `{text, author}`
- `POST /api/task/<id>/promote` — backlog → ready
- `POST /api/agents` `{agent_id, name, role, status, task_id, activity}` — agent heartbeat/registry

## Lifecycle mapping

| Event | API | Board column |
|---|---|---|
| Hermes creates a task | `POST /api/tasks` | BACKLOG by default (`"ready": true` → READY) |
| Agent starts | `claim` | IN PROGRESS |
| Agent finishes | `review` | REVIEW |
| Verification passes | `complete` | DONE |
| Agent fails / stuck | `block` | BLOCKED |
| Retry / reassign | `unblock` + `assign` | READY |

## State & restart

- Board: `~/.hermes/kanban.db` (Hermes-native, survives everything)
- Telemetry/activity/feed: `~/apps/mission-control/telemetry.db` (SQLite, WAL)
- Service: `mission-control.service` (system), port 8788, LAN-bound.
  Restart: `sudo systemctl restart mission-control`.
- Dashboard URL (phone): `http://<pi-host>:8788` (LAN or tailnet). Type the
  `http://` explicitly — HTTPS-First browsers upgrade bare hostnames to https
  and fail.
