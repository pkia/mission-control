#!/usr/bin/env python3
"""HERMES MISSION CONTROL — view + state/API layer over the native Hermes kanban board.

Architecture (see .design.json):
  - Board of record = ~/.hermes/kanban.db (Hermes' own durable board; dispatcher lives in the gateway).
  - This service NEVER writes kanban.db with raw SQL — all board mutations go through
    hermes_cli.kanban_db in-process (the exact code path the `hermes kanban` CLI uses).
  - Sidecar telemetry.db holds what the native schema lacks: live activity strings,
    progress %, repo/log links, standalone agent heartbeats.
  - SSE stream pushes a full board snapshot when its content hash changes (board is small;
    hash-diff avoids re-render churn) + heartbeat events.

Auth model: reads unauthenticated on the LAN (same trust as hermes-webui :8787);
EVERY write requires `Authorization: Bearer $MC_TOKEN` (file ~/.hermes/webui/../mc.token →
sidecar env, loaded from /home/ev/apps/mission-control/.env or HERMES_HOME/.env).

Run: python3 server.py  (port 8788; systemd unit mission-control.service)
"""
from __future__ import annotations

import hashlib
import json
import os
import re
import secrets
import sqlite3
import sys
import threading
import time
import traceback
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from typing import Any, Optional
from urllib.parse import parse_qs, urlparse

HERMES_ROOT = os.environ.get("HERMES_HOME", os.path.expanduser("~/.hermes"))
sys.path.insert(0, os.path.join(HERMES_ROOT, "hermes-agent"))

try:
    from hermes_cli import kanban_db as kb
except Exception:
    traceback.print_exc()
    raise SystemExit("fatal: cannot import hermes_cli.kanban_db — check HERMES_HOME")

APP_DIR = Path(__file__).resolve().parent
BOARD = os.environ.get("MC_BOARD", "default")
SIDECAR_DB = APP_DIR / "telemetry.db"
PORT = int(os.environ.get("MC_PORT", "8788"))
BIND = os.environ.get("MC_BIND", "0.0.0.0")

# ---------------------------------------------------------------- token
def _load_token() -> Optional[str]:
    """Token from .env (MC_TOKEN=...) next to the app, else HERMES_HOME/.env, else None."""
    for p in (APP_DIR / ".env", Path(HERMES_ROOT) / ".env"):
        try:
            for line in p.read_text().splitlines():
                line = line.strip()
                if line.startswith("MC_TOKEN=") and len(line) > 9:
                    return line.split("=", 1)[1].strip().strip('"').strip("'")
        except OSError:
            continue
    return None

TOKEN = _load_token()

# ---------------------------------------------------------------- sidecar schema
_sid_lock = threading.Lock()

def _sidecar() -> sqlite3.Connection:
    con = sqlite3.connect(SIDECAR_DB, timeout=10)
    con.row_factory = sqlite3.Row
    con.execute("PRAGMA journal_mode=WAL")
    con.execute("PRAGMA busy_timeout=5000")
    return con

def _init_sidecar() -> None:
    with _sid_lock, _sidecar() as con:
        con.executescript(
            """
            CREATE TABLE IF NOT EXISTS task_meta (
                task_id TEXT PRIMARY KEY,
                activity TEXT,
                progress REAL,
                repo_url TEXT,
                log_url TEXT,
                agent_role TEXT,
                est_seconds INTEGER,
                updated_at INTEGER
            );
            CREATE TABLE IF NOT EXISTS agents (
                agent_id TEXT PRIMARY KEY,
                name TEXT,
                role TEXT,
                status TEXT,
                task_id TEXT,
                activity TEXT,
                started_at INTEGER,
                last_heartbeat INTEGER
            );
            CREATE TABLE IF NOT EXISTS feed (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                ts INTEGER,
                kind TEXT,
                task_id TEXT,
                agent TEXT,
                text TEXT
            );
            CREATE INDEX IF NOT EXISTS idx_feed_ts ON feed(ts DESC);
            """
        )

_init_sidecar()

def _feed(kind: str, task_id: Optional[str], agent: Optional[str], text: str) -> None:
    with _sid_lock, _sidecar() as con:
        con.execute(
            "INSERT INTO feed(ts, kind, task_id, agent, text) VALUES (?,?,?,?,?)",
            (int(time.time()), kind, task_id, agent, text),
        )
    _bump_version()

# ---------------------------------------------------------------- board reads
_kb_lock = threading.Lock()

def _kconn():
    conn = kb.connect(board=BOARD)
    conn.row_factory = sqlite3.Row
    return conn

COLUMN_MAP = {
    "triage": "BACKLOG", "todo": "BACKLOG", "scheduled": "BACKLOG",
    "ready": "READY", "running": "IN PROGRESS",
    "review": "REVIEW", "blocked": "BLOCKED", "done": "DONE",
    "archived": "DONE",
}
PRIORITY_LABEL = {3: "critical", 2: "high", 1: "normal", 0: "normal", -1: "low"}

def _age(ts: Optional[int]) -> Optional[int]:
    return int(time.time()) - ts if ts else None

def _snapshot() -> dict:
    """Full board snapshot: tasks + agents + feed + summary. Raises on DB error."""
    with _kb_lock:
        conn = _kconn()
        try:
            tasks_rows = kb.list_tasks(conn, include_archived=False)
        finally:
            conn.close()
    tasks = []
    with _sidecar() as con:
        meta_rows = {r["task_id"]: r for r in con.execute("SELECT * FROM task_meta")}
        agent_rows = [dict(r) for r in con.execute(
            "SELECT * FROM agents ORDER BY COALESCE(last_heartbeat, started_at) DESC")]
        feed_rows = [dict(r) for r in con.execute(
            "SELECT * FROM feed ORDER BY id DESC LIMIT 60")]

    for t in tasks_rows:
        tid = t.id
        m = meta_rows.get(tid)
        run = None
        if t.current_run_id:
            with _kb_lock, _kconn() as c2:
                r = c2.execute("SELECT * FROM task_runs WHERE id=?", (t.current_run_id,)).fetchone()
                run = dict(r) if r else None
        deps = []
        with _kb_lock, _kconn() as c3:
            deps = [r["child_id"] for r in c3.execute(
                "SELECT child_id FROM task_links WHERE parent_id=?", (tid,))]
            parents = [r["parent_id"] for r in c3.execute(
                "SELECT parent_id FROM task_links WHERE child_id=?", (tid,))]
        status = t.status if t.status != "archived" else "done"
        tasks.append({
            "id": tid, "title": t.title, "body": t.body, "status": status,
            "column": COLUMN_MAP.get(status, "BACKLOG"),
            "priority": t.priority,
            "priority_label": PRIORITY_LABEL.get(t.priority, "normal"),
            "assignee": t.assignee, "created_by": t.created_by,
            "created_at": t.created_at, "started_at": t.started_at,
            "completed_at": t.completed_at,
            "age_s": _age(t.created_at), "runtime_s": _age(t.started_at) if t.started_at and status == "running" else None,
            "last_update": (t.last_heartbeat_at or t.started_at or t.created_at),
            "last_update_age_s": _age(t.last_heartbeat_at or t.started_at or t.created_at),
            "heartbeat_age_s": _age(t.last_heartbeat_at),
            "consecutive_failures": t.consecutive_failures,
            "last_error": t.last_failure_error,
            "workspace": t.workspace_path,
            "session_id": t.session_id,
            "result": t.result,
            "activity": m["activity"] if m else None,
            "progress": m["progress"] if m else None,
            "repo_url": m["repo_url"] if m else None,
            "log_url": m["log_url"] if m else None,
            "agent_role": m["agent_role"] if m else None,
            "est_seconds": m["est_seconds"] if m else None,
            "deps": deps, "parents": parents,
        })

    # summary
    col_counts: dict[str, int] = {c: 0 for c in COLUMN_ORDER}
    done_today = 0
    day_start = time.mktime(time.strptime(time.strftime("%Y-%m-%d"), "%Y-%m-%d"))
    for t in tasks:
        col_counts[t["column"]] = col_counts.get(t["column"], 0) + 1
        if t["status"] == "done" and t["completed_at"] and t["completed_at"] >= day_start:
            done_today += 1
    active_agents = [a for a in agent_rows if a["status"] == "working" and a["last_heartbeat"]
                     and _age(a["last_heartbeat"]) is not None and _age(a["last_heartbeat"]) < 300]

    return {
        "generated_at": int(time.time()),
        "board": BOARD,
        "columns": COLUMN_ORDER,
        "tasks": tasks,
        "agents": agent_rows,
        "feed": feed_rows,
        "summary": {
            "active_agents": len(active_agents),
            "in_progress": col_counts["IN PROGRESS"],
            "ready": col_counts["READY"],
            "waiting": col_counts["BACKLOG"],
            "review": col_counts["REVIEW"],
            "blocked": col_counts["BLOCKED"],
            "done_today": done_today,
            "done_total": col_counts["DONE"],
        },
        "statuses_seen": sorted({t.status for t in tasks}),
    }

COLUMN_ORDER = ["BACKLOG", "READY", "IN PROGRESS", "REVIEW", "BLOCKED", "DONE"]

def _snapshot_hash(snap: dict) -> str:
    """Hash the volatile parts so runtime tickers don't spam SSE every poll."""
    volatile = {
        "tasks": [{k: v for k, v in t.items() if k not in ("age_s", "runtime_s", "last_update_age_s", "heartbeat_age_s")}
                  for t in snap["tasks"]],
        "agents": [{k: v for k, v in a.items() if k != "runtime"} for a in snap["agents"]],
        "feed": snap["feed"][:5],
    }
    return hashlib.sha256(json.dumps(volatile, default=str, sort_keys=True).encode()).hexdigest()

# ---------------------------------------------------------------- write API
class ApiError(Exception):
    def __init__(self, status: int, msg: str):
        super().__init__(msg)
        self.status = status
        self.msg = msg

def _require_task(conn, task_id: str):
    t = kb.get_task(conn, task_id)
    if not t:
        raise ApiError(404, f"task {task_id} not found")
    return t

def api_create_task(body: dict) -> dict:
    title = (body.get("title") or "").strip()
    if not title:
        raise ApiError(400, "title required")
    priority = body.get("priority", 0)
    if isinstance(priority, str):
        priority = {"critical": 3, "high": 2, "normal": 0, "low": -1}.get(priority.lower(), 0)
    with _kb_lock, _kconn() as conn:
        tid = kb.create_task(
            conn,
            title=title,
            body=body.get("description") or body.get("body"),
            assignee=body.get("assignee"),
            created_by=body.get("created_by") or "mission-control-api",
            priority=int(priority),
            parents=body.get("depends_on") or (),
            triage=bool(body.get("triage")),
            idempotency_key=body.get("idempotency_key"),
        )
    _feed("created", tid, None, f"Task created: {title}")
    if body.get("activity") or body.get("progress") is not None or body.get("repo_url") or body.get("log_url"):
        _upsert_meta(tid, body)
    _bump_version()
    return {"ok": True, "task_id": tid}

def api_promote(task_id: str) -> dict:
    with _kb_lock, _kconn() as conn:
        t = _require_task(conn, task_id)
        if t.status not in ("todo", "blocked", "triage"):
            raise ApiError(409, f"cannot promote from {t.status}")
        kb.promote_task(conn, task_id, reason="promoted via Mission Control API")
    _feed("promoted", task_id, None, "Moved to READY")
    _bump_version()
    return {"ok": True}

def api_assign(task_id: str, assignee: str) -> dict:
    with _kb_lock, _kconn() as conn:
        _require_task(conn, task backlog_id := task_id)
        kb.assign_task(conn, task_id, assignee or None)
    _feed("assigned", task_id, assignee, f"Assigned to {assignee or 'nobody'}")
    _bump_version()
    return {"ok": True}

def api_claim(task_id: str, claimer: str) -> dict:
    """Agent starts work: ready -> running, records claim + started time."""
    with _kb_lock, _kconn() as conn:
        t = _require_task(conn, task_id)
        claimed = kb.claim_task(conn, task_id, claimer=claimer)
        if not claimed:
            raise ApiError(409, f"task {task_id} not claimable (status={t.status})")
    _feed("started", task_id, claimer, f"{claimer} started work")
    _bump_version()
    return {"ok": True, "task": _public_task(task_id)}

def api_review(task_id: str, body: dict) -> dict:
    with _kb_lock, _kconn() as conn:
        _require_task(conn, task_id)
        kb.request_review(conn, task_id, summary=body.get("summary"))
    _feed("review", task_id, None, "Submitted for review")
    _bump_version()
    return {"ok": True}

def api_complete(task_id: str, body: dict) -> dict:
    with _kb_lock, _kconn() as conn:
        _require_task(conn, task_id)
        ok = kb.complete_task(conn, task_id, result=body.get("result"),
                              summary=body.get("summary"))
        if not ok:
            raise ApiError(409, "complete failed")
    _feed("done", task_id, None, f"Completed: {body.get('result') or 'ok'}")
    _bump_version()
    return {"ok": True}

def api_block(task_id: str, body: dict) -> dict:
    with _kb_lock, _kconn() as conn:
        _require_task(conn, task_id)
        ok = kb.block_task(conn, task_id, reason=body.get("reason"), kind=body.get("kind"))
        if not ok:
            raise ApiError(409, "block failed")
    _feed("blocked", task_id, None, f"Blocked: {body.get('reason') or 'no reason given'}")
    _bump_version()
    return {"ok": True}

def api_unblock(task_id: str) -> dict:
    with _kb_lock, _kconn() as conn:
        _require_task(conn, task_id)
        ok = kb.unblock_task(conn, task_id)
        if not ok:
            raise ApiError(409, "unblock failed")
    _feed("unblocked", task_id, None, "Unblocked")
    _buff_version_safe()
    return {"ok": True}

def api_heartbeat(task_id: str, body: dict) -> dict:
    with _kb_lock, _kconn() as conn:
        _require_task(conn, task_id)
        kb.heartbeat_worker(conn, task_id, note=body.get("note"))
    if body.get("activity"):
        _upsert_meta(task_id, {"activity": body.get("activity")})
    _feed("heartbeat", task_id, body.get("agent"), body.get("note") or "heartbeat")
    _bump_version()
    return {"ok": True}

def api_meta(task_id: str, body: dict) -> dict:
    _require_task_exists(task_id)
    _upsert_meta(task_id, body)
    _bump_version()
    return {"ok":}

def api_retry(task_id: str) -> dict:
    """Failure path: unblock-or-promote back to ready for reassignment."""
    with _kb_lock, _kconn() as conn:
        t = _require_task(conn, task_id)
        if t.status == "blocked":
            kb.unblock_task(conn, task_id)
        else:
            raise ApiError(409, f"retry expects blocked task, got {t.status}")
    _feed("retry", task_id, None, "Requeued for retry")
    _bump_version()
    return {"ok": True}

def api_comment(task_id: str, body: dict) -> dict:
    text = (body.get("text") or "").strip()
    if not text:
        raise ApiError(400, "text required")
    with _kb_lock, _kconn() as conn:
        _require_task(conn, task_id)
        kb.add_comment(conn, task_id, body=text, author=body.get("author") or "mission-control")
    _feed("comment", task_id, body.get("author"), text[:120])
    _bump_version()
    return {"ok": True}

def api_agent_upsert(body: dict) -> dict:
    aid = (body.get("agent_id") or body.get("name") or "").strip()
    if not aid:
        raise ApiError(400, "agent_id or name required")
    with _sid_lock, _sidecar() as con:
        row = con.execute("SELECT * FROM agents WHERE agent_id=?", (aid,)).fetchone()
        now = int(time.time())
        if row is None:
            con.execute(
                "INSERT INTO agents(agent_id, name, role, status, task_id, activity, started_at, last_heartbeat) "
                "VALUES (?,?,?,?,?,?,?,?)",
                (aid, body.get("name", aid), body.get("role"), body.get("status", "working"),
                 body.get("task_id"), body.get("activity"), body.get("started_at") or now, now),
            )
            created = True
        else:
            con.execute(
                "UPDATE agents SET name=?, role=?, status=?, task_id=?, activity=?, last_heartbeat=? WHERE agent_id=?",
                (body.get("name", row["name"]), body.get("role", row["role"]),
                 body.get("status", row["status"]), body.get("task_id", row["task_id"]),
                 body.get("activity", row["activity"]), now, aid),
            )
            created = False
    _feed("agent", body.get("task_id"), aid, f"Agent {aid}: {body.get('status') or 'heartbeat'}")
    _bump_version()
    return {"ok": True, "created": created}

def _require_task_exists(task_id: str) -> None:
    with _kb_lock, _kconn() as conn:
        _require_task(conn, task_id)

def _upsert_meta(task_id: str, body: dict) -> None:
    fields = {}
    for k in ("activity", "repo_url", "log_url", "agent_role", "est_seconds"):
        if body.get(k) is not None:
            fields[k] = body[k]
    if body.get("progress") is not None:
        try:
            fields["progress"] = max(0.0, min(100.0, float(body["progress"])))
        except (TypeError, ValueError):
            raise ApiError(400, "progress must be a number 0-100")
    if not fields:
        return
    fields["updated_at"] = int(time.time())
    with _sid_lock, _sidecar() as con:
        con.execute(
            "INSERT INTO task_meta(task_id, {cols}) VALUES ({qs}) "
            "ON CONFLICT(task_id) DO UPDATE SET {sets}".format(
                cols=", ".join(fields), qs=", ".join("?" * len(fields)),
                sets=", ".join(f"{k}=excluded.{k}" for k in fields),
            ),
            [task_id] + list(fields.values()),
        )

def _public_task(task_id: str) -> Optional[dict]:
    snap = None
    try:
        with _kb_lock, _kconn() as conn:
            t = kb.get_task(conn, task_id)
            if not t:
                return None
            return {"id": t.id, "status": t.status, "title": t.title, "assignee": t.assignee}
    except Exception:
        return None

# ---------------------------------------------------------------- SSE hub
_version = 0
_version_lock = threading.Lock()
_sse_clients: list = []
_sse_lock = threading.Lock()

def _bump_version() -> None:
    global _version
    with _version_lock:
        _version += 1
    _push_snapshot_to_all()

def _buff_version_safe() -> None:  # typo-safe alias; both bump
    _bump_version()

def _push_snapshot_to_all() -> None:
    with _sse_lock:
        clients = list(_sse_clients)
    if not clients:
        return
    try:
        snap = _snapshot()
    except Exception:
        return
    payload = json.dumps(snap, default=str)
    for q in clients:
        try:
            q.put_nowait(payload)
        except Exception:
            pass

# ---------------------------------------------------------------- HTTP handler
class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "MissionControl/1.0"

    # ---- helpers
    def _json(self, code: int, obj: Any) -> None:
        data = json.dumps(obj, default=str).encode()
        self.send_response(code)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(data)

    def _authed(self) -> bool:
        if TOKEN is None:
            return True  # no token configured → local-only trust
        h = self.headers.get("Authorization", "")
        return bool(h) and secrets.compare_digest(h, f"Bearer {TOKEN}")

    def log_message(self, fmt, *args):  # quiet
        pass

    # ---- routes
    def do_GET(self) -> None:
        u = urlparse(self.path)
        p = u.path.rstrip("/") or "/"
        try:
            if p == "/health":
                self._json(200, {"ok": True, "ts": int(time.time()), "board": BOARD})
            elif p == "/api/snapshot":
                self._json(200, _snapshot())
            elif p == "/api/feed":
                with _sidecar() as con:
                    rows = [dict(r) for r in con.execute(
                        "SELECT * FROM feed ORDER BY id DESC LIMIT 200")]
                self._json(200, {"feed": rows})
            elif p == "/api/agents":
                with _sidecar() as con:
                    rows = [dict(r) for r in con.execute("SELECT * FROM agents")]
                self._json(200, {"agents": rows})
            elif p == "/api/task" and u.query:
                qs = parse_qs(u.query)
                tid = (qs.get("id") or [""])[0]
                if not tid:
                    self._json(400, {"error": "id param required"})
                    return
                with _kb_lock, _kconn() as conn:
                    t = kb.get_task(conn, tid)
                    if not t:
                        self._json(404, {"error": "not found"})
                        return
                    comments = [dict(c._asdict()) if hasattr(c, "_asdict") else dict(c)
                                for c in kb.list_comments(conn, tid)]
                    events = [{"id": e.id, "kind": e.kind, "created_at": e.created_at,
                               "payload": e.payload} for e in kb.list_events(conn, tid)]
                self._json(200, {"task": _public_task(tid), "comments": comments, "events": events})
            elif p == "/events":
                self._sse()
            elif p == "/" or p == "/index.html":
                self._serve_file("index.html", "text/html; charset=utf-8")
            elif p == "/app.js":
                self._serve_file("app.js", "application/javascript")
            elif p == "/style.css":
                self._serve static_file("style.css", "text/css")
            else:
                self._json(404, {"error": "not found", "path": p})
        except ApiError as e:
            self._json(e.status, {"error": e.msg})
        except sqlite3.Error as e:
            self._json(500, {"error": f"db error: {e}"})
        except BrokenPipeError:
            pass
        except Exception as e:
            traceback.print_exc()
            try:
                self._json(500, {"error": str(e)})
            except Exception:
                pass

    def do_POST(self) -> None:
        u = urlparse(self.path)
        p = u.path.rstrip("/")
        if not self._authed():
            self._json(401, {"error": "unauthorized — Bearer token required for writes"})
            return
        try:
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length) if length else b"{}"
            try:
                body = json.loads(raw or b"{}")
            except json.JSONDecodeError:
                self._json(400, {"error": "invalid JSON"})
                return
            if p == "/api/tasks":
                self._json(200, api_create_task(body))
            elif p.startswith("/api/task/"):
                parts = p.split("/")
                tid = parts[3] if len(parts) > 3 else ""
                action = parts[4] if len(parts) > 4 else ""
                if not tid:
                    self._json(400, {"error": "task id required"})
                    return
                if action == "promote":
                    self._json(200, api_promote(tid))
                elif action == "assign":
                    self._json(200, api_assign(tid, body.get("assignee") or body.get("profile")))
                elif action == "claim":
                    self._json(200, api_claim(tid, body.get("agent") or "api-agent"))
                elif action == "review":
                    self._json(200, api_review(tid, body))
                elif action == "complete":
                    self._json(200, api_complete(tid, body))
                elif action == "block":
                    self._json(200, api_block(tid, body))
                elif action == "unblock":
                    self._json(200, api_unblock(tid))
                elif action == "heartbeat":
                    self._json(200, api_heartbeat(tid, body))
                elif action == "meta":
                    self._json(200, api_meta(tid, body))
                elif action == "retry":
                    self._json(200, api_retry(tid))
                elif action == "comment":
                    self._json(200, api_comment(tid, body))
                else:
                    self._json(404, {"error": f"unknown action {action!r}"})
            elif p == "/api/agents":
                self._json(200, api_agent_upsert(body))
            else:
                self._json(404, {"error": "not found", "path": p})
        except ApiError as e:
            self._json(e.status, {"error": e.msg})
        except sqlite3.Error as e:
            self._json(500, {"error": f"db error: {e}"})
        except BrokenPipeError:
            pass
        background_tasks_notify = None
        except Exception as e:
            traceback.print_exc()
            try:
                self._json(500, {"error": str(e)})
            except Exception:
                pass

    # ---- static files
    def _serve_file(self, name: str, ctype: str) -> None:
        f = APP_DIR / name
        if not f.exists():
            self._json(404, {"error": f"{name} missing"})
            return
        data = f.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(data)))
        self.send_header("Cache-Control", "no-cache")
        self.end_headers()
        self.wfile.write(data)

    def _serve_static_file(self, name: str, ctype: str) -> None:
        self._serve_file(name, ctype)

    # ---- SSE
    def _sse(self) -> None:
        import queue
        q: queue.Queue = queue.Queue(maxsize=8)
        with _sse_lock:
            _sse_clients.append(q)
        try:
            self.send_response(200)
            self.send_header("Content-Type", "text/event-stream")
            self._sse_stream(q)
        finally:
            with _sse_lock:
                try:
                    _sse_clients.remove(q)
                except ValueError:
                    pass

    def _sse_stream(self, q) -> None:
        import queue
        self.send_header("Cache-Control", "no-store")
        self.send_header("Connection", "keep-alive")
        self.end_headers()
        # initial snapshot
        try:
            snap = _snapshot()
            self._sse_write(json.dumps(snap, default=str))
        except Exception as e:
            self._sse_write(json.dumps({"error": str(e)}))
        last_hash = None
        deadline = time.time() + 3600
        while time.time() < deadline:
            try:
                payload = q.get(timeout=15)
            except queue.Empty:
                # heartbeat comment keeps proxies alive
                self.wfile.write(b": hb\n\n")
                self.wfile.flush()
                continue
            h = _snapshot_hash(json.loads(payload)) if payload.startswith("{") else None
            if h and h == last_hash:
                continue
            last_hash = h
            self._sse_write(payload)
        self.close_connection = True

    def _sse_write(self, data: str) -> None:
        self.wfile.write(f"data: {data}\n\n".encode())
        self.wfile.flush()


def main() -> None:
    httpd = ThreadingHTTPServer((BIND, PORT), Handler)
    httpd.daemon_threads = True
    print(f"[mission-control] board={BOARD} listening on {BIND}:{PORT} (token={'yes' if TOKEN else 'NONE — local trust'})", flush=True)
    try:
        httpd.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
