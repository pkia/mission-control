#!/usr/bin/env python3
"""HERMES MISSION CONTROL — view + state/API layer over the native Hermes kanban board.

Architecture (see .design.json):
  - Board of record = ~/.hermes/kanban.db (Hermes' own durable board; dispatcher lives in the gateway).
  - This service NEVER writes kanban.db with raw SQL — all board mutations go through
    hermes_cli.kanban_db in-process (the exact code path the `hermes kanban` CLI uses).
  - Sidecar telemetry.db holds what the native schema lacks: live activity strings,
    progress %, repo/log links, standalone agent heartbeats.
  - One watcher thread snapshots the board every 1.5s; SSE clients are woken via a
    Condition and receive the latest snapshot only when its content hash changed.

Auth: reads unauthenticated on the LAN (same trust as hermes-webui :8787);
EVERY write requires `Authorization: Bearer $MC_TOKEN`.

Run: python3 server.py   (port 8788; systemd unit mission-control.service)
"""
from __future__ import annotations

import hashlib
import json
import os
import queue
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

COLUMN_ORDER = ["BACKLOG", "READY", "IN PROGRESS", "REVIEW", "BLOCKED", "DONE"]
COLUMN_MAP = {
    "triage": "BACKLOG", "todo": "BACKLOG", "scheduled": "BACKLOG",
    "ready": "READY", "running": "IN PROGRESS",
    "review": "REVIEW", "blocked": "BLOCKED",
    "done": "DONE", "archived": "DONE",
}
PRIORITY_LABEL = {3: "critical", 2: "high", 1: "normal", 0: "normal", -1: "low"}
PRIORITY_FROM_LABEL = {"critical": 3, "high": 2, "normal": 0, "low": -1}
HEARTBEAT_STALE_S = 300

# ---------------------------------------------------------------- token


def _load_token() -> Optional[str]:
    for p in (APP_DIR / ".env", Path(HERMES_ROOT) / ".env"):
        try:
            for line in p.read_text().splitlines():
                line = line.strip()
                if line.startswith("MC_TOKEN=") and len(line) > len("MC_TOKEN="):
                    return line.split("=", 1)[1].strip().strip('"').strip("'")
        except OSError:
            continue
    return None


TOKEN = _load_token()

# ---------------------------------------------------------------- sidecar (telemetry.db)

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

# ---------------------------------------------------------------- kanban access

_kb_lock = threading.Lock()


def _kconn() -> sqlite3.Connection:
    conn = kb.connect(board=BOARD)
    conn.row_factory = sqlite3.Row
    return conn


def _age(ts: Optional[int]) -> Optional[int]:
    return int(time.time()) - ts if ts else None


class ApiError(Exception):
    def __init__(self, status: int, msg: str):
        super().__init__(msg)
        self.status = status
        self.msg = msg


# ---------------------------------------------------------------- snapshot

def _snapshot() -> dict:
    """Full board snapshot: tasks + agents + feed + summary."""
    with _kb_lock:
        conn = _kconn()
        try:
            tasks_rows = kb.list_tasks(conn, include_archived=False)
            links = [(r["parent_id"], r["child_id"]) for r in conn.execute(
                "SELECT parent_id, child_id FROM task_links")]
            runs = {r["id"]: dict(r) for r in conn.execute(
                "SELECT * FROM task_runs WHERE ended_at IS NULL")}
        finally:
            conn.close()

    children_of: dict[str, list[str]] = {}
    parents_of: dict[str, list[str]] = {}
    for pid, cid in links:
        children_of.setdefault(pid, []).append(cid)
        parents_of.setdefault(cid, []).append(pid)

    with _sidecar() as con:
        meta_rows = {r["task_id"]: dict(r) for r in con.execute("SELECT * FROM task_meta")}
        agent_rows = [dict(r) for r in con.execute(
            "SELECT * FROM agents ORDER BY COALESCE(last_heartbeat, started_at) DESC")]
        feed_rows = [dict(r) for r in con.execute(
            "SELECT * FROM feed ORDER BY id DESC LIMIT 60")]

    tasks = []
    for t in tasks_rows:
        tid = t.id
        m = meta_rows.get(tid, {})
        run = runs.get(t.current_run_id) if t.current_run_id else None
        status = t.status
        last_update = t.last_heartbeat_at or t.started_at or t.created_at
        tasks.append({
            "id": tid,
            "title": t.title,
            "body": t.body,
            "status": status,
            "column": COLUMN_MAP.get(status, "BACKLOG"),
            "priority": t.priority,
            "priority_label": PRIORITY_LABEL.get(t.priority, "normal"),
            "assignee": t.assignee,
            "created_by": t.created_by,
            "created_at": t.created_at,
            "started_at": t.started_at,
            "completed_at": t.completed_at,
            "runtime_s": _age(t.started_at) if t.started_at and status == "running" else None,
            "last_update": last_update,
            "last_update_age_s": _age(last_update),
            "heartbeat_age_s": _age(t.last_heartbeat_at),
            "consecutive_failures": t.consecutive_failures,
            "last_error": t.last_failure_error,
            "workspace": t.workspace_path,
            "session_id": t.session_id,
            "result": t.result,
            "worker_pid": t.worker_pid,
            "run": {"id": run["id"], "profile": run["profile"],
                    "started_at": run["started_at"]} if run else None,
            "activity": m.get("activity"),
            "progress": m.get("progress"),
            "repo_url": m.get("repo_url"),
            "log_url": m.get("log_url"),
            "agent_role": m.get("agent_role"),
            "est_seconds": m.get("est_seconds"),
            "deps": children_of.get(tid, []),
            "parents": parents_of.get(tid, []),
        })

    col_counts: dict[str, int] = {c: 0 for c in COLUMN_ORDER}
    done_today = 0
    day_start = time.mktime(time.strptime(time.strftime("%Y-%m-%d"), "%Y-%m-%d"))
    for t in tasks:
        col_counts[t["column"]] = col_counts.get(t["column"], 0) + 1
        if t["status"] == "done" and t["completed_at"] and t["completed_at"] >= day_start:
            done_today += 1

    now = time.time()
    active_agents = [a for a in agent_rows
                     if a["status"] == "working" and a["last_heartbeat"]
                     and now - a["last_heartbeat"] < HEARTBEAT_STALE_S]

    return {
        "generated_at": int(now),
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
    }


_VOLATILE_SKIP = {"age_s", "runtime_s", "last_update_age_s", "heartbeat_age_s", "generated_at"}


def _snapshot_hash(snap: dict) -> str:
    """Hash the stable parts so runtime tickers don't spam SSE every poll."""
    stable = {
        "tasks": [{k: v for k, v in t.items() if k not in _VOLATILE_SKIP}
                  for t in snap["tasks"]],
        "agents": snap["agents"],
        "feed_ids": [f["id"] for f in snap["feed"][:10]],
        "summary": snap["summary"],
    }
    return hashlib.sha256(json.dumps(stable, default=str, sort_keys=True).encode()).hexdigest()


# ---------------------------------------------------------------- watcher + SSE hub

class Hub:
    """One watcher thread polls the board; SSE clients wait on a Condition."""

    def __init__(self, interval: float = 1.5):
        self.interval = interval
        self.cond = threading.Condition()
        self.latest: Optional[str] = None   # JSON payload
        self.epoch = 0
        self.last_hash: Optional[str] = None
        self.error: Optional[str] = None
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="mc-watcher")

    def start(self) -> None:
        self._thread.start()

    def force_refresh(self) -> None:
        """Called after a write — refresh immediately instead of waiting the interval."""
        self._poll()

    def _poll(self) -> None:
        try:
            snap = _snapshot()
            self.error = None
        except Exception as e:  # board read failed — keep last good snapshot
            self.error = str(e)
            return
        h = _snapshot_hash(snap)
        if h == self.last_hash and self.latest is not None:
            # still refresh runtime tickers at most every ~5s
            if not getattr(self, "_last_tick", None) or time.time() - self._last_tick > 5:
                self._last_tick = time.time()
                self.latest = json.dumps(snap, default=str)
                with self.cond:
                    self.epoch += 1
                    self.cond.notify_all()
            return
        self.last_hash = h
        self._last_tick = time.time()
        self.latest = json.dumps(snap, default=str)
        with self.cond:
            self.epoch += 1
            self.cond.notify_all()

    def _loop(self) -> None:
        while not self._stop.wait(self.interval):
            self._poll()


HUB = Hub()
HUB.start()

# ---------------------------------------------------------------- write API

def _feed(kind: str, task_id: Optional[str], agent: Optional[str], text: str) -> None:
    with _sid_lock, _sidecar() as con:
        con.execute(
            "INSERT INTO feed(ts, kind, task_id, agent, text) VALUES (?,?,?,?,?)",
            (int(time.time()), kind, task_id, agent, text),
        )


def _require_task(conn, task_id: str):
    t = kb.get_task(conn, task_id)
    if not t:
        raise ApiError(404, f"task {task_id} not found")
    return t


def _bump() -> None:
    HUB.force_refresh()


def api_create_task(body: dict) -> dict:
    title = (body.get("title") or "").strip()
    if not title:
        raise ApiError(400, "title required")
    priority = body.get("priority", 0)
    if isinstance(priority, str):
        priority = PRIORITY_FROM_LABEL.get(priority.strip().lower(), 0)
    with _kb_lock, _kconn() as conn:
        tid = kb.create_task(
            conn,
            title=title,
            body=body.get("description") or body.get("body"),
            assignee=body.get("assignee"),
            created_by=body.get("created_by") or "mission-control",
            priority=int(priority),
            parents=body.get("depends_on") or (),
            # default: park in BACKLOG (triage) until an agent promotes/claims;
            # callers can opt out with "ready": true
            triage=bool(body.get("triage", not body.get("ready"))),
            idempotency_key=body.get("idempotency_key"),
        )
    _upsert_meta(tid, body)
    _feed("created", tid, None, f"Task created: {title}")
    _bump()
    return {"ok": True, "task_id": tid}


def api_promote(task_id: str) -> dict:
    with _kb_lock, _kconn() as conn:
        t = _require_task(conn, task_id)
        if t.status == "triage":
            # triage -> todo (flesh-out path), then todo -> ready
            kb.specify_triage_task(conn, task_id, assignee=t.assignee)
            t = _require_task(conn, task_id)
        if t.status == "ready":
            pass  # already there — idempotent success
        elif t.status not in ("todo", "blocked"):
            raise ApiError(409, f"cannot promote from {t.status}")
        else:
            ok, why = kb.promote_task(conn, task_id, actor="mission-control",
                                      reason="promoted via Mission Control API")
            if not ok:
                # re-fetch: the dispatcher's recompute_ready may have beaten us
                t2 = _require_task(conn, task_id)
                if t2.status != "ready":
                    raise ApiError(409, why or "promote refused")
    _feed("promoted", task_id, None, "Moved to READY")
    _bump()
    return {"ok": True}


def api_assign(task_id: str, assignee: str) -> dict:
    with _kb_lock, _kconn() as conn:
        _require_task(conn, task_id)
        kb.assign_task(conn, task_id, (assignee or "").strip() or None)
    _feed("assigned", task_id, assignee, f"Assigned to {assignee or 'nobody'}")
    _bump()
    return {"ok": True}


def api_claim(task_id: str, claimer: str) -> dict:
    with _kb_lock, _kconn() as conn:
        t = _require_task(conn, task_id)
        if t.status == "triage":
            kb.specify_triage_task(conn, task_id, assignee=claimer)
            t = _require_task(conn, task_id)
        if t.status == "todo":
            # agent picked it up from the backlog — promote to ready first
            ok, why = kb.promote_task(conn, task_id, actor=claimer,
                                      reason="claimed via Mission Control")
            if not ok:
                raise ApiError(409, why or "promote refused")
        claimed = kb.claim_task(conn, task_id, claimer=claimer)
        if not claimed:
            raise ApiError(409, f"task {task_id} not claimable (status={t.status})")
    _feed("started", task_id, claimer, f"{claimer} started work")
    _bump()
    return {"ok": True, "task_id": task_id, "status": "running"}


def api_review(task_id: str, body: dict) -> dict:
    with _kb_lock, _kconn() as conn:
        _require_task(conn, task_id)
        # force=True: API callers are the operator/orchestrator, not the live
        # worker — the native guard wants run ownership we don't hold.
        ok = kb.request_review(conn, task_id, summary=body.get("summary"), force=True)
        if not ok:
            raise ApiError(409, "review failed (parents unsatisfied?)")
    _feed("review", task_id, None, "Submitted for review")
    _bump()
    return {"ok": True}


def api_complete(task_id: str, body: dict) -> dict:
    with _kb_lock, _kconn() as conn:
        _require_task(conn, task_id)
        ok = kb.complete_task(conn, task_id, result=body.get("result"),
                              summary=body.get("summary"))
        if not ok:
            raise ApiError(409, "complete failed")
    _feed("done", task_id, None, f"Completed: {body.get('result') or 'ok'}")
    _bump()
    return {"ok": True}


def api_block(task_id: str, body: dict) -> dict:
    with _kb_lock, _kconn() as conn:
        t = _require_task(conn, task_id)
        if t.status == "triage":
            kb.specify_triage_task(conn, task_id)
            t = _require_task(conn, task_id)
        if t.status == "todo":
            ok, why = kb.promote_task(conn, task_id, actor="mission-control",
                                      reason="promoted for blocking")
            if not ok:
                raise ApiError(409, why or "promote refused")
        ok = kb.block_task(conn, task_id, reason=body.get("reason"), kind=body.get("kind"))
        if not ok:
            raise ApiError(409, "block failed")
    _feed("blocked", task_id, None, f"Blocked: {body.get('reason') or 'no reason given'}")
    _bump()
    return {"ok": True}


def api_unblock(task_id: str) -> dict:
    with _kb_lock, _kconn() as conn:
        _require_task(conn, task_id)
        ok = kb.unblock_task(conn, task_id)
        if not ok:
            raise ApiError(409, "unblock failed")
    _feed("unblocked", task_id, None, "Unblocked → ready for retry")
    _bump()
    return {"ok": True}


def api_heartbeat(task_id: str, body: dict) -> dict:
    with _kb_lock, _kconn() as conn:
        _require_task(conn, task_id)
        kb.heartbeat_worker(conn, task_id, note=body.get("note"))
    meta = {}
    if body.get("activity"):
        meta["activity"] = body["activity"]
    if body.get("progress") is not None:
        meta["progress"] = body["progress"]
    if meta:
        _upsert_meta(task_id, meta)
    _feed("heartbeat", task_id, body.get("agent"), body.get("note") or body.get("activity") or "heartbeat")
    _bump()
    return {"ok": True}


def api_meta(task_id: str, body: dict) -> dict:
    with _kb_lock, _kconn() as conn:
        _require_task(conn, task_id)
    _upsert_meta(task_id, body)
    _bump()
    return {"ok": True}


def api_retry(task_id: str) -> dict:
    with _kb_lock, _kconn() as conn:
        t = _require_task(conn, task_id)
        if t.status != "blocked":
            raise ApiError(409, f"retry expects a blocked task, got {t.status}")
        kb.unblock_task(conn, task_id)
    _feed("retry", task_id, None, "Requeued for retry")
    _bump()
    return {"ok": True}


def api_comment(task_id: str, body: dict) -> dict:
    text = (body.get("text") or "").strip()
    if not text:
        raise ApiError(400, "text required")
    with _kb_lock, _kconn() as conn:
        _require_task(conn, task_id)
        kb.add_comment(conn, task_id, body=text, author=body.get("author") or "mission-control")
    _feed("comment", task_id, body.get("author"), text[:120])
    _bump()
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
                "INSERT INTO agents(agent_id, name, role, status, task_id, activity,"
                " started_at, last_heartbeat) VALUES (?,?,?,?,?,?,?,?)",
                (aid, body.get("name", aid), body.get("role"), body.get("status", "working"),
                 body.get("task_id"), body.get("activity"), body.get("started_at") or now, now))
            created = True
        else:
            con.execute(
                "UPDATE agents SET name=?, role=?, status=?, task_id=?, activity=?,"
                " last_heartbeat=? WHERE agent_id=?",
                (body.get("name", row["name"]), body.get("role", row["role"]),
                 body.get("status", row["status"]), body.get("task_id", row["task_id"]),
                 body.get("activity", row["activity"]), now, aid))
            created = False
    _feed("agent", body.get("task_id"), aid,
          f"Agent {aid}: {body.get('status') or 'heartbeat'} — {body.get('activity') or ''}".strip(" —"))
    _bump()
    return {"ok": True, "created": created}


def _upsert_meta(task_id: str, body: dict) -> None:
    fields: dict[str, Any] = {}
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
                cols=", ".join(fields), qs=", ".join("?" * (len(fields) + 1)),
                sets=", ".join(f"{k}=excluded.{k}" for k in fields)),
            [task_id] + list(fields.values()))


# ---------------------------------------------------------------- HTTP handler

class Handler(BaseHTTPRequestHandler):
    protocol_version = "HTTP/1.1"
    server_version = "MissionControl/1.0"

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
            return False  # writes disabled entirely when no token is configured
        h = self.headers.get("Authorization", "")
        return bool(h) and secrets.compare_digest(h, f"Bearer {TOKEN}")

    def log_message(self, fmt, *args):
        pass

    # ---- GET
    def do_GET(self) -> None:
        u = urlparse(self.path)
        p = u.path.rstrip("/") or "/"
        try:
            if p == "/health":
                self._json(200, {"ok": True, "ts": int(time.time()), "board": BOARD,
                                 "watcher_error": HUB.error})
            elif p == "/api/snapshot":
                if HUB.latest and not HUB.error:
                    self.send_response(200)
                    self.send_header("Content-Type", "application/json")
                    b = HUB.latest.encode()
                    self.send_header("Content-Length", str(len(b)))
                    self.send_header("Cache-Control", "no-store")
                    self.end_headers()
                    self.wfile.write(b)
                else:
                    self._json(200, _snapshot())
            elif p == "/api/feed":
                with _sidecar() as con:
                    rows = [dict(r) for r in con.execute(
                        "SELECT * FROM feed ORDER BY id DESC LIMIT 200")]
                self._json(200, {"feed": rows})
            elif p == "/api/agents":
                with _sidecar() as con:
                    rows = [dict(r) for r in con.execute(
                        "SELECT * FROM agents ORDER BY last_heartbeat DESC")]
                self._json(200, {"agents": rows})
            elif p == "/api/task" and u.query:
                self._task_detail(parse_qs(u.query).get("id", [""])[0])
            elif p == "/events":
                self._sse()
            elif p in ("/", "/index.html"):
                self._serve_file("index.html", "text/html; charset=utf-8")
            elif p == "/app.js":
                self._serve_file("app.js", "application/javascript")
            elif p == "/style.css":
                self._serve_file("style.css", "text/css")
            else:
                self._json(404, {"error": "not found", "path": p})
        except (BrokenPipeError, ConnectionResetError):
            pass
        except ApiError as e:
            self._safe_json(e.status, {"error": e.msg})
        except sqlite3.Error as e:
            self._safe_json(500, {"error": f"db error: {e}"})
        except Exception as e:
            traceback.print_exc()
            self._safe_json(500, {"error": str(e)})

    def _task_detail(self, tid: str) -> None:
        if not tid:
            self._json(400, {"error": "id param required"})
            return
        with _kb_lock, _kconn() as conn:
            t = kb.get_task(conn, tid)
            if not t:
                self._json(404, {"error": "not found"})
                return
            comments = []
            for c in kb.list_comments(conn, tid):
                d = c.__dict__.copy() if hasattr(c, "__dict__") else {}
                comments.append(d)
            events = [{"id": e.id, "kind": e.kind, "created_at": e.created_at,
                       "payload": e.payload} for e in kb.list_events(conn, tid)]
        task = {k: getattr(t, k) for k in
                ("id", "title", "body", "status", "assignee", "priority", "result",
                 "created_at", "started_at", "completed_at", "last_heartbeat_at",
                 "consecutive_failures", "last_failure_error", "workspace_path", "session_id")}
        self._json(200, {"task": task, "comments": comments, "events": events})

    # ---- POST
    def do_POST(self) -> None:
        u = urlparse(self.path)
        p = u.path.rstrip("/")
        if not self._authed():
            self._json(401, {"error": "unauthorized — Bearer token required for writes"})
            return
        try:
            length = int(self.headers.get("Content-Length") or 0)
            raw = self.rfile.read(length) if length > 0 else b"{}"
            try:
                body = json.loads(raw or b"{}")
                if not isinstance(body, dict):
                    raise ValueError("body must be a JSON object")
            except (json.JSONDecodeError, ValueError) as e:
                self._json(400, {"error": f"invalid JSON: {e}"})
                return

            if p == "/api/tasks":
                self._json(200, api_create_task(body))
                return
            if p == "/api/agents":
                self._json(200, api_agent_upsert(body))
                return
            parts = p.split("/")  # /api/task/<id>/<action>
            if len(parts) >= 4 and parts[1] == "api" and parts[2] == "task":
                tid = parts[3]
                action = parts[4] if len(parts) > 4 else ""
                if not tid:
                    self._json(400, {"error": "task id required"})
                elif action == "promote":
                    self._json(200, api_promote(tid))
                elif action == "assign":
                    self._json(200, api_assign(tid, body.get("assignee") or body.get("profile") or ""))
                elif action == "claim":
                    self._json(200, api_claim(tid, body.get("agent") or body.get("claimer") or "api-agent"))
                elif action == "review":
                    self._json(200, api_review(tid, body))
                elif action == "complete":
                    self._json(200, api_complete(tid, body))
                elif action == "block":
                    self._json(200, api_block(tid, body))
                elif action == "unblock":
                    self._json(200, api_unblock(tid))
                elif action == "retry":
                    self._json(200, api_retry(tid))
                elif action == "heartbeat":
                    self._json(200, api_heartbeat(tid, body))
                elif action == "meta":
                    self._json(200, api_meta(tid, body))
                elif action == "comment":
                    self._json(200, api_comment(tid, body))
                else:
                    self._json(404, {"error": f"unknown action {action!r}"})
                return
            self._json(404, {"error": "not found", "path": p})
        except (BrokenPipeError, ConnectionResetError):
            pass
        except ApiError as e:
            self._safe_json(e.status, {"error": e.msg})
        except sqlite3.Error as e:
            self._safe_json(500, {"error": f"db error: {e}"})
        except Exception as e:
            traceback.print_exc()
            self._safe_json(500, {"error": str(e)})

    def _safe_json(self, code: int, obj: Any) -> None:
        try:
            self._json(code, obj)
        except Exception:
            pass

    # ---- static
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

    # ---- SSE
    def _sse(self) -> None:
        self.send_response(200)
        self.send_header("Content-Type", "text/event-stream")
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        my_epoch = -1
        deadline = time.time() + 3600
        while time.time() < deadline:
            with HUB.cond:
                if HUB.epoch == my_epoch:
                    HUB.cond.wait(timeout=15)
                if HUB.epoch != my_epoch and HUB.latest:
                    my_epoch = HUB.epoch
                    payload = HUB.latest
                else:
                    payload = None
            if payload:
                self.wfile.write(f"data: {payload}\n\n".encode())
                self.wfile.flush()
            else:
                self.wfile.write(b": hb\n\n")
                self.wfile.flush()
        self.close_connection = True


def main() -> None:
    httpd = ThreadingHTTPServer((BIND, PORT), Handler)
    httpd.daemon_threads = True
    print(f"[mission-control] board={BOARD} listening on {BIND}:{PORT} "
          f"(writes: {'bearer token' if TOKEN else 'DISABLED — set MC_TOKEN in .env'})",
          flush=True)
    try:
        httpd.serve_forever(poll_interval=0.5)
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
