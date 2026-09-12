#!/usr/bin/env python3
"""mc — Mission Control helper CLI.

Lets Hermes agents (and humans) drive the board without remembering curl flags.
All writes hit the Mission Control HTTP API (bearer-token auth).

Usage:
  mc create "Title" [--desc TEXT] [--assignee NAME] [--priority high] [--depends-on ID ...]
  mc start   <task-id> [--agent NAME]          # claim: ready -> running
  mc activity <task-id> "what I'm doing now"   # heartbeat + activity string
  mc progress <task-id> 65                     # completion %
  mc review  <task-id> [--summary TEXT]        # running -> review
  mc done    <task-id> [--result TEXT]         # -> done
  mc block   <task-id> "reason"                # -> blocked
  mc unblock <task-id>                         # blocked -> ready (retry)
  mc assign  <task-id> <profile|none>
  mc comment <task-id> "text"
  mc agent   <agent-id> [--name N] [--role R] [--status working|idle|error] [--task ID] [--activity "text"]
  mc board                                     # one-line board summary
  mc show    <task-id>                         # task detail JSON
"""
from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.request

APP = os.environ.get("MC_URL", "http://127.0.0.1:8788")
APP_DIR = os.path.dirname(os.path.abspath(__file__))


def _token() -> str:
    tok = os.environ.get("MC_TOKEN", "")
    if tok:
        return tok
    for p in (os.path.join(APP_DIR, ".env"), os.path.expanduser("~/.hermes/.env")):
        try:
            with open(p) as f:
                for line in f:
                    line = line.strip()
                    if line.startswith("MC_TOKEN=") and len(line) > 9:
                        return line.split("=", 1)[1].strip().strip('"').strip("'")
        except OSError:
            continue
    return ""


def _call(method: str, path: str, body: dict | None = None) -> dict:
    url = APP.rstrip("/") + path
    data = json.dumps(body or {}).encode()
    req = urllib.request.Request(url, data=data, method=method)
    req.add_header("Content-Type", "application/json")
    tok = _token()
    if tok:
        req.add_header("Authorization", f"Bearer {tok}")
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return json.loads(r.read() or b"{}")
    except urllib.error.HTTPError as e:
        try:
            err = json.loads(e.read()).get("error", "")
        except Exception:
            err = e.reason
        print(f"error ({e.code}): {err}", file=sys.stderr)
        sys.exit(1)
    except urllib.error.URLError as e:
        print(f"error: cannot reach {url}: {e.reason}", file=sys.stderr)
        sys.exit(2)


def main() -> None:
    ap = argparse.ArgumentParser(prog="mc", description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("create")
    p.add_argument("title")
    p.add_argument("--desc")
    p.add_argument("--assignee")
    p.add_argument("--priority", default="normal")
    p.add_argument("--depends-on", nargs="+", default=[])
    p.add_argument("--repo-url")
    p.add_argument("--log-url")
    p.add_argument("--role")

    p = sub.add_parser("start"); p.add_argument("task"); p.add_argument("--agent", default="hermes")
    p = sub.add_parser("activity"); p.add_argument("task"); p.add_argument("text")
    p = sub.add_parser("progress"); p.add_argument("task"); p.add_argument("pct", type=float)
    p = sub.add_parser("review"); p.add_argument("task"); p.add_argument("--summary")
    p = sub.add_parser("done"); p.add_argument("task"); p.add_argument("--result")
    p = sub.add_parser("block"); p.add_argument("task"); p.add_argument("reason")
    p = sub.add_parser("unblock"); p.add_argument("task")
    p = sub.add_parser("assign"); p.add_argument("task"); p.add_argument("profile")
    p = sub.add_parser("comment"); p.add_argument("task"); p.add_argument("text"); p.add_argument("--author")
    p = sub.add_parser("show"); p.add_argument("task")
    p = sub.add_parser("board")

    p = sub.add_parser("agent")
    p.add_argument("agent_id")
    p.add_argument("--name"); p.add_argument("--role")
    p.add_argument("--status", default="working")
    p.add_argument("--task"); p.add_argument("--activity")

    a = ap.parse_args()
    M = APP  # noqa

    if a.cmd == "create":
        out = _call("POST", "/api/tasks", {
            "title": a.title, "description": a.desc, "assignee": a.assignee,
            "priority": a.priority, "depends_on": a.depends_on,
            "repo_url": a.repo_url, "log_url": a.log_url, "agent_role": a.role})
        print(out.get("task_id") or json.dumps(out))
    elif a.cmd == "start":
        _call("POST", f"/api/task/{a.task}/claim", {"agent": a.agent}); print("ok: running")
    elif a.cmd == "activity":
        _call("POST", f"/api/task/{a.task}/heartbeat", {"activity": a.text}); print("ok")
    elif a.cmd == "progress":
        _call("POST", f"/api/task/{a.task}/meta", {"progress": a.pct}); print("ok")
    elif a.cmd == "review":
        _call("POST", f"/api/task/{a.task}/review", {"summary": a.summary}); print("ok: review")
    elif a.cmd == "done":
        _call("POST", f"/api/task/{a.task}/complete", {"result": a.result}); print("ok: done")
    elif a.cmd == "block":
        _call("POST", f"/api/task/{a.task}/block", {"reason": a.reason}); print("ok: blocked")
    elif a.cmd == "unblock":
        _call("POST", f"/api/task/{a.task}/unblock", {}); print("ok: ready")
    elif a.cmd == "assign":
        _call("POST", f"/api/task/{a.task}/assign", {"assignee": a.profile}); print("ok")
    elif a.cmd == "comment":
        _call("POST", f"/api/task/{a.task}/comment", {"text": a.text, "author": a.author}); print("ok")
    elif a.cmd == "show":
        print(json.dumps(_call("GET", f"/api/task?id={a.task}"), indent=2))
    elif a.cmd == "board":
        s = _call("GET", "/api/snapshot")["summary"]
        print(f"agents={s['active_agents']} inprog={s['in_progress']} ready={s['ready']} "
              f"waiting={s['waiting']} review={s['review']} blocked={s['blocked']} "
              f"done_today={s['done_today']} done_total={s['done_total']}")
    elif a.cmd == "agent":
        _call("POST", "/api/agents", {
            "agent_id": a.agent_id, "name": a.name, "role": a.role,
            "status": a.status, "task_id": a.task, "activity": a.activity}); print("ok")
    else:
        ap.print_help(); sys.exit(2)


if __name__ == "__main__":
    main()
