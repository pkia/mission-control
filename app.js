/* HERMES MISSION CONTROL — front-end (SSE-driven, no deps) */
"use strict";

const COLUMNS = ["BACKLOG", "READY", "IN PROGRESS", "REVIEW", "BLOCKED", "DONE"];
const $ = (id) => document.getElementById(id);

let snap = null;
let openTaskId = null;

/* ---------- formatting ---------- */
function fmtDur(s) {
  if (s == null) return "—";
  s = Math.max(0, Math.floor(s));
  if (s < 60) return s + "s";
  const m = Math.floor(s / 60), h = Math.floor(m / 60);
  if (h > 0) return h + "h " + String(m % 60).padStart(2, "0") + "m";
  return m + "m";
}
function fmtTime(ts) {
  if (!ts) return "—";
  return new Date(ts * 1000).toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
}
function esc(s) {
  return String(s ?? "").replace(/[&<>"']/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
}
function trunc(s, n) { s = String(s ?? ""); return s.length > n ? s.slice(0, n - 1) + "…" : s; }

/* ---------- summary ---------- */
function renderSummary(s) {
  $("s-agents").textContent = s.active_agents;
  $("s-progress").textContent = s.in_progress;
  $("s-waiting").textContent = s.waiting;
  $("s-review").textContent = s.review;
  $("s-blocked").textContent = s.blocked;
  $("s-done").textContent = s.done_today;
  $("s-blocked-wrap").classList.toggle("dim-hide", s.blocked === 0);
}

/* ---------- board ---------- */
function renderBoard() {
  const byCol = {};
  COLUMNS.forEach((c) => (byCol[c] = []));
  (snap.tasks || []).forEach((t) => { (byCol[t.column] = byCol[t.column] || []).push(t); });

  const order = { critical: 0, high: 1, normal: 2, low: 3 };
  Object.values(byCol).forEach((arr) =>
    arr.sort((a, b) =>
      (order[a.priority_label] ?? 2) - (order[b.priority_label] ?? 2) ||
      (b.created_at || 0) - (a.created_at || 0)));

  const board = $("board");
  const colsHtml = COLUMNS.map((col) => {
    const cards = (byCol[col] || []).map(cardHtml).join("");
    return `
      <div class="column" data-col="${esc(col)}">
        <div class="col-head"><span>${esc(col)}</span>
          <span class="count">${(byCol[col] || []).length}</span>
          <span class="bar"></span>
        </div>
        ${cards || ""}
      </div>`;
  }).join("");
  board.innerHTML = colsHtml;
}

function cardHtml(t) {
  const status = t.status || "";
  const cls = ["card"];
  if (status === "running") cls.push("running");
  if (status === "blocked") cls.push("blocked");
  if (status === "review") cls.push("review");
  if (status === "done") cls.push("done");
  if (t.priority_label === "critical" || t.priority_label === "high") cls.push("prio-critical");

  const chips = [];
  if (t.assignee) chips.push(`<span class="chip agent" title="assignee">${esc(t.assignee)}</span>`);
  if (t.agent_role) chips.push(`<span class="chip" >${esc(t.agent_role)}</span>`);
  if (t.priority_label === "critical" || t.priority_label === "high")
    chips.push(`<span class="chip prio ${esc(t.priority_label)}">${esc(t.priority_label)}</span>`);
  if (status === "running" && t.runtime_s != null)
    chips.push(`<span class="chip live">▸ ${fmtDur(t.runtime_s)}</span>`);
  if (t.heartbeat_age_s != null && t.heartbeat_age_s < 300 && status === "running")
    chips.push(`<span class="chip live">♥ ${fmtDur(t.heartbeat_age_s)} ago</span>`);
  if (t.consecutive_failures > 0)
    chips.push(`<span class="chip fail">⚠ ${t.consecutive_failures} fail${t.consecutive_failures > 1 ? "s" : ""}</span>`);
  if (t.repo_url) chips.push(`<span class="chip link"><a href="${esc(t.repo_url)}" target="_blank">repo ↗</a></span>`);
  if (t.log_url) chips.push(`<span class="chip link"><a href="${esc(t.log_url)}" target="_blank">logs ↗</a></span>`);
  if (t.parents && t.parents.length)
    chips.push(`<span class="chip">⇠ ${t.parents.length} dep${t.parents.length > 1 ? "s" : ""}</span>`);
  if (t.deps && t.deps.length)
    chips.push(`<span class="chip">⇢ ${t.deps.length} child${t.deps.length > 1 ? "ren" : ""}</span>`);

  const times = [];
  if (t.started_at) times.push(`started <b>${fmtTime(t.started_at)}</b>`);
  if (status === "running" && t.runtime_s != null) times.push(`runtime <b>${fmtDur(t.runtime_s)}</b>`);
  if (t.est_seconds && status === "running")
    times.push(`est <b>${fmtDur(Math.max(0, t.est_seconds - (t.runtime_s || 0)))} left</b>`);
  times.push(`upd <b>${t.last_update_age_s != null ? fmtDur(t.last_update_age_s) + " ago" : "—"}</b>`);

  const progress = t.progress != null && t.progress >= 0 && t.status !== "done"
    ? `<div class="progress-wrap"><div class="progress-bar" style="width:${Math.min(100, t.progress)}%"></div></div>` : "";

  const activity = t.activity
    ? `<div class="card-activity">${esc(trunc(t.activity, 140))}</div>` : "";
  const desc = t.body ? `<p class="card-desc">${esc(trunc(t.body, 160))}</p>` : "";

  return `
  <div class="card ${cls.join(" ")}" data-task="${esc(t.id)}">
    <div class="card-top">
      <span class="card-title">${esc(trunc(t.title, 72))}</span>
      <span class="card-id">${esc(t.id)}</span>
    </div>
    ${desc}
    ${activity}
    ${progress}
    <div class="card-meta">${chips.join("")}</div>
    <div class="card-times">${times.map((x) => `<span>${x}</span>`).join("")}</div>
  </div>`;
}

/* ---------- agents ---------- */
function renderAgents() {
  const list = $("agents-list");
  const agents = snap.agents || [];
  if (!agents.length) {
    list.innerHTML = `<div class="empty">No agents registered.<br>Agents appear here via <code>POST /api/agents</code> heartbeats.</div>`;
    return;
  }
  list.innerHTML = agents.map((a) => {
    const stale = a.last_heartbeat && (Date.now() / 1000 - a.last_heartbeat > 300);
    const dot = a.status === "working" && !stale ? "working" : a.status === "error" ? "error" : "idle";
    const runtime = a.started_at ? fmtDur(Date.now() / 1000 - a.started_at) : "—";
    const hb = a.last_heartbeat ? fmtDur(Date.now() / 1000 - a.last_heartbeat) + " ago" : "—";
    return `
    <div class="agent-card">
      <div class="agent-head">
        <div class="avatar">${esc((a.name || a.agent_id || "?").slice(0, 2).toUpperCase())}</div>
        <div>
          <div class="agent-name">${esc(a.name || a.agent_id)}</div>
          <div class="agent-role">${esc(a.role || "agent")}</div>
        </div>
        <div class="agent-dot ${dot}" title="${a.status || ""}"></div>
      </div>
      <div class="agent-row">
        <span>task <b>${a.task_id ? esc(a.task_id) : "—"}</b></span>
        <span>runtime <b>${runtime}</b></span>
        <span>heartbeat <b>${hb}</b></span>
        <span>status <b>${esc(a.status || "—")}${stale && a.status === "working" ? " (stale)" : ""}</b></span>
      </div>
      ${a.activity ? `<div class="agent-activity">${esc(trunc(a.activity, 200))}</div>` : ""}
    </div>`;
  }).join("");
}

/* ---------- feed ---------- */
function renderFeed() {
  const feed = $("feed");
  const items = snap.feed || [];
  if (!items.length) {
    feed.innerHTML = `<div class="empty">No activity yet.</div>`;
    return;
  }
  feed.innerHTML = items.map((f) => `
    <div class="feed-item">
      <span class="feed-time">${fmtTime(f.ts)}</span>
      <div class="feed-body k-${esc(f.kind)}">
        ${f.agent ? `<span class="feed-agent">${esc(f.agent)}</span>` : ""}
        ${esc(f.text)}
        ${f.task_id ? ` <span class="card-id">· ${esc(f.task_id)}</span>` : ""}
      </div>
    </div>`).join("");
}

/* ---------- detail dialog ---------- */
async function openTask(id) {
  openTaskId = id;
  $("dlg-title").textContent = id;
  $("dlg-body").innerHTML = "<p>loading…</p>";
  $("taskdlg").showModal();
  try {
    const r = await fetch(`/api/task?id=${encodeURIComponent(id)}`);
    if (!r.ok) throw new Error(await r.text());
    const d = await r.json();
    const t = d.task || {};
    const comments = d.comments || [];
    const events = d.events || [];
    const rows = [];
    const kv = (k, v) => rows.push(`<span>${esc(k)}</span><span>${v == null || v === "" ? "—" : esc(v)}</span>`);
    kv("status", t.status); kv("assignee", t.assignee); kv("priority", t.priority);
    kv("started", t.started_at ? fmtTime(t.started_at) : null);
    kv("heartbeat", t.last_heartbeat_at ? fmtDur(Date.now() / 1000 - t.last_heartbeat_at) + " ago" : null);
    kv("workspace", t.workspace_path); kv("session", t.session_id);
    kv("result", t.result ? trunc(t.result, 300) : null);
    $("dlg-title").textContent = t.title || id;
    $("dlg-body").innerHTML = `
      <p style="color:var(--dim)">${esc(t.body || "")}</p>
      <h4>Details</h4>
      <div class="kv">${rows.join("")}</div>
      ${comments.length ? `<h4>Comments</h4>` + comments.map((c) =>
        `<p><b>${esc(c.author || "?")}</b> <span class="card-id">${fmtTime(c.created_at)}</span><br>${esc(trunc(c.body || c.text || "", 500))}</p>`).join("") : ""}
      <h4>Events</h4>
      ${events.slice(0, 30).map((e) =>
        `<div class="evt"><time>${fmtTime(e.created_at)}</time>${esc(e.kind)}${e.payload && e.payload.note ? " — " + esc(trunc(String(e.payload.note), 120)) : ""}</div>`).join("") || "<p>—</p>"}
    `;
  } catch (e) {
    $("dlg-body").innerHTML = `<p style="color:var(--bad)">failed: ${esc(e.message)}</p>`;
  }
}

/* ---------- render ---------- */
function render() {
  if (!snap) return;
  renderSummary(snap.summary || {});
  renderBoard();
  renderAgents();
  renderFeed();
  if (openTaskId && $("taskdlg").open) {
    // refresh open dialog silently (no fetch spam — only on epoch change)
  }
}

/* ---------- SSE ---------- */
let es = null;
function connect() {
  es = new EventSource("/events");
  es.onopen = () => $("conn").classList.add("live");
  es.onerror = () => { $("conn").classList.remove("live"); };
  es.onmessage = (m) => {
    try {
      const s = JSON.parse(m.data);
      if (s.error) return;
      snap = s;
      render();
    } catch (_) { /* ignore partial */ }
  };
}

/* ---------- init ---------- */
document.querySelectorAll(".tab").forEach((btn) =>
  btn.addEventListener("click", () => {
    document.querySelectorAll(".tab").forEach((b) => b.classList.remove("active"));
    document.querySelectorAll(".view").forEach((v) => v.classList.remove("active"));
    btn.classList.add("active");
    $(`view-${btn.dataset.view}`).classList.add("active");
    window.scrollTo({ top: 0 });
  }));

document.getElementById("board").addEventListener("click", (e) => {
  const card = e.target.closest(".card");
  if (card && !e.target.closest("a")) openTask(card.dataset.task);
});

connect();
