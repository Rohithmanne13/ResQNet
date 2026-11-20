/* ERMS web UI - plain JavaScript, no build step.
 * Talks only to its own origin; nginx forwards  /api -> request-service,  /coord -> coordinator (read-only),
 * /prom -> Prometheus (queries). Grafana is embedded from its own port (see /config.json). */
"use strict";

// ================================================================ constants
const TYPES = ["water", "food", "medicine", "transportation", "shelter", "rescue", "other"];
const REQUEST_STATUSES = ["pending", "matched", "in_progress", "resolved", "cancelled", "unmatched"];
const RESOURCE_STATUSES = ["available", "busy", "offline"];
const PRESETS = [
  ["Kochi", 9.9312, 76.2673], ["Bengaluru", 12.9716, 77.5946], ["Chennai", 13.0827, 80.2707],
  ["Hyderabad", 17.3850, 78.4867], ["Mumbai", 19.0760, 72.8777], ["Kolkata", 22.5726, 88.3639],
  ["Delhi", 28.6139, 77.2090],
];
const STATUS_COLORS = { pending: "#d29922", matched: "#1f6feb", in_progress: "#8250df", resolved: "#1f8a4c",
  cancelled: "#8c959f", unmatched: "#c93c37" };
const Q = { // Prometheus queries
  waiting: 'sum(rabbitmq_detailed_queue_messages_ready{queue="matching.requests"})',
  unacked: 'sum(rabbitmq_detailed_queue_messages_unacked{queue="matching.requests"})',
  retry: 'sum(rabbitmq_detailed_queue_messages_ready{queue="matching.retry"})',
  consumers: 'sum(rabbitmq_detailed_queue_consumers{queue="matching.requests"})',
  pods: 'count(up{job="matching-service"} == 1)',
  matched15: 'sum(increase(erms_matches_total{result="matched"}[15m]))',
  p50: 'histogram_quantile(0.50, sum by (le) (rate(erms_time_to_match_seconds_bucket{via="queue"}[5m])))',
  p95: 'histogram_quantile(0.95, sum by (le) (rate(erms_time_to_match_seconds_bucket{via="queue"}[5m])))',
  createdPerMin: "sum(increase(erms_requests_created_total[1m]))",
  matchedPerMin: 'sum(increase(erms_matches_total{result="matched"}[1m]))',
  failovers: "sum(erms_storage_failovers_total)",
};

// ================================================================ small helpers
const $ = (sel, el = document) => el.querySelector(sel);
const $$ = (sel, el = document) => [...el.querySelectorAll(sel)];
const esc = (v) => String(v ?? "").replace(/[&<>"']/g, (c) => ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c]));
const pill = (s) => `<span class="pill st-${esc(s)}">${esc(String(s).replace("_", " "))}</span>`;
const num = (v, digits = 0) => (v === null || v === undefined || Number.isNaN(v)) ? "–" : Number(v).toFixed(digits);
const options = (list, selected) => list.map((v) => `<option value="${esc(v)}"${v === selected ? " selected" : ""}>${esc(v)}</option>`).join("");

function ago(iso) {
  if (!iso) return "";
  const s = (Date.now() - new Date(iso).getTime()) / 1000;
  if (s < 60) return `${Math.max(0, Math.round(s))}s ago`;
  if (s < 3600) return `${Math.round(s / 60)}m ago`;
  if (s < 86400) return `${Math.round(s / 3600)}h ago`;
  return new Date(iso).toLocaleDateString();
}
const when = (iso) => (iso ? `${new Date(iso).toLocaleString()} (${ago(iso)})` : "");

function toast(msg, kind = "") {
  const t = document.createElement("div");
  t.className = `toast ${kind}`;
  t.textContent = msg;
  $("#toasts").appendChild(t);
  setTimeout(() => t.remove(), kind === "error" ? 7000 : 4000);
}

function errText(r) {
  const d = r.data && r.data.detail !== undefined ? r.data.detail : r.data;
  if (Array.isArray(d)) return d.map((e) => `${(e.loc || []).slice(1).join(".")}: ${e.msg}`).join("; ");
  if (d && typeof d === "object") return d.message || JSON.stringify(d);
  return d ? String(d) : `HTTP ${r.status}`;
}

async function http(method, url, body, timeoutMs = 15000) {
  const ctl = new AbortController();
  const timer = setTimeout(() => ctl.abort(), timeoutMs);      // a stuck call must never block a tab's refreshes
  try {
    const res = await fetch(url, { method, signal: ctl.signal, headers: body ? { "Content-Type": "application/json" } : {},
      body: body ? JSON.stringify(body) : undefined });
    let data = null;
    try { data = await res.json(); } catch (_) { /* non-JSON */ }
    return { ok: res.ok, status: res.status, data };
  } catch (e) {
    return { ok: false, status: 0, data: { detail: e.name === "AbortError" ? "timed out" : `network error: ${e.message}` } };
  } finally {
    clearTimeout(timer);
  }
}
const api = (method, path, body) => http(method, `/api${path}`, body);
const coord = (path) => http("GET", `/coord${path}`);

async function prom(query) {
  const r = await http("GET", `/prom/api/v1/query?query=${encodeURIComponent(query)}`);
  return r.ok && r.data && r.data.status === "success" ? r.data.data.result : [];
}
async function promValue(query) {
  const res = await prom(query);
  if (!res.length) return null;
  const v = parseFloat(res[0].value[1]);
  return Number.isFinite(v) ? v : null;
}
async function promRange(query, minutes = 15, step = 15) {
  const end = Date.now() / 1000;
  const r = await http("GET", `/prom/api/v1/query_range?query=${encodeURIComponent(query)}&start=${end - minutes * 60}&end=${end}&step=${step}`);
  if (!r.ok || !r.data || r.data.status !== "success" || !r.data.data.result.length) return [];
  return r.data.data.result[0].values.map(([t, v]) => [t * 1000, parseFloat(v)]).filter(([, v]) => Number.isFinite(v));
}

function table(headers, rows, empty = "Nothing yet.") {
  if (!rows.length) return `<div class="empty">${esc(empty)}</div>`;
  return `<div class="table-wrap"><table><thead><tr>${headers.map((h) => `<th>${esc(h)}</th>`).join("")}</tr></thead>` +
    `<tbody>${rows.join("")}</tbody></table></div>`;
}

function bars(el, entries, colorOf) {
  const max = Math.max(1, ...entries.map(([, v]) => v));
  el.innerHTML = entries.length ? entries.map(([k, v]) => `
    <div class="bar-row"><span>${esc(k.replace("_", " "))}</span>
      <div class="track"><div class="fill" style="width:${(100 * v) / max}%;background:${colorOf ? colorOf(k) : "var(--accent)"}"></div></div>
      <span class="num">${v}</span></div>`).join("") : `<div class="empty">No data yet.</div>`;
}

function kpi(label, value, sub = "", cls = "") {
  return `<div class="kpi ${cls}"><div class="label">${esc(label)}</div><div class="value">${esc(value)}</div>` +
    `<div class="sub">${esc(sub)}</div></div>`;
}

// Simple SVG line chart: series = [{name, color, points: [[ms, value], ...]}]
function lineChart(el, series, unit = "") {
  const W = 600, H = 170, L = 40, R = 8, T = 8, B = 20;
  const end = Date.now(), start = end - 15 * 60 * 1000;
  const max = Math.max(1, ...series.flatMap((s) => s.points.map(([, v]) => v))) * 1.1;
  const x = (t) => L + ((t - start) / (end - start)) * (W - L - R);
  const y = (v) => T + (1 - v / max) * (H - T - B);
  const lines = series.map((s) => s.points.length
    ? `<polyline fill="none" stroke="${s.color}" stroke-width="2" points="${s.points.map(([t, v]) => `${x(t).toFixed(1)},${y(v).toFixed(1)}`).join(" ")}"/>`
    : "").join("");
  const fmt = (v) => (v >= 100 ? v.toFixed(0) : v.toFixed(1));
  el.innerHTML = `<svg viewBox="0 0 ${W} ${H}" preserveAspectRatio="none">
      <line class="axis" x1="${L}" y1="${H - B}" x2="${W - R}" y2="${H - B}"/><line class="axis" x1="${L}" y1="${T}" x2="${L}" y2="${H - B}"/>
      <line class="axis" x1="${L}" y1="${y(max / 2)}" x2="${W - R}" y2="${y(max / 2)}" stroke-dasharray="3 4"/>
      <text x="${L - 4}" y="${T + 8}" text-anchor="end">${fmt(max)}${unit}</text>
      <text x="${L - 4}" y="${y(max / 2) + 4}" text-anchor="end">${fmt(max / 2)}${unit}</text>
      <text x="${L - 4}" y="${H - B}" text-anchor="end">0</text>
      <text x="${L}" y="${H - 4}">-15 min</text><text x="${W - R}" y="${H - 4}" text-anchor="end">now</text>
      ${lines}</svg>
    <div class="chart-legend">${series.map((s) => `<span><i style="background:${s.color}"></i>${esc(s.name)}${s.points.length ? "" : " (no data)"}</span>`).join("")}</div>`;
}

// ================================================================ app state + routing
const state = { config: {}, tab: null, inited: new Set(), busy: new Set(), map: null, mapLayer: null, mapPoints: [],
  seq: { requests: 0, resources: 0 } };      // newest list refresh wins: an older, slower one is discarded
const tabs = {};

function grafanaBase() { return `${location.protocol}//${location.hostname}:${state.config.grafanaPort || 3000}`; }
function portUrl(port, path = "") { return `${location.protocol}//${location.hostname}:${port}${path}`; }

async function showTab(name) {
  if (!tabs[name]) name = "overview";
  state.tab = name;
  $$(".tab").forEach((s) => s.classList.toggle("active", s.id === `tab-${name}`));
  $$("#tabs a").forEach((a) => a.classList.toggle("active", a.dataset.tab === name));
  if (!state.inited.has(name) && tabs[name].init) { tabs[name].init(); state.inited.add(name); }
  refresh(name);
}

async function refresh(name) {
  if (state.busy.has(name) || !tabs[name].refresh) return;
  state.busy.add(name);
  try { await tabs[name].refresh(); } catch (e) { console.error(e); } finally { state.busy.delete(name); }
}

// ================================================================ header health
async function refreshHealth() {
  const set = (id, cls, text) => { const el = $(id); el.className = `hpill ${cls}`; el.textContent = text; };
  const [h, c, pods] = await Promise.all([api("GET", "/health"), coord("/cluster"), promValue(Q.pods)]);
  set("#h-api", h.ok ? "ok" : "bad", h.ok ? "API up" : "API down");
  set("#h-broker", h.ok && h.data.broker_connected ? "ok" : "bad", h.ok && h.data.broker_connected ? "RabbitMQ connected" : "RabbitMQ disconnected");
  if (c.ok) {
    const shards = Object.values(c.data.shards);
    const healthy = shards.filter((s) => s.healthy).length;
    const nodes = shards.flatMap((s) => s.nodes);
    const ready = nodes.filter((n) => n.status === "ready").length;
    set("#h-shards", healthy === c.data.num_shards ? "ok" : "bad", `Shards ${healthy}/${c.data.num_shards}`);
    set("#h-nodes", ready === nodes.length ? "ok" : "warn", `Nodes ${ready}/${nodes.length} ready`);
  } else {
    set("#h-shards", "bad", "Coordinator down"); set("#h-nodes", "bad", "Nodes ?");
  }
  set("#h-matching", pods ? "ok" : "warn", `Matching pods ${num(pods)}`);
}

// ================================================================ forms: presets + prefill
function setupPresets() {
  $$("select.preset").forEach((sel) => {
    sel.innerHTML = `<option value="">choose a city…</option>` + PRESETS.map(([n], i) => `<option value="${i}">${esc(n)}</option>`).join("");
    sel.addEventListener("change", () => {
      if (sel.value === "") return;
      const [, lat, lon] = PRESETS[+sel.value];
      const form = sel.closest("form");
      if (form.lat) { form.lat.value = lat; form.lon.value = lon; }
      if (form.area && !form.area.value) form.area.value = PRESETS[+sel.value][0];
    });
  });
  $$("form select[name=type]").forEach((s) => { s.innerHTML = options(TYPES, "water"); });
  for (const id of ["#req-form", "#res-form"]) {
    const f = $(id); f.lat.value = PRESETS[0][1]; f.lon.value = PRESETS[0][2]; f.querySelector(".preset").value = "0";
  }
  $("#load-form").preset.value = "0";
}

function prefill(formId, lat, lon) {
  const f = $(formId);
  f.lat.value = lat.toFixed(5); f.lon.value = lon.toFixed(5);
  f.querySelector(".preset").value = "";
  if (f.area) f.area.value = "";
}

// ================================================================ OVERVIEW
tabs.overview = {
  async refresh() {
    const [s, latest, c, waiting, pods] = await Promise.all([
      api("GET", "/summary"), api("GET", "/requests?order=desc&limit=8"), coord("/cluster"), promValue(Q.waiting), promValue(Q.pods)]);
    const req = s.ok ? s.data.requests : { total: 0, by_status: {} };
    const res = s.ok ? s.data.resources : { total: 0, by_status: {}, free_units: 0 };
    const st = req.by_status;
    const nodes = c.ok ? Object.values(c.data.shards).flatMap((x) => x.nodes) : [];
    const ready = nodes.filter((n) => n.status === "ready").length;
    const waitingHelp = (st.pending || 0) + (st.unmatched || 0);
    $("#ov-kpis").innerHTML = [
      kpi("Requests", req.total, "all time"),
      kpi("Waiting for help", waitingHelp, `${st.pending || 0} pending · ${st.unmatched || 0} unmatched`, waitingHelp ? "warn" : ""),
      kpi("Being helped", (st.matched || 0) + (st.in_progress || 0), `${st.matched || 0} matched · ${st.in_progress || 0} in progress`),
      kpi("Resolved", st.resolved || 0, `${st.cancelled || 0} cancelled`, "ok"),
      kpi("Resources available", res.by_status.available || 0, `${res.free_units} free units · ${res.total} registered`),
      kpi("Storage nodes ready", c.ok ? `${ready}/${nodes.length}` : "–", c.ok ? "3 shards × 2 replicas" : "coordinator unreachable", ready === nodes.length && c.ok ? "ok" : "bad"),
      kpi("Queue (waiting)", num(waiting), "RabbitMQ matching.requests"),
      kpi("Matching pods", num(pods), state.config.platform === "k8s" ? "autoscaled by KEDA (2–6)" : "Docker Compose: 2"),
    ].join("");
    if (s.ok && s.data.unavailable_shards.length) toast(`Shard(s) ${s.data.unavailable_shards.join(", ")} unavailable: counts are partial`, "error");
    bars($("#ov-req-status"), REQUEST_STATUSES.map((k) => [k, st[k] || 0]), (k) => STATUS_COLORS[k]);
    bars($("#ov-res-status"), [...RESOURCE_STATUSES.map((k) => [k, res.by_status[k] || 0]),
      ...Object.entries(res.by_kind || {}).map(([k, v]) => [`kind: ${k}`, v])],
    (k) => ({ available: "#1f8a4c", busy: "#d29922", offline: "#8c959f" }[k] || "#7a8794"));
    $("#ov-latest").innerHTML = latest.ok ? requestTable(latest.data.items, true) : `<div class="empty">${esc(errText(latest))}</div>`;
    bindRequestRows($("#ov-latest"));
    $("#ov-storage").innerHTML = nodes.map((n) => `<span class="n st-${esc(n.status)}" title="${esc(n.status)}">${esc(n.node_id)}</span>`).join("");
  },
};

// ================================================================ REQUESTS
function requestTable(items, compact = false) {
  const rows = items.map((r) => `<tr class="click" data-req="${esc(r.id)}">
      <td class="mono">${esc(r.id)}</td><td>${esc(r.type)}</td><td>${pill(r.status)}</td>
      ${compact ? "" : `<td>${esc(r.priority || "")}</td><td>${esc(r.location?.area || `${num(r.location?.lat, 3)}, ${num(r.location?.lon, 3)}`)}</td>`}
      <td>${esc(r.assignment?.resource_name || "")}</td><td title="${esc(r.created_at)}">${esc(ago(r.created_at))}</td></tr>`);
  const head = compact ? ["ID", "Type", "Status", "Assigned to", "Created"]
    : ["ID", "Type", "Status", "Priority", "Area / location", "Assigned to", "Created"];
  return table(head, rows, "No requests yet.");
}
function bindRequestRows(root) {
  $$("tr[data-req]", root).forEach((tr) => tr.addEventListener("click", () => showRequest(tr.dataset.req)));
}

tabs.requests = {
  init() {
    $("#req-f-status").innerHTML += options(REQUEST_STATUSES);
    $("#req-f-type").innerHTML += options(TYPES);
    ["#req-f-status", "#req-f-type", "#req-f-limit"].forEach((s) => $(s).addEventListener("change", () => {
      $("#req-table").innerHTML = `<div class="empty">Loading…</div>`;   // immediate feedback, the new list follows
      tabs.requests.refresh();
    }));
    $("#req-form").addEventListener("submit", async (ev) => {
      ev.preventDefault();
      const f = ev.target;
      const body = { type: f.type.value, details: f.details.value, priority: f.priority.value, auto_match: f.auto_match.checked,
        location: { lat: parseFloat(f.lat.value), lon: parseFloat(f.lon.value), ...(f.area.value ? { area: f.area.value } : {}) },
        ...(f.requester.value ? { requester: f.requester.value } : {}) };
      const r = await api("POST", "/requests", body);
      if (!r.ok) return toast(`Could not create the request: ${errText(r)}`, "error");
      toast(`Request ${r.data.id} created on shard ${r.data._storage.shard}` +
        (r.data.queued ? " and queued in RabbitMQ" : "") + (r.data._storage.degraded ? " (degraded: one replica down)" : ""), "ok");
      f.details.value = "";
      refresh("requests");
      showRequest(r.data.id);
    });
  },
  async refresh() {
    const q = new URLSearchParams({ order: "desc", limit: $("#req-f-limit").value });
    if ($("#req-f-status").value) q.set("status", $("#req-f-status").value);
    if ($("#req-f-type").value) q.set("type", $("#req-f-type").value);
    const my = ++state.seq.requests;
    const r = await api("GET", `/requests?${q}`);
    if (my !== state.seq.requests) return;                       // a newer refresh (e.g. a filter change) is running
    if (!r.ok) { $("#req-table").innerHTML = `<div class="empty">${esc(errText(r))}</div>`; return; }
    $("#req-count").textContent = `Newest ${r.data.count} of ${r.data.total}` +
      (r.data.unavailable_shards.length ? ` · shard(s) ${r.data.unavailable_shards.join(", ")} unavailable` : "");
    $("#req-table").innerHTML = requestTable(r.data.items);
    bindRequestRows($("#req-table"));
  },
};

async function showRequest(id) {
  const r = await api("GET", `/requests/${encodeURIComponent(id)}`);
  if (!r.ok) return toast(`Request ${id}: ${errText(r)}`, "error");
  const q = r.data, a = q.assignment;
  const terminal = ["resolved", "cancelled"].includes(q.status);
  const actions = [];
  if (["pending", "unmatched"].includes(q.status)) actions.push(`<button data-act="match" class="primary" style="width:auto">Match now</button>`);
  if (q.status === "matched") actions.push(`<button data-status="in_progress">Mark in progress</button>`);
  if (["matched", "in_progress"].includes(q.status)) actions.push(`<button data-status="resolved">Mark resolved</button>`, `<button data-status="pending">Re-open</button>`);
  if (q.status === "unmatched") actions.push(`<button data-status="pending">Retry automatic matching</button>`);
  if (!terminal) actions.push(`<button data-status="cancelled" class="danger">Cancel</button>`);
  openDialog(`Request ${q.id}`, `
    <div class="actions">${pill(q.status)} ${actions.join("")}</div>
    ${terminal ? `<p class="hint">This request is ${esc(q.status)}: its status can no longer change.</p>` : ""}
    <dl class="fields">
      <dt>Type</dt><dd>${esc(q.type)}</dd><dt>Priority</dt><dd>${esc(q.priority)}</dd>
      <dt>Details</dt><dd>${esc(q.details) || '<span class="muted">none</span>'}</dd>
      <dt>Location</dt><dd>${num(q.location.lat, 5)}, ${num(q.location.lon, 5)} ${esc(q.location.area || "")}</dd>
      <dt>Requester</dt><dd>${esc(q.requester || "")}</dd>
      <dt>Created</dt><dd>${esc(when(q.created_at))}</dd>
      <dt>Automatic matching</dt><dd>${q.auto_match ? "yes (RabbitMQ)" : "no (match manually)"} · attempts: ${q.match_attempts || 0}${q.last_match_error ? ` · last: ${esc(q.last_match_error)}` : ""}</dd>
    </dl>
    <h3>Assignment</h3>
    ${a ? `<dl class="fields">
      <dt>Resource</dt><dd><a href="#" data-res="${esc(a.resource_id)}">${esc(a.resource_name)}</a> <span class="mono muted">${esc(a.resource_id)}</span></dd>
      <dt>Distance</dt><dd>${num(a.distance_km, 2)} km</dd><dt>Matched</dt><dd>${esc(when(a.matched_at))}</dd>
      <dt>Matched by</dt><dd class="mono">${esc(a.matched_by)} (${a.via === "queue" ? "RabbitMQ consumer" : "Match now / API"})</dd></dl>`
    : `<p class="muted">Not assigned to a resource.</p>`}
    <h3>Storage</h3>
    <dl class="fields"><dt>Shard</dt><dd>${esc(q._storage.shard)} <span class="muted">(sha256(key) mod 3)</span></dd>
      <dt>Read from</dt><dd class="mono">${esc(q._storage.served_by)}</dd><dt>Version</dt><dd class="mono">${esc(q._storage.version)}</dd></dl>
    <h3>History</h3>
    <ul class="timeline">${(q.history || []).map((h) => `<li>${pill(h.status)} <span class="when">${esc(when(h.at))} · ${esc(h.by || "")}</span>${h.note ? `<div>${esc(h.note)}</div>` : ""}</li>`).join("")}</ul>`);
  const body = $("#detail-body");
  $$("[data-res]", body).forEach((el) => el.addEventListener("click", (e) => { e.preventDefault(); showResource(el.dataset.res); }));
  const match = $("[data-act=match]", body);
  if (match) match.addEventListener("click", async () => {
    const m = await api("POST", `/match/${encodeURIComponent(q.id)}`);
    if (m.ok && m.data.result === "matched") toast(`Matched to ${m.data.resource_name} (${num(m.data.distance_km, 2)} km)`, "ok");
    else if (m.ok) toast(`Nothing to do: request is ${m.data.status}`);
    else toast(`No match: ${errText(m)}`, "error");
    afterChange(() => showRequest(q.id));
  });
  $$("[data-status]", body).forEach((b) => b.addEventListener("click", async () => {
    const u = await api("PUT", `/requests/${encodeURIComponent(q.id)}/status`, { status: b.dataset.status });
    if (!u.ok) return toast(`Could not update: ${errText(u)}`, "error");
    toast(`Status: ${u.data.previous_status} → ${u.data.status}` + (u.data.released_resource ? " (resource unit released)" : "") +
      (u.data.requeued ? " (queued for matching)" : ""), "ok");
    afterChange(() => showRequest(q.id));
  }));
}

// ================================================================ RESOURCES
function resourceTable(items) {
  const rows = items.map((r) => `<tr class="click" data-res="${esc(r.id)}">
      <td>${esc(r.name)}<div class="mono muted">${esc(r.id)}</div></td><td>${esc(r.type)}</td><td>${esc(r.kind)}</td>
      <td>${pill(r.status)}</td><td>${esc(r.availability)}</td><td>${(r.assigned_requests || []).length}</td>
      <td title="${esc(r.created_at)}">${esc(ago(r.created_at))}</td></tr>`);
  return table(["Name", "Type", "Kind", "Status", "Free units", "Assigned", "Registered"], rows, "No resources yet.");
}

tabs.resources = {
  init() {
    $("#res-f-status").innerHTML += options(RESOURCE_STATUSES);
    $("#res-f-type").innerHTML += options(TYPES);
    ["#res-f-status", "#res-f-type", "#res-f-limit"].forEach((s) => $(s).addEventListener("change", () => {
      $("#res-table").innerHTML = `<div class="empty">Loading…</div>`;
      tabs.resources.refresh();
    }));
    $("#res-form").addEventListener("submit", async (ev) => {
      ev.preventDefault();
      const f = ev.target;
      const body = { type: f.type.value, kind: f.kind.value, availability: parseInt(f.availability.value, 10),
        location: { lat: parseFloat(f.lat.value), lon: parseFloat(f.lon.value) },
        ...(f.name.value ? { name: f.name.value } : {}), ...(f.contact.value ? { contact: f.contact.value } : {}) };
      const r = await api("POST", "/resources", body);
      if (!r.ok) return toast(`Could not register: ${errText(r)}`, "error");
      toast(`${r.data.name} registered (${r.data.id}, shard ${r.data._storage.shard})`, "ok");
      f.name.value = "";
      refresh("resources");
      showResource(r.data.id);
    });
  },
  async refresh() {
    const q = new URLSearchParams({ order: "desc", limit: $("#res-f-limit").value });
    if ($("#res-f-status").value) q.set("status", $("#res-f-status").value);
    if ($("#res-f-type").value) q.set("type", $("#res-f-type").value);
    const my = ++state.seq.resources;
    const r = await api("GET", `/resources?${q}`);
    if (my !== state.seq.resources) return;
    if (!r.ok) { $("#res-table").innerHTML = `<div class="empty">${esc(errText(r))}</div>`; return; }
    $("#res-count").textContent = `Newest ${r.data.count} of ${r.data.total}`;
    $("#res-table").innerHTML = resourceTable(r.data.items);
    $$("tr[data-res]", $("#res-table")).forEach((tr) => tr.addEventListener("click", () => showResource(tr.dataset.res)));
  },
};

async function showResource(id) {
  const r = await api("GET", `/resources/${encodeURIComponent(id)}`);
  if (!r.ok) return toast(`Resource ${id}: ${errText(r)}`, "error");
  const x = r.data;
  openDialog(x.name, `
    <div class="actions">${pill(x.status)}
      <button data-rstatus="available">Set available</button><button data-rstatus="offline" class="danger">Set offline</button></div>
    <dl class="fields">
      <dt>ID</dt><dd class="mono">${esc(x.id)}</dd><dt>Type / kind</dt><dd>${esc(x.type)} · ${esc(x.kind)}</dd>
      <dt>Location</dt><dd>${num(x.location.lat, 5)}, ${num(x.location.lon, 5)}</dd>
      <dt>Contact</dt><dd>${esc(x.contact || "")}</dd><dt>Registered</dt><dd>${esc(when(x.created_at))}</dd>
      <dt>Storage</dt><dd>shard ${esc(x._storage.shard)} · read from <span class="mono">${esc(x._storage.served_by)}</span></dd>
    </dl>
    <h3>Availability</h3>
    <form class="row" id="avail-form"><label>Free units <input name="availability" type="number" min="0" max="100000" value="${esc(x.availability)}"></label>
      <button type="submit">Save</button></form>
    <p class="hint">0 free units makes it busy automatically; matching uses only available resources with free units.</p>
    <h3>Assigned requests (${(x.assigned_requests || []).length})</h3>
    ${(x.assigned_requests || []).length ? `<ul>${x.assigned_requests.map((q) => `<li><a href="#" class="mono" data-req="${esc(q)}">${esc(q)}</a></li>`).join("")}</ul>` : `<p class="muted">None.</p>`}`);
  const body = $("#detail-body");
  $$("a[data-req]", body).forEach((el) => el.addEventListener("click", (e) => { e.preventDefault(); showRequest(el.dataset.req); }));
  const update = async (payload) => {
    const u = await api("PUT", `/resources/${encodeURIComponent(x.id)}/status`, payload);
    if (!u.ok) return toast(`Could not update: ${errText(u)}`, "error");
    toast(`${u.data.name}: ${u.data.status}, ${u.data.availability} free units`, "ok");
    afterChange(() => showResource(x.id));
  };
  $$("[data-rstatus]", body).forEach((b) => b.addEventListener("click", () => update({ status: b.dataset.rstatus })));
  $("#avail-form", body).addEventListener("submit", (e) => { e.preventDefault(); update({ availability: parseInt(e.target.availability.value, 10) }); });
}

// ================================================================ dialog
function openDialog(title, html) {
  $("#detail-title").textContent = title;
  $("#detail-body").innerHTML = html;
  const d = $("#detail");
  if (!d.open) d.showModal();
}
function afterChange(reopen) {
  reopen();
  if (state.tab) refresh(state.tab);
}

// ================================================================ MAP
tabs.map = {
  init() {
    state.map = L.map("map").setView([20.5, 78.9], 5);          // India; "Show all points" zooms to every marker
    $("#map-fit").addEventListener("click", () => { if (state.mapPoints?.length) state.map.fitBounds(state.mapPoints, { padding: [30, 30] }); });
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png",
      { maxZoom: 18, attribution: "&copy; OpenStreetMap contributors" }).addTo(state.map);
    state.mapLayer = L.layerGroup().addTo(state.map);
    state.map.on("click", (e) => {
      const { lat, lng } = e.latlng;
      const div = document.createElement("div");
      div.innerHTML = `<b>${lat.toFixed(4)}, ${lng.toFixed(4)}</b><div class="map-actions">
        <button data-go="req">New request here</button><button data-go="res">New resource here</button></div>`;
      div.querySelector("[data-go=req]").onclick = () => { prefill("#req-form", lat, lng); location.hash = "#requests"; };
      div.querySelector("[data-go=res]").onclick = () => { prefill("#res-form", lat, lng); location.hash = "#resources"; };
      L.popup().setLatLng(e.latlng).setContent(div).openOn(state.map);
    });
  },
  async refresh() {
    setTimeout(() => state.map.invalidateSize(), 50);
    const [rq, rs] = await Promise.all([api("GET", "/requests?order=desc&limit=300"), api("GET", "/resources?order=desc&limit=500")]);
    if (!rq.ok || !rs.ok) return;
    const layer = state.mapLayer;
    layer.clearLayers();
    const resById = Object.fromEntries(rs.data.items.map((r) => [r.id, r]));
    const pts = [];
    for (const r of rs.data.items) {
      const ll = [r.location.lat, r.location.lon];
      pts.push(ll);
      L.circleMarker(ll, { radius: 7, color: "#0f5132", weight: 2, fillColor: r.status === "available" ? "#ffffff" : "#c7cdd3", fillOpacity: 1 })
        .bindTooltip(`${esc(r.name)} · ${esc(r.type)} · ${esc(r.status)} · ${esc(r.availability)} free`)
        .on("click", () => showResource(r.id)).addTo(layer);
    }
    for (const q of rq.data.items) {
      const ll = [q.location.lat, q.location.lon];
      pts.push(ll);
      const res = q.assignment && resById[q.assignment.resource_id];
      if (res && ["matched", "in_progress"].includes(q.status)) {
        L.polyline([ll, [res.location.lat, res.location.lon]], { color: "#1f6feb", weight: 1.5, dashArray: "4 4" }).addTo(layer);
      }
      L.circleMarker(ll, { radius: 6, color: "#fff", weight: 1, fillColor: STATUS_COLORS[q.status] || "#8c959f", fillOpacity: .95 })
        .bindTooltip(`${esc(q.id)} · ${esc(q.type)} · ${esc(q.status)}`)
        .on("click", () => showRequest(q.id)).addTo(layer);
    }
    state.mapPoints = pts;
  },
};

// ================================================================ STORAGE
tabs.storage = {
  init() {
    const k8s = state.config.platform === "k8s";
    $("#st-cmds").textContent = k8s
      ? "kubectl -n erms scale sts/storage-shard-1 --replicas=1   # stop storage-shard-1-1\n" +
        "kubectl -n erms scale sts/storage-shard-1 --replicas=2   # restart it (same volume), it resyncs\n" +
        "kubectl -n erms delete pod storage-shard-1-0             # kill the read replica: reads fail over"
      : "docker compose stop storage-1a     # stop the node that serves shard 1's reads\n" +
        "docker compose start storage-1a    # restart it: it resyncs the writes it missed";
    $("#st-lookup").addEventListener("submit", async (ev) => {
      ev.preventDefault();
      const id = ev.target.id.value.trim();
      const out = $("#st-lookup-out");
      const isRes = id.startsWith("res-");
      const key = isRes ? `resource:${id.split("-")[1]}:${id}` : `request:${id}`;
      const [so, rec] = await Promise.all([coord(`/internal/shard-of/${encodeURIComponent(key)}`),
        api("GET", `/${isRes ? "resources" : "requests"}/${encodeURIComponent(id)}`)]);
      if (!so.ok) { out.innerHTML = `<p class="muted">${esc(errText(so))}</p>`; return; }
      let local = "";
      if (window.crypto && crypto.subtle) {
        const h = new Uint8Array(await crypto.subtle.digest("SHA-256", new TextEncoder().encode(key)));
        let big = 0n;
        for (let i = 0; i < 8; i++) big = (big << 8n) | BigInt(h[i]);
        local = ` (computed here too: ${big % 3n})`;
      }
      out.innerHTML = `<dl class="fields" style="margin-top:12px">
        <dt>Storage key</dt><dd class="mono">${esc(key)}</dd>
        <dt>Shard</dt><dd><b>${esc(so.data.shard)}</b>${esc(local)}</dd>
        <dt>Replicas</dt><dd class="mono">${esc(so.data.replicas.join(", "))}</dd>
        <dt>Record</dt><dd>${rec.ok ? `exists · read from <span class="mono">${esc(rec.data._storage.served_by)}</span>` : esc(errText(rec))}</dd></dl>`;
    });
  },
  async refresh() {
    const c = await coord("/cluster");
    if (!c.ok) { $("#st-shards").innerHTML = `<div class="card">${esc(errText(c))}</div>`; return; }
    const shards = Object.entries(c.data.shards);
    $("#st-shards").innerHTML = shards.map(([sid, s]) => `<div class="card shard">
        <h2>Shard ${esc(sid)} ${s.healthy ? '<span class="pill st-ready">healthy</span>' : '<span class="pill st-down">unavailable</span>'}</h2>
        ${s.nodes.map((n) => `<div class="node"><div><b class="mono">${esc(n.node_id)}</b>
            <div class="meta">${esc(n.key_count)} records · heartbeat ${esc(num(n.heartbeat_age_s, 1))}s ago${n.down_reason ? ` · ${esc(n.down_reason)}` : ""}</div></div>
            ${pill(n.status)}</div>`).join("")}</div>`).join("");
    bars($("#st-dist"), shards.map(([sid, s]) => [`shard ${sid}`, Math.max(0, ...s.nodes.map((n) => n.key_count))]));
    const st = c.data.stats;
    $("#st-stats").innerHTML = `Since the coordinator started: ${esc(st.reads)} reads, ${esc(st.writes)} replicated writes, ` +
      `<b>${esc(st.failovers)} read failovers</b>. Reads by node: ` +
      Object.entries(st.read_by_node).map(([n, v]) => `<span class="mono">${esc(n)}</span> ${esc(v)}`).join(" · ");
  },
};

// ================================================================ MESSAGING + AUTOSCALING
tabs.messaging = {
  init() {
    $("#load-hint").textContent = state.config.platform === "k8s"
      ? "Requests pile up in RabbitMQ; KEDA adds matching pods (up to 6) and removes them when the queue is empty. Watch the charts."
      : "Requests pile up in RabbitMQ and the 2 matching workers drain the queue. (Autoscaling with KEDA runs on Kubernetes.)";
    $("#load-form").addEventListener("submit", async (ev) => {
      ev.preventDefault();
      const f = ev.target, btn = f.querySelector("button"), bar = f.querySelector(".progress"), out = $("#load-out");
      const n = Math.min(300, Math.max(1, parseInt(f.count.value, 10)));
      const [, lat, lon] = PRESETS[+f.preset.value || 0];
      btn.disabled = true; bar.hidden = false; out.textContent = "";
      if (f.with_resource.checked) {
        const r = await api("POST", "/resources", { type: f.type.value, name: `Burst store (${n} units)`, availability: n,
          location: { lat: lat + 0.01, lon } });
        if (!r.ok) toast(`Could not register the resource: ${errText(r)}`, "error");
      }
      let done = 0, ok = 0, next = 0;
      const t0 = performance.now();
      const worker = async () => {
        while (next < n) {
          next++;
          const r = await api("POST", "/requests", { type: f.type.value, details: "burst from the web UI", location: { lat, lon } });
          done++; if (r.ok) ok++;
          $(".bar", bar).style.width = `${(100 * done) / n}%`;
        }
      };
      await Promise.all(Array.from({ length: 10 }, worker));
      out.textContent = `${ok}/${n} requests accepted in ${((performance.now() - t0) / 1000).toFixed(1)}s and queued in RabbitMQ.`;
      toast(`Burst sent: ${ok}/${n} accepted`, ok === n ? "ok" : "error");
      btn.disabled = false;
      refresh("messaging");
    });
  },
  async refresh() {
    const [waiting, unacked, retry, consumers, pods, matched15, p50, p95] = await Promise.all(
      [Q.waiting, Q.unacked, Q.retry, Q.consumers, Q.pods, Q.matched15, Q.p50, Q.p95].map(promValue));
    $("#mq-kpis").innerHTML = [
      kpi("Waiting in queue", num(waiting), "matching.requests (ready)", waiting > 20 ? "warn" : ""),
      kpi("Being processed", num(unacked), "delivered, not yet acked"),
      kpi("Delayed retry", num(retry), "matching.retry (TTL 5 s)"),
      kpi("Consumers", num(consumers), "one per matching worker"),
      kpi("Matching pods", num(pods), state.config.platform === "k8s" ? "KEDA: 2–6 on queue depth" : "fixed (Compose)"),
      kpi("Matched (15 min)", num(matched15), "through queue + Match now"),
      kpi("Time to match p50", p50 === null ? "–" : `${num(p50, 1)} s`, "request created → assigned"),
      kpi("Time to match p95", p95 === null ? "–" : `${num(p95, 1)} s`, "last 5 minutes"),
    ].join("");
    // each chart is drawn as soon as its own data is there (one slow query does not hold back the others)
    const chart = async (el, defs, unit) => {
      const pts = await Promise.all(defs.map((d) => promRange(d.q)));
      lineChart(el, defs.map((d, i) => ({ name: d.name, color: d.color, points: pts[i] })), unit);
    };
    await Promise.all([
      chart($("#ch-queue"), [{ name: "messages waiting", color: "#d29922", q: Q.waiting }, { name: "matching pods", color: "#1f6feb", q: Q.pods }]),
      chart($("#ch-ttm"), [{ name: "p50", color: "#1f8a4c", q: Q.p50 }, { name: "p95", color: "#8250df", q: Q.p95 }], "s"),
      chart($("#ch-rate"), [{ name: "created", color: "#d29922", q: Q.createdPerMin }, { name: "matched", color: "#1f6feb", q: Q.matchedPerMin }]),
    ]);
  },
};

// ================================================================ MONITORING
tabs.monitoring = {
  init() {
    const g = grafanaBase();
    $("#grafana").src = `${g}/d/erms-overview/erms-overview?orgId=1&kiosk&theme=light&refresh=5s&from=now-15m&to=now`;
    const c = state.config;
    $("#mon-links").innerHTML = [
      [`${g}/d/erms-overview/erms-overview`, "Open Grafana"],
      [portUrl(c.prometheusPort || 9090, "/targets"), "Prometheus"],
      [portUrl(c.rabbitPort || 15672), "RabbitMQ UI"],
      [portUrl(c.apiPort || 8080, "/docs"), "API docs (Swagger)"],
      [portUrl(c.coordinatorPort || 8001, "/cluster"), "Coordinator /cluster"],
    ].map(([href, label]) => `<a href="${esc(href)}" target="_blank" rel="noopener">${esc(label)}</a>`).join("");
  },
  async refresh() {
    const r = await http("GET", "/prom/api/v1/targets");
    if (!r.ok) { $("#mon-targets").innerHTML = `<div class="empty">Prometheus unreachable</div>`; return; }
    const ts = r.data.data.activeTargets.sort((a, b) => (a.labels.job + a.labels.instance).localeCompare(b.labels.job + b.labels.instance));
    $("#mon-targets").innerHTML = table(["Job", "Instance", "State", "Last scrape", "Error"], ts.map((t) => `<tr>
      <td>${esc(t.labels.job)}</td><td class="mono">${esc(t.labels.instance)}</td>
      <td>${t.health === "up" ? '<span class="pill st-ready">up</span>' : `<span class="pill st-down">${esc(t.health)}</span>`}</td>
      <td>${esc(ago(t.lastScrape))}</td><td class="muted">${esc(t.lastError)}</td></tr>`));
  },
};

// ================================================================ start
async function start() {
  const c = await http("GET", "/config.json");
  state.config = c.ok ? c.data : {};
  $("#platform-badge").textContent = state.config.platform === "k8s" ? "Kubernetes" : "Docker Compose";
  setupPresets();
  $("#detail-close").addEventListener("click", () => $("#detail").close());
  $("#detail").addEventListener("click", (e) => { if (e.target.id === "detail") $("#detail").close(); });
  window.addEventListener("hashchange", () => showTab(location.hash.slice(1)));
  showTab(location.hash.slice(1) || "overview");
  refreshHealth();
  setInterval(() => { if (!document.hidden) { refreshHealth(); if (state.tab) refresh(state.tab); } }, 5000);
}
start();
