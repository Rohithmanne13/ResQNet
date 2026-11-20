"""Tests for the web UI (frontend/), against the RUNNING system, in a real headless browser (Playwright/Chromium):
every check clicks / types in the UI like a user and then confirms the effect through the API or Prometheus.

    ./scripts/spec-test.sh compose -k ui_        ./scripts/spec-test.sh k8s -k ui_

Requirement ids UI.* are listed in tests/spec/requirements.py. Runs after the PDF and extension tests.
"""
import re
import time

import httpx
import pytest
from playwright.sync_api import sync_playwright

import spec_env
from test_spec_compliance import AREAS, P, api, node_status, req, wait_for
from test_spec_compliance import made, system_ready  # noqa: F401  (fixtures used by these tests)

FE = P.frontend
fe = httpx.Client(base_url=FE, timeout=20)
TABS = ["overview", "requests", "resources", "map", "storage", "messaging", "monitoring"]


# ============================================================================= browser fixtures
@pytest.fixture(scope="module")
def browser():
    with sync_playwright() as p:
        b = p.chromium.launch()
        yield b
        b.close()


@pytest.fixture
def ui(browser):
    """A fresh browser tab on the UI; collects JavaScript errors and failed calls (map tiles excluded)."""
    ctx = browser.new_context(viewport={"width": 1400, "height": 1000})
    page = ctx.new_page()
    page.errors = []
    page.on("pageerror", lambda e: page.errors.append(f"JavaScript error: {e}"))
    page.on("console", lambda m: page.errors.append(f"console error: {m.text}") if m.type == "error" else None)
    page.on("response", lambda r: page.errors.append(f"HTTP {r.status} {r.url}")
            if r.status >= 400 and "openstreetmap" not in r.url else None)
    page.goto(f"{FE}/#overview")
    page.wait_for_selector("#ov-kpis .kpi")
    yield page
    ctx.close()


def tab(page, name: str) -> None:
    page.click(f"#tabs a[data-tab={name}]")
    page.wait_for_selector(f"#tab-{name}.active")


def last_toast(page, pattern: str, timeout: float = 20) -> str:
    deadline = time.time() + timeout
    while time.time() < deadline:
        texts = [t.inner_text() for t in page.query_selector_all(".toast")]
        hit = next((t for t in reversed(texts) if re.search(pattern, t)), None)
        if hit:
            return hit
        time.sleep(0.2)
    raise AssertionError(f"no toast matching {pattern!r}; toasts: {texts}")


def fill_location(page, form: str, loc: dict) -> None:
    page.select_option(f"{form} select.preset", "")
    page.fill(f"{form} input[name=lat]", str(loc["lat"]))
    page.fill(f"{form} input[name=lon]", str(loc["lon"]))


def ui_register_resource(page, made, loc, type_="water", kind="ngo", name="UI water tanker", units=3) -> str:
    tab(page, "resources")
    page.select_option("#res-form select[name=type]", type_)
    page.select_option("#res-form select[name=kind]", kind)
    page.fill("#res-form input[name=name]", name)
    fill_location(page, "#res-form", loc)
    page.fill("#res-form input[name=availability]", str(units))
    page.click("#res-form button[type=submit]")
    page.wait_for_selector("#detail[open]")
    rid = page.inner_text("#detail-body .fields dd.mono")
    made.resources.append(rid)
    return rid


def ui_create_request(page, made, loc, type_="water", details="UI test", auto=False, priority="high") -> str:
    tab(page, "requests")
    page.select_option("#req-form select[name=type]", type_)
    fill_location(page, "#req-form", loc)
    page.fill("#req-form input[name=area]", "UI test area")
    page.fill("#req-form textarea[name=details]", details)
    page.select_option("#req-form select[name=priority]", priority)
    page.set_checked("#req-form input[name=auto_match]", auto)
    page.click("#req-form button[type=submit]")
    page.wait_for_selector("#detail[open]")
    rid = page.inner_text("#detail-title").replace("Request ", "")
    made.requests.append(rid)
    return rid


def pill(page) -> str:
    return page.inner_text("#detail-body .actions .pill")


def close_dialog(page) -> None:
    page.click("#detail-close")


# ============================================================================= serving + proxying
@req("UI.serve", "UI.both")
def test_ui_is_served_on_this_deployment(check):
    """The web UI runs as its own container / pod and serves the app, its assets and its deployment settings."""
    if P.name == "compose":
        check("container 'frontend' is running", P.component_count("frontend") == 1, "docker compose service frontend")
    else:
        d = P.kjson("get", "deploy/frontend")
        check("Deployment 'frontend' is available", d["status"].get("availableReplicas") == d["spec"]["replicas"],
              f"{d['status'].get('availableReplicas')}/{d['spec']['replicas']} pods")
        svc = P.kjson("get", "svc/frontend")["spec"]
        check("Service 'frontend' is a NodePort on 30088", svc["type"] == "NodePort" and svc["ports"][0]["nodePort"] == 30088,
              svc["type"])
    for path, ctype in (("/", "text/html"), ("/app.js", "javascript"), ("/styles.css", "text/css"),
                        ("/vendor/leaflet/leaflet.js", "javascript"), ("/healthz", "text/plain")):
        r = fe.get(path)
        check(f"GET {path} -> 200 ({ctype})", r.status_code == 200 and ctype in r.headers["content-type"], r.headers["content-type"])
    cfg = fe.get("/config.json").json()
    check("/config.json tells the app which deployment it is on and where Grafana etc. are", cfg["platform"] == P.name and
          cfg["grafanaPort"] == ("3000" if P.name == "compose" else "30030"), cfg)


@req("UI.proxy")
def test_ui_proxies_the_backend_with_the_right_restrictions(check):
    """The browser only talks to the UI's origin: /api, /coord (read-only), /prom (queries) are forwarded."""
    h = fe.get("/api/health")
    check("/api/... reaches the request-service", h.status_code == 200 and h.json()["status"] == "ok", h.json())
    s = fe.get("/api/summary").json()
    check("/api/summary (dashboard counts) through the UI", s["requests"]["total"] >= 1, s["requests"]["by_status"])
    lst = fe.get("/api/requests", params={"order": "desc", "limit": 3}).json()
    check("/api/requests?order=desc&limit=3 returns the 3 newest", lst["count"] == 3 and lst["total"] >= 3 and
          lst["items"][0]["created_at"] >= lst["items"][-1]["created_at"], f"3 of {lst['total']}")
    c = fe.get("/coord/cluster")
    check("/coord/cluster (read) is allowed", c.status_code == 200 and c.json()["num_shards"] == 3, "200")
    for method in ("PUT", "DELETE", "POST"):
        r = fe.request(method, "/coord/internal/data/ui-must-not-write", json={"value": 1})
        check(f"/coord {method} (a storage write) is blocked", r.status_code == 403, r.status_code)
    q = fe.get("/prom/api/v1/query", params={"query": "sum(erms_storage_node_up)"}).json()
    check("/prom/api/v1/query works", q["status"] == "success", q["data"]["result"][0]["value"][1])
    for path in ("/prom/api/v1/admin/tsdb/snapshot", "/prom/-/reload", "/prom/api/v1/status/config"):
        check(f"{path} (Prometheus admin / config) is blocked", fe.get(path).status_code == 403, "403")
    framing = httpx.get(f"{P.grafana}/d/erms-overview", timeout=10).headers.get("x-frame-options")
    check("Grafana may be embedded in the UI (no X-Frame-Options)", framing is None, framing or "none")
    # a restarted backend gets a new address: the UI must find it again (nginx re-resolves every 10 s)
    if P.name == "compose":
        spec_env.sh(["docker", "compose", "restart", "request-service"], timeout=120)
        how = "docker compose restart request-service"
    else:
        P.kubectl("rollout", "restart", "deploy/request-service")
        P.kubectl("rollout", "status", "deploy/request-service", "--timeout=180s", timeout=200)
        how = "kubectl rollout restart deploy/request-service"
    t0 = time.time()
    wait_for(lambda: fe.get("/api/health").status_code == 200 and fe.get("/api/health").json()["broker_connected"],
             timeout=120, interval=1, desc="API through the UI after restart")
    check("after the API is restarted (new container / pods) the UI reaches it again", True,
          f"{how}: back {time.time() - t0:.0f}s later")


# ============================================================================= every tab
@req("UI.errors", "UI.health")
def test_ui_every_tab_loads_without_errors(check, ui):
    """Open every tab; no JavaScript error, no failed call; the header shows live health."""
    for name in TABS:
        tab(ui, name)
        time.sleep(3 if name != "monitoring" else 6)
        check(f"tab '{name}' renders", ui.is_visible(f"#tab-{name}"), name)
    check("no JavaScript errors or failed calls on any tab", not ui.errors, ui.errors[:3] or "none")
    pills = {p: ui.inner_text(f"#h-{p}") for p in ("api", "broker", "shards", "nodes", "matching")}
    check("header: API up", pills["api"] == "API up", pills["api"])
    check("header: RabbitMQ connected", pills["broker"] == "RabbitMQ connected", pills["broker"])
    check("header: all 3 shards healthy", pills["shards"] == "Shards 3/3", pills["shards"])
    check("header: all 6 storage nodes ready", pills["nodes"] == "Nodes 6/6 ready", pills["nodes"])
    check("header: matching pods counted (from Prometheus)", re.fullmatch(r"Matching pods [1-9]", pills["matching"]),
          pills["matching"])


@req("UI.overview")
def test_ui_overview_matches_the_backend(check, ui):
    """The Overview numbers are the backend's numbers."""
    time.sleep(1)
    s = api.get("/summary").json()
    kpis = {k.query_selector(".label").inner_text(): k.query_selector(".value").inner_text()
            for k in ui.query_selector_all("#ov-kpis .kpi")}
    check("8 key figures are shown", len(kpis) == 8, list(kpis))
    check("'Requests' equals the backend total", abs(int(kpis["Requests"]) - s["requests"]["total"]) <= 2,
          f"UI {kpis['Requests']}, API {s['requests']['total']}")
    check("'Storage nodes ready' shows 6/6", kpis["Storage nodes ready"] == "6/6", kpis["Storage nodes ready"])
    rows = ui.query_selector_all("#ov-req-status .bar-row")
    check("requests by status: one bar per status (6)", len(rows) == 6, [r.inner_text().split()[0] for r in rows])
    check("latest requests table is filled", len(ui.query_selector_all("#ov-latest tbody tr")) == 8, "8 rows")
    check("the storage nodes are listed with their status", len(ui.query_selector_all("#ov-storage .n.st-ready")) == 6,
          "6 ready")


# ============================================================================= resources + requests
@req("UI.resources")
def test_ui_register_and_update_a_resource(check, ui, made):
    """Register a resource in the form, then change its availability and status from its detail view."""
    area = AREAS.new()
    rid = ui_register_resource(ui, made, area.at(1, 0), kind="ngo", name="UI water tanker", units=3)
    check("registered from the form: detail view opens with the new id", rid.startswith("res-water-"), rid)
    r = api.get(f"/resources/{rid}").json()
    check("the backend has it as entered", (r["name"], r["kind"], r["availability"], r["status"]) ==
          ("UI water tanker", "ngo", 3, "available"), f"{r['name']}, {r['kind']}, {r['availability']} units, {r['status']}")
    ui.fill("#avail-form input[name=availability]", "5")
    ui.click("#avail-form button")
    last_toast(ui, "5 free units")
    check("availability changed in the UI -> backend has 5 units", api.get(f"/resources/{rid}").json()["availability"] == 5, "5")
    ui.click("#detail-body [data-rstatus=offline]")
    last_toast(ui, "offline")
    check("'Set offline' -> backend status offline", api.get(f"/resources/{rid}").json()["status"] == "offline", "offline")
    ui.click("#detail-body [data-rstatus=available]")
    last_toast(ui, "available")
    check("'Set available' -> backend status available", api.get(f"/resources/{rid}").json()["status"] == "available", "available")
    close_dialog(ui)
    ui.select_option("#res-f-type", "water")
    ui.wait_for_selector(f"#res-table tr[data-res='{rid}']", timeout=15000)
    types = {t.inner_text() for t in ui.query_selector_all("#res-table tbody td:nth-child(2)")}
    check("the list (type filter 'water') shows it, and only water resources", types == {"water"}, types)
    for kind in ("volunteer", "other"):
        k = ui_register_resource(ui, made, area.at(2, 0), type_="food", kind=kind, name=f"UI {kind}", units=1)
        check(f"a '{kind}' can be registered from the form", api.get(f"/resources/{k}").json()["kind"] == kind, k)
        close_dialog(ui)


@req("UI.requests", "UI.match")
def test_ui_request_lifecycle_and_match_now(check, ui, made):
    """Create a request in the form, match it with 'Match now', move it through its statuses - all in the UI."""
    area = AREAS.new()
    res = ui_register_resource(ui, made, area.at(2, 0), name="UI tanker", units=2)
    close_dialog(ui)
    rid = ui_create_request(ui, made, area.at(), details="family of four needs drinking water")
    q = api.get(f"/requests/{rid}").json()
    check("created from the form: the backend has type, location, details, priority", (q["type"], q["details"], q["priority"],
          q["location"]["area"]) == ("water", "family of four needs drinking water", "high", "UI test area"), rid)
    check("detail view: status pending, storage shard and serving node shown", pill(ui) == "pending" and
          f"{q['_storage']['shard']}" in ui.inner_text("#detail-body"), f"shard {q['_storage']['shard']}")
    ui.click("#detail-body [data-act=match]")
    msg = last_toast(ui, "Matched to")
    ui.wait_for_selector("#detail-body .actions .pill.st-matched")
    check("'Match now' -> matched to the nearest water resource", "UI tanker" in msg and
          api.get(f"/requests/{rid}").json()["assigned_resource"] == res, msg)
    check("the assignment (resource, distance, matched by) is shown", "UI tanker" in ui.inner_text("#detail-body") and
          "km" in ui.inner_text("#detail-body"), "assignment section")
    check("the resource lost one unit (2 -> 1)", api.get(f"/resources/{res}").json()["availability"] == 1, "1")
    ui.click("#detail-body [data-status=in_progress]")
    ui.wait_for_selector("#detail-body .actions .pill.st-in_progress")
    check("'Mark in progress' -> in_progress", api.get(f"/requests/{rid}").json()["status"] == "in_progress", "in_progress")
    ui.click("#detail-body [data-status=resolved]")
    ui.wait_for_selector("#detail-body .actions .pill.st-resolved")
    check("'Mark resolved' -> resolved, unit released (1 -> 2)", api.get(f"/requests/{rid}").json()["status"] == "resolved"
          and api.get(f"/resources/{res}").json()["availability"] == 2, "resolved")
    check("a resolved request offers no more actions", "can no longer change" in ui.inner_text("#detail-body") and
          not ui.query_selector("#detail-body [data-status]"), "terminal")
    hist = [p.inner_text() for p in ui.query_selector_all("#detail-body .timeline .pill")]
    check("history: pending -> matched -> in progress -> resolved", hist == ["pending", "matched", "in progress", "resolved"], hist)
    close_dialog(ui)
    rid2 = ui_create_request(ui, made, area.at(), details="second request")
    ui.click("#detail-body [data-act=match]")
    ui.wait_for_selector("#detail-body .actions .pill.st-matched")
    ui.click("#detail-body [data-status=pending]")
    ui.wait_for_selector("#detail-body .actions .pill.st-pending")
    check("'Re-open' a matched request -> pending, unit released", api.get(f"/requests/{rid2}").json()["status"] == "pending", rid2)
    ui.click("#detail-body [data-status=cancelled]")
    ui.wait_for_selector("#detail-body .actions .pill.st-cancelled")
    check("'Cancel' -> cancelled", api.get(f"/requests/{rid2}").json()["status"] == "cancelled", "cancelled")
    close_dialog(ui)
    lonely = AREAS.new()
    rid3 = ui_create_request(ui, made, lonely.at(), type_="shelter", details="no shelter anywhere near")
    ui.errors.clear()
    ui.click("#detail-body [data-act=match]")
    msg = last_toast(ui, "No match")
    check("'Match now' without a suitable resource shows the API's 409 reason", "no suitable available resource" in msg, msg)
    close_dialog(ui)
    ui.select_option("#req-f-status", "resolved")
    ui.wait_for_selector(f"#req-table tr[data-req='{rid}']", timeout=15000)
    st = {p.inner_text() for p in ui.query_selector_all("#req-table .pill")}
    check("list filter status=resolved applies at once: only resolved requests (incl. ours)", st == {"resolved"}, st)
    made.requests.append(rid3)


@req("UI.async")
def test_ui_request_matched_automatically_through_rabbitmq(check, ui, made):
    """A request created with 'Match automatically' goes through RabbitMQ and shows up as matched by a worker."""
    area = AREAS.new()
    ui_register_resource(ui, made, area.at(3, 0), name="UI auto store", units=2)
    close_dialog(ui)
    rid = ui_create_request(ui, made, area.at(), details="auto", auto=True)
    msg = last_toast(ui, "created")
    check("the UI reports it stored and queued in RabbitMQ", "queued in RabbitMQ" in msg, msg)
    close_dialog(ui)
    got = wait_for(lambda: (g := api.get(f"/requests/{rid}").json())["status"] == "matched" and g, timeout=90, desc="matched")
    check("matched through the queue consumer", got["assignment"]["via"] == "queue", got["assignment"]["matched_by"])
    tab(ui, "requests")
    ui.select_option("#req-f-status", "")
    ui.wait_for_selector(f"#req-table tr[data-req='{rid}']", timeout=20000)
    ui.click(f"#req-table tr[data-req='{rid}']")
    ui.wait_for_selector("#detail-body .actions .pill.st-matched")
    check("its detail view says it was matched by the RabbitMQ consumer", "RabbitMQ consumer" in ui.inner_text("#detail-body"),
          "via queue")


# ============================================================================= map, storage, messaging, monitoring
@req("UI.map")
def test_ui_map(check, ui, made):
    """The map shows requests and resources; clicking the map pre-fills the forms with that location."""
    tab(ui, "map")
    ui.click("#map-fit")
    wait_for(lambda: len(ui.query_selector_all("#map path.leaflet-interactive")) > 10, timeout=30, desc="markers")
    check("requests and resources are drawn on the map", True,
          f"{len(ui.query_selector_all('#map path.leaflet-interactive'))} markers / lines")
    ui.evaluate("() => state.map.setView([-80, 0], 4)")          # an empty spot (Antarctica)
    time.sleep(1)
    box = ui.query_selector("#map").bounding_box()
    ui.mouse.click(box["x"] + box["width"] / 2, box["y"] + box["height"] / 2)
    ui.wait_for_selector(".leaflet-popup [data-go=req]")
    ui.click(".leaflet-popup [data-go=req]")
    ui.wait_for_selector("#tab-requests.active")
    lat = float(ui.input_value("#req-form input[name=lat]"))
    check("clicking the map -> 'New request here' opens the form with that location", -85 < lat < -75, f"latitude {lat}")


@req("UI.storage")
def test_ui_storage_cluster_and_shard_lookup(check, ui, made):
    """Shards, nodes, records and the shard lookup of any record."""
    tab(ui, "storage")
    ui.wait_for_selector("#st-shards .shard")
    check("3 shard cards, each healthy", len(ui.query_selector_all("#st-shards .shard")) == 3 and
          len(ui.query_selector_all("#st-shards .pill.st-ready")) >= 6, "3 shards")
    check("6 storage nodes with records and heartbeat age", len(ui.query_selector_all("#st-shards .node")) == 6,
          ui.query_selector("#st-shards .node .meta").inner_text())
    check("records per shard and the read / failover statistics are shown",
          len(ui.query_selector_all("#st-dist .bar-row")) == 3 and "read failovers" in ui.inner_text("#st-stats"), "")
    rq = made.request(AREAS.new().at(), details="lookup me")
    ui.fill("#st-lookup input[name=id]", rq["id"])
    ui.click("#st-lookup button")
    ui.wait_for_selector("#st-lookup-out dd b")
    shard = ui.inner_text("#st-lookup-out dd b")
    out = ui.inner_text("#st-lookup-out")
    check("lookup of a request id: the shard the coordinator chose and its replicas", shard == str(rq["_storage"]["shard"])
          and all(r in out for r in rq["_storage"]["replicas"]), f"shard {shard}, replicas {rq['_storage']['replicas']}")
    if P.name == "compose":                  # crypto.subtle exists on http://localhost (a secure context) only
        check("the browser computes sha256(key) mod 3 itself and gets the same shard", f"computed here too: {shard}" in out, out.splitlines()[1])
    cmds = ui.inner_text("#st-cmds")
    check("the failure-demo commands match this deployment",
          ("docker compose stop" in cmds) if P.name == "compose" else ("kubectl -n erms" in cmds), cmds.splitlines()[0])


@req("UI.failure")
def test_ui_shows_a_storage_failure_live(check, ui, made):
    """Stop a storage node: the UI shows it down (and 5/6 in the header); restart it: ready again."""
    F = P.failure_node(1)
    tab(ui, "storage")
    node_pill = f"#st-shards .node:has(b:text-is('{F}')) .pill"
    ui.wait_for_selector(node_pill)
    check(f"before: {F} shown ready", ui.inner_text(node_pill) == "ready", "ready")
    check.note("STOP", P.stop_node(F))
    try:
        wait_for(lambda: ui.inner_text(node_pill) == "down", timeout=60, interval=1, desc="UI shows down")
        check(f"during: the UI shows {F} DOWN", True, ui.query_selector(f"#st-shards .node:has(b:text-is('{F}')) .meta").inner_text())
        wait_for(lambda: ui.inner_text("#h-nodes") == "Nodes 5/6 ready", timeout=20, interval=1, desc="header 5/6")
        check("during: the header shows Nodes 5/6 ready", True, ui.inner_text("#h-nodes"))
    finally:
        check.note("RESTART", P.start_node(F))
    wait_for(lambda: node_status(F) == "ready", timeout=180, desc="node ready")
    wait_for(lambda: ui.inner_text(node_pill) == "ready" and ui.inner_text("#h-nodes") == "Nodes 6/6 ready",
             timeout=30, interval=1, desc="UI back to ready")
    check(f"after: the UI shows {F} ready again and Nodes 6/6", True, ui.inner_text("#h-nodes"))


@req("UI.messaging")
def test_ui_messaging_charts_and_burst(check, ui, made):
    """Queue / matching figures from Prometheus, 3 charts, and the burst generator sending requests."""
    tab(ui, "messaging")
    ui.wait_for_selector("#mq-kpis .kpi")
    labels = [k.inner_text() for k in ui.query_selector_all("#mq-kpis .kpi .label")]
    check("8 figures: queue depth, processing, retry, consumers, matching pods, matched, time to match p50 / p95",
          len(labels) == 8, labels)
    pods = ui.query_selector_all("#mq-kpis .kpi .value")[4].inner_text()
    check("'Matching pods' equals the running matching instances", pods == str(len(P.matching_workers())), pods)
    # state=attached: a flat line (e.g. an empty queue) has zero height, which Playwright calls "not visible"
    ui.wait_for_selector("#ch-queue svg polyline", state="attached", timeout=45000)
    check("3 charts drawn from Prometheus (queue vs pods, time to match, created vs matched)",
          len(ui.query_selector_all(".chart svg")) == 3, "svg charts")
    ui.fill("#load-form input[name=count]", "30")
    ui.select_option("#load-form select[name=type]", "other")
    ui.click("#load-form button[type=submit]")
    ui.wait_for_function("() => document.querySelector('#load-out').textContent.includes('accepted')", timeout=90000)
    out = ui.inner_text("#load-out")
    check("'Send burst' sends 30 requests (and registers a resource for them)", out.startswith("30/30 requests accepted"), out)


@req("UI.monitoring")
def test_ui_monitoring_tab(check, ui):
    """Grafana dashboard embedded and live, links to the other tools, scrape-target health."""
    tab(ui, "monitoring")
    src = ui.get_attribute("#grafana", "src")
    check("the Grafana 'ERMS overview' dashboard is embedded (kiosk mode)", "erms-overview" in src and "kiosk" in src, src)
    frame = ui.frame_locator("#grafana")
    frame.get_by_text("Healthy shards").first.wait_for(timeout=45000)
    check("... and it really renders inside the UI (panel 'Healthy shards' visible)", True, "Grafana panel text found")
    links = {a.inner_text(): a.get_attribute("href") for a in ui.query_selector_all("#mon-links a")}
    ports = ("3000", "9090", "15672", "8080", "8001") if P.name == "compose" else ("30030", "30090", "30672", "30080", "30081")
    check("links to Grafana, Prometheus, RabbitMQ UI, Swagger, coordinator with this deployment's ports",
          all(any(f":{p}" in h for h in links.values()) for p in ports), list(links))
    ui.wait_for_selector("#mon-targets tbody tr")
    rows = ui.query_selector_all("#mon-targets tbody tr")
    up = sum("up" == r.query_selector("td:nth-child(3)").inner_text() for r in rows)
    check("Prometheus scrape targets listed, all up", up == len(rows) and len(rows) >= 12, f"{up}/{len(rows)} up")
