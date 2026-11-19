"""Tests for the two extensions, against the RUNNING system (same harness and report as the PDF suite):

  #39  Prometheus + Grafana monitoring           (Docker Compose and Kubernetes)
  #18  KEDA autoscaling on RabbitMQ queue depth  (Kubernetes)

    ./scripts/spec-test.sh compose -k x39          ./scripts/spec-test.sh k8s -k "x39 or x18"

Requirement ids X39.* / X18.* are listed in tests/spec/requirements.py. Runs after test_spec_compliance.py
(file order), so the system already carries varied data and traffic.
"""
import concurrent.futures as cf
import os
import threading
import time
from collections import Counter
from datetime import datetime

import httpx
import pytest

import spec_env
from test_spec_compliance import (P, ROOT, Probe, all_matched, api, coord, fresh_client, get_resource, http_from,
                                  node_status, queue, req, wait_for)
from test_spec_compliance import area, made, system_ready  # noqa: F401  (fixtures used by these tests)

prom_http = httpx.Client(base_url=P.prometheus, timeout=20)
graf = httpx.Client(base_url=P.grafana, timeout=20)
graf_admin = httpx.Client(base_url=P.grafana, timeout=20, auth=P.grafana_auth)
DASHBOARD = os.path.join(ROOT, "monitoring", "grafana", "dashboards", "erms-overview.json")

k8s_only = pytest.mark.skipif(P.name != "k8s", reason="KEDA runs on Kubernetes only: verified by the run "
                                                      "against the minikube deployment (SPEC_TARGET=k8s)")


# ============================================================================= helpers
def prom(query: str) -> list[dict]:
    r = prom_http.get("/api/v1/query", params={"query": query}).json()
    assert r["status"] == "success", r
    return r["data"]["result"]


def prom_value(query: str) -> float | None:
    res = prom(query)
    return float(res[0]["value"][1]) if res else None


def grafana_query(expr: str) -> list:
    """Run a panel expression through Grafana's own datasource proxy (what the dashboard really does)."""
    r = graf_admin.post("/api/ds/query", json={
        "queries": [{"refId": "A", "datasource": {"uid": "prometheus"}, "expr": expr, "instant": True}],
        "from": "now-10m", "to": "now"})
    assert r.status_code == 200, r.text
    frames = r.json()["results"]["A"].get("frames", [])
    return [f for f in frames if f.get("data", {}).get("values") and f["data"]["values"][-1]]


def dashboard() -> dict:
    import json
    return json.load(open(DASHBOARD))


def panel_queries() -> list[tuple[str, str]]:
    return [(p["title"], t["expr"]) for p in dashboard()["panels"] for t in p.get("targets", [])]


def scrape_metrics(text: str, name: str) -> list[str]:
    return [line for line in text.splitlines() if line.startswith(name)]


def traffic(area, made, n: int = 3) -> list[str]:
    """Some real API traffic (create, read, match) so rate()-based panels have something to show."""
    made.resource(area.at(1, 0), availability=n + 2, name="monitoring traffic")
    ids = [made.request(area.at(), auto_match=True, details=f"monitoring {i}")["id"] for i in range(n)]
    for i in ids:
        api.get(f"/requests/{i}")
    wait_for(lambda: all_matched(ids), timeout=90, desc="traffic matched")
    return ids


# ============================================================================= #39 Prometheus + Grafana
@req("X39.both")
def test_x39_monitoring_is_deployed(check):
    """#39: Prometheus and Grafana run as part of this deployment (Compose services / Kubernetes Deployments)."""
    if P.name == "compose":
        for svc in ("prometheus", "grafana"):
            check(f"'{svc}' container is running", P.component_count(svc) == 1, f"{P.component_count(svc)} running")
    else:
        for name in ("prometheus", "grafana"):
            d = P.kjson("get", f"deploy/{name}")
            check(f"Deployment '{name}' is available", d["status"].get("availableReplicas") == 1,
                  d["spec"]["template"]["spec"]["containers"][0]["image"])
        svcs = {s["metadata"]["name"]: s["spec"] for s in P.kjson("get", "svc")["items"]}
        check("Prometheus Service NodePort 30090, Grafana NodePort 30030",
              svcs["prometheus"]["ports"][0].get("nodePort") == 30090 and svcs["grafana"]["ports"][0].get("nodePort") == 30030,
              "30090 / 30030")
        role = P.kjson("get", "role", "prometheus-discovery")
        check("Prometheus discovers pods with a read-only Role limited to this namespace",
              all(set(r["verbs"]) <= {"get", "list", "watch"} for r in role["rules"]), role["rules"][0]["verbs"])
    ready = prom_http.get("/-/ready")
    check("Prometheus is ready", ready.status_code == 200, f"{P.prometheus}/-/ready -> {ready.status_code}")
    h = graf.get("/api/health").json()
    check("Grafana is healthy", h.get("database") == "ok", f"Grafana {h.get('version')}")


@req("X39.metrics")
def test_x39_every_service_exposes_metrics(check):
    """#39: every ERMS service serves Prometheus metrics at /metrics."""
    r = api.get("/metrics")
    check("request-service: GET /metrics -> 200 Prometheus text", r.status_code == 200 and
          r.headers["content-type"].startswith("text/plain"), r.headers["content-type"])
    for name in ("erms_http_requests_total", "erms_requests_created_total", "erms_requests_published_total"):
        check(f"   ... with {name}", scrape_metrics(r.text, name), scrape_metrics(r.text, name)[:1])
    check("   /metrics is not part of the documented client API", "/metrics" not in api.get("/openapi.json").json()["paths"],
          "hidden from /docs")
    c = coord.get("/metrics").text
    up = scrape_metrics(c, "erms_storage_node_up{")
    check("coordinator: /metrics with the live state of all 6 storage nodes", len(up) == 6, up[:2])
    for name in ("erms_shard_ready_replicas", "erms_storage_failovers_total", "erms_storage_node_down_total",
                 "erms_storage_degraded_writes_total"):
        check(f"   ... with {name}", scrape_metrics(c, name), scrape_metrics(c, name)[:1])
    workers = sorted(P.matching_workers())
    m = http_from("request-service", "GET", "http://matching-service:8000/metrics")
    check("matching-service: /metrics (called from inside the cluster)", m["status"] == 200, f"{len(workers)} instance(s)")
    for name in ("erms_matches_total", "erms_time_to_match_seconds_bucket", "erms_messages_consumed_total",
                 "erms_reconciler_fixes_total"):
        check(f"   ... with {name}", scrape_metrics(m["body"], name), scrape_metrics(m["body"], name)[:1])
    for node in sorted(n["node_id"] for n in coord.get("/internal/nodes").json()["nodes"]):
        out = http_from(P.node_exec_target(node), "GET", "http://localhost:8000/metrics")
        keys = scrape_metrics(out.get("body", ""), "erms_storage_local_keys")
        check(f"storage node {node}: /metrics with its own records and resync metrics",
              out["status"] == 200 and keys and scrape_metrics(out["body"], "erms_storage_sync_duration_seconds_count"),
              keys[0] if keys else out)


@req("X39.scrape", "X39.rabbit")
def test_x39_prometheus_scrapes_every_component(check):
    """#39: Prometheus scrapes every component and every replica, including RabbitMQ's built-in plugin."""
    expect = {"coordinator": 1, "storage-node": 6, "request-service": P.component_count("request-service"),
              "matching-service": len(P.matching_workers()), "rabbitmq": 1}

    def targets():
        ts = prom_http.get("/api/v1/targets").json()["data"]["activeTargets"]
        by: dict[str, list] = {}
        for t in ts:
            by.setdefault(t["labels"]["job"], []).append(t)
        ok = all(len(by.get(j, [])) == n and all(t["health"] == "up" for t in by[j]) for j, n in expect.items())
        return by if ok else None
    by = wait_for(targets, timeout=90, interval=2, desc="all scrape targets up")
    for job, n in expect.items():
        check(f"job '{job}': {n} target(s), all up", True, sorted(t["labels"]["instance"] for t in by[job]))
    ready = prom('rabbitmq_detailed_queue_messages_ready{queue="matching.requests"}')
    check("RabbitMQ plugin: depth of queue matching.requests is collected", ready, f"{ready[0]['value'][1]} waiting")
    cons = prom_value('sum(rabbitmq_detailed_queue_consumers{queue="matching.requests"})')
    check("RabbitMQ plugin: its consumer count is collected (= matching pods)", cons is not None and cons >= 1, cons)
    check("the retry queue matching.retry is collected too", prom('rabbitmq_detailed_queue_messages_ready{queue="matching.retry"}'),
          "matching.retry")
    names = prom_http.get("/api/v1/label/__name__/values").json()["data"]
    erms = [n for n in names if n.startswith("erms_")]
    check("ERMS metric families stored in Prometheus", len(erms) >= 40, f"{len(erms)} erms_* metric names")


@req("X39.grafana", "X39.secure")
def test_x39_grafana_is_provisioned_and_secured(check):
    """#39: Grafana comes up with the Prometheus datasource and the ERMS dashboard, viewable without login."""
    ds = graf_admin.get("/api/datasources").json()
    check("datasource 'Prometheus' provisioned (http://prometheus:9090)",
          any(d["uid"] == "prometheus" and d["url"] == "http://prometheus:9090" for d in ds), [d["name"] for d in ds])
    hz = graf_admin.get("/api/datasources/uid/prometheus/health").json()
    check("   and Grafana can reach Prometheus through it", hz.get("status") == "OK", hz.get("message"))
    found = graf.get("/api/search", params={"query": "ERMS overview"}).json()
    check("dashboard 'ERMS overview' provisioned in folder 'ERMS'",
          any(d["uid"] == "erms-overview" and d.get("folderTitle") == "ERMS" for d in found), [d["title"] for d in found])
    home = graf.get("/api/dashboards/home").json()
    # Grafana <= 12 returns the home dashboard itself, Grafana 13 a redirect to it
    target = home.get("redirectUri") or home.get("dashboard", {}).get("title", "")
    check("it is Grafana's home page (opening Grafana shows it directly)",
          "erms-overview" in target or target == "ERMS overview", target)
    anon = graf.get("/api/dashboards/uid/erms-overview")
    check("anyone can VIEW the dashboard without logging in (anonymous viewer)", anon.status_code == 200,
          f"GET without login -> {anon.status_code}")
    edit = graf.post("/api/dashboards/db", json={"dashboard": {"title": "x"}, "overwrite": True})
    check("... but nobody can change it without logging in", edit.status_code in (401, 403),
          f"POST without login -> {edit.status_code}")
    check("the admin login uses the generated password (from .env / Secret grafana-credentials)",
          graf_admin.get("/api/user").status_code == 200 and len(P.grafana_auth[1]) >= 16,
          f"admin / {len(P.grafana_auth[1])}-character random password")
    bad = httpx.get(f"{P.grafana}/api/user", auth=("admin", "admin"), timeout=10).status_code
    check("the default admin/admin is rejected", bad == 401, f"admin/admin -> {bad}")


@req("X39.panels", "X39.nodata")
def test_x39_dashboard_shows_everything_promised_with_data(check, area, made):
    """#39: the dashboard has a panel for each promised view (queue depth, time to match, failovers, shard health,
    node up/down, resync duration, matching pods, API) and every panel query returns data, through Grafana."""
    titles = [p["title"] for p in dashboard()["panels"] if p["type"] != "row"]
    promised = {
        "queue depth": ("Queue depth", "RabbitMQ queue depth"),
        "time from request to match": ("Time from request to match",),
        "failovers": ("Read failovers", "Failures (per minute)"),
        "shard health": ("Healthy shards", "Ready replicas per shard"),
        "storage node up / down (red / green)": ("Storage nodes (green = ready, red = down)", "Storage nodes ready"),
        "resync duration": ("Last resync duration per node",),
        "matching pods (autoscaling)": ("Matching pods", "Autoscaling: matching pods vs queue depth"),
        "API rate / latency / errors": ("API requests per second, by status", "API latency p95, by route"),
    }
    for what, panels in promised.items():
        check(f"panel(s) for {what}", all(t in titles for t in panels), " | ".join(panels))
    check.note("dashboard size", f"{len(titles)} panels in 4 sections, refresh 5 s")
    traffic(area, made)
    time.sleep(12)                       # two scrapes, so rate() panels have points
    empty = []
    for title, expr in panel_queries():
        if not grafana_query(expr):
            empty.append(f"{title}: {expr}")
    total = len(panel_queries())
    check(f"all {total} panel queries return data when run through Grafana", not empty,
          f"{total - len(empty)}/{total} with data" + (f"; empty: {empty}" if empty else ""))


@req("X39.traffic")
def test_x39_metrics_follow_real_traffic(check, area, made):
    """#39: creating and matching requests moves the counters by exactly that much, and time-to-match is right."""
    q = {"created": "sum(erms_requests_created_total)",
         "published": 'sum(erms_requests_published_total{result="ok"})',
         "matched via queue": 'sum(erms_matches_total{result="matched",via="queue"})',
         "time-to-match samples": 'sum(erms_time_to_match_seconds_count{via="queue"})',
         "time-to-match total seconds": 'sum(erms_time_to_match_seconds_sum{via="queue"})',
         "POST /requests 201": 'sum(erms_http_requests_total{job="request-service",method="POST",route="/requests",status="201"})'}
    before = {k: prom_value(v) or 0.0 for k, v in q.items()}
    n = 6
    made.resource(area.at(2, 0), availability=n, name="metrics check")
    ids = [made.request(area.at(), auto_match=True, details=f"metrics {i}")["id"] for i in range(n)]
    done = wait_for(lambda: all_matched(ids), timeout=90, desc="matched")

    def moved():
        now = {k: prom_value(v) or 0.0 for k, v in q.items()}
        ok = all(now[k] - before[k] >= n for k in ("created", "published", "matched via queue",
                                                   "time-to-match samples", "POST /requests 201"))
        return now if ok else None
    now = wait_for(moved, timeout=60, interval=2, desc="Prometheus caught up")
    for k in ("created", "published", "POST /requests 201", "matched via queue", "time-to-match samples"):
        check(f"{k}: +{n} after {n} requests", now[k] - before[k] >= n, f"{before[k]:.0f} -> {now[k]:.0f}")
    recorded = (now["time-to-match total seconds"] - before["time-to-match total seconds"]) / \
        (now["time-to-match samples"] - before["time-to-match samples"])
    actual = [(datetime.fromisoformat(x["assignment"]["matched_at"]) - datetime.fromisoformat(x["created_at"])).total_seconds()
              for x in done]
    avg = sum(actual) / len(actual)
    check("the recorded time-to-match equals the real one (created_at -> matched_at of the requests)",
          abs(recorded - avg) < 3, f"metric average {recorded:.2f}s, requests' own timestamps {avg:.2f}s")


@req("X39.failure")
def test_x39_storage_failure_is_visible_on_the_dashboard(check, area, made):
    """#39 demo moment: stop a storage node -> it turns DOWN (red) on the dashboard and failures are counted;
    restart it -> READY (green) again, with its resync and resync duration recorded."""
    S = 1
    F = P.failure_node(S)
    up = f'erms_storage_node_up{{node="{F}"}}'
    wait_for(lambda: prom_value(up) == 1, timeout=60, desc="node up in Prometheus")
    check(f"before: {F} shows READY (1, green)", prom_value(up) == 1, f"{up} = 1")
    downs_q = f'sum(erms_storage_node_down_total{{node="{F}"}})'
    degraded_q = f'sum(erms_storage_degraded_writes_total{{shard="{S}"}})'
    downs0 = prom_value(downs_q) or 0
    degraded0 = prom_value(degraded_q) or 0
    t_stop = time.time()
    check.note("STOP the storage node", P.stop_node(F))
    try:
        wait_for(lambda: prom_value(up) == 0, timeout=60, interval=1, desc="node down in Prometheus")
        check(f"during: the dashboard shows {F} DOWN (0, red)", True, f"{time.time() - t_stop:.0f}s after the stop")
        check("during: 'Grafana -> Prometheus' returns the same (the panel's own query)",
              any(f["data"]["values"][-1][-1] == 0 for f in grafana_query(up)), "erms_storage_node_up = 0")
        check(f"during: 'Ready replicas per shard' drops to 1 for shard {S}",
              prom_value(f'erms_shard_ready_replicas{{shard="{S}"}}') == 1, "1 of 2")
        check("during: 'Healthy shards' stays 3 (the replica serves the shard)", prom_value("sum(erms_shard_healthy)") == 3, "3")
        made.request_on_shard(S, area.at(), details="written during the failure")
        wait_for(lambda: (prom_value(degraded_q) or 0) > degraded0, timeout=30, desc="degraded write counted")
        check("during: a write with one copy is counted as a degraded write", True,
              f"{degraded0:.0f} -> {prom_value(degraded_q):.0f}")
        wait_for(lambda: (prom_value(downs_q) or 0) > downs0, timeout=30, desc="node-down counted")
        check("during: the node-down event is counted", True, f"{downs0:.0f} -> {prom_value(downs_q):.0f}")
    finally:
        t_start = time.time()
        check.note("RESTART the storage node", P.start_node(F))
    wait_for(lambda: node_status(F) == "ready", timeout=180, desc="node ready")
    wait_for(lambda: prom_value(up) == 1, timeout=60, interval=1, desc="node up again in Prometheus")
    check(f"after: the dashboard shows {F} READY (1, green) again", True, f"{time.time() - t_start:.0f}s after the restart")
    span = int(time.time() - t_stop) + 30
    low = prom_value(f"min_over_time({up}[{span}s])")
    check("the node's timeline over the test shows the red stretch (min 0) and ends green (1)", low == 0 and
          prom_value(up) == 1, f"min over last {span}s = {low:.0f}, now = 1")
    sync_q = f'erms_storage_last_sync_duration_seconds{{node="{F}"}}'
    started = f'process_start_time_seconds{{job="storage-node",instance="{F}"}}'
    wait_for(lambda: (prom_value(started) or 0) > t_stop and prom_value(sync_q) is not None, timeout=60,
             desc="restarted node scraped with its resync")
    dur = prom_value(sync_q)
    ok_syncs = prom_value(f'sum(erms_storage_syncs_total{{node="{F}",result="ok"}})')
    applied = prom_value(f'sum(erms_storage_sync_records_applied_total{{node="{F}"}})')
    check("after: the restarted node's resync is recorded (count, records applied, duration on the dashboard)",
          ok_syncs and ok_syncs >= 1 and dur is not None,
          f"resyncs {ok_syncs:.0f}, missed writes applied {applied:.0f}, resync took {dur * 1000:.0f} ms")


@req("X39.failure")
def test_x39_killing_the_read_replica_makes_failovers_rise(check, area, made):
    """#39 demo moment, the 'failovers rise' part: kill the storage replica that SERVES a shard's reads while
    reads keep coming -> reads switch to the other replica, the failover counter on the dashboard rises, the node
    is counted down, and after its restart it is READY (green) again."""
    S = 1
    rq = made.request_on_shard(S, area.at(), details="read while its replica is killed")
    reader = api.get(f"/requests/{rq['id']}").json()["_storage"]["served_by"]
    check.note(f"BEFORE: reads of shard {S} are served by {reader}")
    fo_q = "sum(erms_storage_failovers_total)"
    down_q = f'sum(erms_storage_node_down_total{{node="{reader}"}})'
    up_q = f'erms_storage_node_up{{node="{reader}"}}'
    wait_for(lambda: prom_value(fo_q) is not None and prom_value(up_q) == 1, timeout=60, desc="metrics present")
    fo0, down0 = prom_value(fo_q), prom_value(down_q) or 0
    served: Counter = Counter()

    def read():
        with fresh_client() as c:
            r = c.get(f"/requests/{rq['id']}")
        if r.status_code == 200:
            served[r.json()["_storage"]["served_by"]] += 1
        return r.status_code == 200

    with Probe(read, 0.2) as probe:
        time.sleep(1)
        if P.name == "compose":
            check.note("KILL the read replica", P.stop_node(reader))
            wait_for(lambda: node_status(reader) == "down", timeout=60, interval=0.3, desc="reader down")
            time.sleep(6)                                         # keep reading from the replica for a while
            check.note("RESTART it", P.start_node(reader))
        else:
            check.note("KILL the read replica", f"kubectl -n {spec_env.NS} delete pod {reader}")
            P.kubectl("delete", "pod", reader, "--wait=false")
            wait_for(lambda: node_status(reader) != "ready", timeout=60, interval=0.2, desc="reader not ready")
            check.note("RESTART", "automatic: the StatefulSet recreates the pod with the same name and volume")
        wait_for(lambda: node_status(reader) == "ready", timeout=180, interval=0.5, desc="reader ready again")
        time.sleep(2)
    other = [n for n in served if n != reader]
    check("reads kept working: none failed while the replica was killed and came back", probe.total >= 10 and
          probe.fail == 0, probe.summary())
    check("reads switched to the other replica of the shard", other, dict(served))
    fo1 = wait_for(lambda: (v := prom_value(fo_q)) is not None and v > fo0 and v, timeout=60, desc="failovers rose")
    check("the dashboard's failover counter ROSE ('Read failovers' / 'Failures (per minute)' panels)", fo1 > fo0,
          f"erms_storage_failovers_total {fo0:.0f} -> {fo1:.0f}")
    panel = next(e for t, e in panel_queries() if t == "Read failovers")
    frames = grafana_query(panel)
    check("   ... the same value through Grafana's own panel query", frames and frames[0]["data"]["values"][-1][-1] >= fo1,
          f"{panel} = {frames[0]['data']['values'][-1][-1]:.0f}" if frames else "no data")
    down1 = wait_for(lambda: (v := prom_value(down_q) or 0) > down0 and v, timeout=60, desc="down counted")
    check(f"{reader} was counted DOWN (red on the node timeline)", down1 > down0, f"node-down events {down0:.0f} -> {down1:.0f}")
    wait_for(lambda: prom_value(up_q) == 1, timeout=60, desc="green again")
    check(f"after: {reader} is READY (green) again on the dashboard", prom_value(up_q) == 1, f"{up_q} = 1")


# ============================================================================= #18 KEDA
@k8s_only
@req("X18.install")
def test_x18_keda_is_installed(check):
    """#18: KEDA's components run in the cluster and serve the external-metrics API used by the HPA."""
    import json
    deps = json.loads(spec_env.sh(["kubectl", "-n", "keda", "get", "deploy", "-o", "json"]))["items"]
    by = {d["metadata"]["name"]: d for d in deps}
    for role, needle in (("operator", "operator"), ("metrics API server", "metrics-apiserver"),
                         ("admission webhooks", "admission")):
        name = next((n for n in by if needle in n), None)
        d = by.get(name, {})
        check(f"KEDA {role} ({name}) is available", name and d["status"].get("availableReplicas") == 1,
              d.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [{}])[0].get("image"))
    for crd in ("scaledobjects.keda.sh", "triggerauthentications.keda.sh"):
        check(f"CRD {crd} installed", spec_env.sh(["kubectl", "get", "crd", crd, "-o", "name"], check=False).strip(), crd)
    api_svc = spec_env.sh(["kubectl", "get", "apiservice", "v1beta1.external.metrics.k8s.io", "-o",
                           "jsonpath={.status.conditions[?(@.type=='Available')].status}"])
    check("the external metrics API (queue depth for the HPA) is Available", api_svc == "True", api_svc)


@k8s_only
@req("X18.scaledobject", "X18.hpa", "X18.secret")
def test_x18_scaledobject_hpa_and_credentials(check):
    """#18: the ScaledObject, the HPA KEDA creates from it, and where KEDA's broker credentials come from."""
    so = P.kjson("get", "scaledobject", "matching-service")
    spec, trig = so["spec"], so["spec"]["triggers"][0]
    check("ScaledObject targets Deployment matching-service", spec["scaleTargetRef"]["name"] == "matching-service", "")
    check("between 2 and 6 pods", (spec["minReplicaCount"], spec["maxReplicaCount"]) == (2, 6),
          f"min {spec['minReplicaCount']}, max {spec['maxReplicaCount']}, polling every {spec['pollingInterval']}s")
    check("trigger: RabbitMQ queue matching.requests, 5 waiting messages per pod",
          trig["type"] == "rabbitmq" and trig["metadata"]["queueName"] == "matching.requests" and
          trig["metadata"]["mode"] == "QueueLength" and trig["metadata"]["value"] == "5", trig["metadata"])
    conds = {c["type"]: c["status"] for c in so["status"]["conditions"]}
    check("ScaledObject is Ready and not paused", conds.get("Ready") == "True" and
          "autoscaling.keda.sh/paused-replicas" not in so["metadata"].get("annotations", {}), conds)
    hpa = P.kjson("get", "hpa", "keda-hpa-matching-service")
    check("KEDA created HPA keda-hpa-matching-service (min 2, max 6) on an external queue metric",
          hpa["spec"]["scaleTargetRef"]["name"] == "matching-service" and hpa["spec"]["minReplicas"] == 2 and
          hpa["spec"]["maxReplicas"] == 6 and hpa["spec"]["metrics"][0]["type"] == "External",
          hpa["spec"]["metrics"][0]["external"]["metric"]["name"])
    b = hpa["spec"]["behavior"]
    check("fast reaction: scale up immediately, scale down after 30 s (instead of the default 5 min)",
          b["scaleUp"]["stabilizationWindowSeconds"] == 0 and b["scaleDown"]["stabilizationWindowSeconds"] == 30, "0 s / 30 s")
    hpas = [h["metadata"]["name"] for h in P.kjson("get", "hpa")["items"]
            if h["spec"]["scaleTargetRef"]["name"] == "matching-service"]
    check("no second autoscaler fights over matching-service", hpas == ["keda-hpa-matching-service"], hpas)
    check("the old CPU-based HPA manifest is gone", not os.path.exists(os.path.join(ROOT, "k8s", "60-hpa-optional.yaml")),
          "k8s/60-hpa-optional.yaml removed")
    ta = P.kjson("get", "triggerauthentication", "rabbitmq-trigger-auth")["spec"]["secretTargetRef"][0]
    check("KEDA reads the broker URL from Secret keda-rabbitmq (TriggerAuthentication)",
          (ta["name"], ta["key"], ta["parameter"]) == ("keda-rabbitmq", "host", "host"), ta)
    sec = P.kubectl("get", "secret", "keda-rabbitmq", "-o", "jsonpath={.data.host}")
    check("   the Secret exists (contents not printed)", len(sec) > 20, f"{len(sec)} base64 characters")
    manifest = open(os.path.join(ROOT, "k8s", "80-keda-autoscaling.yaml")).read()
    check("   no password / amqp URL in the manifest", "amqp://" not in manifest and P.rabbit_auth[1] not in manifest,
          "k8s/80-keda-autoscaling.yaml")


@k8s_only
@req("X18.up", "X18.down", "X18.safe", "X18.visible")
def test_x18_burst_scales_up_then_back_down(check, area, made):
    """#18 demo moment: 200 requests pile up in RabbitMQ -> KEDA scales matching-service 2 -> 6 by itself, the
    queue drains, and it scales back to 2; nothing is lost, nothing matched twice, the API stays up."""
    P.matching_replicas("auto")
    wait_for(lambda: len(P.matching_workers()) == 2 and int(P.kubectl("get", "hpa", "keda-hpa-matching-service", "-o",
                                                                       "jsonpath={.status.desiredReplicas}") or 0) == 2,
             timeout=240, interval=3, desc="steady state: 2 matching pods")
    before = P.matching_workers()
    check("before: 2 matching pods, queue empty", len(before) == 2, sorted(before))
    n = 200
    res = made.resource(area.at(2, 0), availability=n + 10, name="KEDA burst store")
    samples: list[tuple[float, int, int, int]] = []        # (t, waiting, ready pods, desired)
    stop = threading.Event()

    def sample():
        while not stop.is_set():
            try:
                hpa = P.kjson("get", "hpa", "keda-hpa-matching-service")
                dep = P.kjson("get", "deploy", "matching-service")
                samples.append((time.time(), queue().get("messages_ready", 0), dep["status"].get("readyReplicas", 0),
                                hpa["status"].get("desiredReplicas", 0)))
            except Exception:
                pass
            stop.wait(2)
    sampler = threading.Thread(target=sample, daemon=True)

    def health():
        with fresh_client() as c:
            return c.get("/health").status_code == 200

    t0 = time.time()
    sampler.start()
    with Probe(health, 0.5) as probe:
        with cf.ThreadPoolExecutor(25) as ex:
            reqs = list(ex.map(lambda i: made.request(area.at(), auto_match=True, details=f"burst {i}"), range(n)))
        check(f"{n} requests accepted and queued", all(r["queued"] for r in reqs), f"in {time.time() - t0:.1f}s")
        ids = [r["id"] for r in reqs]
        wait_for(lambda: max((s[2] for s in samples), default=0) >= 6, timeout=150, interval=2, desc="scaled to 6")
        t_six = next(s[0] for s in samples if s[2] >= 6) - t0
        done = wait_for(lambda: all_matched(ids), timeout=300, interval=3, desc="burst matched")
        t_done = time.time() - t0
        wait_for(lambda: samples and samples[-1][2] == 2 and len(P.pods("app=matching-service")) == 2,
                 timeout=300, interval=3, desc="scaled back to 2")
        t_back = time.time() - t0
    stop.set()
    sampler.join(timeout=10)
    peak_wait = max(s[1] for s in samples)
    first_up = next((s[0] - t0 for s in samples if s[3] > 2), None)
    check("RabbitMQ buffered the burst", peak_wait >= 30, f"up to {peak_wait} messages waiting")
    check("KEDA scaled matching-service UP by itself as the queue grew", first_up is not None,
          f"HPA asked for more pods {first_up:.0f}s after the burst started")
    check("... up to the maximum of 6 pods", max(s[2] for s in samples) == 6, f"6 Ready pods {t_six:.0f}s after the burst")
    timeline = []
    for s in samples:
        if not timeline or (s[2], s[3]) != timeline[-1][1:]:
            timeline.append((round(s[0] - t0), s[2], s[3]))
    check.note("timeline (seconds, ready pods, desired)", timeline[:14])
    dist = Counter(x["assignment"]["matched_by"] for x in done)
    check("the extra pods did real work (requests matched by pods added by KEDA)",
          len([w for w in dist if w not in before]) >= 3, dict(dist))
    check(f"all {n} requests matched through the queue", len(done) == n and all(x["assignment"]["via"] == "queue" for x in done),
          f"queue drained and all matched {t_done:.0f}s after the burst")
    rs = get_resource(res["id"])
    check("no request lost or matched twice: capacity exact", rs["availability"] == 10 and
          sorted(rs["assigned_requests"]) == sorted(ids), f"availability {n + 10} -> {rs['availability']}")
    check("KEDA scaled matching-service back DOWN to 2 after the queue drained", True,
          f"2 pods again {t_back:.0f}s after the burst ({t_back - t_done:.0f}s after the last match)")
    check("the API stayed available the whole time", probe.total >= 20 and probe.fail == 0, probe.summary())
    peak_prom = prom_value('max_over_time(count(up{job="matching-service"} == 1)[15m:5s])')
    check("Prometheus recorded the scale-out (dashboard panel 'Autoscaling: matching pods vs queue depth')",
          peak_prom is not None and peak_prom >= 5, f"peak matching pods seen by Prometheus: {peak_prom:.0f}")
    wait_for(lambda: prom_value('count(up{job="matching-service"} == 1)') == 2, timeout=60, desc="Prometheus sees 2")
    check("... and the scale-in back to 2", True, "count(up{job=\"matching-service\"} == 1) = 2")


@k8s_only
@req("X18.manual")
def test_x18_manual_pin_then_automatic_again(check):
    """#18: with KEDA in charge a plain `kubectl scale` is undone; scripts/matching-replicas.sh pins the replica
    count (paused-replicas) and hands it back to KEDA."""
    P.kubectl("scale", "deploy/matching-service", "--replicas=5")
    back = wait_for(lambda: len(P.pods("app=matching-service")) == 2 and P.kjson("get", "deploy", "matching-service")
                    ["spec"]["replicas"] == 2, timeout=150, interval=3, desc="HPA restores 2")
    check("a plain `kubectl scale --replicas=5` is undone by KEDA's HPA (back to 2 with an empty queue)", back, "2 pods")
    cmd = P.matching_replicas(3)
    so = P.kjson("get", "scaledobject", "matching-service")
    check("matching-replicas.sh 3 pins 3 pods (annotation autoscaling.keda.sh/paused-replicas=3)",
          so["metadata"].get("annotations", {}).get("autoscaling.keda.sh/paused-replicas") == "3" and
          len(P.matching_workers()) == 3, cmd)
    time.sleep(45)
    check("the pin holds even with an empty queue (45 s later still 3 pods)", len(P.matching_workers()) == 3,
          sorted(P.matching_workers()))
    cmd = P.matching_replicas("auto")
    wait_for(lambda: len(P.pods("app=matching-service")) == 2, timeout=150, interval=3, desc="back to 2")
    so = P.kjson("get", "scaledobject", "matching-service")
    check("matching-replicas.sh auto removes the pin: KEDA scales back to 2 and its HPA is active again",
          "autoscaling.keda.sh/paused-replicas" not in so["metadata"].get("annotations", {}) and
          P.kjson("get", "hpa", "keda-hpa-matching-service")["spec"]["minReplicas"] == 2, cmd)
