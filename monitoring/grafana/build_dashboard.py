"""Generates monitoring/grafana/dashboards/erms-overview.json (the provisioned "ERMS overview" dashboard).

    python3 monitoring/grafana/build_dashboard.py

Edit the panels here rather than in the JSON. The spec suite queries every expression of the dashboard against
Prometheus and fails if a panel would show "No data".
"""
import json
import os

DS = {"type": "prometheus", "uid": "prometheus"}
OUT = os.path.join(os.path.dirname(os.path.abspath(__file__)), "dashboards", "erms-overview.json")
QUEUE = 'queue="matching.requests"'
panels: list[dict] = []


def _next_id() -> int:
    return len(panels) + 1


def row(title: str, y: int) -> None:
    panels.append({"id": _next_id(), "type": "row", "title": title, "collapsed": False,
                   "gridPos": {"x": 0, "y": y, "w": 24, "h": 1}, "panels": []})


def _targets(queries):
    return [{"datasource": DS, "refId": chr(65 + i), "expr": expr, "legendFormat": legend, "range": True}
            for i, (expr, legend) in enumerate(queries)]


def stat(title, expr, x, y, w, h=4, steps=None, unit="none", desc=""):
    panels.append({
        "id": _next_id(), "type": "stat", "title": title, "description": desc, "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h}, "targets": _targets([(expr, title)]),
        "options": {"reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False},
                    "colorMode": "background", "graphMode": "area", "textMode": "value", "justifyMode": "center"},
        "fieldConfig": {"defaults": {"unit": unit, "decimals": 0, "thresholds": {"mode": "absolute", "steps": steps or [
            {"color": "blue", "value": None}]}}, "overrides": []},
    })


def timeseries(title, queries, x, y, w, h=8, unit="none", desc="", stack=False, decimals=None):
    defaults = {"unit": unit, "custom": {"lineWidth": 2, "fillOpacity": 15, "showPoints": "never",
                                         "stacking": {"mode": "normal" if stack else "none"}}}
    if decimals is not None:
        defaults["decimals"] = decimals
    panels.append({
        "id": _next_id(), "type": "timeseries", "title": title, "description": desc, "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h}, "targets": _targets(queries),
        "options": {"legend": {"displayMode": "list", "placement": "bottom"}, "tooltip": {"mode": "multi"}},
        "fieldConfig": {"defaults": defaults, "overrides": []},
    })


def node_timeline(title, expr, legend, x, y, w, h=8, desc=""):
    panels.append({
        "id": _next_id(), "type": "state-timeline", "title": title, "description": desc, "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h}, "targets": _targets([(expr, legend)]),
        "options": {"showValue": "never", "mergeValues": True, "rowHeight": 0.8,
                    "legend": {"showLegend": False}, "tooltip": {"mode": "single"}},
        "fieldConfig": {"defaults": {
            "mappings": [{"type": "value", "options": {
                "0": {"text": "DOWN / not ready", "color": "red", "index": 0},
                "1": {"text": "READY", "color": "green", "index": 1}}}],
            "thresholds": {"mode": "absolute", "steps": [{"color": "red", "value": None}, {"color": "green", "value": 1}]},
            "color": {"mode": "thresholds"}}, "overrides": []},
    })


def bargauge(title, expr, legend, x, y, w, h=8, unit="s", desc=""):
    panels.append({
        "id": _next_id(), "type": "bargauge", "title": title, "description": desc, "datasource": DS,
        "gridPos": {"x": x, "y": y, "w": w, "h": h}, "targets": _targets([(expr, legend)]),
        "options": {"orientation": "horizontal", "displayMode": "gradient",
                    "reduceOptions": {"calcs": ["lastNotNull"], "fields": "", "values": False}},
        "fieldConfig": {"defaults": {"unit": unit, "min": 0, "thresholds": {"mode": "absolute", "steps": [
            {"color": "green", "value": None}, {"color": "orange", "value": 2}, {"color": "red", "value": 10}]}},
            "overrides": []},
    })


# ------------------------------------------------------------------------------------------------ layout
row("Cluster health", 0)
red_green = lambda ok: [{"color": "red", "value": None}, {"color": "green", "value": ok}]  # noqa: E731
stat("Healthy shards", "sum(erms_shard_healthy)", 0, 1, 4, steps=red_green(3),
     desc="Shards with at least one ready replica (3 = all data available)")
stat("Storage nodes ready", "sum(erms_storage_node_up)", 4, 1, 4,
     steps=[{"color": "red", "value": None}, {"color": "orange", "value": 3}, {"color": "green", "value": 6}],
     desc="Ready storage nodes out of 6 (3 shards x 2 replicas)")
stat("Matching pods", 'count(up{job="matching-service"} == 1)', 8, 1, 4,
     desc="Running matching-service instances (on Kubernetes scaled automatically by KEDA)")
stat("API pods", 'count(up{job="request-service"} == 1)', 12, 1, 4, desc="Running request-service instances")
stat("Queue depth", f"sum(rabbitmq_detailed_queue_messages_ready{{{QUEUE}}})", 16, 1, 4,
     steps=[{"color": "green", "value": None}, {"color": "orange", "value": 10}, {"color": "red", "value": 50}],
     desc="Requests waiting in RabbitMQ queue matching.requests")
stat("Read failovers", "sum(erms_storage_failovers_total)", 20, 1, 4,
     steps=[{"color": "green", "value": None}, {"color": "orange", "value": 1}],
     desc="Reads that had to switch to another replica since the coordinator started")
node_timeline("Storage nodes (green = ready, red = down)", "erms_storage_node_up", "{{node}}", 0, 5, 16, h=9,
              desc="Kill a storage pod/container and watch it turn red, then green again after it resyncs")
timeseries("Ready replicas per shard", [("erms_shard_ready_replicas", "shard {{shard}}")], 16, 5, 8, h=9, decimals=0)

row("Messaging and autoscaling", 14)
timeseries("RabbitMQ queue depth", [
    (f"sum(rabbitmq_detailed_queue_messages_ready{{{QUEUE}}})", "waiting (matching.requests)"),
    (f"sum(rabbitmq_detailed_queue_messages_unacked{{{QUEUE}}})", "being processed (unacked)"),
    ('sum(rabbitmq_detailed_queue_messages_ready{queue="matching.retry"})', "delayed retry (matching.retry)"),
], 0, 15, 12, decimals=0)
timeseries("Autoscaling: matching pods vs queue depth", [
    ('count(up{job="matching-service"} == 1)', "matching pods"),
    (f"sum(rabbitmq_detailed_queue_consumers{{{QUEUE}}})", "RabbitMQ consumers"),
    (f"sum(rabbitmq_detailed_queue_messages_ready{{{QUEUE}}})", "messages waiting"),
], 12, 15, 12, decimals=0, desc="KEDA adds matching pods while messages pile up and removes them when the queue drains")
timeseries("Matches per second, by matching pod",
           [('sum by (instance) (rate(erms_matches_total{result="matched"}[1m]))', "{{instance}}")],
           0, 23, 12, unit="reqps", stack=True)
timeseries("Requests created vs matched (per minute)", [
    ("sum(increase(erms_requests_created_total[1m]))", "created"),
    ('sum(increase(erms_matches_total{result="matched"}[1m]))', "matched"),
    ("sum(increase(erms_match_retries_total[1m]))", "sent to retry"),
], 12, 23, 12)

row("Latency and API", 31)
timeseries("Time from request to match", [
    ('histogram_quantile(0.50, sum by (le) (rate(erms_time_to_match_seconds_bucket{via="queue"}[2m])))', "p50"),
    ('histogram_quantile(0.95, sum by (le) (rate(erms_time_to_match_seconds_bucket{via="queue"}[2m])))', "p95"),
], 0, 32, 12, unit="s", desc="From POST /requests to the resource being assigned through RabbitMQ")
timeseries("API requests per second, by status", [
    ('sum by (status) (rate(erms_http_requests_total{job="request-service"}[1m]))', "HTTP {{status}}"),
], 12, 32, 12, unit="reqps", stack=True)
timeseries("API latency p95, by route", [
    ('histogram_quantile(0.95, sum by (le, route) (rate(erms_http_request_duration_seconds_bucket'
     '{job="request-service"}[2m])))', "{{route}}"),
], 0, 40, 12, unit="s")
timeseries("Storage operations per second", [
    ("sum(rate(erms_storage_writes_total[1m]))", "replicated writes"),
    ("sum(rate(erms_storage_reads_total[1m]))", "reads"),
], 12, 40, 12, unit="ops")

row("Storage failures and recovery", 48)
timeseries("Failures (per minute)", [
    ("sum(increase(erms_storage_failovers_total[1m]))", "read failovers"),
    ("sum(increase(erms_storage_node_down_total[1m]))", "nodes marked down"),
    ("sum(increase(erms_storage_degraded_writes_total[1m]))", "degraded writes (1 of 2 copies)"),
], 0, 49, 8)
bargauge("Last resync duration per node", "max by (node) (erms_storage_last_sync_duration_seconds)", "{{node}}",
         8, 49, 8, desc="How long each node took to catch up from its replica the last time it (re)joined")
timeseries("Records per storage node", [("erms_storage_node_keys", "{{node}}")], 16, 49, 8, decimals=0)
timeseries("Resyncs and records applied (per 5 min)", [
    ('sum by (node) (increase(erms_storage_syncs_total{result="ok"}[5m]))', "resyncs {{node}}"),
    ("sum(increase(erms_storage_sync_records_applied_total[5m]))", "missed writes applied"),
], 0, 57, 12)
timeseries("Self-healing (per 5 min)", [
    ("sum by (kind) (increase(erms_reconciler_fixes_total[5m]))", "reconciler {{kind}}"),
    ("sum by (reason) (increase(erms_sweeper_requeued_total[5m]))", "sweeper {{reason}}"),
], 12, 57, 12)

dashboard = {
    "uid": "erms-overview", "title": "ERMS overview", "tags": ["erms"], "timezone": "browser",
    "schemaVersion": 39, "version": 1, "editable": True, "graphTooltip": 1, "refresh": "5s",
    "time": {"from": "now-15m", "to": "now"}, "timepicker": {"refresh_intervals": ["5s", "10s", "30s", "1m"]},
    "templating": {"list": []}, "annotations": {"list": []}, "panels": panels,
}

if __name__ == "__main__":
    os.makedirs(os.path.dirname(OUT), exist_ok=True)
    with open(OUT, "w") as f:
        json.dump(dashboard, f, indent=2)
        f.write("\n")
    print(f"wrote {OUT}: {sum(p['type'] != 'row' for p in panels)} panels")
