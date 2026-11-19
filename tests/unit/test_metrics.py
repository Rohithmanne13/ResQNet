"""Unit tests for the Prometheus instrumentation (extension #39), no Docker needed."""
import asyncio

import httpx
from fastapi import FastAPI
from fastapi.testclient import TestClient
from prometheus_client import REGISTRY

from common.metrics import instrument


def sample(name, **labels):
    return REGISTRY.get_sample_value(name, labels) or 0.0


def test_instrument_exposes_metrics_with_route_templates():
    app = FastAPI()

    @app.get("/things/{thing_id}")
    def get_thing(thing_id: str):
        return {"id": thing_id}

    instrument(app, "unit-test-svc")
    c = TestClient(app)
    before = sample("erms_http_requests_total", service="unit-test-svc", method="GET", route="/things/{thing_id}",
                    status="200")
    for i in range(3):
        assert c.get(f"/things/id-{i}").status_code == 200
    assert c.get("/nope").status_code == 404
    r = c.get("/metrics")
    assert r.status_code == 200 and r.headers["content-type"].startswith("text/plain")
    text = r.text
    # the route TEMPLATE is the label, never the raw id (bounded number of time series)
    assert 'route="/things/{thing_id}"' in text and "id-1" not in text
    assert sample("erms_http_requests_total", service="unit-test-svc", method="GET", route="/things/{thing_id}",
                  status="200") == before + 3
    assert sample("erms_http_requests_total", service="unit-test-svc", method="GET", route="unmatched",
                  status="404") >= 1
    assert sample("erms_http_request_duration_seconds_count", service="unit-test-svc", method="GET",
                  route="/things/{thing_id}") >= 3
    assert 'erms_service_info{instance=' in text
    assert "/metrics" not in app.openapi()["paths"]          # not part of the documented client API


def test_coordinator_exports_live_cluster_state():
    import coordinator.app as c
    c.nodes.clear()
    c.nodes["n0a"] = c.Node("n0a", 0, "http://n0a:8000", c.READY, key_count=7)
    c.nodes["n0b"] = c.Node("n0b", 0, "http://n0b:8000", c.READY, key_count=7)
    c.nodes["n1a"] = c.Node("n1a", 1, "http://n1a:8000", c.READY, key_count=3)
    c.nodes["n1b"] = c.Node("n1b", 1, "http://n1b:8000", c.READY, key_count=3)
    assert sample("erms_storage_node_up", node="n0a", shard="0") == 1
    assert sample("erms_shard_ready_replicas", shard="0") == 2
    assert sample("erms_shard_healthy", shard="2") == 0            # no node for shard 2
    assert sample("erms_storage_node_keys", node="n1b", shard="1") == 3
    downs = sample("erms_storage_node_down_total", node="n0a", shard="0")
    c.mark_down(c.nodes["n0a"], "unit test")
    assert sample("erms_storage_node_up", node="n0a", shard="0") == 0
    assert sample("erms_storage_node_status", node="n0a", shard="0", status="down") == 1
    assert sample("erms_storage_node_status", node="n0a", shard="0", status="ready") == 0
    assert sample("erms_shard_ready_replicas", shard="0") == 1
    assert sample("erms_storage_node_down_total", node="n0a", shard="0") == downs + 1
    c.mark_down(c.nodes["n0a"], "again")                             # already down: not counted twice
    assert sample("erms_storage_node_down_total", node="n0a", shard="0") == downs + 1
    failovers = sample("erms_storage_failovers_total")
    c.stats["failovers"] += 2
    assert sample("erms_storage_failovers_total") == failovers + 2
    text = TestClient(c.app).get("/metrics").text
    assert "erms_storage_node_up" in text and "erms_storage_degraded_writes_total" in text
    c.nodes.clear()


def test_storage_node_records_resync_duration_and_counts():
    import storage_node.app as sn
    lbl = {"node": sn.NODE_ID, "shard": str(sn.SHARD_ID)}
    rows = [{"key": f"spec:k{i}", "value": {"i": i}, "version": 10 + i, "deleted": False} for i in range(4)]
    transport = httpx.MockTransport(lambda req: httpx.Response(200, json={"items": rows, "node_id": "peer"}))
    ok0 = sample("erms_storage_syncs_total", result="ok", **lbl)
    count0 = sample("erms_storage_sync_duration_seconds_count", **lbl)
    recv0 = sample("erms_storage_sync_records_received_total", **lbl)

    async def go():
        async with httpx.AsyncClient(transport=transport) as client:
            await sn.sync_from(client, ["http://peer:8000"])
    asyncio.run(go())
    assert sn.state["status"] == "ready"
    assert sample("erms_storage_syncs_total", result="ok", **lbl) == ok0 + 1
    assert sample("erms_storage_sync_duration_seconds_count", **lbl) == count0 + 1
    assert sample("erms_storage_sync_records_received_total", **lbl) == recv0 + 4
    assert sample("erms_storage_last_sync_duration_seconds", **lbl) >= 0
    assert sample("erms_storage_local_keys", **lbl) >= 4
    assert sample("erms_storage_local_ready", **lbl) == 1


def test_matcher_counts_outcomes_and_time_to_match():
    from test_edge_cases import FakeStore, put_request, put_resource
    from matching_service.matcher import match_request
    store = FakeStore()
    put_resource(store, availability=1)
    put_request(store, "req-m1")
    store.data["request:req-m1"][0]["created_at"] = "2026-01-01T00:00:00.000+00:00"
    put_request(store, "req-m2")
    m0 = sample("erms_matches_total", result="matched", via="api")
    n0 = sample("erms_matches_total", result="no_match", via="api")
    t0 = sample("erms_time_to_match_seconds_count", via="api")
    assert asyncio.run(match_request(store, "req-m1", source="api"))["result"] == "matched"
    assert asyncio.run(match_request(store, "req-m2", source="api"))["result"] == "no_match"
    assert sample("erms_matches_total", result="matched", via="api") == m0 + 1
    assert sample("erms_matches_total", result="no_match", via="api") == n0 + 1
    assert sample("erms_time_to_match_seconds_count", via="api") == t0 + 1
    assert sample("erms_time_to_match_seconds_sum", via="api") > 1000     # created long ago -> large wait
    # every label combination exists from the start (dashboards show 0 rather than "no data")
    assert REGISTRY.get_sample_value("erms_reconciler_fixes_total", {"kind": "orphan_claim"}) is not None
    assert REGISTRY.get_sample_value("erms_matches_total", {"result": "skipped", "via": "queue"}) is not None


def test_request_service_exposes_metrics():
    import request_service.app as rs
    text = TestClient(rs.app).get("/metrics").text
    for name in ("erms_requests_created_total", "erms_requests_published_total", "erms_resources_registered_total"):
        assert name in text
    assert 'erms_requests_created_total{type="water"}' in text


def test_scan_that_switches_replica_counts_as_failover():
    """Any read path that has to switch to another replica (also prefix scans) raises erms_storage_failovers_total,
    so killing the replica that serves the reads always shows on the dashboard's failover panels."""
    import coordinator.app as c
    c.nodes.clear()
    c.nodes["dead"] = c.Node("dead", 0, "http://dead:8000", c.READY)
    c.nodes["live"] = c.Node("live", 0, "http://live:8000", c.READY)

    def handler(req):
        if req.url.host == "dead":
            raise httpx.ConnectError("connection refused")
        return httpx.Response(200, json={"items": [{"key": "request:x", "value": {}, "version": 1}]})
    c.http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    before = sample("erms_storage_failovers_total")
    result = asyncio.run(c.scan(prefix="request:"))
    assert [i["served_by"] for i in result["items"]] == ["live"]          # served by the replica
    assert c.nodes["dead"].status == c.DOWN                               # failed node detected
    assert sample("erms_storage_failovers_total") == before + 1
    c.nodes.clear()
    c.http = None
