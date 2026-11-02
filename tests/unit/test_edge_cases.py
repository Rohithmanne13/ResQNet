"""Unit tests for the edge-case fixes (no Docker needed):
coordinator bootstrap after restart / version floor, type-partitioned resource keys, idempotent
claims, the capacity reconciler, and the /match proxy's error mapping."""
import asyncio
import copy
import time

import httpx
import pytest
from fastapi.testclient import TestClient

from common.models import new_resource_id, request_key, resource_key, resource_prefix, resource_type_of
from common.resources import try_assign
from common.storage_client import Conflict, NotFound
from storage_node.app import Store


# ----------------------------------------------------------------------------- in-memory storage fake
class FakeStore:
    """Same contract as StorageClient: versions, compare-and-set, NotFound/Conflict, prefix scans."""

    def __init__(self):
        self.data: dict[str, tuple[dict, int]] = {}
        self.v = 0
        self.scanned: list[str] = []
        self.unavailable: list[int] = []

    async def get(self, key):
        if key not in self.data:
            raise NotFound(f"{key} not found")
        value, version = self.data[key]
        return {"key": key, "value": copy.deepcopy(value), "version": version}

    async def put(self, key, value, if_version=None):
        current = self.data.get(key, (None, 0))[1]
        if if_version is not None and current != if_version:
            raise Conflict("version conflict")
        self.v += 1
        self.data[key] = (copy.deepcopy(value), self.v)
        return {"key": key, "version": self.v, "shard": 0, "replicas": ["n0"]}

    async def list(self, prefix):
        self.scanned.append(prefix)
        items = [{"key": k, "value": copy.deepcopy(v), "version": ver, "shard": 0}
                 for k, (v, ver) in sorted(self.data.items()) if k.startswith(prefix)]
        return {"items": items, "unavailable_shards": list(self.unavailable)}

    def value(self, key):
        return self.data[key][0]


def run(coro):
    return asyncio.run(coro)


def put_resource(store, rtype="water", availability=2, assigned=(), status=None, lat=12.97, lon=77.59):
    rid = new_resource_id(rtype, f"{len(store.data):012x}")
    res = {"id": rid, "type": rtype, "name": rid, "location": {"lat": lat, "lon": lon},
           "availability": availability, "assigned_requests": list(assigned),
           "status": status or ("available" if availability > 0 else "busy")}
    run(store.put(resource_key(rid), res))
    return rid


def put_request(store, rid, rtype="water", status="pending", assigned=None, lat=12.97, lon=77.59):
    req = {"id": rid, "type": rtype, "location": {"lat": lat, "lon": lon}, "status": status,
           "assigned_resource": assigned, "history": [], "auto_match": True, "updated_at": "2026-01-01T00:00:00+00:00"}
    run(store.put(request_key(rid), req))
    return rid


# ----------------------------------------------------------------------------- full scans per match
def test_resource_ids_and_keys_are_partitioned_by_type():
    rid = new_resource_id("water", "1a2b3c4d5e6f")
    assert rid == "res-water-1a2b3c4d5e6f" and resource_type_of(rid) == "water"
    assert resource_key(rid) == "resource:water:res-water-1a2b3c4d5e6f"
    assert resource_key(rid).startswith(resource_prefix("water"))
    assert not resource_key(rid).startswith(resource_prefix("food"))
    for bad in ("res-nope", "res-spaceships-123", "req-water-123", "", "res-water"):
        assert resource_type_of(bad) is None
        assert resource_key(bad).startswith("resource:_invalid:")   # never stored -> 404


def test_matcher_scans_only_the_requested_type():
    from matching_service.matcher import match_request
    store = FakeStore()
    water = put_resource(store, "water")
    for _ in range(20):
        put_resource(store, "food")
    put_request(store, "req-1")
    result = run(match_request(store, "req-1", source="test"))
    assert result["result"] == "matched" and result["resource_id"] == water
    assert store.scanned == ["resource:water:"]


# ----------------------------------------------------------------------------- idempotent claims
def test_try_assign_is_idempotent_per_request():
    store = FakeStore()
    rid = put_resource(store, availability=2)
    assert run(try_assign(store, rid, "req-1", "water"))
    assert run(try_assign(store, rid, "req-1", "water"))      # redelivered message: no second unit
    res = store.value(resource_key(rid))
    assert res["availability"] == 1 and res["assigned_requests"] == ["req-1"]


def test_matcher_reuses_a_claim_it_already_holds():
    """Crash after claiming but before marking the request matched; the redelivered message must
    finish the match on the same resource - even though that resource is now busy (0 left)."""
    from matching_service.matcher import match_request
    store = FakeStore()
    held = put_resource(store, availability=0, assigned=["req-1"], status="busy", lat=12.98)
    other = put_resource(store, availability=5)            # closer, plenty of capacity
    put_request(store, "req-1")
    result = run(match_request(store, "req-1", source="test"))
    assert result["result"] == "matched" and result["resource_id"] == held
    assert store.value(resource_key(other))["availability"] == 5   # no second unit taken


# ----------------------------------------------------------------------------- capacity reconciler
def make_reconciler(store, grace=20):
    from matching_service.reconciler import Reconciler
    requeued = []

    async def requeue(rid):
        requeued.append(rid)
        return True
    return Reconciler(store, grace, requeue, "test"), requeued


def test_find_inconsistencies():
    from matching_service.reconciler import find_inconsistencies
    resources = [{"id": "res-water-a", "assigned_requests": ["r-ok", "r-done", "r-gone"]},
                 {"id": "res-water-b", "assigned_requests": []}]
    requests = [{"id": "r-ok", "status": "matched", "assigned_resource": "res-water-a"},
                {"id": "r-done", "status": "resolved", "assigned_resource": "res-water-a"},
                {"id": "r-lost", "status": "in_progress", "assigned_resource": "res-water-b"},
                {"id": "r-pending", "status": "pending", "assigned_resource": None}]
    assert find_inconsistencies(resources, requests) == {
        ("orphan_claim", "res-water-a", "r-done"), ("orphan_claim", "res-water-a", "r-gone"),
        ("missing_claim", "r-lost", "res-water-b")}


def test_reconciler_releases_leaked_capacity_after_grace():
    store = FakeStore()
    rid = put_resource(store, availability=0, assigned=["req-1"], status="busy")
    put_request(store, "req-1", status="resolved", assigned=rid)    # crashed before release
    rec, _ = make_reconciler(store, grace=20)
    t = time.time()
    assert run(rec.run_once(now=t))["fixed"] == []                  # first sighting: wait
    assert store.value(resource_key(rid))["availability"] == 0
    assert run(rec.run_once(now=t + 21))["fixed"] == [("orphan_claim", rid, "req-1")]
    res = store.value(resource_key(rid))
    assert res["availability"] == 1 and res["status"] == "available" and res["assigned_requests"] == []
    assert run(rec.run_once(now=t + 50))["inconsistent"] == 0


def test_reconciler_ignores_transient_inconsistency():
    """Claimed but not yet marked matched (a match in progress) must not be released if it resolves."""
    store = FakeStore()
    rid = put_resource(store, availability=0, assigned=["req-1"], status="busy")
    put_request(store, "req-1", status="pending")
    rec, _ = make_reconciler(store, grace=20)
    t = time.time()
    run(rec.run_once(now=t))
    req = store.value(request_key("req-1"))
    run(store.put(request_key("req-1"), {**req, "status": "matched", "assigned_resource": rid}))
    assert run(rec.run_once(now=t + 30))["fixed"] == []
    assert store.value(resource_key(rid))["assigned_requests"] == ["req-1"]


def test_reconciler_reclaims_missing_claim_or_reopens_request():
    store = FakeStore()
    has_room = put_resource(store, availability=1)
    full = put_resource(store, availability=0, status="busy")
    put_request(store, "req-a", status="matched", assigned=has_room)
    put_request(store, "req-b", status="in_progress", assigned=full)
    rec, requeued = make_reconciler(store, grace=0)
    fixed = run(rec.run_once())["fixed"]
    assert sorted(fixed) == [("missing_claim", "req-a", has_room), ("missing_claim", "req-b", full)]
    assert store.value(resource_key(has_room))["assigned_requests"] == ["req-a"]
    assert store.value(resource_key(has_room))["availability"] == 0
    reopened = store.value(request_key("req-b"))
    assert reopened["status"] == "pending" and reopened["assigned_resource"] is None and requeued == ["req-b"]


def test_reconciler_skips_partial_view():
    store = FakeStore()
    rid = put_resource(store, availability=0, assigned=["req-1"], status="busy")  # request on a down shard
    store.unavailable = [2]
    rec, _ = make_reconciler(store, grace=0)
    assert run(rec.run_once())["skipped"]
    assert store.value(resource_key(rid))["assigned_requests"] == ["req-1"]


# ----------------------------------------------------------------------------- coordinator restart
@pytest.fixture()
def coord():
    import coordinator.app as c
    c.nodes.clear()
    c.shard_wait_since.clear()
    return c


def reg(client, node, shard=0, **kw):
    r = client.post("/internal/nodes/register", json={"node_id": node, "shard_id": shard,
                                                     "url": f"http://{node}:8000", **kw})
    assert r.status_code == 200
    return r.json()


def hb(client, node, shard=0, status="waiting", **kw):
    return client.post("/internal/nodes/heartbeat", json={"node_id": node, "shard_id": shard,
                                                         "url": f"http://{node}:8000", "status": status, **kw}).json()


def test_first_returning_replica_is_not_trusted_alone(coord):
    client = TestClient(coord.app)
    assert reg(client, "s0a")["status"] == "waiting"            # possibly stale: no ready peer to check
    assert client.get("/internal/data/request:x").status_code == 503   # never serves (stale) reads
    coord.resolve_waiting()
    assert hb(client, "s0a") == {"status": "waiting", "resync": False, "peers": []}
    b = reg(client, "s0b")                                      # both replicas back -> merge each other,
    assert b["status"] == "syncing" and b["resync"] and b["peers"] == ["http://s0a:8000"]   # immediately
    a = hb(client, "s0a")
    assert a["resync"] and a["peers"] == ["http://s0b:8000"]
    hb(client, "s0a", status="ready")
    assert coord.nodes["s0a"].status == "ready"


def test_lone_replica_is_promoted_after_grace(coord):
    client = TestClient(coord.app)
    reg(client, "s1a", shard=1)
    coord.resolve_waiting(now=time.time() + coord.BOOTSTRAP_GRACE - 1)
    assert coord.nodes["s1a"].status == "waiting"
    coord.resolve_waiting(now=time.time() + coord.BOOTSTRAP_GRACE + 1)
    assert hb(client, "s1a", shard=1) == {"status": "syncing", "resync": True, "peers": []}


def test_replica_rejoining_a_healthy_shard_syncs_immediately(coord):
    client = TestClient(coord.app)
    reg(client, "s2a", shard=2)
    reg(client, "s2b", shard=2)
    coord.resolve_waiting()
    hb(client, "s2a", shard=2), hb(client, "s2a", shard=2, status="ready")
    reply = reg(client, "s2c", shard=2)
    assert reply["status"] == "syncing" and reply["peers"] == ["http://s2a:8000"]


def test_down_replica_with_no_ready_peer_waits_instead_of_serving(coord):
    client = TestClient(coord.app)
    reg(client, "s0a"), reg(client, "s0b")
    coord.resolve_waiting()
    for n in ("s0a", "s0b"):
        hb(client, n), hb(client, n, status="ready")
    coord.mark_down(coord.nodes["s0a"], "test")
    coord.mark_down(coord.nodes["s0b"], "test")
    assert hb(client, "s0a", status="ready")["status"] == "waiting"   # old code: READY with stale data


def test_versions_never_go_backwards_after_coordinator_restart(coord):
    client = TestClient(coord.app)
    future = time.time_ns() + 3_600 * 10**9                      # data written by a clock 1h ahead
    reg(client, "s0a", max_version=future)
    assert coord.next_version() > future


def test_store_reports_max_version(tmp_path):
    s = Store(str(tmp_path / "n.db"))
    assert s.max_version() == 0
    s.put("a", 1, 5), s.put("b", None, 9, deleted=True)
    assert s.max_version() == 9


# ----------------------------------------------------------------------------- /match proxy errors
@pytest.fixture()
def request_api():
    import request_service.app as rs
    clients = []

    def with_matching(handler):
        rs.http = httpx.AsyncClient(transport=httpx.MockTransport(handler))
        c = TestClient(rs.app)
        clients.append(c)
        return c
    yield with_matching
    rs.http = None


def test_match_proxy_maps_non_json_5xx_to_503(request_api):
    client = request_api(lambda req: httpx.Response(502, text="<html>Bad Gateway</html>"))
    r = client.post("/match/req-1")
    assert r.status_code == 503 and "HTTP 502" in r.json()["detail"]


def test_match_proxy_maps_json_5xx_to_503_and_passes_4xx(request_api):
    client = request_api(lambda req: httpx.Response(500, json={"detail": "boom"}))
    assert client.post("/match/req-1").status_code == 503
    client = request_api(lambda req: httpx.Response(409, json={"detail": {"message": "no suitable available resource"}}))
    r = client.post("/match/req-1")
    assert r.status_code == 409 and r.json()["detail"]["message"] == "no suitable available resource"


def test_match_proxy_unreachable_is_503_and_ids_are_trimmed(request_api):
    seen = []

    def down(req):
        seen.append(req.url.path)
        raise httpx.ConnectError("refused")
    client = request_api(down)
    assert client.post("/match/%20req-1%20").status_code == 503
    assert seen == ["/match/req-1"]
