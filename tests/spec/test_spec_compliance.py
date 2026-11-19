"""Specification-compliance suite: verifies, against the RUNNING system, every mandatory requirement of
'Emergency Response Matching.pdf' (catalogue: tests/spec/requirements.py).

    ./scripts/spec-test.sh compose        # against Docker Compose   (docker compose up -d --build)
    ./scripts/spec-test.sh k8s            # against Kubernetes       (./k8s/deploy.sh on minikube)
    .venv/bin/python tests/spec/report.py # merge both runs into one requirement matrix

Every assertion goes through `check(claim, condition, evidence)`, so the report printed at the end lists each
individual thing that was verified together with what was observed. Tests run in file order: normal operation
first, then the failure tests (storage node stopped / restarted, matching service down, a whole shard down),
then the Kubernetes pod-failure and scaling tests. Every test uses its own empty patch of ocean as location, so
data from earlier runs can never be matched by mistake, and puts back what it changed.
"""
import concurrent.futures as cf
import hashlib
import json
import math
import os
import random
import threading
import time
import uuid
from collections import Counter
from datetime import datetime, timezone
from urllib.parse import urlparse

import httpx
import pytest

import spec_env
from spec_env import HTTP_SNIPPET, NODE_KEYS_SNIPPET, ROOT, TCP_SNIPPET

P = spec_env.platform()
api = httpx.Client(base_url=P.api, timeout=20)
coord = httpx.Client(base_url=P.coord, timeout=20)
rabbit = httpx.Client(base_url=P.rabbit, timeout=10, auth=P.rabbit_auth)

RUN = uuid.uuid4().hex[:6]
SPEC_TYPES = ["water", "food", "medicine", "transportation", "shelter"]      # §1 / §3.1
SPEC_ENDPOINTS = [("post", "/requests"), ("get", "/requests/{request_id}"), ("put", "/requests/{request_id}/status"),
                  ("post", "/resources"), ("get", "/resources"), ("put", "/resources/{resource_id}/status"),
                  ("post", "/match/{request_id}")]                              # §5 Table 2
HEARTBEAT_TIMEOUT = 6      # coordinator HEARTBEAT_TIMEOUT in both deployments
MAX_KM = 50                # matching-service MAX_MATCH_DISTANCE_KM in both deployments

req = pytest.mark.req
k8s_only = pytest.mark.skipif(P.name != "k8s", reason="Kubernetes requirement: verified by the run against the "
                                                      "minikube deployment (SPEC_TARGET=k8s)")


# ============================================================================= helpers
def wait_for(fn, timeout=60.0, interval=0.5, desc="condition"):
    deadline, last = time.time() + timeout, None
    while time.time() < deadline:
        try:
            last = fn()
        except (httpx.HTTPError, KeyError, ValueError) as e:   # a service may be restarting
            last = repr(e)
        else:
            if last:
                return last
        time.sleep(interval)
    raise AssertionError(f"timed out after {timeout:.0f}s waiting for {desc}; last={last!r}")


def expected_shard(key: str) -> int:
    """The partitioning rule, recomputed independently of the services: sha256(key) mod 3."""
    return int.from_bytes(hashlib.sha256(key.encode()).digest()[:8], "big") % 3


def req_key(rid: str) -> str:
    return f"request:{rid}"


def res_key(rid: str) -> str:
    return f"resource:{rid.split('-')[1]}:{rid}"


def cluster() -> dict:
    return coord.get("/cluster").json()


def shard_node_ids(shard: int) -> list[str]:
    return [n["node_id"] for n in cluster()["shards"][str(shard)]["nodes"]]


def node_info(node_id: str) -> dict | None:
    for s in cluster()["shards"].values():
        for n in s["nodes"]:
            if n["node_id"] == node_id:
                return n
    return None


def node_status(node_id: str) -> str | None:
    n = node_info(node_id)
    return n["status"] if n else None


def all_ready() -> bool:
    shards = cluster()["shards"].values()
    return all(len(s["nodes"]) >= 2 and all(n["status"] == "ready" for n in s["nodes"]) for s in shards)


def node_keys(node_id: str, keys: list[str]) -> dict:
    """What a storage node holds on its OWN disk for these keys (exec into the container / pod)."""
    return json.loads(P.exec_py(P.node_exec_target(node_id), NODE_KEYS_SNIPPET, json.dumps(keys)))


def http_from(target: str, method: str, url: str, body: str = "") -> dict:
    """HTTP call made from inside another container / pod (service-to-service path)."""
    return json.loads(P.exec_py(target, HTTP_SNIPPET, method, url, body))


def tcp_from(target: str, host: str, port: int) -> str:
    return P.exec_py(target, TCP_SNIPPET, host, str(port)).strip()


def queue(name: str = "matching.requests") -> dict:
    return rabbit.get(f"/api/queues/%2F/{name}").json()


def qstat(q: dict, key: str) -> int:
    return q.get("message_stats", {}).get(key, 0)


def consumers() -> int:
    return queue().get("consumers", 0)


def storage_env() -> dict:
    return P.env_of("storage-0a" if P.name == "compose" else "storage-node")


def get_request(rid: str) -> dict:
    return api.get(f"/requests/{rid}").json()


def get_resource(rid: str) -> dict:
    return api.get(f"/resources/{rid}").json()


def all_matched(ids: list[str]) -> list[dict] | None:
    """The requests if every one of them is matched, else None (for wait_for)."""
    xs = [get_request(i) for i in ids]
    return xs if all(x["status"] == "matched" for x in xs) else None


def replaced(label: str, old: str, n: int) -> list[str] | None:
    """Ready pods for label once there are n of them again and the deleted pod `old` is gone (for wait_for)."""
    pods = P.ready_pods(label)
    return pods if len(pods) == n and old not in pods else None


def haversine(lat1, lon1, lat2, lon2) -> float:
    p1, p2 = math.radians(lat1), math.radians(lat2)
    a = math.sin((p2 - p1) / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(math.radians(lon2 - lon1) / 2) ** 2
    return 2 * 6371.0 * math.asin(math.sqrt(a))


class Area:
    """A patch of ocean owned by one test; at(km_north, km_east) gives a location relative to its centre."""

    def __init__(self, lat: float, lon: float):
        self.lat, self.lon = lat, lon

    def at(self, km_north: float = 0.0, km_east: float = 0.0) -> dict:
        lat = self.lat + km_north / 111.32
        lon = self.lon + km_east / (111.32 * math.cos(math.radians(self.lat)))
        return {"lat": round(lat, 6), "lon": round(lon, 6), "area": f"spec-{RUN}"}


class Areas:
    """Gives every test its own empty area: >= 250 km from any resource already in the system and from each other
    (the matching radius is 50 km), so leftovers from earlier runs can never be matched by mistake."""

    def __init__(self):
        rng = random.Random()
        self.lat0, self.lon0, self.i, self.taken = rng.uniform(-55, -35), rng.uniform(-180, 180), 0, None

    def new(self) -> Area:
        if self.taken is None:
            self.taken = [(r["location"]["lat"], r["location"]["lon"]) for r in api.get("/resources").json()["items"]]
        while True:
            lat = self.lat0 - (self.i // 60) * 3.0
            lon = ((self.lon0 + (self.i % 60) * 4.5 + 180) % 360) - 180
            self.i += 1
            if all(haversine(lat, lon, a, b) > 250 for a, b in self.taken):
                self.taken.append((lat, lon))
                return Area(lat, lon)


AREAS = Areas()


class Made:
    """Creates test data and cleans it up afterwards (open requests cancelled, resources taken offline)."""

    def __init__(self):
        self.requests: list[str] = []
        self.resources: list[str] = []

    def request(self, loc: dict, type_: str = "water", details: str = "spec test", auto_match: bool = False,
                **extra) -> dict:
        r = api.post("/requests", json={"type": type_, "location": loc, "details": details,
                                        "auto_match": auto_match, **extra})
        assert r.status_code == 201, r.text
        self.requests.append(r.json()["id"])
        return r.json()

    def resource(self, loc: dict, type_: str = "water", availability: int = 1, kind: str = "resource",
                 name: str | None = None) -> dict:
        r = api.post("/resources", json={"type": type_, "location": loc, "availability": availability,
                                         "kind": kind, "name": name or f"spec-{type_}-{RUN}"})
        assert r.status_code == 201, r.text
        self.resources.append(r.json()["id"])
        return r.json()

    def request_on_shard(self, shard: int, loc: dict, **kw) -> dict:
        for _ in range(60):
            r = self.request(loc, **kw)
            if r["_storage"]["shard"] == shard:
                return r
        raise AssertionError(f"no request landed on shard {shard}")

    def resource_on_shard(self, shard: int, loc: dict, **kw) -> dict:
        for _ in range(60):
            r = self.resource(loc, **kw)
            if r["_storage"]["shard"] == shard:
                return r
            api.put(f"/resources/{r['id']}/status", json={"status": "offline"})   # keep only the one we want
        raise AssertionError(f"no resource landed on shard {shard}")

    def cleanup(self) -> None:
        for rid in self.requests:
            try:
                r = api.get(f"/requests/{rid}")
                if r.status_code == 200 and r.json()["status"] not in ("resolved", "cancelled"):
                    api.put(f"/requests/{rid}/status", json={"status": "cancelled", "note": "spec test cleanup"})
            except httpx.HTTPError:
                pass
        for rid in self.resources:
            try:
                api.put(f"/resources/{rid}/status", json={"status": "offline"})
            except httpx.HTTPError:
                pass


class Probe:
    """Calls fn() repeatedly in a background thread (e.g. while pods are scaled or killed) and counts results."""

    def __init__(self, fn, interval: float = 0.25):
        self.fn, self.interval = fn, interval
        self.ok = self.fail = 0
        self.errors: list[str] = []
        self.values: list = []
        self._stop = threading.Event()
        self._t = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        while not self._stop.is_set():
            try:
                v = self.fn()
                good = bool(v)
                if good:
                    self.values.append(v)
                else:
                    self.errors.append(f"returned {v!r}")
            except Exception as e:
                good = False
                self.errors.append(repr(e)[:150])
            self.ok += good
            self.fail += not good
            self._stop.wait(self.interval)

    def __enter__(self):
        self._t.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._t.join(timeout=30)

    @property
    def total(self) -> int:
        return self.ok + self.fail

    def summary(self) -> str:
        rate = self.ok / self.total if self.total else 0
        return f"{self.ok}/{self.total} succeeded ({rate:.0%})" + (f"; errors: {self.errors[:2]}" if self.errors else "")


def fresh_client() -> httpx.Client:
    """A client that opens a new TCP connection per call, so a Service load-balances every call across pods."""
    return httpx.Client(base_url=P.api, timeout=10, headers={"Connection": "close"})


@pytest.fixture
def area() -> Area:
    return AREAS.new()


@pytest.fixture
def made():
    m = Made()
    yield m
    m.cleanup()


@pytest.fixture(scope="session", autouse=True)
def system_ready():
    """Waits until the deployment is fully up: 3 healthy shards, API connected to RabbitMQ, consumers attached."""
    wait_for(lambda: coord.get("/health").json()["status"] == "ok", timeout=240, interval=1, desc="storage healthy")
    wait_for(all_ready, timeout=240, interval=1, desc="all storage nodes ready")
    wait_for(lambda: api.get("/health").json().get("broker_connected"), timeout=240, interval=1, desc="API on RabbitMQ")
    wait_for(lambda: consumers() >= 1, timeout=240, interval=1, desc="matching consumers")


# ============================================================================= §2 system configuration
@req("2.coord", "4.b")
def test_exactly_one_coordinator(check):
    """§2 Table 1 'Coordinator: 1' / §4 'one coordinator': exactly one coordinator runs, every storage node is
    registered with it and every service sends its storage operations to it."""
    n = P.component_count("coordinator")
    check("exactly one coordinator container/pod is running", n == 1, f"{n} running")
    if P.name == "k8s":
        spec = P.kjson("get", "deploy/coordinator")["spec"]
        check("the coordinator Deployment declares replicas: 1", spec["replicas"] == 1,
              f"replicas={spec['replicas']}, strategy={spec['strategy']['type']}")
    h = coord.get("/health")
    check("the coordinator answers GET /health", h.status_code == 200 and h.json()["status"] == "ok", h.json())
    nodes = sorted(n["node_id"] for n in coord.get("/internal/nodes").json()["nodes"])
    check("all 6 storage nodes are registered with this one coordinator", len(nodes) == 6, nodes)
    for svc in ("request-service", "matching-service"):
        url = P.env_of(svc).get("COORDINATOR_URL")
        check(f"{svc} sends every storage operation to the coordinator", url == "http://coordinator:8000",
              f"COORDINATOR_URL={url}")
    url = storage_env().get("COORDINATOR_URL")
    check("storage nodes register / heartbeat with the coordinator", url == "http://coordinator:8000",
          f"COORDINATOR_URL={url}")


@req("2.ms")
def test_two_to_three_microservices(check):
    """§2 Table 1 'Microservices: 2-3': the application is split into separately deployed microservices."""
    roles = {"request-service": "client REST API; stores requests; publishes them to RabbitMQ",
             "matching-service": "consumes RabbitMQ messages; finds and assigns resources",
             "coordinator": "storage-layer service: shard routing, replication, failure detection"}
    for app, role in roles.items():
        n = P.component_count(app)
        check(f"microservice '{app}' is deployed on its own ({role})", n >= 1, f"{n} instance(s)")
    h = api.get("/health").json()
    check("request-service is healthy", h["status"] == "ok", h)
    mh = http_from("request-service", "GET", "http://matching-service:8000/health")
    check("matching-service is healthy (called from inside the request-service container)", mh["status"] == 200,
          mh.get("body"))
    check("2-3 microservices: 2 application services + the coordinator = 3", 2 <= len(roles) <= 3,
          "request-service, matching-service, coordinator; storage nodes are the shards, RabbitMQ the broker")


@req("2.mq", "6.mq")
def test_one_rabbitmq_instance(check):
    """§2 Table 1 'Message broker: 1 RabbitMQ instance', used by the request and matching services."""
    n = P.component_count("rabbitmq")
    check("exactly one RabbitMQ container/pod is running", n == 1, f"{n} running")
    if P.name == "k8s":
        spec = P.kjson("get", "statefulset/rabbitmq")["spec"]
        check("the RabbitMQ StatefulSet declares replicas: 1", spec["replicas"] == 1, f"replicas={spec['replicas']}")
    nodes = rabbit.get("/api/nodes").json()
    check("the broker is a single RabbitMQ node (not a cluster)", len(nodes) == 1, nodes[0]["name"])
    check.note("RabbitMQ version", rabbit.get("/api/overview").json()["rabbitmq_version"])
    for svc in ("request-service", "matching-service"):
        host = urlparse(P.env_of(svc).get("RABBITMQ_URL", "")).hostname
        check(f"{svc} is configured to use that broker", host == "rabbitmq", f"RABBITMQ_URL host = {host}:5672")
    conns = rabbit.get("/api/connections").json()
    expected = P.component_count("request-service") + P.component_count("matching-service")
    check("every request-service and matching-service instance is connected to it", len(conns) >= expected,
          f"{len(conns)} AMQP connections (expected >= {expected})")


@req("2.shards", "2.repl", "4.b")
def test_three_shards_each_with_a_replica(check):
    """§2 'Storage shards: 3', 'Replication: at least 1 replica per shard' / §4 'three storage shards, and at least
    one replica for each shard'."""
    c = cluster()
    check("the storage layer has exactly 3 shards", c["num_shards"] == 3 and sorted(c["shards"]) == ["0", "1", "2"],
          f"num_shards={c['num_shards']}")
    for sid, s in sorted(c["shards"].items()):
        ids = [n["node_id"] for n in s["nodes"]]
        check(f"shard {sid} = 1 primary copy + at least 1 replica", len(ids) >= 2, ids)
        check(f"shard {sid}: every copy is up and ready", all(n["status"] == "ready" for n in s["nodes"]),
              {n["node_id"]: n["status"] for n in s["nodes"]})
    if P.name == "compose":
        nodes = [s for s in ("storage-0a", "storage-0b", "storage-1a", "storage-1b", "storage-2a", "storage-2b")
                 if P.running(s)]
        check("6 storage-node containers run (3 shards x 2)", len(nodes) == 6, nodes)
    else:
        for s in range(3):
            st = P.kjson("get", f"statefulset/storage-shard-{s}")
            check(f"StatefulSet storage-shard-{s} runs 2 ready pods",
                  st["spec"]["replicas"] == 2 and st["status"].get("readyReplicas") == 2,
                  f"replicas={st['spec']['replicas']} ready={st['status'].get('readyReplicas')}")

    def counts():
        return {sid: [n["key_count"] for n in s["nodes"]] for sid, s in cluster()["shards"].items()}

    def equal_counts():
        k = counts()
        return k if all(len(set(v)) == 1 for v in k.values()) else None

    got = wait_for(equal_counts, timeout=20, desc="replica key counts equal")
    check("the replicas of each shard hold the same number of records", True, f"key counts per shard: {got}")


@req("2.clients")
def test_one_or_more_clients(check, area):
    """§2 Table 1 'Clients: 1 or more': the API is usable by any HTTP/JSON client and by several at the same time."""
    docs = api.get("/docs")
    check("an interactive client (Swagger UI) is served at /docs", docs.status_code == 200 and "swagger" in docs.text.lower(),
          f"{P.api}/docs -> {docs.status_code}")
    paths = api.get("/openapi.json").json()["paths"]
    for method, path in SPEC_ENDPOINTS:
        check(f"{method.upper()} {path} is published in the machine-readable API description",
              method in paths.get(path, {}), "openapi.json")

    def client(i: int) -> tuple[int, int]:
        with httpx.Client(base_url=P.api, timeout=20) as c:
            r = c.post("/requests", json={"type": "water", "location": area.at(i), "details": f"client {i}",
                                          "auto_match": False})
            rid = r.json()["id"]
            g = c.get(f"/requests/{rid}")
            c.put(f"/requests/{rid}/status", json={"status": "cancelled"})
            return r.status_code, g.status_code

    with cf.ThreadPoolExecutor(5) as ex:
        results = list(ex.map(client, range(5)))
    check("5 independent clients working at the same time are all served (create 201, read 200)",
          all(r == (201, 200) for r in results), results)


# ============================================================================= §1 operations
@req("1.op1", "5.1", "7.1", "10.create", "11.a")
def test_createRequest(check, area, made):
    """§1 createRequest(type, location, details) - 'Create an emergency request' = POST /requests (201)."""
    body = {"type": "water", "location": area.at(), "details": "two people stranded on a roof, need drinking water"}
    r = api.post("/requests", json=body)        # exactly the three parameters of createRequest
    check("POST /requests with exactly {type, location, details} -> 201 Created", r.status_code == 201, r.status_code)
    d = r.json()
    made.requests.append(d["id"])
    check("the response is JSON", r.headers["content-type"].startswith("application/json"), r.headers["content-type"])
    check("the new request got an id", d["id"].startswith("req-"), d["id"])
    for f in ("type", "location", "details"):
        check(f"'{f}' is stored as submitted", d[f] == body[f], d[f])
    check("a new request starts in status 'pending'", d["status"] == "pending", d["status"])
    check("it was handed to RabbitMQ for asynchronous matching", d["queued"] is True, f"queued={d['queued']}")
    check("it was written to its shard on both copies", len(d["_storage"]["replicas"]) == 2, d["_storage"])


@req("1.op5", "5.2", "10.create", "11.a")
def test_getRequest(check, area, made):
    """§1 getRequest(requestId) - 'Retrieve request information' = GET /requests/{id} (200)."""
    created = made.request(area.at(), type_="food", details="food for 12 people in a flooded school")
    r = api.get(f"/requests/{created['id']}")
    check("GET /requests/{id} -> 200 OK", r.status_code == 200, r.status_code)
    d = r.json()
    for f in ("id", "type", "location", "details", "status", "created_at"):
        check(f"returned '{f}' equals what was created", d[f] == created[f], d[f])
    nodes = shard_node_ids(created["_storage"]["shard"])
    check("the read was served by a copy of the request's own shard", d["_storage"]["served_by"] in nodes,
          f"shard {d['_storage']['shard']}, served_by {d['_storage']['served_by']}")
    check("reading again returns the same data", get_request(created["id"])["updated_at"] == d["updated_at"], "")


@req("1.op2", "5.4", "10.register", "11.b")
def test_registerResource(check, area, made):
    """§1 registerResource(type, location, availability) - 'Register a resource or volunteer' = POST /resources."""
    body = {"type": "water", "location": area.at(1, 0), "availability": 5}
    r = api.post("/resources", json=body)       # exactly the three parameters of registerResource
    check("POST /resources with exactly {type, location, availability} -> 201 Created", r.status_code == 201,
          r.status_code)
    d = r.json()
    made.resources.append(d["id"])
    check("the resource got an id", d["id"].startswith("res-"), d["id"])
    for f in ("type", "location", "availability"):
        check(f"'{f}' is stored as submitted", d[f] == body[f], d[f])
    check("a resource with capacity starts 'available'", d["status"] == "available", d["status"])
    v = api.post("/resources", json={"type": "transportation", "location": area.at(2, 0), "availability": 1,
                                     "kind": "volunteer", "name": "Volunteer driver with a 4x4"})
    check("a volunteer can be registered too (kind=volunteer) -> 201", v.status_code == 201 and v.json()["kind"] == "volunteer",
          v.json().get("id"))
    made.resources.append(v.json()["id"])
    listed = {x["id"] for x in api.get("/resources", params={"type": "water"}).json()["items"]}
    check("the registered resource appears in GET /resources", d["id"] in listed, f"{len(listed)} water resources")
    g = api.get(f"/resources/{d['id']}")
    check("and can be read back by id", g.status_code == 200 and g.json()["availability"] == 5, g.status_code)


@req("1.op3", "5.7", "10.match", "11.c", "7.5", "7.6")
def test_matchRequest(check, area, made):
    """§1 matchRequest(requestId) - 'Find and assign a suitable available resource' = POST /match/{requestId}."""
    res = made.resource(area.at(2, 0), availability=3, name="Water tanker")
    rq = made.request(area.at(), details="village well contaminated")
    r = api.post(f"/match/{rq['id']}")
    check("POST /match/{requestId} -> 200 OK", r.status_code == 200, r.status_code)
    m = r.json()
    check("a resource was found and assigned", m["result"] == "matched" and m["resource_id"] == res["id"],
          f"{m.get('resource_name')} ({m.get('resource_id')}) at {m.get('distance_km')} km")
    check("the distance is computed from the two locations", abs(m["distance_km"] - 2.0) < 0.1, m["distance_km"])
    got = get_request(rq["id"])
    check("the request is now 'matched' to that resource", got["status"] == "matched" and got["assigned_resource"] == res["id"],
          got["assignment"])
    rs = get_resource(res["id"])
    check("one unit of the resource is assigned to the request", rs["availability"] == 2 and rq["id"] in rs["assigned_requests"],
          f"availability 3 -> {rs['availability']}, assigned_requests={rs['assigned_requests']}")
    again = api.post(f"/match/{rq['id']}")
    check("matching an already-matched request again does not take a second unit", again.status_code == 200 and
          again.json()["result"] == "skipped" and get_resource(res["id"])["availability"] == 2, again.json())


@req("1.op4", "5.3", "11.a")
def test_updateStatus(check, area, made):
    """§1 updateStatus(requestId, status) - 'Update the status of a request' = PUT /requests/{id}/status (200)."""
    res = made.resource(area.at(1, 0), availability=1, name="Boat")
    rq = made.request(area.at(), type_="water")
    api.post(f"/match/{rq['id']}")
    for new in ("in_progress", "resolved"):
        r = api.put(f"/requests/{rq['id']}/status", json={"status": new, "note": "set by spec test"})
        check(f"PUT /requests/{{id}}/status {{'status': '{new}'}} -> 200 OK", r.status_code == 200, r.status_code)
        check(f"the stored status is now '{new}'", get_request(rq["id"])["status"] == new,
              f"previous_status={r.json()['previous_status']}")
    rs = get_resource(res["id"])
    check("resolving the request released the resource's capacity", rs["availability"] == 1 and rs["status"] == "available",
          f"availability back to {rs['availability']}, status {rs['status']}")
    got = get_request(rq["id"])
    hist = [h["status"] for h in got["history"]]
    check("every status change is recorded in the request history", hist == ["pending", "matched", "in_progress", "resolved"],
          hist)
    check("updated_at moves forward", got["updated_at"] > got["created_at"], f"{got['created_at']} -> {got['updated_at']}")
    rq2 = made.request(area.at(), type_="water")
    c = api.put(f"/requests/{rq2['id']}/status", json={"status": "cancelled"})
    check("a pending request can be cancelled -> 200", c.status_code == 200 and get_request(rq2["id"])["status"] == "cancelled",
          c.status_code)


@req("1.b", "3.1.types", "1.c")
def test_supported_request_types(check, area, made):
    """§1 'requests such as food, water, medicine, transportation, or shelter' / §3.1 example request types."""
    for t in SPEC_TYPES:
        res = made.resource(area.at(1, 0), type_=t, availability=1, name=f"{t} provider")
        rq = made.request(area.at(), type_=t, details=f"need {t}")
        check(f"a '{t}' request is accepted (201)", rq["type"] == t, rq["id"])
        m = api.post(f"/match/{rq['id']}").json()
        check(f"the '{t}' request is matched to the '{t}' resource", m.get("resource_id") == res["id"],
              f"{m.get('result')} -> {m.get('resource_name')}")
    bad = api.post("/requests", json={"type": "pizza", "location": area.at(), "details": "x"})
    check("an unknown request type is rejected (422)", bad.status_code == 422, bad.json()["detail"][0]["msg"][:90])


# ============================================================================= §3 data
@req("3.1.id", "3.1.type", "3.1.loc", "3.1.details", "3.1.status", "3.1.created", "11.a")
def test_request_record_fields(check, area, made):
    """§3.1 'Each request should contain, at minimum: unique request ID, request type, location, request details,
    current status, creation time'."""
    t0 = datetime.now(timezone.utc)
    rq = made.request(area.at(), type_="medicine", details="insulin for a diabetic patient")
    d = get_request(rq["id"])
    check("unique request ID: present", isinstance(d["id"], str) and len(d["id"]) > 4, d["id"])
    with cf.ThreadPoolExecutor(10) as ex:
        ids = list(ex.map(lambda i: made.request(area.at(), details=f"unique {i}")["id"], range(25)))
    check("unique request ID: 25 requests created at the same time all got different ids", len(set(ids)) == 25,
          f"{len(set(ids))} distinct of 25")
    check("request type", d["type"] == "medicine", d["type"])
    check("location (latitude / longitude)", isinstance(d["location"]["lat"], float) and isinstance(d["location"]["lon"], float),
          d["location"])
    check("request details", d["details"] == "insulin for a diabetic patient", d["details"])
    check("current status", d["status"] in {"pending", "matched", "in_progress", "resolved", "cancelled", "unmatched"},
          d["status"])
    created = datetime.fromisoformat(d["created_at"])
    check("creation time (ISO-8601 timestamp with time zone, taken when the request was created)",
          created.tzinfo is not None and abs((created - t0).total_seconds()) < 120, d["created_at"])


@req("3.2.id", "3.2.type", "3.2.loc", "3.2.avail", "3.2.status", "3.2.kinds", "11.b")
def test_resource_record_fields_and_kinds(check, area, made):
    """§3.2 'Each resource should contain, at minimum: unique resource ID, resource type, location, availability,
    current status' and 'may represent a physical resource, volunteer, NGO, or other available source'."""
    res = made.resource(area.at(), type_="shelter", availability=40, name="School gym shelter")
    d = get_resource(res["id"])
    check("unique resource ID: present", d["id"].startswith("res-"), d["id"])
    with cf.ThreadPoolExecutor(10) as ex:
        ids = list(ex.map(lambda i: made.resource(area.at(), type_="food", availability=0)["id"], range(15)))
    check("unique resource ID: 15 resources registered at the same time all got different ids", len(set(ids)) == 15,
          f"{len(set(ids))} distinct of 15")
    check("resource type", d["type"] == "shelter", d["type"])
    check("location", d["location"]["lat"] == res["location"]["lat"] and d["location"]["lon"] == res["location"]["lon"],
          d["location"])
    check("availability (units of capacity)", d["availability"] == 40, d["availability"])
    check("current status", d["status"] in {"available", "busy", "offline"}, d["status"])
    for kind in ("resource", "volunteer", "ngo", "other"):
        r = made.resource(area.at(), type_="food", availability=1, kind=kind, name=f"a {kind}")
        check(f"a resource of kind '{kind}' can be registered", r["kind"] == kind, r["id"])
    bad = api.post("/resources", json={"type": "food", "location": area.at(), "availability": 1, "kind": "alien"})
    check("an unknown kind is rejected (422)", bad.status_code == 422, bad.status_code)


@req("3.maintain", "5.5")
def test_system_maintains_requests_and_resources(check, area, made):
    """§3 'The system should maintain information about emergency requests and available resources' and
    §5 GET /resources 'List resources' (200)."""
    w = made.resource(area.at(), type_="water", availability=2)
    f = made.resource(area.at(), type_="food", availability=1)
    rq = made.request(area.at(), type_="water")
    r = api.get("/resources")
    body = r.json()
    check("GET /resources -> 200 OK with a JSON list", r.status_code == 200 and body["count"] == len(body["items"]),
          f"{body['count']} resources across all shards")
    ids = {x["id"] for x in body["items"]}
    check("the list contains the resources just registered (from whichever shard holds them)",
          {w["id"], f["id"]} <= ids, f"{w['id']} (shard {w['_storage']['shard']}), {f['id']} (shard {f['_storage']['shard']})")
    water = api.get("/resources", params={"type": "water"}).json()["items"]
    check("GET /resources?type=water lists only water resources", all(x["type"] == "water" for x in water) and
          w["id"] in {x["id"] for x in water} and f["id"] not in {x["id"] for x in water}, f"{len(water)} items")
    avail = api.get("/resources", params={"status": "available"}).json()["items"]
    check("GET /resources?status=available lists only available ones", all(x["status"] == "available" for x in avail),
          f"{len(avail)} items")
    check("no shard was missing from the list", body["unavailable_shards"] == [], body["unavailable_shards"])
    reqs = api.get("/requests", params={"type": "water"}).json()["items"]
    check("requests are kept as well (GET /requests)", rq["id"] in {x["id"] for x in reqs}, f"{len(reqs)} water requests")


@req("5.6", "11.b", "3.2.avail", "3.2.status")
def test_resource_availability_updates(check, area, made):
    """§5 PUT /resources/{id}/status (200) and §11 'Resource registration and availability updates'."""
    res = made.resource(area.at(3, 0), availability=3, name="Food truck", type_="food")
    rid = res["id"]
    r = api.put(f"/resources/{rid}/status", json={"availability": 5})
    check("PUT /resources/{id}/status {'availability': 5} -> 200 OK", r.status_code == 200 and get_resource(rid)["availability"] == 5,
          f"availability 3 -> {get_resource(rid)['availability']}")
    r = api.put(f"/resources/{rid}/status", json={"status": "offline"})
    check("PUT {'status': 'offline'} -> 200, resource offline", r.status_code == 200 and get_resource(rid)["status"] == "offline",
          get_resource(rid)["status"])
    rq = made.request(area.at(), type_="food")
    m = api.post(f"/match/{rq['id']}")
    check("an offline resource is not used for matching (409: no suitable resource)", m.status_code == 409, m.status_code)
    api.put(f"/resources/{rid}/status", json={"status": "available"})
    m = api.post(f"/match/{rq['id']}")
    check("back to 'available' -> the same request is now matched to it", m.status_code == 200 and m.json()["resource_id"] == rid,
          m.json().get("result"))
    check("matching consumed one unit (5 -> 4)", get_resource(rid)["availability"] == 4, get_resource(rid)["availability"])
    r = api.put(f"/resources/{rid}/status", json={"availability": 0})
    check("availability 0 -> status becomes 'busy' automatically", r.status_code == 200 and get_resource(rid)["status"] == "busy",
          get_resource(rid)["status"])
    r = api.put(f"/resources/{rid}/status", json={"availability": 2})
    check("availability 2 -> 'available' again", get_resource(rid)["status"] == "available", get_resource(rid)["status"])


# ============================================================================= §1.1 + §7 asynchronous processing
@req("1.1.a", "1.1.b", "1.a", "7.1", "7.2", "7.3", "7.4", "7.5", "7.6", "6.mq", "11.e", "11.cond", "10.match")
def test_flood_victim_needs_drinking_water_end_to_end(check, area, made):
    """§1.1 example scenario and §7 steps 1-6: 'a user affected by flooding requires drinking water. The request is
    submitted, processed asynchronously, matched with an available resource, and the request status is updated.'
    User -> Request Service -> RabbitMQ -> Matching Service -> Distributed Storage -> Resource Assigned."""
    tanker = made.resource(area.at(3, 0), availability=2, name="NGO water tanker", kind="ngo")
    made.resource(area.at(-20, 0), availability=5, name="Bottled-water depot (20 km)")
    q0 = queue()
    pub0, ack0 = qstat(q0, "publish"), qstat(q0, "ack")
    t0 = time.time()
    r = api.post("/requests", json={"type": "water", "location": area.at(), "priority": "high",
                                    "details": "Flooded home: family of four needs drinking water"})
    ms = (time.time() - t0) * 1000
    # 1. client submits
    check("1. the client submits the request: POST /requests -> 201", r.status_code == 201, r.status_code)
    d = r.json()
    made.requests.append(d["id"])
    key = req_key(d["id"])
    check("   the API answers at once, before any matching happened (asynchronous): status 'pending'",
          d["status"] == "pending", f"answered in {ms:.0f} ms")
    # 2. stored
    st = coord.get(f"/internal/data/{key}")
    check("2. the Request Service stored the request in the distributed storage", st.status_code == 200,
          f"shard {st.json()['shard']}, read from {st.json()['served_by']}")
    on_disk = {n: node_keys(n, [key])[key]["status"] for n in d["_storage"]["replicas"]}
    check("   the record is physically on both copies of its shard", list(on_disk.values()) == [200, 200], on_disk)
    # 3. published
    check("3. the request was published to RabbitMQ (queued: true)", d["queued"] is True, d["queued"])
    wait_for(lambda: qstat(queue(), "publish") > pub0, timeout=30, desc="RabbitMQ publish counter")
    check("   RabbitMQ received it into queue 'matching.requests'", True,
          f"publish counter {pub0} -> {qstat(queue(), 'publish')}")
    # 4. consumed
    final = wait_for(lambda: (x := get_request(d["id"]))["status"] == "matched" and x, timeout=90, desc="matched")
    a = final["assignment"]
    check("4. the Matching Service consumed the message (matched by the queue consumer, not the sync API)",
          a["via"] == "queue", f"via={a['via']}")
    workers = P.matching_workers()
    check("   ... in one of the matching-service instances", P.is_matching_worker(a["matched_by"]),
          f"{a['matched_by']} (running now: {sorted(workers)})")
    wait_for(lambda: qstat(queue(), "ack") > ack0, timeout=30, desc="RabbitMQ ack counter")
    check("   the message was acknowledged to RabbitMQ after processing", True,
          f"ack counter {ack0} -> {qstat(queue(), 'ack')}")
    # 5. suitable resource
    check("5. a suitable available resource was identified: same type, available, the nearest one",
          final["assigned_resource"] == tanker["id"], f"{a['resource_name']} at {a['distance_km']} km "
                                                       f"(the other water resource is 20 km away)")
    # 6. assigned + statuses
    check("6. the request status was updated to 'matched'", final["status"] == "matched", final["status"])
    rs = get_resource(tanker["id"])
    check("   the resource was assigned: capacity 2 -> 1, request in its assigned list",
          rs["availability"] == 1 and d["id"] in rs["assigned_requests"], f"availability {rs['availability']}")
    stored = coord.get(f"/internal/data/{key}").json()["value"]
    check("   the assignment is persisted in the distributed storage",
          stored["status"] == "matched" and stored["assigned_resource"] == tanker["id"], stored["assignment"]["matched_at"])
    for s in ("in_progress", "resolved"):
        api.put(f"/requests/{d['id']}/status", json={"status": s})
    check("help delivered: status -> in_progress -> resolved, tanker capacity released (1 -> 2)",
          get_request(d["id"])["status"] == "resolved" and get_resource(tanker["id"])["availability"] == 2,
          get_resource(tanker["id"])["availability"])
    hist = get_request(d["id"])["history"]
    check("the request history shows the whole lifecycle",
          [h["status"] for h in hist] == ["pending", "matched", "in_progress", "resolved"], [h["status"] for h in hist])
    check.note("the path the request took",
               f"User -> request-service ({hist[0]['by']}) -> RabbitMQ (erms.requests -> matching.requests) -> "
               f"matching-service ({a['matched_by']}) -> storage shard {d['_storage']['shard']} "
               f"{d['_storage']['replicas']} -> resource assigned ({tanker['name']})")


@req("7.type", "1.c", "11.c")
def test_matching_considers_resource_type(check, area, made):
    """§7 'The matching mechanism should consider at least resource type' (and location)."""
    food = made.resource(area.at(1, 0), type_="food", availability=5, name="food, 1 km (wrong type)")
    med = made.resource(area.at(10, 0), type_="medicine", availability=5, name="pharmacy, 10 km")
    rq = made.request(area.at(), type_="medicine")
    m = api.post(f"/match/{rq['id']}").json()
    check("a medicine request is matched to the medicine resource, not the closer food resource",
          m["resource_id"] == med["id"], f"{m['resource_name']} at {m['distance_km']} km")
    check("the food resource was not touched", get_resource(food["id"])["availability"] == 5, "")
    rq2 = made.request(area.at(), type_="shelter")
    r = api.post(f"/match/{rq2['id']}")
    check("a shelter request with only food/medicine nearby gets no match (409)", r.status_code == 409,
          r.json()["detail"]["message"])


@req("7.loc", "1.c", "11.c")
def test_matching_considers_location(check, area, made):
    """§7 'The matching mechanism should consider at least resource type and location'."""
    far = made.resource(area.at(25, 0), availability=1, name="25 km")
    near = made.resource(area.at(0, 5), availability=1, name="5 km")
    mid = made.resource(area.at(-15, 0), availability=1, name="15 km")
    order = []
    for _ in range(3):
        rq = made.request(area.at())
        order.append(api.post(f"/match/{rq['id']}").json())
    check("closest first: 3 requests at the same place get the 5 km, then 15 km, then 25 km resource",
          [m["resource_id"] for m in order] == [near["id"], mid["id"], far["id"]],
          [f"{m['resource_name']} ({m['distance_km']} km)" for m in order])
    remote = AREAS.new()
    made.resource(remote.at(80, 0), availability=3, name="80 km away")
    rq = made.request(remote.at())
    r = api.post(f"/match/{rq['id']}")
    check(f"a resource beyond the matching radius ({MAX_KM} km) is not assigned: 80 km -> 409", r.status_code == 409,
          r.json()["detail"])
    ok = made.resource(remote.at(40, 0), availability=1, name="40 km away")
    r = api.post(f"/match/{rq['id']}")
    check(f"a resource within {MAX_KM} km is assigned: 40 km -> 200", r.status_code == 200 and r.json()["resource_id"] == ok["id"],
          f"{r.json().get('distance_km')} km")


@req("7.5", "11.c")
def test_matching_uses_only_available_resources(check, area, made):
    """§7 step 5 'A suitable AVAILABLE resource is identified': offline or fully-used resources are skipped."""
    off = made.resource(area.at(1, 0), availability=5, name="offline, 1 km")
    api.put(f"/resources/{off['id']}/status", json={"status": "offline"})
    used = made.resource(area.at(2, 0), availability=0, name="no capacity left, 2 km")
    ok = made.resource(area.at(9, 0), availability=1, name="available, 9 km")
    check("a resource with availability 0 is registered as 'busy'", used["status"] == "busy", used["status"])
    rq = made.request(area.at())
    m = api.post(f"/match/{rq['id']}").json()
    check("the request skips the offline (1 km) and busy (2 km) ones and gets the available one (9 km)",
          m["resource_id"] == ok["id"], f"{m['resource_name']} at {m['distance_km']} km")
    check("after its last unit is taken the resource is 'busy'", get_resource(ok["id"])["status"] == "busy",
          get_resource(ok["id"])["status"])
    rq2 = made.request(area.at())
    check("so a further request gets no match (409)", api.post(f"/match/{rq2['id']}").status_code == 409, "")


@req("10.async", "11.d", "11.e", "6.mq", "1.d")
def test_multiple_requests_processed_through_rabbitmq(check, area, made):
    """§10 'Asynchronous Processing: submit multiple requests and demonstrate processing through RabbitMQ' and
    §11 'RabbitMQ message delivery and processing'."""
    n = 15
    res = made.resource(area.at(2, 0), availability=n, name="Relief camp water store")
    q0 = queue()
    pub0, ack0 = qstat(q0, "publish"), qstat(q0, "ack")
    t0 = time.time()
    with cf.ThreadPoolExecutor(n) as ex:
        reqs = list(ex.map(lambda i: made.request(area.at(0, i * 0.1), details=f"burst {i}", auto_match=True), range(n)))
    took = time.time() - t0
    check(f"{n} requests submitted at the same time are all accepted (201) and queued",
          all(r["queued"] for r in reqs), f"in {took:.2f}s")
    ids = [r["id"] for r in reqs]
    peak = [0]

    def drained():
        q = queue()            # RabbitMQ's statistics, sampled for as long as the burst is being processed
        peak[0] = max(peak[0], q.get("messages_ready", 0) + q.get("messages_unacknowledged", 0))
        return all_matched(ids)
    done = wait_for(drained, timeout=150, interval=0.5, desc="all burst requests matched")
    check("RabbitMQ buffered the burst while the matching workers processed it", peak[0] >= 1,
          f"peak backlog in matching.requests: {peak[0]} messages")
    check(f"all {n} requests were matched", len(done) == n, f"{time.time() - t0:.1f}s after submission")
    check("every one of them came through the RabbitMQ consumer", all(x["assignment"]["via"] == "queue" for x in done), "")
    workers = P.matching_workers()
    dist = Counter(x["assignment"]["matched_by"] for x in done)
    check("the work was shared by the matching-service instances (competing consumers)",
          all(P.is_matching_worker(w) for w in dist) and len(dist) >= min(2, len(workers)), dict(dist))
    wait_for(lambda: qstat(queue(), "ack") >= ack0 + n, timeout=30, desc="acks")
    q = queue()
    check(f"RabbitMQ counted >= {n} messages published and acknowledged",
          qstat(q, "publish") - pub0 >= n and qstat(q, "ack") - ack0 >= n,
          f"publish +{qstat(q, 'publish') - pub0}, ack +{qstat(q, 'ack') - ack0}")
    rs = get_resource(res["id"])
    check(f"exactly {n} units were assigned, one per request, none twice",
          rs["availability"] == 0 and sorted(rs["assigned_requests"]) == sorted(ids) and rs["status"] == "busy",
          f"availability {n} -> {rs['availability']}, status {rs['status']}")


@req("11.d", "11.c")
def test_simultaneous_requests_never_double_assign(check, area, made):
    """§11 'Multiple simultaneous requests': many requests created and matched at the same moment never over-assign
    a resource, and racing matches of the same request take only one unit."""
    n, cap = 30, 20
    res = made.resource(area.at(2, 0), availability=cap, name="Limited stock")
    with cf.ThreadPoolExecutor(n) as ex:
        reqs = list(ex.map(lambda i: made.request(area.at(0, i * 0.05), details=f"concurrent {i}"), range(n)))
    ids = [r["id"] for r in reqs]
    shards = Counter(r["_storage"]["shard"] for r in reqs)
    check(f"{n} simultaneous creations: all 201 with distinct ids", len(set(ids)) == n, f"spread over shards {dict(shards)}")
    with cf.ThreadPoolExecutor(n) as ex:
        results = list(ex.map(lambda i: api.post(f"/match/{i}"), ids))
    codes = Counter(r.status_code for r in results)
    check(f"{n} simultaneous POST /match against {cap} units: exactly {cap} succeed (200), {n - cap} get 409",
          codes == Counter({200: cap, 409: n - cap}), dict(codes))
    matched = {i for i, r in zip(ids, results) if r.status_code == 200}
    rs = get_resource(res["id"])
    check("the resource is never over-assigned: availability exactly 0, 'busy'",
          rs["availability"] == 0 and rs["status"] == "busy", f"availability {rs['availability']}, {rs['status']}")
    check("its assigned list is exactly the matched requests (no duplicates, no strays)",
          len(rs["assigned_requests"]) == cap and set(rs["assigned_requests"]) == matched, len(rs["assigned_requests"]))
    one = made.resource(area.at(1, 0), availability=5, name="race target")
    rq = made.request(area.at())
    with cf.ThreadPoolExecutor(8) as ex:
        race = list(ex.map(lambda _: api.post(f"/match/{rq['id']}"), range(8)))
    check("8 simultaneous POST /match of the SAME request all answer 200", all(r.status_code == 200 for r in race),
          Counter(r.json()["result"] for r in race))
    rs1 = get_resource(one["id"])
    check("... and it holds exactly one unit (5 -> 4), listed once",
          rs1["availability"] == 4 and rs1["assigned_requests"] == [rq["id"]], rs1["assigned_requests"])
    got = get_request(rq["id"])
    check("the request is matched to that resource", got["status"] == "matched" and got["assigned_resource"] == one["id"],
          got["status"])


@req("6.mq", "11.e")
def test_rabbitmq_topology(check):
    """§6 'RabbitMQ should be used for asynchronous communication between the request-processing and matching
    components' - exchange, queue, binding, consumers and retry path."""
    ex = rabbit.get("/api/exchanges/%2F/erms.requests").json()
    check("request-service publishes to exchange 'erms.requests' (topic, durable)", ex["type"] == "topic" and ex["durable"],
          f"type={ex['type']} durable={ex['durable']}")
    q = queue()
    check("queue 'matching.requests' is durable (survives broker restarts)", q["durable"], q["durable"])
    binds = rabbit.get("/api/exchanges/%2F/erms.requests/bindings/source").json()
    check("erms.requests is bound to matching.requests with routing key 'request.#'",
          any(b["destination"] == "matching.requests" and b["routing_key"] == "request.#" for b in binds),
          [(b["destination"], b["routing_key"]) for b in binds])
    pair = wait_for(lambda: (c := consumers()) == len(w := P.matching_workers()) and (c, len(w)), timeout=60,
                    desc="one consumer per matching pod")
    check("every matching-service instance is a consumer of matching.requests", True,
          f"{pair[0]} consumers, {pair[1]} matching instances")
    rq = queue("matching.retry")
    args = rq.get("arguments", {})
    check("unmatched requests are retried later via 'matching.retry' (TTL, dead-lettered back to erms.requests)",
          args.get("x-dead-letter-exchange") == "erms.requests" and args.get("x-message-ttl", 0) > 0,
          f"ttl={args.get('x-message-ttl')} ms")
    check("request-service is connected to the broker", api.get("/health").json()["broker_connected"] is True, "")


# ============================================================================= §4 distributed storage
@req("4.a", "10.dist", "11.f", "1.d")
def test_data_distributed_across_three_shards_and_nodes(check, area, made):
    """§4 'Requests and resources must be stored across multiple storage nodes' / §10 'Data Distribution:
    demonstrate distribution across the three storage shards' - checked on each node's own disk."""
    reqs, ress = [], []
    while len(reqs) < 60 and min(Counter(r["_storage"]["shard"] for r in reqs).get(s, 0) for s in range(3)) < 3:
        reqs.append(made.request(area.at(), details="distribution"))
    while len(ress) < 60 and min(Counter(r["_storage"]["shard"] for r in ress).get(s, 0) for s in range(3)) < 2:
        ress.append(made.resource(area.at(), type_=SPEC_TYPES[len(ress) % 5], availability=0))
    rc, sc = Counter(r["_storage"]["shard"] for r in reqs), Counter(r["_storage"]["shard"] for r in ress)
    check("requests are spread over all 3 shards", set(rc) == {0, 1, 2}, f"{len(reqs)} requests: {dict(sorted(rc.items()))}")
    check("resources are spread over all 3 shards", set(sc) == {0, 1, 2}, f"{len(ress)} resources: {dict(sorted(sc.items()))}")
    kc = {sid: [n["key_count"] for n in s["nodes"]] for sid, s in cluster()["shards"].items()}
    check("every shard holds data", all(min(v) > 0 for v in kc.values()), f"records per node: {kc}")
    sample = {}
    for s in range(3):
        sample[req_key(next(r["id"] for r in reqs if r["_storage"]["shard"] == s))] = s
        sample[res_key(next(r["id"] for r in ress if r["_storage"]["shard"] == s))] = s
    keys = list(sample)
    disk = {}
    nodes_of = {s: shard_node_ids(s) for s in range(3)}
    for s, ns in nodes_of.items():
        for nid in ns:
            disk[nid] = node_keys(nid, keys)
    for key, s in sample.items():
        holders = sorted(n for n in disk if disk[n][key]["status"] == 200)
        check(f"{key.split(':')[0]} on shard {s} is on the disk of exactly its shard's 2 nodes", holders == sorted(nodes_of[s]),
              f"{key} -> {holders}")
        versions = {disk[n][key]["version"] for n in holders}
        check("   both copies are identical (same version)", len(versions) == 1, versions)


@req("4.c", "4.d", "11.f")
def test_coordinator_selects_shard_by_hash_mod_3(check, area, made):
    """§4 'The coordinator determines the appropriate shard and routes storage operations. A simple hash-based
    partitioning mechanism may be used. For example: hash(request_id) mod 3'."""
    items = [(req_key(r["id"]), r["_storage"]) for r in (made.request(area.at()) for _ in range(30))]
    items += [(res_key(r["id"]), r["_storage"]) for r in
              (made.resource(area.at(), type_=SPEC_TYPES[i % 5], availability=0) for i in range(15))]
    wrong = [(k, st["shard"], expected_shard(k)) for k, st in items if st["shard"] != expected_shard(k)]
    check("every record was stored on shard = sha256(key) mod 3 (recomputed by the test)", not wrong,
          f"{len(items) - len(wrong)}/{len(items)} correct" + (f"; wrong: {wrong[:3]}" if wrong else ""))
    for key, st in items[:2] + items[30:31]:
        check.note(f"example: {key}", f"sha256 mod 3 = {expected_shard(key)} -> stored on shard {st['shard']} "
                                      f"by {st['replicas']}")
    so = [(k, coord.get(f"/internal/shard-of/{k}").json()["shard"]) for k, _ in items[:10]]
    check("the coordinator's own routing table agrees (GET /internal/shard-of/{key})",
          all(s == expected_shard(k) for k, s in so), f"{len(so)}/{len(so)} keys")
    check("the shard choice is deterministic (same key -> same shard every time)",
          all(coord.get(f"/internal/shard-of/{k}").json()["shard"] == s for k, s in so[:5]), "5 keys asked twice")
    nodes_of = {s: set(shard_node_ids(s)) for s in range(3)}
    bad = []
    for k, st in items[:15]:
        g = coord.get(f"/internal/data/{k}").json()
        if g["served_by"] not in nodes_of[expected_shard(k)]:
            bad.append((k, g["served_by"]))
    check("reads are routed to a node of the key's shard", not bad, "15/15 reads routed correctly" if not bad else bad)
    acked_ok = all(set(st["replicas"]) == nodes_of[st["shard"]] for _, st in items)
    check("writes are routed to (all) the replicas of that shard", acked_ok, "45/45 writes acked by both copies")
    dist = Counter(st["shard"] for _, st in items)
    check.note("load spread of these 45 keys over the shards", dict(sorted(dist.items())))


@req("4.e")
def test_client_does_not_need_the_storage_location(check, area, made):
    """§4 'The client should not need to know the physical storage location of the requested data.'"""
    paths = api.get("/openapi.json").json()["paths"]
    params = sorted({p["name"] for m, path in SPEC_ENDPOINTS for p in paths[path][m].get("parameters", [])})
    check("no client endpoint takes a shard / node / replica parameter",
          not any(w in p.lower() for p in params for w in ("shard", "node", "replica", "host")), params)
    schemas = api.get("/openapi.json").json()["components"]["schemas"]
    fields = sorted(set(schemas["CreateRequest"]["properties"]) | set(schemas["RegisterResource"]["properties"]))
    check("request bodies carry no storage location either", not any("shard" in f or "node" in f for f in fields), fields)
    per_shard = {}
    while len(per_shard) < 3:
        r = made.request(area.at())
        per_shard.setdefault(r["_storage"]["shard"], r["id"])
    ok = {s: api.get(f"/requests/{rid}").status_code for s, rid in sorted(per_shard.items())}
    check("records on all 3 shards are read with the same URL, only the id is needed", set(ok.values()) == {200},
          f"GET /requests/{{id}} for ids on shards {list(ok)} -> {list(ok.values())}")
    env = P.env_of("request-service")
    check("the request-service knows only the coordinator, not any storage node",
          "COORDINATOR_URL" in env and not any("STORAGE" in k or "storage" in str(v) for k, v in env.items()),
          f"COORDINATOR_URL={env.get('COORDINATOR_URL')}")
    if P.name == "compose":
        pubs = {}
        for c in P.containers():
            if c["Service"].startswith("storage-"):
                pubs[c["Service"]] = [p for p in (c.get("Publishers") or []) if p.get("PublishedPort")]
        check("storage nodes are not reachable from outside (no published ports)", all(not v for v in pubs.values()),
              {k: v for k, v in pubs.items()})
    else:
        svcs = {s["metadata"]["name"]: s["spec"] for s in P.kjson("get", "svc")["items"]}
        st = {n: (svcs[n]["type"], svcs[n].get("clusterIP")) for n in svcs if n.startswith("storage-shard-")}
        check("storage pods sit behind headless in-cluster Services only (no NodePort)",
              all(t == "ClusterIP" and ip == "None" for t, ip in st.values()), st)


# ============================================================================= §5 client REST API
@req("5.ok", "5.1", "5.2", "5.3", "5.4", "5.5", "5.6", "5.7")
def test_client_api_success_codes(check, area, made):
    """§5 Table 2: each client endpoint returns the success code listed in the PDF."""
    res = api.post("/resources", json={"type": "food", "location": area.at(1, 0), "availability": 2})
    made.resources.append(res.json()["id"])
    rid = res.json()["id"]
    calls = [("POST", "/resources", res, 201)]
    calls.append(("GET", "/resources", api.get("/resources"), 200))
    calls.append(("PUT", "/resources/{id}/status", api.put(f"/resources/{rid}/status", json={"availability": 3}), 200))
    rq = api.post("/requests", json={"type": "food", "location": area.at(), "details": "codes", "auto_match": False})
    made.requests.append(rq.json()["id"])
    qid = rq.json()["id"]
    calls.append(("POST", "/requests", rq, 201))
    calls.append(("GET", "/requests/{id}", api.get(f"/requests/{qid}"), 200))
    calls.append(("POST", "/match/{requestId}", api.post(f"/match/{qid}"), 200))
    calls.append(("PUT", "/requests/{id}/status", api.put(f"/requests/{qid}/status", json={"status": "in_progress"}), 200))
    for method, path, r, want in calls:
        check(f"{method:<4} {path:<26} -> {want}", r.status_code == want, f"got {r.status_code}")


@req("5.json")
def test_json_request_and_response_bodies(check, area, made):
    """§5 'Structured request and response data should use JSON.'"""
    rq = made.request(area.at())
    responses = {
        "POST /requests (201)": api.post("/requests", json={"type": "food", "location": area.at(), "details": "j",
                                                            "auto_match": False}),
        "GET /requests/{id} (200)": api.get(f"/requests/{rq['id']}"),
        "GET /resources (200)": api.get("/resources", params={"type": "food"}),
        "POST /match/{id} (409)": api.post(f"/match/{rq['id']}"),
        "GET /requests/{missing} (404)": api.get("/requests/req-000000000000"),
        "POST /requests invalid (422)": api.post("/requests", json={"type": "x"}),
        "PUT /resources/{missing}/status (404)": api.put("/resources/res-food-000000000000/status", json={"status": "busy"}),
    }
    made.requests.append(responses["POST /requests (201)"].json()["id"])
    for name, r in responses.items():
        ok = r.headers.get("content-type", "").startswith("application/json")
        try:
            r.json()
        except ValueError:
            ok = False
        check(f"{name}: Content-Type application/json with a JSON body", ok, r.headers.get("content-type"))
    form = api.post("/requests", data={"type": "water"}, headers={"Content-Type": "application/x-www-form-urlencoded"})
    check("a non-JSON request body is rejected (422)", form.status_code == 422, form.status_code)
    broken = api.post("/requests", content=b'{"type": "water", ', headers={"Content-Type": "application/json"})
    check("malformed JSON is rejected (422)", broken.status_code == 422, broken.json()["detail"][0]["type"])


@req("5.400")
def test_invalid_requests_get_4xx(check, area, made):
    """§5 'appropriate HTTP status codes for ... invalid requests' (422 validation, 400 bad input, 409 conflict)."""
    rq = made.request(area.at())
    res = made.resource(area.at(), availability=1)
    good = {"type": "water", "location": area.at(), "details": "x"}
    cases = [
        ("POST /requests without type", api.post("/requests", json={"location": area.at(), "details": "x"}), 422),
        ("POST /requests with type 'pizza'", api.post("/requests", json={**good, "type": "pizza"}), 422),
        ("POST /requests without location", api.post("/requests", json={"type": "water", "details": "x"}), 422),
        ("POST /requests with latitude 123", api.post("/requests", json={**good, "location": {"lat": 123, "lon": 0}}), 422),
        ("POST /requests with longitude -200", api.post("/requests", json={**good, "location": {"lat": 0, "lon": -200}}), 422),
        ("POST /requests with 2001-char details", api.post("/requests", json={**good, "details": "x" * 2001}), 422),
        ("POST /resources with availability -1",
         api.post("/resources", json={"type": "water", "location": area.at(), "availability": -1}), 422),
        ("POST /resources with availability 'lots'",
         api.post("/resources", json={"type": "water", "location": area.at(), "availability": "lots"}), 422),
        ("PUT /requests/{id}/status 'flying'", api.put(f"/requests/{rq['id']}/status", json={"status": "flying"}), 422),
        ("PUT /requests/{id}/status without status", api.put(f"/requests/{rq['id']}/status", json={}), 422),
        ("PUT /resources/{id}/status with an empty body", api.put(f"/resources/{res['id']}/status", json={}), 400),
        ("PUT /resources/{id}/status availability 0 + 'available'",
         api.put(f"/resources/{res['id']}/status", json={"availability": 0, "status": "available"}), 400),
        ("PUT /resources/{id}/status 'sleeping'", api.put(f"/resources/{res['id']}/status", json={"status": "sleeping"}), 422),
        ("GET /resources?type=pizza", api.get("/resources", params={"type": "pizza"}), 422),
        ("PUT /requests/{id}/status 'matched' without a resource",
         api.put(f"/requests/{rq['id']}/status", json={"status": "matched"}), 409),
    ]
    api.put(f"/requests/{rq['id']}/status", json={"status": "cancelled"})
    cases.append(("PUT 'pending' on a cancelled request", api.put(f"/requests/{rq['id']}/status", json={"status": "pending"}), 409))
    lonely = made.request(AREAS.new().at())
    cases.append(("POST /match/{id} with no suitable resource", api.post(f"/match/{lonely['id']}"), 409))
    for name, r, want in cases:
        detail = r.json().get("detail")
        msg = detail if isinstance(detail, str) else (detail[0]["msg"] if isinstance(detail, list) else detail.get("message"))
        check(f"{name} -> {want}", r.status_code == want and detail, f"got {r.status_code}: {str(msg)[:70]}")
    check("nothing was stored by the rejected creates (resource still has 1 unit)", get_resource(res["id"])["availability"] == 1, "")


@req("5.404")
def test_missing_resources_get_404(check):
    """§5 'appropriate HTTP status codes for ... missing resources' (404)."""
    cases = [
        ("GET /requests/{unknown id}", api.get("/requests/req-000000000000")),
        ("PUT /requests/{unknown id}/status", api.put("/requests/req-000000000000/status", json={"status": "cancelled"})),
        ("GET /resources/{unknown id}", api.get("/resources/res-water-000000000000")),
        ("PUT /resources/{unknown id}/status", api.put("/resources/res-water-000000000000/status", json={"status": "busy"})),
        ("PUT /resources/{malformed id}/status", api.put("/resources/not-an-id/status", json={"status": "busy"})),
        ("POST /match/{unknown id}", api.post("/match/req-000000000000")),
        ("GET /no-such-endpoint", api.get("/no-such-endpoint")),
    ]
    for name, r in cases:
        check(f"{name} -> 404", r.status_code == 404, f"got {r.status_code}: {str(r.json().get('detail'))[:70]}")


# ============================================================================= §6 internal communication
@req("6.reg")
def test_internal_node_register(check):
    """§6 POST /internal/nodes/register - 'Register storage node'."""
    bad = coord.post("/internal/nodes/register", json={"node_id": f"spec-bogus-{RUN}", "shard_id": 7,
                                                       "url": "http://nowhere:1"})
    check("a node for a non-existent shard is refused (400) and not added", bad.status_code == 400 and
          f"spec-bogus-{RUN}" not in {n["node_id"] for n in coord.get("/internal/nodes").json()["nodes"]}, bad.json())
    nodes = coord.get("/internal/nodes").json()["nodes"]
    check("all storage nodes registered themselves on start-up", len(nodes) == 6,
          [(n["node_id"], n["shard_id"]) for n in nodes])
    victim = next(n for n in nodes if n["shard_id"] == 0 and n["node_id"] in shard_node_ids(0)[1:])
    sibling = next(n for n in nodes if n["shard_id"] == 0 and n["node_id"] != victim["node_id"])
    r = coord.post("/internal/nodes/register", json={"node_id": victim["node_id"], "shard_id": 0, "url": victim["url"],
                                                     "key_count": victim["key_count"]})
    body = r.json()
    check("POST /internal/nodes/register with a node's own details -> 200", r.status_code == 200, body)
    check("a (re)joining node is told to sync from the ready replica of its shard before serving reads",
          body["status"] == "syncing" and body["resync"] is True and body["peers"] == [sibling["url"]],
          f"status={body['status']} peers={body['peers']} num_shards={body['num_shards']}")
    wait_for(lambda: node_status(victim["node_id"]) == "ready", timeout=20, desc="node ready again")
    check("the real node reports itself ready again with its next heartbeat", True, victim["node_id"])
    line = next((l for l in P.logs(victim["node_id"], tail=5000).splitlines() if "registered with coordinator" in l), "")
    check.note("storage node log", line.strip()[-150:] or "(log rotated)")


@req("6.hb", "8.hb")
def test_internal_heartbeats_are_periodic(check):
    """§6 POST /internal/nodes/heartbeat 'Report node availability' / §8 'Storage nodes should periodically report
    their availability to the coordinator using heartbeat messages.'"""
    r = coord.post("/internal/nodes/heartbeat", json={"node_id": f"spec-unknown-{RUN}", "shard_id": 0, "url": "http://x"})
    check("a heartbeat from an unknown node -> 404 (it must register first)", r.status_code == 404, r.json()["detail"])
    s1 = {n["node_id"]: n for n in coord.get("/internal/nodes").json()["nodes"]}
    time.sleep(5)
    s2 = {n["node_id"]: n for n in coord.get("/internal/nodes").json()["nodes"]}
    for nid in sorted(s1):
        a, b = s1[nid], s2[nid]
        check(f"{nid} heartbeats periodically (last beat advanced, never older than the 6 s timeout)",
              b["last_heartbeat"] > a["last_heartbeat"] and max(a["heartbeat_age_s"], b["heartbeat_age_s"]) < HEARTBEAT_TIMEOUT,
              f"beat age {a['heartbeat_age_s']}s -> {b['heartbeat_age_s']}s, advanced {b['last_heartbeat'] - a['last_heartbeat']:.1f}s in 5s")
    n = s2[sorted(s2)[0]]
    hb = coord.post("/internal/nodes/heartbeat", json={"node_id": n["node_id"], "shard_id": n["shard_id"], "url": n["url"],
                                                       "status": "ready", "key_count": n["key_count"]})
    check("POST /internal/nodes/heartbeat from a registered node -> 200", hb.status_code == 200, hb.json())
    check.note("configuration", f"storage HEARTBEAT_INTERVAL={storage_env().get('HEARTBEAT_INTERVAL', '2 (default)')}s, "
                                f"coordinator HEARTBEAT_TIMEOUT={P.env_of('coordinator').get('HEARTBEAT_TIMEOUT')}s")


@req("6.put", "6.get", "6.del")
def test_internal_data_api(check):
    """§6 PUT / GET / DELETE /internal/data/{key} - 'Store or update data', 'Retrieve data', 'Delete data' (both at
    the coordinator and on the storage nodes it routes to)."""
    key = f"spec-test:{RUN}"
    s = expected_shard(key)
    nodes = shard_node_ids(s)
    p = coord.put(f"/internal/data/{key}", json={"value": {"hello": "world", "run": RUN}})
    check("PUT /internal/data/{key} -> 200, stored on the key's shard", p.status_code == 200 and p.json()["shard"] == s,
          p.json())
    check("   and written to every replica of that shard", sorted(p.json()["replicas"]) == sorted(nodes), p.json()["replicas"])
    g = coord.get(f"/internal/data/{key}")
    check("GET /internal/data/{key} -> 200 with the stored value", g.status_code == 200 and g.json()["value"]["run"] == RUN,
          f"version {g.json()['version']}, served_by {g.json()['served_by']}")
    disk = {n: node_keys(n, [key])[key] for n in nodes}
    check("   the value is on the disk of both replicas with the same version",
          all(d["status"] == 200 and d["version"] == p.json()["version"] for d in disk.values()), list(disk))
    v1 = p.json()["version"]
    u = coord.put(f"/internal/data/{key}", params={"if_version": v1}, json={"value": {"hello": "again", "run": RUN}})
    check("PUT again = update -> 200, new version", u.status_code == 200 and u.json()["version"] > v1,
          f"{v1} -> {u.json()['version']}")
    stale = coord.put(f"/internal/data/{key}", params={"if_version": v1}, json={"value": {}})
    check("an update based on an old version is refused (409, optimistic concurrency)", stale.status_code == 409, stale.json())
    scan = coord.get("/internal/data", params={"prefix": key}).json()
    check("prefix scan GET /internal/data?prefix= finds it", [i["key"] for i in scan["items"]] == [key], scan["unavailable_shards"])
    d = coord.delete(f"/internal/data/{key}")
    check("DELETE /internal/data/{key} -> 200", d.status_code == 200 and d.json()["deleted"], d.json())
    check("   afterwards GET -> 404", coord.get(f"/internal/data/{key}").status_code == 404, "")
    disk = {n: node_keys(n, [key])[key]["status"] for n in nodes}
    check("   and it is deleted on both replicas", set(disk.values()) == {404}, disk)
    nk = f"spec-node:{RUN}"
    node = nodes[0]
    target = P.node_exec_target(node)
    put = json.loads(P.exec_py(target, HTTP_SNIPPET, "PUT", f"http://localhost:8000/internal/data/{nk}",
                               json.dumps({"value": 1, "version": 1})))
    get = json.loads(P.exec_py(target, HTTP_SNIPPET, "GET", f"http://localhost:8000/internal/data/{nk}"))
    dele = json.loads(P.exec_py(target, HTTP_SNIPPET, "DELETE", f"http://localhost:8000/internal/data/{nk}?version=2"))
    gone = json.loads(P.exec_py(target, HTTP_SNIPPET, "GET", f"http://localhost:8000/internal/data/{nk}"))
    check(f"the storage node itself ({node}) serves PUT / GET / DELETE /internal/data/{{key}} for the coordinator",
          (put["status"], get["status"], dele["status"], gone["status"]) == (200, 200, 200, 404),
          f"PUT {put['status']}, GET {get['status']}, DELETE {dele['status']}, GET after delete {gone['status']}")


# ============================================================================= §9.1 Docker
SERVICES = {"request_service": "request-service", "matching_service": "matching-service",
            "coordinator": "coordinator", "storage_node": "storage-node"}


@req("9.1.a", "2.docker")
def test_dockerfiles_for_every_service(check):
    """§9.1 'Create Dockerfiles for the required services.'"""
    for pkg, name in SERVICES.items():
        path = os.path.join(ROOT, pkg, "Dockerfile")
        text = open(path).read() if os.path.exists(path) else ""
        check(f"{pkg}/Dockerfile exists and builds the {name} service",
              "FROM " in text and f"COPY {pkg}" in text and f"{pkg}.app:app" in text,
              next((l for l in text.splitlines() if l.startswith("FROM")), "missing") + " ... CMD uvicorn " + f"{pkg}.app:app")
    cfg = json.loads(spec_env.sh(["docker", "compose", "config", "--format", "json"]))["services"]
    for svc, conf in sorted(cfg.items()):
        if "build" not in conf:
            check.note(f"{svc} uses an official image (no Dockerfile needed)", conf["image"])
            continue
        df = conf["build"].get("dockerfile")
        pkg = next(p for p, n in SERVICES.items() if svc.startswith(n.split("-")[0]))
        check(f"docker-compose builds '{svc}' from its Dockerfile", df == f"{pkg}/Dockerfile", df)


@req("9.1.b", "2.docker")
def test_docker_images_are_built(check):
    """§9.1 'Build Docker images.'"""
    for pkg, name in SERVICES.items():
        out = spec_env.sh(["docker", "image", "inspect", f"erms/{name}:latest"], check=False)
        info = json.loads(out)[0] if out.strip().startswith("[") and json.loads(out) else None
        check(f"image erms/{name}:latest is built", info is not None and f"{pkg}.app:app" in " ".join(info["Config"]["Cmd"]),
              f"created {info['Created'][:19]}, {info['Size'] / 1e6:.0f} MB, CMD {' '.join(info['Config']['Cmd'][:2])}"
              if info else "missing")
    if P.name == "k8s":
        listed = spec_env.sh(["minikube", "image", "ls"], timeout=120)
        for name in SERVICES.values():
            check(f"erms/{name}:latest is loaded into the Kubernetes cluster", f"erms/{name}:latest" in listed, "minikube image ls")


@req("9.1.c", "2.docker", "1.d")
def test_components_run_as_containers(check):
    """§9.1 'Run application components as containers.'"""
    if P.name == "compose":
        rows = P.containers()
        by = Counter(r["Service"] for r in rows if r["State"] == "running")
        expect = {"rabbitmq": 1, "coordinator": 1, "request-service": 1, "matching-service": 2,
                  **{f"storage-{s}{c}": 1 for s in range(3) for c in "ab"}, "prometheus": 1, "grafana": 1}
        for svc, n in expect.items():
            check(f"'{svc}' runs as {n} container(s)", by.get(svc, 0) >= n,
                  ", ".join(f"{r['Name']} ({r['Image']}, {r['Status']})" for r in rows if r["Service"] == svc))
        total = sum(expect.values())
        check(f"all {total} containers of the deployment are running", sum(by.values()) == total,
              f"{sum(by.values())} running: " + ", ".join(f"{k} x{v}" for k, v in sorted(by.items())))
    else:
        pods = P.kjson("get", "pods")["items"]
        for p in sorted(pods, key=lambda p: p["metadata"]["name"]):
            cs = p["status"].get("containerStatuses", [{}])[0]
            check(f"pod {p['metadata']['name']} runs its container", p["status"]["phase"] == "Running" and cs.get("ready"),
                  f"{cs.get('image')}, restarts {cs.get('restartCount')}")
        apps = Counter(p["metadata"]["labels"].get("app") for p in pods)
        expect = {"coordinator": 1, "request-service": 2, "rabbitmq": 1, "storage-node": 6, "prometheus": 1, "grafana": 1}
        check("every component runs in its pods (matching-service: 2+ pods, autoscaled by KEDA)",
              all(apps.get(a, 0) == n for a, n in expect.items()) and apps.get("matching-service", 0) >= 2,
              dict(sorted(apps.items())))


@req("9.1.d", "9.2.d")
def test_containers_communicate(check):
    """§9.1 'Configure communication between containers' (service DNS names on the Docker network / Kubernetes
    Services): each component reaches the ones it depends on from inside its own container."""
    calls = [("request-service", "GET", "http://coordinator:8000/health", "request-service -> coordinator"),
             ("request-service", "GET", "http://matching-service:8000/health", "request-service -> matching-service"),
             ("matching-service", "GET", "http://coordinator:8000/health", "matching-service -> coordinator")]
    for src, method, url, label in calls:
        r = http_from(src, method, url)
        check(f"{label} ({url})", r["status"] == 200, r.get("body", r.get("error"))[:80])
    for src in ("request-service", "matching-service"):
        check(f"{src} -> rabbitmq:5672 (AMQP)", tcp_from(src, "rabbitmq", 5672) == "open", "TCP connect")
    for n in sorted(coord.get("/internal/nodes").json()["nodes"], key=lambda n: n["node_id"]):
        r = http_from("coordinator", "GET", f"{n['url']}/health")
        check(f"coordinator -> storage node {n['node_id']} ({n['url']})", r["status"] == 200,
              json.loads(r["body"])["status"] if r["status"] == 200 else r)
    node = shard_node_ids(1)[0]
    r = http_from(P.node_exec_target(node), "GET", "http://coordinator:8000/health")
    check(f"storage node {node} -> coordinator (register / heartbeat path)", r["status"] == 200, r.get("body", "")[:60])
    check.note("how it is configured", "services address each other by name: COORDINATOR_URL=http://coordinator:8000, "
                                       "MATCHING_SERVICE_URL=http://matching-service:8000, RABBITMQ_URL=amqp://...@rabbitmq:5672, "
                                       "storage NODE_URL = " + ("compose DNS name" if P.name == "compose" else "pod IP"))


# ============================================================================= §12
@req("12")
def test_design_choices_are_documented(check):
    """§12 'Students should be able to justify their choices' - the README explains every left-to-design item."""
    text = open(os.path.join(ROOT, "README.md")).read()
    design = text[text.find("## Design decisions"):]
    items = {"programming language and web framework": "Python 3.12 + FastAPI",
             "storage mechanism of the shards": "SQLite per node",
             "sharding / partitioning strategy": "sha256(key) mod 3",
             "replica synchronization mechanism": "Recovery / synchronisation",
             "resource-matching strategy": "Matching strategy",
             "RabbitMQ exchange / queue / routing": "RabbitMQ topology",
             "microservice boundaries and internal API": "Microservice boundaries",
             "monitoring and logging": "Monitoring / logging"}
    for item, needle in items.items():
        check(f"README justifies: {item}", needle in design, f"'{needle}'")
    check("README justifies: Kubernetes deployment configuration", "## Kubernetes" in text and "StatefulSet" in text,
          "'## Kubernetes' section, manifest table")
    check("README has the internal API description", "## Internal API" in text, "'## Internal API'")


# ============================================================================= §9.2 Kubernetes (non-disruptive)
@k8s_only
@req("9.2.a", "2.k8s", "1.d")
def test_k8s_deployments(check):
    """§9.2 'Kubernetes Deployments.'"""
    for name in ("request-service", "matching-service", "coordinator"):
        d = P.kjson("get", f"deploy/{name}")
        want, have = d["spec"]["replicas"], d["status"].get("availableReplicas", 0)
        check(f"Deployment '{name}' is fully available", have == want, f"{have}/{want} replicas available")
    for name in ("rabbitmq", "storage-shard-0", "storage-shard-1", "storage-shard-2"):
        s = P.kjson("get", f"statefulset/{name}")
        want, have = s["spec"]["replicas"], s["status"].get("readyReplicas", 0)
        check(f"StatefulSet '{name}' (stateful: stable identity + persistent volume) is ready", have == want,
              f"{have}/{want} ready")


@k8s_only
@req("9.2.b")
def test_k8s_services(check):
    """§9.2 'Kubernetes Services.'"""
    svcs = {s["metadata"]["name"]: s["spec"] for s in P.kjson("get", "svc")["items"]}
    expect = {"request-service": "NodePort", "coordinator": "NodePort", "matching-service": "ClusterIP",
              "rabbitmq": "NodePort", "rabbitmq-headless": "ClusterIP", "storage-shard-0": "ClusterIP",
              "storage-shard-1": "ClusterIP", "storage-shard-2": "ClusterIP"}
    for name, typ in expect.items():
        spec = svcs.get(name, {})
        ports = [f"{p['port']}" + (f"->node:{p['nodePort']}" if p.get("nodePort") else "") for p in spec.get("ports", [])]
        check(f"Service '{name}' exists ({typ}{', headless' if spec.get('clusterIP') == 'None' else ''})",
              spec.get("type") == typ, f"ports {ports}")
        eps = P.kjson("get", "endpointslices", "-l", f"kubernetes.io/service-name={name}")["items"]
        n = sum(len(e.get("endpoints") or []) for e in eps)
        check(f"   '{name}' routes to running pods", n >= 1, f"{n} endpoint(s)")
    check("clients reach the API through the request-service Service (NodePort 30080)",
          api.get("/health").status_code == 200, P.api)


@k8s_only
@req("9.2.c")
def test_k8s_pod_replicas(check):
    """§9.2 'Pod replicas.' Several identical pods per microservice, load-balanced by their Service."""
    for app in ("request-service", "matching-service"):
        pods = [p for p in P.pods(f"app={app}") if P.pod_ready(p)]
        owners = {p["metadata"]["ownerReferences"][0]["name"] for p in pods}
        check(f"{app} runs >= 2 replica pods from one ReplicaSet", len(pods) >= 2 and len(owners) == 1,
              f"{[p['metadata']['name'] for p in pods]} <- {owners}")
    for s in range(3):
        check(f"storage-shard-{s} runs 2 pods (primary + replica)", len(P.ready_pods(f"app=storage-node,shard={s}")) == 2,
              P.ready_pods(f"app=storage-node,shard={s}"))
    seen = Counter()
    with fresh_client() as c:
        for _ in range(40):
            seen[c.get("/health").json()["instance"]] += 1
    check("the request-service Service spreads calls over its replica pods", len(seen) >= 2 and
          set(seen) <= set(P.ready_pods("app=request-service")), dict(seen))


@k8s_only
@req("9.2.local")
def test_k8s_runs_on_a_local_cluster(check):
    """§9.2 'The Kubernetes deployment may be performed locally using Minikube, kind, or Docker Desktop Kubernetes.'"""
    ctx = spec_env.sh(["kubectl", "config", "current-context"]).strip()
    nodes = json.loads(spec_env.sh(["kubectl", "get", "nodes", "-o", "json"]))["items"]
    check("the cluster is a local Minikube cluster", ctx == "minikube" and nodes[0]["metadata"]["name"] == "minikube",
          f"context {ctx}, node {nodes[0]['metadata']['name']}, Kubernetes {nodes[0]['status']['nodeInfo']['kubeletVersion']}")
    n = len(P.kjson("get", "pods")["items"])
    check(f"everything runs in namespace '{spec_env.NS}'", n >= 14, f"{n} pods (12 application + Prometheus + Grafana)")


# ============================================================================= §8 failure handling (disruptive)
@req("8.detect", "8.avoid", "8.replica", "8.continue", "8.sync", "8.demo", "10.fail", "10.recover",
     "11.g", "11.h", "11.cond", "1.d")
def test_storage_node_failure_before_during_after(check, area, made):
    """§8 'A storage-node failure should be demonstrated by stopping the corresponding container or Kubernetes pod
    and subsequently restarting it ... before, during, and after the failure': detect the failed node, avoid routing
    to it, use the replica, continue processing, synchronize the data when it recovers."""
    S = 1
    F = P.failure_node(S)
    L = next(n for n in shard_node_ids(S) if n != F)
    # ---------------- before
    check.note(f"BEFORE: shard {S} = {shard_node_ids(S)}; the node that will be stopped: {F}; its replica: {L}")
    check("before: every storage node is ready", all_ready(), {n["node_id"]: n["status"] for s in cluster()["shards"].values()
                                                                for n in s["nodes"]})
    res = made.resource_on_shard(S, area.at(1, 0), availability=30, name="Shard-1 water point")
    r0 = made.request_on_shard(S, area.at(), details="written BEFORE the failure")
    check(f"before: a request on shard {S} is written to both copies",
          sorted(r0["_storage"]["replicas"]) == sorted([F, L]), r0["_storage"]["replicas"])
    b = api.get(f"/requests/{r0['id']}").json()
    check("before: it is readable", b["details"] == "written BEFORE the failure", f"served_by {b['_storage']['served_by']}")
    reads_f = cluster()["stats"]["read_by_node"].get(F, 0)
    # ---------------- stop
    t_stop = time.time()
    check.note("STOP the storage node", P.stop_node(F))
    wait_for(lambda: node_status(F) == "down", timeout=HEARTBEAT_TIMEOUT + 30, interval=0.3, desc=f"{F} down")
    info = node_info(F)
    check(f"during: the coordinator detected that {F} is unavailable", info["status"] == "down",
          f"{time.time() - t_stop:.1f}s after the stop; reason: {info.get('down_reason')}")
    check(f"during: shard {S} stays available through its replica {L}", cluster()["shards"][str(S)]["healthy"], "")
    g = api.get(f"/requests/{r0['id']}")
    check("during: data written before the failure is still readable - served by the replica",
          g.status_code == 200 and g.json()["_storage"]["served_by"] == L and g.json()["details"] == r0["details"],
          f"{g.status_code}, served_by {g.json()['_storage']['served_by']}")
    r1 = made.request_on_shard(S, area.at(), details="created DURING the failure", auto_match=True)
    check(f"during: a new request for shard {S} is accepted (201) and routed only to the live replica",
          r1["_storage"]["replicas"] == [L], f"acked by {r1['_storage']['replicas']}")
    check("during: the write is flagged degraded (1 of 2 copies), to be copied on recovery",
          r1["_storage"]["degraded"] is True, r1["_storage"])
    m = api.post(f"/match/{r0['id']}")
    check(f"during: matching keeps working on shard {S} (POST /match -> 200)", m.status_code == 200,
          m.json().get("resource_name"))
    u = api.put(f"/requests/{r0['id']}/status", json={"status": "in_progress", "note": "updated during the failure"})
    check("during: status updates keep working (PUT /requests/{id}/status -> 200)", u.status_code == 200, "in_progress")
    m1 = wait_for(lambda: (x := get_request(r1["id"]))["status"] == "matched" and x, timeout=90, desc="r1 matched")
    check("during: the asynchronous pipeline (RabbitMQ -> matching -> storage) keeps processing",
          m1["assignment"]["via"] == "queue", f"{r1['id']} matched by {m1['assignment']['matched_by']}")
    check(f"during: no read was sent to the failed node {F}", cluster()["stats"]["read_by_node"].get(F, 0) == reads_f,
          f"reads served by {F}: {reads_f} before, {cluster()['stats']['read_by_node'].get(F, 0)} now")
    check("during: listing all requests is still complete (no unavailable shard)",
          api.get("/requests").json()["unavailable_shards"] == [], "unavailable_shards = []")
    # ---------------- restart
    t_start = time.time()
    check.note("RESTART the storage node", P.start_node(F))
    wait_for(lambda: node_status(F) == "ready", timeout=180, interval=0.5, desc=f"{F} ready")
    check(f"after: {F} re-registered, synchronized from {L} and is ready again", True,
          f"{time.time() - t_start:.1f}s after the restart command")
    keys = [req_key(r0["id"]), req_key(r1["id"]), res_key(res["id"])]
    fk, lk = node_keys(F, keys), node_keys(L, keys)
    check(f"after: the request created during the failure is now also on {F}'s disk",
          fk[keys[1]]["status"] == 200 and fk[keys[1]]["version"] == lk[keys[1]]["version"], f"version {fk[keys[1]].get('version')}")
    check(f"after: the updates made during the failure reached {F} (request 'in_progress', resource capacity)",
          fk[keys[0]]["value"]["status"] == "in_progress" and fk[keys[0]]["version"] == lk[keys[0]]["version"] and
          fk[keys[2]]["version"] == lk[keys[2]]["version"],
          f"request status {fk[keys[0]]['value']['status']}, resource availability {fk[keys[2]]['value']['availability']}")
    counts = wait_for(lambda: (c := [n["key_count"] for n in cluster()["shards"][str(S)]["nodes"]])[0] == c[1] and c,
                      timeout=20, desc="equal key counts")
    check(f"after: both copies of shard {S} hold the same number of records", True, counts)
    sync = [l for l in P.logs(F, tail=300).splitlines() if "SYNC complete" in l]
    check.note(f"{F} log", sync[-1].strip()[-140:] if sync else "(no sync line)")
    w = made.request_on_shard(S, area.at(), details="written AFTER the recovery")
    check("after: new writes go to both copies again (not degraded)",
          sorted(w["_storage"]["replicas"]) == sorted([F, L]) and not w["_storage"]["degraded"], w["_storage"]["replicas"])
    if P.name == "compose":
        g = api.get(f"/requests/{r1['id']}").json()
        check(f"after: reads are served by the recovered node {F} again, including the data it missed",
              g["_storage"]["served_by"] == F and g["details"] == "created DURING the failure", g["_storage"]["served_by"])


@req("5.503", "11.e", "11.cond", "6.mq")
def test_matching_service_down_503_and_queue_buffers(check, area, made):
    """§5 'appropriate HTTP status codes for ... unavailable services' (503) and §11 RabbitMQ delivery: with the
    matching microservice stopped, /match answers 503 while new requests are still accepted and wait in RabbitMQ;
    after the restart they are delivered and processed."""
    res = made.resource(area.at(2, 0), availability=5, name="Outage-test water")
    r0 = made.request(area.at())
    try:
        check.note("STOP the matching microservice", P.stop_matching())
        wait_for(lambda: consumers() == 0, timeout=60, desc="no consumers")
        r = api.post(f"/match/{r0['id']}")
        check("POST /match while the matching service is down -> 503 Service Unavailable (JSON)",
              r.status_code == 503 and "detail" in r.json(), str(r.json()["detail"])[:90])
        q0 = queue().get("messages_ready", 0)
        queued = [made.request(area.at(), auto_match=True, details=f"during matching outage {i}") for i in range(3)]
        check("the API stays available: new requests are still accepted (201) and queued",
              all(x["queued"] for x in queued), [x["id"] for x in queued])
        ready = wait_for(lambda: (n := queue().get("messages_ready", 0)) >= q0 + 3 and n, timeout=30, desc="ready msgs")
        check("RabbitMQ holds the messages while no consumer runs", ready >= 3, f"{ready} message(s) waiting in matching.requests")
        check("nothing is matched while the matching service is down",
              all(get_request(x["id"])["status"] == "pending" for x in queued), "all 3 pending")
    finally:
        check.note("RESTART the matching microservice", P.start_matching())
    pair = wait_for(lambda: (c := consumers()) >= 1 and c == len(w := P.matching_workers()) and (c, len(w)),
                    timeout=120, desc="consumers back")
    check("the matching consumers re-attach to the queue", True, f"{pair[0]} consumers for {pair[1]} matching pods")
    done = wait_for(lambda: all_matched([x["id"] for x in queued]), timeout=120, interval=1, desc="queued requests matched")
    check("the buffered messages are delivered and processed: all 3 requests matched",
          all(x["assignment"]["via"] == "queue" for x in done), [x["assignment"]["matched_by"] for x in done])
    r = api.post(f"/match/{r0['id']}")
    check("POST /match works again (200)", r.status_code == 200 and r.json()["resource_id"] == res["id"], r.status_code)


@req("5.503", "8.detect", "11.cond")
def test_whole_shard_down_503_then_recovers(check, area, made):
    """§5 'unavailable services' at the storage layer: with BOTH copies of a shard stopped, data on that shard
    answers 503 (not 500 / wrong data) while the other shards keep working; after the restart the data is back."""
    S = 2
    rq = made.request_on_shard(S, area.at(), details="survives a full shard outage")
    nodes = shard_node_ids(S)
    try:
        check.note("STOP both copies of the shard", P.stop_shard(S))
        wait_for(lambda: all(node_status(n) == "down" for n in nodes), timeout=HEARTBEAT_TIMEOUT + 30, desc="shard down")
        h = coord.get("/health").json()
        check(f"the coordinator reports shard {S} unavailable", h["status"] == "degraded" and S not in h["healthy_shards"], h)
        g = api.get(f"/requests/{rq['id']}")
        check(f"GET of a request on shard {S} -> 503 Service Unavailable", g.status_code == 503, g.json()["detail"])
        codes = Counter()
        for i in range(15):
            r = api.post("/requests", json={"type": "water", "location": area.at(), "details": "x", "auto_match": False})
            codes[r.status_code] += 1
            if r.status_code == 201:
                made.requests.append(r.json()["id"])
        check(f"creating requests: ids that hash to shard {S} -> 503, the others are still stored (201)",
              set(codes) == {201, 503}, dict(codes))
        lst = api.get("/requests")
        check("listing still answers 200 and names the unavailable shard", lst.status_code == 200 and
              lst.json()["unavailable_shards"] == [S], lst.json()["unavailable_shards"])
    finally:
        check.note("RESTART both copies of the shard", P.start_shard(S))
    wait_for(all_ready, timeout=240, interval=1, desc="all nodes ready")
    g = api.get(f"/requests/{rq['id']}")
    check("after the restart the shard's data is back (persistent volumes) -> 200",
          g.status_code == 200 and g.json()["details"] == "survives a full shard outage", g.status_code)


# ============================================================================= §9.2 Kubernetes (disruptive)
@k8s_only
@req("9.2.e", "10.k8sscale", "11.j")
def test_k8s_scale_matching_service_with_continued_availability(check, area, made):
    """§9.2 'Microservice scaling' / §10 'Increase the number of replicas of a microservice and demonstrate continued
    availability' - matching-service 2 -> 4 -> 2 while traffic keeps flowing."""
    res = made.resource(area.at(2, 0), availability=500, name="Scaling-test store")
    before = P.matching_workers()
    created: list[str] = []
    lock = threading.Lock()

    def traffic():
        with fresh_client() as c:
            r = c.post("/requests", json={"type": "water", "location": area.at(), "details": "probe"})
            h = c.get("/health")
        if r.status_code == 201:
            with lock:
                created.append(r.json()["id"])
                made.requests.append(r.json()["id"])
        return r.status_code == 201 and h.status_code == 200

    burst: list[str] = []
    try:
        with Probe(traffic, 0.3) as probe:
            up_cmd = P.matching_replicas(4)
            wait_for(lambda: consumers() == 4, timeout=60, desc="4 consumers")
            time.sleep(2)
        after = P.matching_workers()
        check.note("SCALE UP", up_cmd + ("   (KEDA installed: pins the Deployment to 4 pods)" if P.keda() else ""))
        check("4 matching-service pods are Ready", len(after) == 4, sorted(after))
        check("RabbitMQ sees 4 competing consumers", consumers() == 4, "")
        check("continued availability during the scale-up: every create + health call succeeded",
              probe.total >= 5 and probe.fail == 0, probe.summary())
        burst = [made.request(area.at(), auto_match=True, details=f"after scale-up {i}")["id"] for i in range(20)]
        done = wait_for(lambda: all_matched(burst), timeout=150, interval=1, desc="burst matched")
        dist = Counter(x["assignment"]["matched_by"] for x in done)
        check("the new pods take part: messages were processed by pods added by the scale-up",
              any(w in after - before for w in dist), dict(dist))
        with Probe(traffic, 0.3) as probe2:
            down_cmd = P.matching_replicas(2)
            wait_for(lambda: len(P.matching_workers()) == 2 and consumers() == 2, timeout=90, desc="2 workers")
        check.note("SCALE DOWN", down_cmd)
        check("continued availability during the scale-down", probe2.total >= 3 and probe2.fail == 0, probe2.summary())
    finally:
        P.matching_replicas("auto")
    ids = created + burst
    done = wait_for(lambda: all_matched(ids), timeout=180, interval=1, desc="all scaling-test requests matched")
    check(f"no request was lost while pods were added and removed: all {len(ids)} matched", len(done) == len(ids), "")
    rs = get_resource(res["id"])
    check("capacity is exact afterwards: one unit per matched request",
          rs["availability"] == 500 - len(ids) and sorted(rs["assigned_requests"]) == sorted(ids),
          f"availability {rs['availability']} = 500 - {len(ids)}")


@k8s_only
@req("9.2.e", "9.2.c", "10.k8sscale", "11.j")
def test_k8s_scale_request_service_with_continued_availability(check, area, made):
    """§9.2 'Microservice scaling' for the client-facing microservice: request-service 2 -> 3 -> 2 under traffic."""
    def traffic():
        with fresh_client() as c:
            r = c.post("/requests", json={"type": "food", "location": area.at(), "details": "probe", "auto_match": False})
            ok = r.status_code == 201 and c.get(f"/requests/{r.json()['id']}").status_code == 200
        if r.status_code == 201:
            made.requests.append(r.json()["id"])
        return ok

    try:
        with Probe(traffic, 0.25) as up:
            P.kubectl("scale", "deploy/request-service", "--replicas=3")
            P.kubectl("rollout", "status", "deploy/request-service", "--timeout=180s", timeout=200)
            time.sleep(3)
        pods = P.ready_pods("app=request-service")
        check.note("SCALE UP", f"kubectl -n {spec_env.NS} scale deploy/request-service --replicas=3")
        check("3 request-service pods are Ready", len(pods) == 3, pods)
        check("continued availability during the scale-up (create + read)", up.total >= 5 and up.fail == 0, up.summary())
        seen = Counter()
        with fresh_client() as c:
            for _ in range(60):
                seen[c.get("/health").json()["instance"]] += 1
        check("the Service sends traffic to all 3 pods", set(seen) == set(pods), dict(seen))
        with Probe(traffic, 0.25) as down:
            P.kubectl("scale", "deploy/request-service", "--replicas=2")
            wait_for(lambda: len(P.pods("app=request-service")) == 2, timeout=120, desc="2 pods")
            time.sleep(2)
        check.note("SCALE DOWN", f"kubectl -n {spec_env.NS} scale deploy/request-service --replicas=2")
        check("continued availability during the scale-down", down.total >= 5 and down.fail == 0, down.summary())
    finally:
        P.kubectl("scale", "deploy/request-service", "--replicas=2")


@k8s_only
@req("9.2.f", "10.k8srecover", "11.i")
def test_k8s_pod_failure_and_automatic_recovery(check, area, made):
    """§9.2 'Pod failure and automatic recovery' / §10 'Terminate a running application pod and demonstrate automatic
    recovery': a request-service pod, a matching-service pod in the middle of work, and the coordinator pod."""
    # request-service
    pods0 = P.ready_pods("app=request-service")
    victim = pods0[0]

    def health():
        with fresh_client() as c:
            return c.get("/health").status_code == 200

    with Probe(health, 0.2) as probe:
        check.note("DELETE pod", f"kubectl -n {spec_env.NS} delete pod {victim}")
        P.kubectl("delete", "pod", victim, "--wait=false")
        t0 = time.time()
        new = wait_for(lambda: replaced("app=request-service", victim, len(pods0)), timeout=120, desc="replacement pod")
        took = time.time() - t0
        time.sleep(2)
    check("the Deployment replaced the deleted request-service pod automatically", victim not in new and len(new) == len(pods0),
          f"{sorted(set(new) - set(pods0))} Ready {took:.0f}s later")
    check("the API kept answering while the pod was replaced", probe.total >= 5 and probe.fail == 0, probe.summary())
    # matching-service, while messages are being processed (count pinned so only the Deployment restores it)
    P.matching_replicas(2)
    res = made.resource(area.at(2, 0), availability=10, name="Pod-kill test water")
    ids = [made.request(area.at(), auto_match=True, details=f"in flight {i}")["id"] for i in range(8)]
    m0 = P.ready_pods("app=matching-service")
    time.sleep(1)
    check.note("DELETE pod while it is processing messages", f"kubectl -n {spec_env.NS} delete pod {m0[0]}")
    P.kubectl("delete", "pod", m0[0], "--wait=false")
    newm = wait_for(lambda: replaced("app=matching-service", m0[0], len(m0)), timeout=120, desc="matching replacement")
    check("the Deployment replaced the deleted matching-service pod", m0[0] not in newm, sorted(newm))
    P.matching_replicas("auto")
    done = wait_for(lambda: all_matched(ids), timeout=150, interval=1, desc="in-flight requests matched")
    check("no message was lost: all 8 requests queued around the pod kill were matched", len(done) == 8,
          dict(Counter(x["assignment"]["matched_by"] for x in done)))
    rs = get_resource(res["id"])
    check("and none took two units", rs["availability"] == 2 and sorted(rs["assigned_requests"]) == sorted(ids),
          f"availability 10 -> {rs['availability']}")
    # coordinator
    keep = made.request(area.at(), details="written before the coordinator pod is killed")
    c0 = P.ready_pods("app=coordinator")[0]
    check.note("DELETE pod", f"kubectl -n {spec_env.NS} delete pod {c0}")
    P.kubectl("delete", "pod", c0, "--wait=false")
    c1 = wait_for(lambda: (p := P.ready_pods("app=coordinator")) and c0 not in p and p, timeout=120, desc="new coordinator")
    wait_for(all_ready, timeout=120, interval=1, desc="nodes re-registered")
    check("the coordinator pod was recreated and all 6 storage nodes re-registered with it", all_ready(), c1)
    g = api.get(f"/requests/{keep['id']}")
    check("data written before the coordinator restart is still served", g.status_code == 200, g.json().get("details"))


@k8s_only
@req("8.replica", "8.sync", "9.2.f", "11.g", "11.h", "10.fail", "10.recover")
def test_k8s_storage_pod_killed_reads_fail_over_then_resync(check, area, made):
    """§8 with a Kubernetes pod: delete the storage pod that serves the reads of a shard; reads keep working from the
    replica, the StatefulSet recreates the pod with the same name and volume, and it catches up."""
    S = 1
    p0, p1 = f"storage-shard-{S}-0", f"storage-shard-{S}-1"
    rq = made.request_on_shard(S, area.at(), details="read during the pod kill")
    check("before: reads of the shard are served by " + p0, api.get(f"/requests/{rq['id']}").json()["_storage"]["served_by"] == p0, "")
    pvc0 = P.kubectl("get", "pvc", f"data-{p0}", "-o", "jsonpath={.metadata.uid}")
    served = Counter()

    def read():
        with fresh_client() as c:
            r = c.get(f"/requests/{rq['id']}")
        if r.status_code == 200:
            served[r.json()["_storage"]["served_by"]] += 1
        return r.status_code == 200

    with Probe(read, 0.2) as probe:
        check.note("DELETE pod", f"kubectl -n {spec_env.NS} delete pod {p0}")
        P.kubectl("delete", "pod", p0, "--wait=false")
        wait_for(lambda: node_status(p0) != "ready", timeout=60, interval=0.2, desc="p0 not ready")
        during = made.request_on_shard(S, area.at(), details="written while the pod was gone")
        wait_for(lambda: p0 in P.ready_pods(f"app=storage-node,shard={S}") and node_status(p0) == "ready",
                 timeout=180, desc="p0 back")
        time.sleep(2)
    check("reads never failed while the pod was down and recreated", probe.total >= 10 and probe.fail == 0, probe.summary())
    check(f"reads failed over to the replica {p1}", served[p1] > 0, dict(served))
    check("the StatefulSet recreated the pod with the same name and the same persistent volume",
          P.kubectl("get", "pvc", f"data-{p0}", "-o", "jsonpath={.metadata.uid}") == pvc0, f"{p0}, PVC data-{p0}")
    k = req_key(during["id"])
    disk = node_keys(p0, [k])[k]
    check(f"the write made while {p0} was gone was synchronized to it", disk["status"] == 200, f"version {disk.get('version')}")
