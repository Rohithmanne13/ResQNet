"""End-to-end tests against a running deployment (normal + failure conditions).

  docker compose up -d --build
  .venv/bin/python -m pytest -v tests/integration

Env: API_URL (default http://localhost:8080), COORD_URL (default http://localhost:8001),
RABBIT_URL (management API, default http://localhost:15672), RABBIT_USER / RABBIT_PASS (default: .env),
STOP_CMD / START_CMD: shell templates to stop/start a storage node; {node} = node id,
{statefulset} = node id without its ordinal. FAIL_REPLICA = first|last (which replica to kill).
BROKER_STOP_CMD / BROKER_START_CMD / BROKER_RESTART_CMD, MATCHING_STOP_CMD / MATCHING_START_CMD,
COORD_RESTART_CMD: how to stop/start/restart those services.
Kubernetes: see README ("Against Kubernetes") for the full set of commands.
Tests run in file order.
"""
import concurrent.futures
import os
import subprocess
import time
import uuid

import httpx
import pytest

API = os.getenv("API_URL", "http://localhost:8080")
COORD = os.getenv("COORD_URL", "http://localhost:8001")
STOP_CMD = os.getenv("STOP_CMD", "docker compose stop {node}")
START_CMD = os.getenv("START_CMD", "docker compose start {node}")
FAIL_REPLICA = os.getenv("FAIL_REPLICA", "first")
RABBIT = os.getenv("RABBIT_URL", "http://localhost:15672")


def _dotenv(name: str) -> str | None:
    path = os.path.join(os.path.dirname(__file__), "..", "..", ".env")
    if os.path.exists(path):
        for line in open(path):
            if line.startswith(f"{name}="):
                return line.strip().split("=", 1)[1]
    return None


RABBIT_AUTH = (os.getenv("RABBIT_USER") or _dotenv("RABBITMQ_USER") or "erms",
               os.getenv("RABBIT_PASS") or _dotenv("RABBITMQ_PASSWORD") or "")

api = httpx.Client(base_url=API, timeout=20)
coord = httpx.Client(base_url=COORD, timeout=20)
rabbit = httpx.Client(base_url=RABBIT, timeout=10, auth=RABBIT_AUTH)

# Unique location per test run so leftover resources from previous runs never interfere.
RUN = uuid.uuid4().hex[:6]
BASE_LAT = -60 + (int(RUN, 16) % 10000) / 100.0   # somewhere in the southern ocean, unique per run
BASE_LON = -170 + (int(RUN[:3], 16) % 300)


def loc(dlat=0.0, dlon=0.0):
    return {"lat": round(BASE_LAT + dlat, 5), "lon": round(BASE_LON + dlon, 5), "area": f"test-{RUN}"}


def wait_for(fn, timeout=30, interval=0.5, desc="condition"):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = fn()
        except httpx.HTTPError as e:   # e.g. the coordinator is restarting
            last = repr(e)
        else:
            if last:
                return last
        time.sleep(interval)
    raise AssertionError(f"timed out waiting for {desc}; last={last!r}")


def node_status(node_id):
    for s in coord.get("/cluster").json()["shards"].values():
        for n in s["nodes"]:
            if n["node_id"] == node_id:
                return n["status"]
    return None


def create_request(type_="water", auto_match=True, **kw):
    r = api.post("/requests", json={"type": type_, "location": loc(**kw), "details": f"test {RUN}",
                                    "priority": "high", "auto_match": auto_match})
    assert r.status_code == 201, r.text
    return r.json()


def coord_value(key):
    return coord.get(f"/internal/data/{key}").json()["value"]


def coord_write(key, value):
    """Write a record directly through the coordinator (bypassing the services) to simulate a crash
    between the two writes of an operation."""
    r = coord.put(f"/internal/data/{key}", json={"value": value})
    assert r.status_code == 200, r.text


def cluster_all_ready():
    shards = coord.get("/cluster").json()["shards"].values()
    return all(len(s["nodes"]) >= 2 and all(n["status"] == "ready" for n in s["nodes"]) for s in shards)


def register(type_="water", availability=1, name=None, **kw):
    r = api.post("/resources", json={"type": type_, "location": loc(**kw), "availability": availability,
                                     "name": name or f"{type_}-{RUN}", "kind": "volunteer"})
    assert r.status_code == 201, r.text
    return r.json()


@pytest.fixture(scope="module", autouse=True)
def system_ready():
    """Right after `docker compose up` / a deploy, RabbitMQ needs ~10-20 s before the services connect:
    wait until the storage layer is healthy and the request service is connected to the broker."""
    wait_for(lambda: coord.get("/health").json()["status"] == "ok", timeout=180, interval=1, desc="storage healthy")
    wait_for(lambda: api.get("/health").json().get("broker_connected"), timeout=180, interval=1,
             desc="request service connected to RabbitMQ")


# ----------------------------------------------------------------------------- normal operation
def test_cluster_healthy():
    c = coord.get("/cluster").json()
    assert c["num_shards"] == 3
    for sid, s in c["shards"].items():
        assert len(s["nodes"]) >= 2, f"shard {sid} needs a replica"
        assert all(n["status"] == "ready" for n in s["nodes"]), s


def test_create_and_get_request():
    req = create_request(auto_match=False)
    assert req["status"] == "pending" and req["id"].startswith("req-")
    got = api.get(f"/requests/{req['id']}")
    assert got.status_code == 200
    body = got.json()
    assert body["type"] == "water" and body["details"] == f"test {RUN}" and "created_at" in body
    assert body["_storage"]["shard"] == req["_storage"]["shard"]
    assert len(req["_storage"]["replicas"]) == 2 and req["_storage"]["degraded"] is False
    # ids pasted with surrounding spaces still resolve
    assert api.get(f"/requests/%20{req['id']}%20").json()["id"] == req["id"]


def test_validation_and_not_found():
    assert api.post("/requests", json={"type": "spaceships", "location": loc()}).status_code == 422
    assert api.post("/requests", json={"type": "water"}).status_code == 422
    assert api.get("/requests/req-doesnotexist").status_code == 404
    assert api.put("/requests/req-doesnotexist/status", json={"status": "resolved"}).status_code == 404
    assert api.post("/match/req-doesnotexist").status_code == 404
    assert api.put("/resources/res-nope/status", json={"status": "offline"}).status_code == 404


def test_register_and_list_resources():
    res = register("food", availability=3, dlat=5)
    assert res["status"] == "available" and res["availability"] == 3
    assert res["id"].startswith("res-food-")        # type-partitioned key: matching scans one type only
    assert api.get("/resources", params={"type": "spaceships"}).status_code == 422
    listed = api.get("/resources", params={"type": "food"}).json()
    assert res["id"] in [r["id"] for r in listed["items"]]
    upd = api.put(f"/resources/{res['id']}/status", json={"status": "offline"})
    assert upd.status_code == 200 and upd.json()["status"] == "offline"
    assert api.get(f"/resources/{res['id']}").json()["status"] == "offline"


def test_resource_availability_updates():
    """PUT /resources/{id}/status {"availability": N}: capacity changes drive available <-> busy,
    are persisted, are respected by matching, and invalid updates are rejected."""
    res = register("shelter", availability=2, dlat=10)
    rid = res["id"]
    assert res["status"] == "available" and res["availability"] == 2

    def update(body):
        return api.put(f"/resources/{rid}/status", json=body)

    # increase capacity
    r = update({"availability": 5})
    assert r.status_code == 200 and r.json()["availability"] == 5 and r.json()["status"] == "available"
    stored = api.get(f"/resources/{rid}").json()
    assert stored["availability"] == 5 and stored["status"] == "available"

    # 0 -> busy
    r = update({"availability": 0})
    assert r.status_code == 200 and r.json()["availability"] == 0 and r.json()["status"] == "busy"
    stored = api.get(f"/resources/{rid}").json()
    assert stored["availability"] == 0 and stored["status"] == "busy"

    # a busy (0-capacity) resource is never matched
    req = create_request("shelter", auto_match=False, dlat=10)
    assert api.post(f"/match/{req['id']}").status_code == 409

    # invalid updates are rejected and change nothing
    assert update({"status": "available"}).status_code == 400          # available with 0 capacity
    assert update({"availability": -1}).status_code == 422
    assert update({}).status_code == 400
    assert api.get(f"/resources/{rid}").json()["status"] == "busy"

    # busy -> available again when capacity comes back, and now it gets matched
    r = update({"availability": 1})
    assert r.status_code == 200 and r.json()["status"] == "available" and r.json()["availability"] == 1
    m = api.post(f"/match/{req['id']}")
    assert m.status_code == 200 and m.json()["resource_id"] == rid
    after = api.get(f"/resources/{rid}").json()
    assert after["availability"] == 0 and after["status"] == "busy" and req["id"] in after["assigned_requests"]

    # an offline resource stays offline when only its capacity changes
    assert update({"status": "offline"}).status_code == 200
    r = update({"availability": 3})
    assert r.status_code == 200 and r.json()["status"] == "offline" and r.json()["availability"] == 3


def test_manual_match_picks_nearest_resource():
    far = register("medicine", dlat=0.2, name="far-pharmacy")      # ~22 km away
    near = register("medicine", dlat=0.01, name="near-pharmacy")   # ~1 km away
    req = create_request("medicine", auto_match=False)
    r = api.post(f"/match/{req['id']}")
    assert r.status_code == 200, r.text
    assert r.json()["resource_id"] == near["id"]
    got = api.get(f"/requests/{req['id']}").json()
    assert got["status"] == "matched" and got["assigned_resource"] == near["id"]
    res = api.get(f"/resources/{near['id']}").json()
    assert res["status"] == "busy" and res["availability"] == 0 and req["id"] in res["assigned_requests"]
    # second match is idempotent
    assert api.post(f"/match/{req['id']}").json()["result"] == "skipped"
    # the far one is still free
    assert api.get(f"/resources/{far['id']}").json()["status"] == "available"


def test_status_lifecycle_releases_resource():
    res = register("shelter", dlat=1)
    req = create_request("shelter", auto_match=False, dlat=1)
    assert api.post(f"/match/{req['id']}").status_code == 200
    assert api.put(f"/requests/{req['id']}/status", json={"status": "in_progress"}).status_code == 200
    r = api.put(f"/requests/{req['id']}/status", json={"status": "resolved"})
    assert r.status_code == 200 and r.json()["released_resource"] == res["id"]
    after = api.get(f"/resources/{res['id']}").json()
    assert after["status"] == "available" and after["availability"] == 1
    # terminal state can't be changed
    assert api.put(f"/requests/{req['id']}/status", json={"status": "pending"}).status_code == 409
    assert api.put(f"/requests/{req['id']}/status", json={"status": "bogus"}).status_code == 422


def test_no_match_returns_409():
    req = create_request("transportation", auto_match=False, dlat=2)
    assert api.post(f"/match/{req['id']}").status_code == 409


def test_async_matching_through_rabbitmq():
    res = register("water", dlat=3)
    req = create_request("water", dlat=3)
    assert req["queued"] is True
    got = wait_for(lambda: (g := api.get(f"/requests/{req['id']}").json())["status"] == "matched" and g,
                   desc="async match")
    assert got["assigned_resource"] == res["id"]
    assert got["assignment"]["via"] == "queue"


def test_request_waits_in_retry_queue_until_resource_appears():
    req = create_request("food", dlat=4)
    time.sleep(2)
    assert api.get(f"/requests/{req['id']}").json()["status"] == "pending"
    res = register("food", dlat=4)
    got = wait_for(lambda: (g := api.get(f"/requests/{req['id']}").json())["status"] == "matched" and g,
                   timeout=40, desc="match after retry")
    assert got["assigned_resource"] == res["id"] and got["match_attempts"] >= 1


def test_many_concurrent_requests_no_overallocation():
    """20 simultaneous requests compete for 12 units of capacity: exactly 12 get matched."""
    resources = [register("rescue", availability=4, dlat=6 + i * 0.01) for i in range(3)]
    with concurrent.futures.ThreadPoolExecutor(10) as pool:
        reqs = list(pool.map(lambda i: create_request("rescue", dlat=6 + (i % 3) * 0.01), range(20)))
    assert all(r["queued"] for r in reqs)
    ids = [r["id"] for r in reqs]

    def matched():
        sts = [api.get(f"/requests/{i}").json() for i in ids]
        m = [s for s in sts if s["status"] == "matched"]
        return m if len(m) == 12 else None

    m = wait_for(matched, timeout=60, interval=1, desc="12 matches")
    time.sleep(3)
    assert len(matched() or []) == 12  # and no more than 12
    per_res = {r["id"]: api.get(f"/resources/{r['id']}").json() for r in resources}
    assert all(r["availability"] == 0 and r["status"] == "busy" for r in per_res.values())
    assert sorted(sum((r["assigned_requests"] for r in per_res.values()), [])) == sorted(s["id"] for s in m)
    workers = {s["assignment"]["matched_by"] for s in m}
    print(f"matched by workers: {workers}")


def test_data_distributed_across_three_shards():
    shards = {create_request("other", auto_match=False, dlat=7)["_storage"]["shard"] for _ in range(15)}
    assert shards == {0, 1, 2}
    c = coord.get("/cluster").json()
    for s in c["shards"].values():
        counts = {n["key_count"] for n in s["nodes"]}
        assert min(counts) > 0


# ----------------------------------------------------------------------------- self-healing (edge cases)
def test_capacity_leak_is_reconciled():
    """Crash between 'request resolved' and 'resource released': the unit must not stay locked."""
    res = register("medicine", availability=1, dlat=11)
    req = create_request("medicine", auto_match=False, dlat=11)
    assert api.post(f"/match/{req['id']}").status_code == 200
    assert api.get(f"/resources/{res['id']}").json()["availability"] == 0
    key = f"request:{req['id']}"
    coord_write(key, {**coord_value(key), "status": "resolved"})   # resolved, but release never happened

    def released():
        r = api.get(f"/resources/{res['id']}").json()
        return r["availability"] == 1 and r["status"] == "available" and r["assigned_requests"] == [] and r
    wait_for(released, timeout=90, interval=2, desc="reconciler releases the leaked unit")


def test_lost_claim_is_reconciled():
    """Crash after the request was marked matched but its claim on the resource was lost."""
    res = register("food", availability=2, dlat=12)
    req = create_request("food", auto_match=False, dlat=12)
    assert api.post(f"/match/{req['id']}").status_code == 200
    rkey = f"resource:food:{res['id']}"
    coord_write(rkey, {**coord_value(rkey), "assigned_requests": [], "availability": 2, "status": "available"})

    def reclaimed():
        r = api.get(f"/resources/{res['id']}").json()
        return r["assigned_requests"] == [req["id"]] and r["availability"] == 1 and r
    wait_for(reclaimed, timeout=90, interval=2, desc="reconciler re-claims the lost unit")
    assert api.get(f"/requests/{req['id']}").json()["status"] == "matched"


def test_unmatched_request_is_revived_when_a_resource_appears():
    req = create_request("transportation", auto_match=False, dlat=14)
    key = f"request:{req['id']}"
    coord_write(key, {**coord_value(key), "status": "unmatched", "auto_match": True, "match_attempts": 12})
    assert api.get(f"/requests/{req['id']}").json()["status"] == "unmatched"
    res = register("transportation", dlat=14)
    got = wait_for(lambda: (g := api.get(f"/requests/{req['id']}").json())["status"] == "matched" and g,
                   timeout=60, interval=2, desc="unmatched request revived and matched")
    assert got["assigned_resource"] == res["id"]


# ----------------------------------------------------------------------------- failure handling
def _shard_of(key):
    return coord.get(f"/internal/shard-of/{key}").json()


def test_storage_node_failure_and_recovery():
    # Pick a request and find which shard/replicas hold it.
    req = create_request("water", auto_match=False, dlat=8)
    info = _shard_of(f"request:{req['id']}")
    replicas = info["replicas"]          # sorted; the coordinator prefers the first for reads
    victim, survivor = (replicas[0], replicas[1]) if FAIL_REPLICA == "first" else (replicas[-1], replicas[0])
    fmt = {"node": victim, "statefulset": victim.rsplit("-", 1)[0]}
    print(f"\nshard {info['shard']}: stopping {victim}, survivor {survivor}")

    # --- before failure: served by the preferred replica
    assert api.get(f"/requests/{req['id']}").json()["_storage"]["served_by"] == replicas[0]

    subprocess.run(STOP_CMD.format(**fmt), shell=True, check=True)
    try:
        wait_for(lambda: node_status(victim) == "down", timeout=20, desc=f"{victim} marked down")

        # --- during failure: reads fail over to the replica, writes continue
        got = api.get(f"/requests/{req['id']}")
        assert got.status_code == 200 and got.json()["_storage"]["served_by"] == survivor
        upd = api.put(f"/requests/{req['id']}/status", json={"status": "cancelled", "note": "during failure"})
        assert upd.status_code == 200
        # new requests landing on the degraded shard are still accepted
        written_during = []
        while len(written_during) < 3:
            r = create_request("food", auto_match=False, dlat=8)
            if r["_storage"]["shard"] == info["shard"]:
                assert r["_storage"]["replicas"] == [survivor] and r["_storage"]["degraded"] is True
                written_during.append(r["id"])
    finally:
        subprocess.run(START_CMD.format(**fmt), shell=True, check=True)

    # --- after recovery: node resyncs, becomes ready and holds the writes it missed
    wait_for(lambda: node_status(victim) == "ready", timeout=120, desc=f"{victim} ready again")

    def counts():
        nodes = {n["node_id"]: n for n in coord.get("/cluster").json()["shards"][str(info["shard"])]["nodes"]}
        return nodes[victim]["key_count"] == nodes[survivor]["key_count"]
    wait_for(counts, timeout=15, desc="replica key counts equal")
    if victim == replicas[0]:  # recovered node is preferred for reads again: prove it has the missed data
        got = api.get(f"/requests/{req['id']}").json()
        assert got["_storage"]["served_by"] == victim and got["status"] == "cancelled"
        for rid in written_during:
            assert api.get(f"/requests/{rid}").json()["_storage"]["served_by"] == victim


BROKER_STOP = os.getenv("BROKER_STOP_CMD", "docker compose stop rabbitmq")
BROKER_START = os.getenv("BROKER_START_CMD", "docker compose start rabbitmq")


def test_broker_outage_requests_are_not_lost():
    """Requests accepted while RabbitMQ is down are stored, then re-queued by the
    matching service's sweeper once the broker is back, and matched."""
    res = register("medicine", dlat=9)
    subprocess.run(BROKER_STOP, shell=True, check=True)
    try:
        time.sleep(3)
        req = create_request("medicine", dlat=9)
        assert req["queued"] is False          # accepted & stored, but could not be queued
        assert api.get(f"/requests/{req['id']}").json()["status"] == "pending"
    finally:
        subprocess.run(BROKER_START, shell=True, check=True)
    got = wait_for(lambda: (g := api.get(f"/requests/{req['id']}").json())["status"] == "matched" and g,
                   timeout=120, interval=2, desc="match after broker recovery")
    assert got["assigned_resource"] == res["id"]


MATCHING_STOP = os.getenv("MATCHING_STOP_CMD", "docker compose stop matching-service")
MATCHING_START = os.getenv("MATCHING_START_CMD", "docker compose start matching-service")
BROKER_RESTART = os.getenv("BROKER_RESTART_CMD", "docker compose restart rabbitmq")


def _queue():
    r = rabbit.get("/api/queues/%2F/matching.requests")
    assert r.status_code != 401, "RabbitMQ management API rejected the login (check RABBIT_USER/RABBIT_PASS)"
    return r.json() if r.status_code == 200 else {"http_status": r.status_code}   # broker still starting


def test_queued_requests_survive_broker_restart_and_match_503_when_matching_down():
    """Matching service down: requests stay queued and POST /match answers 503. The broker is then
    restarted: the queued (persistent) messages are still there and get matched once matching is back."""
    res = register("shelter", availability=3, dlat=13)
    subprocess.run(MATCHING_STOP, shell=True, check=True)
    try:
        wait_for(lambda: _queue().get("consumers") == 0, timeout=60, desc="no consumers")
        base = _queue().get("messages", 0)
        reqs = [create_request("shelter", dlat=13) for _ in range(3)]
        assert all(r["queued"] for r in reqs)
        wait_for(lambda: _queue().get("messages") == base + 3, timeout=30, desc="3 messages queued")
        r = api.post(f"/match/{reqs[0]['id']}")
        assert r.status_code == 503, r.text

        subprocess.run(BROKER_RESTART, shell=True, check=True)
        wait_for(lambda: _queue().get("messages") == base + 3, timeout=180, interval=2,
                 desc="queued messages still there after the broker restart")
    finally:
        subprocess.run(MATCHING_START, shell=True, check=True)
    for q in reqs:
        got = wait_for(lambda: (g := api.get(f"/requests/{q['id']}").json())["status"] == "matched" and g,
                       timeout=180, interval=2, desc="match after restart")
        assert got["assigned_resource"] == res["id"]
    assert api.get(f"/resources/{res['id']}").json()["availability"] == 0


COORD_RESTART = os.getenv("COORD_RESTART_CMD", "docker compose restart coordinator")


@pytest.mark.skipif("kubectl" in STOP_CMD, reason="needs per-node stop/start (Compose); "
                                                   "Kubernetes variant: tests/k8s")
def test_coordinator_restart_never_serves_a_stale_replica():
    """Replica A misses an update while it is down. Then the coordinator restarts and A comes back FIRST:
    it must not serve its stale copy; once B is back, A and B merge and A serves the new data."""
    req = create_request("other", auto_match=False, dlat=15)
    replicas = _shard_of(f"request:{req['id']}")["replicas"]
    a, b = replicas[0], replicas[1]                       # a = the replica reads normally go to
    fmt = lambda n: {"node": n, "statefulset": n.rsplit("-", 1)[0]}  # noqa: E731
    print(f"\nstale replica {a}, fresh replica {b}")

    subprocess.run(STOP_CMD.format(**fmt(a)), shell=True, check=True)
    try:
        wait_for(lambda: node_status(a) == "down", timeout=20, desc=f"{a} down")
        upd = api.put(f"/requests/{req['id']}/status", json={"status": "cancelled", "note": "while a is down"})
        assert upd.status_code == 200                     # only b has this update; a still says "pending"
        subprocess.run(STOP_CMD.format(**fmt(b)), shell=True, check=True)
        subprocess.run(COORD_RESTART, shell=True, check=True)
        wait_for(lambda: coord.get("/health").status_code == 200, timeout=60, desc="coordinator back")

        subprocess.run(START_CMD.format(**fmt(a)), shell=True, check=True)
        wait_for(lambda: node_status(a) == "waiting", timeout=30, interval=0.2, desc=f"{a} waiting")
        r = api.get(f"/requests/{req['id']}")
        assert r.status_code == 503, f"stale replica served a read: {r.text}"   # old behaviour: 200 'pending'
    finally:
        subprocess.run(START_CMD.format(**fmt(a)), shell=True, check=True)
        subprocess.run(START_CMD.format(**fmt(b)), shell=True, check=True)

    wait_for(lambda: node_status(a) == "ready" and node_status(b) == "ready", timeout=90, desc="a and b ready")
    got = api.get(f"/requests/{req['id']}").json()
    assert got["_storage"]["served_by"] == a and got["status"] == "cancelled"   # a merged b's newer data
    wait_for(cluster_all_ready, timeout=90, desc="whole cluster ready")
