"""End-to-end tests against a running deployment (normal + failure conditions).

  docker compose up -d --build
  .venv/bin/python -m pytest -v tests/integration

Env: API_URL (default http://localhost:8080), COORD_URL (default http://localhost:8001),
STOP_CMD / START_CMD: shell templates to stop/start a storage node; {node} = node id,
{statefulset} = node id without its ordinal. FAIL_REPLICA = first|last (which replica to kill).
Kubernetes (scaling a StatefulSet down removes its last pod):
  FAIL_REPLICA=last STOP_CMD="kubectl -n erms scale sts {statefulset} --replicas=1" \
  START_CMD="kubectl -n erms scale sts {statefulset} --replicas=2" pytest -v tests/integration
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

api = httpx.Client(base_url=API, timeout=20)
coord = httpx.Client(base_url=COORD, timeout=20)

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
        last = fn()
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


def register(type_="water", availability=1, name=None, **kw):
    r = api.post("/resources", json={"type": type_, "location": loc(**kw), "availability": availability,
                                     "name": name or f"{type_}-{RUN}", "kind": "volunteer"})
    assert r.status_code == 201, r.text
    return r.json()


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
    assert len(req["_storage"]["replicas"]) == 2


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
    listed = api.get("/resources", params={"type": "food"}).json()
    assert res["id"] in [r["id"] for r in listed["items"]]
    upd = api.put(f"/resources/{res['id']}/status", json={"status": "offline"})
    assert upd.status_code == 200 and upd.json()["status"] == "offline"
    assert api.get(f"/resources/{res['id']}").json()["status"] == "offline"


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
                assert r["_storage"]["replicas"] == [survivor]
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
