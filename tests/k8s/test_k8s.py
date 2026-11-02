"""Kubernetes tests (spec §11): pod failure & recovery, scaling with continued availability.

Prereq: ./k8s/deploy.sh (namespace erms). Then:
  IP=$(minikube ip); API_URL=http://$IP:30080 COORD_URL=http://$IP:30081 RABBIT_URL=http://$IP:30672 \
  .venv/bin/python -m pytest -v -s tests/k8s

Env: API_URL, COORD_URL, RABBIT_URL (RabbitMQ management; login read from the rabbitmq-credentials Secret),
K8S_NAMESPACE (default erms),
KUBECTL (default "kubectl"). The whole module is skipped when kubectl cannot reach the namespace.
Tests run in file order and put the cluster back the way they found it (2 matching replicas).
"""
import json
import os
import shlex
import subprocess
import threading
import time
import uuid

import httpx
import pytest

NS = os.getenv("K8S_NAMESPACE", "erms")
KUBECTL = shlex.split(os.getenv("KUBECTL", "kubectl")) + ["-n", NS]
API = os.getenv("API_URL", "http://localhost:30080")
COORD = os.getenv("COORD_URL", "http://localhost:30081")
RABBIT = os.getenv("RABBIT_URL", "http://localhost:30672")

api = httpx.Client(base_url=API, timeout=10)
coord = httpx.Client(base_url=COORD, timeout=10)

RUN = uuid.uuid4().hex[:6]
BASE_LAT = -60 + (int(RUN, 16) % 10000) / 100.0   # unique, empty location per run
BASE_LON = -170 + (int(RUN[:3], 16) % 300)


def kubectl(*args, check=True) -> str:
    return subprocess.run(KUBECTL + list(args), check=check, capture_output=True, text=True).stdout


def _cluster_reachable() -> bool:
    try:
        subprocess.run(KUBECTL + ["get", "deploy/request-service"], check=True, capture_output=True, timeout=15)
        return True
    except (OSError, subprocess.SubprocessError):
        return False


pytestmark = pytest.mark.skipif(not _cluster_reachable(), reason=f"kubectl cannot reach namespace {NS}")


def _secret(key: str) -> str:
    import base64
    try:
        out = subprocess.run(KUBECTL + ["get", "secret", "rabbitmq-credentials", "-o", f"jsonpath={{.data.{key}}}"],
                             capture_output=True, text=True, timeout=15).stdout
        return base64.b64decode(out).decode()
    except (OSError, subprocess.SubprocessError, ValueError):
        return ""


RABBIT_AUTH = (_secret("username"), _secret("password"))
rabbit = httpx.Client(base_url=RABBIT, timeout=10, auth=RABBIT_AUTH)


# ----------------------------------------------------------------------------- helpers
def loc(dlat=0.0):
    return {"lat": round(BASE_LAT + dlat, 5), "lon": round(BASE_LON, 5), "area": f"k8s-{RUN}"}


def wait_for(fn, timeout=60, interval=0.5, desc="condition"):
    deadline = time.time() + timeout
    last = None
    while time.time() < deadline:
        try:
            last = fn()
        except httpx.HTTPError as e:   # a call may land on a pod that is going away; just retry
            last = repr(e)
        else:
            if last:
                return last
        time.sleep(interval)
    raise AssertionError(f"timed out waiting for {desc}; last={last!r}")


def pods(label: str) -> list[dict]:
    return json.loads(kubectl("get", "pods", "-l", label, "-o", "json"))["items"]


def is_ready(pod: dict) -> bool:
    if pod["metadata"].get("deletionTimestamp"):
        return False
    conds = pod.get("status", {}).get("conditions", [])
    return any(c["type"] == "Ready" and c["status"] == "True" for c in conds)


def ready_pod_names(label: str) -> set[str]:
    return {p["metadata"]["name"] for p in pods(label) if is_ready(p)}


def ready_set(label: str, n: int, without: str | None = None):
    """The names of the Ready pods for label if there are exactly n of them (and none is `without`), else None."""
    names = ready_pod_names(label)
    return names if len(names) == n and without not in names else None


def node_status(node_id: str):
    for s in coord.get("/cluster").json()["shards"].values():
        for n in s["nodes"]:
            if n["node_id"] == node_id:
                return n["status"]
    return None


def matching_consumers() -> int:
    return rabbit.get("/api/queues/%2F/matching.requests").json().get("consumers", 0)


class Probe:
    """Calls fn() in a background thread until stopped; records success/failure of every call."""

    def __init__(self, fn, interval=0.2):
        self.fn, self.interval = fn, interval
        self.ok = self.fail = 0
        self.results = []
        self._stop = threading.Event()
        self._t = threading.Thread(target=self._run, daemon=True)

    def _run(self):
        while not self._stop.is_set():
            try:
                res = self.fn()
                self.results.append(res)
                self.ok += 1
            except Exception:  # noqa: BLE001 - any failure counts as unavailability
                self.fail += 1
            time.sleep(self.interval)

    def __enter__(self):
        self._t.start()
        return self

    def __exit__(self, *exc):
        self._stop.set()
        self._t.join(timeout=15)

    @property
    def total(self):
        return self.ok + self.fail

    @property
    def rate(self):
        return self.ok / self.total if self.total else 0.0


def health_call():
    r = httpx.get(f"{API}/health", timeout=3)
    r.raise_for_status()
    return r.json()["instance"]


# ----------------------------------------------------------------------------- pod failure & recovery
def test_request_service_pod_deleted_is_recreated_and_api_keeps_answering():
    label = "app=request-service"
    before = wait_for(lambda: ready_set(label, 2), desc="2 ready pods")
    victim = sorted(before)[0]
    print(f"\nready before: {sorted(before)}; deleting {victim}")

    with Probe(health_call) as probe:
        kubectl("delete", "pod", victim, "--wait=false")
        # a NEW pod (different name) appears and becomes Ready; the deleted one is gone
        after = wait_for(lambda: ready_set(label, 2, without=victim),
                         timeout=120, desc="replacement request-service pod Ready")
        time.sleep(2)
    new = after - before
    print(f"ready after: {sorted(after)}; new pod(s): {sorted(new)}; "
          f"/health during recovery: {probe.ok}/{probe.total} ok ({probe.rate:.0%})")
    assert len(new) == 1, f"expected exactly one replacement pod, got {new}"
    assert probe.total >= 5 and probe.rate >= 0.9, f"API not available during pod recovery: {probe.ok}/{probe.total}"

    # API fully functional after recovery: create + read back through the new set of pods
    r = api.post("/requests", json={"type": "water", "location": loc(), "auto_match": False})
    assert r.status_code == 201, r.text
    assert api.get(f"/requests/{r.json()['id']}").status_code == 200


def test_storage_pod_deleted_fails_over_then_resyncs():
    """Delete the preferred storage pod of shard 1: reads fail over to its replica, writes continue,
    the StatefulSet recreates the pod (same persistent volume), it catches up and serves the missed data."""
    def on_shard_1():
        r = api.post("/requests", json={"type": "food", "location": loc(1), "auto_match": False})
        assert r.status_code == 201, r.text
        return r.json() if r.json()["_storage"]["shard"] == 1 else None

    req = wait_for(on_shard_1, timeout=30, interval=0, desc="request on shard 1")
    replicas = coord.get(f"/internal/shard-of/request:{req['id']}").json()["replicas"]
    victim, survivor = replicas[0], replicas[1]          # coordinator prefers replicas[0] for reads
    assert wait_for(lambda: node_status(victim) == "ready" and node_status(survivor) == "ready",
                    desc="shard 1 healthy")

    # --- before
    assert api.get(f"/requests/{req['id']}").json()["_storage"]["served_by"] == victim
    print(f"\nbefore: {req['id']} served_by={victim}; deleting pod {victim}")

    kubectl("delete", "pod", victim, "--wait=false")

    # --- during: reads fail over to the surviving replica, writes are still accepted
    got = wait_for(lambda: (g := api.get(f"/requests/{req['id']}").json()).get("_storage", {}).get("served_by")
                   == survivor and g, timeout=30, interval=0.2, desc=f"read failover to {survivor}")
    print(f"during: {req['id']} served_by={got['_storage']['served_by']} (status of {victim}: {node_status(victim)})")
    upd = api.put(f"/requests/{req['id']}/status", json={"status": "cancelled", "note": "during pod deletion"})
    assert upd.status_code == 200, upd.text
    written_during = [wait_for(on_shard_1, timeout=30, interval=0, desc="write on shard 1")["id"] for _ in range(3)]

    # --- after: the recreated pod (same name) resyncs from the survivor and serves again
    wait_for(lambda: node_status(victim) == "ready" and victim in ready_pod_names("app=storage-node,shard=1"),
             timeout=180, desc=f"{victim} recreated and ready")

    def counts_equal():
        ns = {n["node_id"]: n for n in coord.get("/cluster").json()["shards"]["1"]["nodes"]}
        return ns[victim]["key_count"] == ns[survivor]["key_count"] > 0
    wait_for(counts_equal, timeout=30, desc="replica key counts equal")

    got = api.get(f"/requests/{req['id']}").json()
    assert got["_storage"]["served_by"] == victim and got["status"] == "cancelled"
    for rid in written_during:
        assert api.get(f"/requests/{rid}").json()["_storage"]["served_by"] == victim
    print(f"after: {victim} ready again, serves {req['id']} (cancelled) and the {len(written_during)} "
          f"requests written while it was gone")


# ----------------------------------------------------------------------------- scaling
def test_scale_matching_service_to_4_spreads_work_and_stays_available():
    label = "app=matching-service"
    res = api.post("/resources", json={"type": "rescue", "name": f"boats-{RUN}", "location": loc(2),
                                       "availability": 200, "kind": "ngo"})
    assert res.status_code == 201, res.text
    res_id = res.json()["id"]

    def create():
        r = httpx.post(f"{API}/requests", json={"type": "rescue", "location": loc(2)}, timeout=5)
        assert r.status_code == 201 and r.json()["queued"] is True, r.text
        return r.json()["id"]

    try:
        # --- continued availability WHILE scaling: requests keep being created and queued
        with Probe(create, interval=0.3) as probe:
            kubectl("scale", "deploy/matching-service", "--replicas=4")
            kubectl("rollout", "status", "deploy/matching-service", "--timeout=180s")
            scaled = wait_for(lambda: ready_set(label, 4),
                              timeout=120, desc="4 ready matching pods")
            wait_for(lambda: matching_consumers() == 4, timeout=60, desc="4 consumers on matching.requests")
        print(f"\n4 ready pods: {sorted(scaled)}; POST /requests during scale-out: "
              f"{probe.ok}/{probe.total} ok ({probe.rate:.0%})")
        assert probe.total >= 5 and probe.fail == 0, f"requests failed during scaling: {probe.fail}/{probe.total}"

        # --- work spreads across all 4 pods
        burst = [create() for _ in range(24)]
        ids = probe.results + burst

        def all_matched():
            sts = [api.get(f"/requests/{i}").json() for i in ids]
            return sts if all(s["status"] == "matched" for s in sts) else None
        sts = wait_for(all_matched, timeout=180, interval=2, desc=f"all {len(ids)} requests matched")
        assert all(s["assigned_resource"] == res_id for s in sts)

        burst_workers = {s["assignment"]["matched_by"] for s in sts if s["id"] in set(burst)}
        all_workers = {s["assignment"]["matched_by"] for s in sts}
        per_worker = {w: sum(s["assignment"]["matched_by"] == w for s in sts) for w in sorted(all_workers)}
        print(f"matches per worker: {per_worker}")
        assert burst_workers == scaled, f"burst after scale-out was not spread over all 4 pods: {burst_workers}"
        res_now = api.get(f"/resources/{res_id}").json()      # no unit lost or taken twice
        assert res_now["availability"] == 200 - len(ids) and sorted(res_now["assigned_requests"]) == sorted(ids)
    finally:
        # --- scale back to 2, still available
        with Probe(health_call) as down_probe:
            kubectl("scale", "deploy/matching-service", "--replicas=2")
            kubectl("rollout", "status", "deploy/matching-service", "--timeout=180s")
        print(f"scaled back to 2: /health during scale-in {down_probe.ok}/{down_probe.total} ok")
    wait_for(lambda: len(ready_pod_names(label)) == 2, timeout=120, desc="2 matching pods after scale-in")
    assert down_probe.fail == 0


# ----------------------------------------------------------------------------- edge cases
def test_rabbitmq_credentials_come_from_a_secret():
    assert RABBIT_AUTH[0] and RABBIT_AUTH[1], "rabbitmq-credentials Secret missing"
    assert httpx.get(f"{RABBIT}/api/overview", auth=("guest", "guest"), timeout=10).status_code == 401
    assert rabbit.get("/api/overview").status_code == 200
    for deploy in ("request-service", "matching-service"):
        spec = json.loads(kubectl("get", "deploy", deploy, "-o", "json"))["spec"]["template"]["spec"]
        env = {e["name"]: e for e in spec["containers"][0]["env"]}
        assert env["RABBITMQ_PASSWORD"]["valueFrom"]["secretKeyRef"]["name"] == "rabbitmq-credentials"
        assert RABBIT_AUTH[1] not in json.dumps(spec)            # the password is not in the pod spec


def test_degraded_write_survives_losing_both_pods_of_a_shard():
    """Shard 1 runs on one pod; a write is acknowledged by that pod only. Then that pod is deleted too:
    with persistent volumes the shard comes back with the write (emptyDir would have lost it)."""
    kubectl("scale", "sts/storage-shard-1", "--replicas=1")
    try:
        wait_for(lambda: node_status("storage-shard-1-1") == "down", timeout=30, desc="storage-shard-1-1 down")

        def on_shard_1():
            r = api.post("/requests", json={"type": "food", "location": loc(3), "auto_match": False})
            assert r.status_code == 201, r.text
            return r.json() if r.json()["_storage"]["shard"] == 1 else None
        w = wait_for(on_shard_1, timeout=30, interval=0, desc="write on shard 1")
        assert w["_storage"]["replicas"] == ["storage-shard-1-0"] and w["_storage"]["degraded"] is True
        print(f"\ndegraded write {w['id']} acked only by storage-shard-1-0; deleting that pod too")

        kubectl("delete", "pod", "storage-shard-1-0", "--wait=true")
        got = wait_for(lambda: (r := api.get(f"/requests/{w['id']}")).status_code == 200 and r.json(),
                       timeout=180, interval=1, desc="shard 1 serving again")
        assert got["status"] == "pending" and got["_storage"]["served_by"] == "storage-shard-1-0"
    finally:
        kubectl("scale", "sts/storage-shard-1", "--replicas=2")
    wait_for(lambda: node_status("storage-shard-1-1") == "ready", timeout=180, desc="storage-shard-1-1 back")

    def counts_equal():
        ns = {n["node_id"]: n["key_count"] for n in coord.get("/cluster").json()["shards"]["1"]["nodes"]}
        return ns["storage-shard-1-0"] == ns["storage-shard-1-1"]
    wait_for(counts_equal, timeout=30, desc="replicas converge")


def test_coordinator_restart_rebuilds_the_cluster_safely():
    """After a coordinator restart every replica re-registers, waits for its sibling and merges with it
    before serving reads (no replica is trusted alone); all data is still readable afterwards."""
    ids = [api.post("/requests", json={"type": "water", "location": loc(4), "auto_match": False}).json()["id"]
           for _ in range(6)]
    pod = pods("app=coordinator")[0]["metadata"]["name"]
    kubectl("delete", "pod", pod, "--wait=false")
    kubectl("rollout", "status", "deploy/coordinator", "--timeout=180s")

    def all_ready():
        shards = coord.get("/cluster").json()["shards"].values()
        return all(len(s["nodes"]) == 2 and all(n["status"] == "ready" for n in s["nodes"]) for s in shards)
    wait_for(all_ready, timeout=120, desc="all 6 storage nodes ready after coordinator restart")
    logs = kubectl("logs", "deploy/coordinator")
    assert "BOOTSTRAP" in logs and "WAITING" in logs, logs[-2000:]
    for i in ids:
        assert api.get(f"/requests/{i}").status_code == 200
