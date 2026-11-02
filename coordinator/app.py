"""Storage coordinator.

* Keeps the registry of storage nodes (shard, url, status) fed by register/heartbeat calls.
* Routes every data operation: shard = sha256(key) mod NUM_SHARDS.
* Writes go to every live replica of the shard (write-all-available); a write
  succeeds if at least one "ready" replica acknowledged it. A replica that fails
  a write is marked down immediately so it is never read while stale.
* Reads go to the first "ready" replica, failing over to the next one.
* A node that comes back (or was marked down while still alive) is told to
  resync from a healthy peer and is only used for reads once it reports "ready".
* If a shard has NO ready replica (coordinator restart, or every replica was down),
  a returning node may be stale, so it is not trusted alone: it "waits" until the
  shard's other replicas are back (REPLICAS_PER_SHARD) or BOOTSTRAP_GRACE expires,
  then all returning replicas merge each other's snapshots before serving reads.
* Versions never go backwards across coordinator restarts: nodes report the highest
  version they store and next_version() continues above it.
* Compare-and-set (if_version) is serialised per key here, which is safe
  because there is exactly one coordinator.
"""
import asyncio
import os
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from dataclasses import asdict, dataclass, field
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from common.hashing import NUM_SHARDS, shard_for
from common.logging_setup import setup

HEARTBEAT_TIMEOUT = float(os.getenv("HEARTBEAT_TIMEOUT", "6"))
NODE_TIMEOUT = float(os.getenv("NODE_REQUEST_TIMEOUT", "2"))
REPLICAS_PER_SHARD = int(os.getenv("REPLICAS_PER_SHARD", "2"))
BOOTSTRAP_GRACE = float(os.getenv("BOOTSTRAP_GRACE", "8"))

log = setup("coordinator")

READY, SYNCING, WAITING, DOWN = "ready", "syncing", "waiting", "down"
ALL_STATUSES = (READY, SYNCING, WAITING, DOWN)


@dataclass
class Node:
    node_id: str
    shard_id: int
    url: str
    status: str
    last_heartbeat: float = field(default_factory=time.time)
    registered_at: float = field(default_factory=time.time)
    key_count: int = 0
    down_since: float | None = None
    down_reason: str | None = None
    pending: dict | None = None   # instruction delivered with the next heartbeat reply


nodes: dict[str, Node] = {}
key_locks: dict[str, asyncio.Lock] = defaultdict(asyncio.Lock)
_last_version = 0
shard_wait_since: dict[int, float] = {}
http: httpx.AsyncClient | None = None
stats = {"reads": 0, "writes": 0, "failovers": 0, "read_by_node": defaultdict(int)}


def next_version() -> int:
    """Monotonic version: wall-clock ns, but never repeats or goes backwards (also across
    coordinator restarts and clock skew: observe_version() raises the floor)."""
    global _last_version
    _last_version = max(time.time_ns(), _last_version + 1)
    return _last_version


def observe_version(v: int | None) -> None:
    global _last_version
    if v and v > _last_version:
        _last_version = v


def shard_nodes(shard: int, statuses: tuple[str, ...]) -> list[Node]:
    return sorted((n for n in nodes.values() if n.shard_id == shard and n.status in statuses),
                  key=lambda n: n.node_id)


def ready_peers(shard: int, exclude: str) -> list[str]:
    return [n.url for n in shard_nodes(shard, (READY,)) if n.node_id != exclude]


def mark_down(node: Node, reason: str) -> None:
    if node.status != DOWN:
        log.warning("NODE DOWN: %s (shard %d) - %s", node.node_id, node.shard_id, reason)
        node.status = DOWN
        node.down_since = time.time()
        node.down_reason = reason


def admit(node: Node) -> dict:
    """Decide how a (re)joining node gets back into service. Returns the reply for the node."""
    node.down_since = node.down_reason = None
    peers = ready_peers(node.shard_id, node.node_id)
    if peers:
        node.status = SYNCING
        return {"status": SYNCING, "resync": True, "peers": peers}
    # No ready replica to copy from: this node may be stale. Wait for its siblings, then merge all.
    node.status = WAITING
    shard_wait_since.setdefault(node.shard_id, time.time())
    log.warning("SHARD %d has no ready replica: %s WAITING for the other replicas (up to %.0fs)",
                node.shard_id, node.node_id, BOOTSTRAP_GRACE)
    return {"status": WAITING, "resync": False, "peers": []}


def resolve_waiting(now: float | None = None) -> None:
    """Release WAITING nodes: sync from a ready peer if one appeared, otherwise (once every expected
    replica is back, or the grace period is over) let all waiting replicas merge each other's data."""
    now = time.time() if now is None else now
    for shard in range(NUM_SHARDS):
        waiting = shard_nodes(shard, (WAITING,))
        if not waiting:
            shard_wait_since.pop(shard, None)
            continue
        ready = [n.url for n in shard_nodes(shard, (READY,))]
        since = shard_wait_since.setdefault(shard, now)
        if not ready and len(waiting) < REPLICAS_PER_SHARD and now - since < BOOTSTRAP_GRACE:
            continue
        for n in waiting:
            peers = ready or [m.url for m in waiting if m is not n]
            n.status = SYNCING
            n.pending = {"status": SYNCING, "resync": True, "peers": peers}
        log.warning("SHARD %d BOOTSTRAP: %s merge from %s", shard, [n.node_id for n in waiting],
                    "ready replica(s)" if ready else f"each other ({len(waiting)} of {REPLICAS_PER_SHARD} back)")
        shard_wait_since.pop(shard, None)


async def failure_detector() -> None:
    while True:
        now = time.time()
        for n in list(nodes.values()):
            if n.status != DOWN and now - n.last_heartbeat > HEARTBEAT_TIMEOUT:
                mark_down(n, f"no heartbeat for {now - n.last_heartbeat:.1f}s")
        resolve_waiting(now)
        await asyncio.sleep(1)


@asynccontextmanager
async def lifespan(_: FastAPI):
    global http
    http = httpx.AsyncClient(timeout=NODE_TIMEOUT)
    task = asyncio.create_task(failure_detector())
    log.info("coordinator started: %d shards, heartbeat timeout %.0fs", NUM_SHARDS, HEARTBEAT_TIMEOUT)
    yield
    task.cancel()
    await http.aclose()


app = FastAPI(title="ERMS Storage Coordinator", lifespan=lifespan)


# --------------------------------------------------------------------------- node management
class NodeInfo(BaseModel):
    node_id: str
    shard_id: int
    url: str
    status: str | None = None
    key_count: int = 0
    max_version: int = 0


@app.post("/internal/nodes/register")
def register(info: NodeInfo):
    if not 0 <= info.shard_id < NUM_SHARDS:
        raise HTTPException(400, f"shard_id must be in [0, {NUM_SHARDS})")
    observe_version(info.max_version)
    prev = nodes.get(info.node_id)
    node = Node(info.node_id, info.shard_id, info.url, WAITING, key_count=info.key_count)
    nodes[info.node_id] = node
    reply = admit(node)
    if node.status == WAITING:
        resolve_waiting()                 # last replica of the shard back? start the merge right away
        if node.pending:
            reply, node.pending = node.pending, None
    log.info("NODE %s: %s shard=%d url=%s keys=%d -> %s (sync peers: %s)",
             "RE-REGISTERED" if prev else "REGISTERED", info.node_id, info.shard_id, info.url,
             info.key_count, node.status.upper(), reply["peers"] or "none")
    return {**reply, "num_shards": NUM_SHARDS}


@app.post("/internal/nodes/heartbeat")
def heartbeat(info: NodeInfo):
    node = nodes.get(info.node_id)
    if node is None:
        raise HTTPException(404, "unknown node; register first")
    node.last_heartbeat = time.time()
    node.key_count = info.key_count
    node.url = info.url
    observe_version(info.max_version)
    if node.status == DOWN:
        # The node missed writes while it was considered down: it must resync before serving reads.
        reply = admit(node)
        if node.status == WAITING:
            resolve_waiting()
            if node.pending:
                reply, node.pending = node.pending, None
        log.info("NODE RECOVERED: %s (shard %d) -> %s", node.node_id, node.shard_id, node.status.upper())
        return reply
    if node.pending:
        reply, node.pending = node.pending, None
        return reply
    if node.status == WAITING:
        return {"status": WAITING, "resync": False, "peers": []}
    if info.status in (READY, SYNCING):
        if node.status != info.status:
            log.info("NODE %s: %s -> %s", node.node_id, node.status.upper(), info.status.upper())
        node.status = info.status
    return {"resync": False}


@app.get("/internal/nodes")
def list_nodes():
    now = time.time()
    return {"nodes": [{**asdict(n), "heartbeat_age_s": round(now - n.last_heartbeat, 1)}
                      for n in sorted(nodes.values(), key=lambda n: n.node_id)]}


# --------------------------------------------------------------------------- data routing
async def read_key(shard: int, key: str) -> dict | None:
    """Read from the first ready replica, failing over. None = authoritative not-found."""
    candidates = shard_nodes(shard, (READY,))
    for i, node in enumerate(candidates):
        try:
            r = await http.get(f"{node.url}/internal/data/{key}")
        except httpx.HTTPError as e:
            mark_down(node, f"read failed: {e!r}")
            continue
        if i > 0:
            stats["failovers"] += 1
        if r.status_code == 404:
            return None
        if r.status_code == 200:
            stats["reads"] += 1
            stats["read_by_node"][node.node_id] += 1
            return {**r.json(), "served_by": node.node_id}
        mark_down(node, f"read returned HTTP {r.status_code}")
    raise HTTPException(503, f"no ready replica available for shard {shard}")


async def replicate(shard: int, method: str, key: str, **kw) -> list[str]:
    """Send a write to all ready+syncing replicas of a shard. Returns ids of nodes that acked."""
    targets = shard_nodes(shard, (READY, SYNCING))
    if not any(n.status == READY for n in targets):
        raise HTTPException(503, f"no ready replica available for shard {shard}")

    async def send(node: Node):
        try:
            r = await http.request(method, f"{node.url}/internal/data/{key}", **kw)
            r.raise_for_status()
            return node, True
        except httpx.HTTPError as e:
            mark_down(node, f"{method} failed: {e!r}")
            return node, False

    results = await asyncio.gather(*(send(n) for n in targets))
    acked = [n for n, ok in results if ok]
    if not any(n.status == READY for n in acked):
        raise HTTPException(503, f"write to shard {shard} failed on all ready replicas")
    stats["writes"] += 1
    return [n.node_id for n in acked]


class PutBody(BaseModel):
    value: Any


@app.put("/internal/data/{key:path}")
async def put_data(key: str, body: PutBody, if_version: int | None = Query(None)):
    """Store or update. if_version: compare-and-set (0 = create only if absent)."""
    shard = shard_for(key)
    async with key_locks[key]:
        if if_version is not None:
            current = await read_key(shard, key)
            current_version = current["version"] if current else 0
            if current_version != if_version:
                raise HTTPException(409, {"message": "version conflict", "current_version": current_version})
        version = next_version()
        acked = await replicate(shard, "PUT", key, json={"value": body.value, "version": version})
    return {"key": key, "version": version, "shard": shard, "replicas": acked,
            "degraded": len(acked) < REPLICAS_PER_SHARD}


@app.get("/internal/data/{key:path}")
async def get_data(key: str):
    shard = shard_for(key)
    rec = await read_key(shard, key)
    if rec is None:
        raise HTTPException(404, f"{key} not found")
    return {"key": key, "value": rec["value"], "version": rec["version"], "shard": shard,
            "served_by": rec["served_by"]}


@app.delete("/internal/data/{key:path}")
async def delete_data(key: str):
    shard = shard_for(key)
    async with key_locks[key]:
        if await read_key(shard, key) is None:
            raise HTTPException(404, f"{key} not found")
        acked = await replicate(shard, "DELETE", key, params={"version": next_version()})
    return {"key": key, "deleted": True, "shard": shard, "replicas": acked}


@app.get("/internal/data")
async def scan(prefix: str = ""):
    """Scatter-gather a prefix scan: one ready replica per shard."""
    items, unavailable = [], []

    async def scan_shard(shard: int):
        for node in shard_nodes(shard, (READY,)):
            try:
                r = await http.get(f"{node.url}/internal/data", params={"prefix": prefix})
                r.raise_for_status()
                return [{**it, "shard": shard, "served_by": node.node_id} for it in r.json()["items"]]
            except httpx.HTTPError as e:
                mark_down(node, f"scan failed: {e!r}")
        unavailable.append(shard)
        return []

    for chunk in await asyncio.gather(*(scan_shard(s) for s in range(NUM_SHARDS))):
        items.extend(chunk)
    return {"items": items, "unavailable_shards": sorted(unavailable)}


# --------------------------------------------------------------------------- observability
@app.get("/internal/shard-of/{key:path}")
def shard_of(key: str):
    shard = shard_for(key)
    return {"key": key, "shard": shard, "replicas": [n.node_id for n in shard_nodes(shard, ALL_STATUSES)]}


@app.get("/cluster")
def cluster():
    now = time.time()
    shards = {}
    for s in range(NUM_SHARDS):
        members = shard_nodes(s, ALL_STATUSES)
        shards[s] = {
            "healthy": any(n.status == READY for n in members),
            "nodes": [{"node_id": n.node_id, "status": n.status, "key_count": n.key_count,
                       "heartbeat_age_s": round(now - n.last_heartbeat, 1), "url": n.url,
                       **({"down_reason": n.down_reason} if n.status == DOWN else {})} for n in members],
        }
    return {"num_shards": NUM_SHARDS, "shards": shards,
            "stats": {**stats, "read_by_node": dict(stats["read_by_node"])}}


@app.get("/health")
def health():
    healthy = [s for s in range(NUM_SHARDS) if shard_nodes(s, (READY,))]
    return {"status": "ok" if len(healthy) == NUM_SHARDS else "degraded",
            "healthy_shards": healthy, "nodes": len(nodes)}
