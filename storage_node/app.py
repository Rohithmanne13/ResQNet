"""Storage node: one replica of one shard.

* Stores versioned key/value records in SQLite (deletes are tombstones so they replicate).
* Writes are last-writer-wins on the coordinator-assigned version, which makes
  replication and recovery merges idempotent and order-independent.
* Registers with the coordinator on startup and sends periodic heartbeats.
* After (re)joining, pulls snapshots from the peers the coordinator names and merges
  them ("syncing" state) before reporting itself "ready" for reads.
* If the coordinator has no ready replica of the shard to sync from (e.g. it just restarted),
  the node is told to wait ("waiting"); the coordinator then has all returning replicas of the
  shard merge each other's data, so a stale replica never serves reads on its own.
* Reports the highest version it stores, so a restarted coordinator never issues versions
  that are older than existing data (which last-writer-wins would silently drop).
"""
import asyncio
import json
import os
import socket
import sqlite3
import threading
import time
from contextlib import asynccontextmanager
from typing import Any

import httpx
from fastapi import FastAPI, HTTPException, Query
from pydantic import BaseModel

from common.logging_setup import setup

NODE_ID = os.getenv("NODE_ID") or socket.gethostname()
SHARD_ID = int(os.getenv("SHARD_ID", "0"))
PORT = int(os.getenv("PORT", "8000"))
NODE_URL = os.getenv("NODE_URL") or f"http://{os.getenv('POD_IP') or socket.gethostname()}:{PORT}"
COORDINATOR_URL = os.getenv("COORDINATOR_URL", "http://coordinator:8000").rstrip("/")
DATA_DIR = os.getenv("DATA_DIR", "/data")
HEARTBEAT_INTERVAL = float(os.getenv("HEARTBEAT_INTERVAL", "2"))
WAITING_POLL_INTERVAL = float(os.getenv("WAITING_POLL_INTERVAL", "0.3"))   # check in fast while waiting

log = setup(f"storage-{NODE_ID}")


class Store:
    def __init__(self, path: str):
        os.makedirs(os.path.dirname(path) or ".", exist_ok=True)
        self._lock = threading.Lock()
        self._db = sqlite3.connect(path, check_same_thread=False)
        self._db.execute("PRAGMA journal_mode=WAL")
        # fsync every commit: a write acknowledged by a single replica (degraded shard) survives a crash
        self._db.execute("PRAGMA synchronous=FULL")
        self._db.execute(
            "CREATE TABLE IF NOT EXISTS kv (key TEXT PRIMARY KEY, value TEXT, version INTEGER NOT NULL,"
            " deleted INTEGER NOT NULL DEFAULT 0, updated_at REAL NOT NULL)"
        )
        self._db.commit()

    @staticmethod
    def _row(r) -> dict:
        return {"key": r[0], "value": json.loads(r[1]) if r[1] is not None else None,
                "version": r[2], "deleted": bool(r[3])}

    def get(self, key: str) -> dict | None:
        with self._lock:
            r = self._db.execute("SELECT key,value,version,deleted FROM kv WHERE key=?", (key,)).fetchone()
        return self._row(r) if r else None

    def put(self, key: str, value: Any, version: int, deleted: bool = False) -> bool:
        """Apply if version is newer than what we have. Returns True if applied."""
        with self._lock:
            cur = self._db.execute(
                "INSERT INTO kv(key,value,version,deleted,updated_at) VALUES(?,?,?,?,?) "
                "ON CONFLICT(key) DO UPDATE SET value=excluded.value, version=excluded.version, "
                "deleted=excluded.deleted, updated_at=excluded.updated_at WHERE excluded.version > kv.version",
                (key, json.dumps(value) if not deleted else None, version, int(deleted), time.time()),
            )
            self._db.commit()
            return cur.rowcount > 0

    def scan(self, prefix: str, include_deleted: bool = False) -> list[dict]:
        q = "SELECT key,value,version,deleted FROM kv WHERE key >= ? AND key < ?"
        if not include_deleted:
            q += " AND deleted=0"
        with self._lock:
            rows = self._db.execute(q + " ORDER BY key", (prefix, prefix + "￿")).fetchall()
        return [self._row(r) for r in rows]

    def merge(self, rows: list[dict]) -> int:
        return sum(self.put(r["key"], r["value"], r["version"], r.get("deleted", False)) for r in rows)

    def count(self) -> int:
        with self._lock:
            return self._db.execute("SELECT COUNT(*) FROM kv WHERE deleted=0").fetchone()[0]

    def max_version(self) -> int:
        with self._lock:
            return self._db.execute("SELECT COALESCE(MAX(version), 0) FROM kv").fetchone()[0]


store = Store(os.path.join(DATA_DIR, f"{NODE_ID}.db"))
state = {"status": "starting", "registered": False, "last_sync": None}
_sync_task: asyncio.Task | None = None
_wake = asyncio.Event()   # set when the status changes, to report it to the coordinator immediately


async def sync_from(client: httpx.AsyncClient, peers: list[str]) -> None:
    """Pull full snapshots (incl. tombstones) from the given peers and merge them by version.
    No peers = the coordinator decided this node is the only copy: serve local data."""
    if not peers:
        state["status"] = "ready"
        log.info("no peers to sync from in shard %d; serving local data (%d keys)", SHARD_ID, store.count())
        return
    state["status"] = "syncing"
    t0, received, applied, ok = time.time(), 0, 0, []
    for peer in peers:   # merge from every peer: after a coordinator restart any of them may hold the newest data
        try:
            r = await client.get(f"{peer}/internal/snapshot", timeout=30)
            r.raise_for_status()
            rows = r.json()["items"]
            received += len(rows)
            applied += await asyncio.to_thread(store.merge, rows)
            ok.append(peer)
        except Exception as e:
            log.warning("sync from %s failed: %r", peer, e)
    if not ok:
        log.warning("sync failed from all peers; will re-register")
        state["registered"] = False
        return
    state["status"] = "ready"
    state["last_sync"] = {"peers": ok, "received": received, "applied": applied,
                          "seconds": round(time.time() - t0, 3), "at": time.time()}
    log.info("SYNC complete from %s: received %d records, applied %d updates; now READY (%d keys)",
             ", ".join(ok), received, applied, store.count())
    _wake.set()


def _start_sync(client: httpx.AsyncClient, peers: list[str]) -> None:
    global _sync_task
    if _sync_task and not _sync_task.done():
        return
    state["status"] = "syncing" if peers else "ready"
    _sync_task = asyncio.create_task(sync_from(client, peers))


def _follow(client: httpx.AsyncClient, body: dict) -> None:
    """Act on the coordinator's instruction from a register/heartbeat reply."""
    if body.get("status") == "waiting":
        if state["status"] != "waiting":
            log.warning("no ready replica of shard %d at the coordinator: WAITING for the other replicas", SHARD_ID)
        state["status"] = "waiting"
    elif body.get("resync"):
        log.warning("coordinator requested sync; peers=%s", body.get("peers"))
        _start_sync(client, body.get("peers", []))


async def lifecycle() -> None:
    """Register with the coordinator, then heartbeat forever."""
    async with httpx.AsyncClient(timeout=5) as client:
        while True:
            info = {"node_id": NODE_ID, "shard_id": SHARD_ID, "url": NODE_URL, "status": state["status"],
                    "key_count": store.count(), "max_version": store.max_version()}
            try:
                if not state["registered"]:
                    r = await client.post(f"{COORDINATOR_URL}/internal/nodes/register", json=info)
                    r.raise_for_status()
                    body = r.json()
                    state["registered"] = True
                    log.info("registered with coordinator (shard %d, url %s): %s; peers=%s",
                             SHARD_ID, NODE_URL, body.get("status"), body.get("peers"))
                    if body.get("status") == "waiting":
                        _follow(client, body)
                    else:
                        _start_sync(client, body.get("peers", []))
                else:
                    r = await client.post(f"{COORDINATOR_URL}/internal/nodes/heartbeat", json=info)
                    if r.status_code == 404:  # coordinator restarted and forgot us
                        log.warning("coordinator does not know this node; re-registering")
                        state["registered"] = False
                        continue
                    r.raise_for_status()
                    _follow(client, r.json())
            except httpx.HTTPError as e:
                log.warning("coordinator unreachable: %r", e)
            interval = WAITING_POLL_INTERVAL if state["status"] == "waiting" else HEARTBEAT_INTERVAL
            try:
                await asyncio.wait_for(_wake.wait(), interval)
            except asyncio.TimeoutError:
                pass
            _wake.clear()


@asynccontextmanager
async def lifespan(_: FastAPI):
    task = asyncio.create_task(lifecycle())
    yield
    task.cancel()


app = FastAPI(title=f"Storage node {NODE_ID}", lifespan=lifespan)


class PutBody(BaseModel):
    value: Any
    version: int | None = None


@app.get("/health")
def health():
    return {"node_id": NODE_ID, "shard_id": SHARD_ID, "status": state["status"],
            "key_count": store.count(), "last_sync": state["last_sync"]}


@app.put("/internal/data/{key:path}")
def put_data(key: str, body: PutBody):
    version = body.version or time.time_ns()
    applied = store.put(key, body.value, version)
    return {"key": key, "version": version, "applied": applied, "node_id": NODE_ID}


@app.get("/internal/data/{key:path}")
def get_data(key: str):
    rec = store.get(key)
    if rec is None or rec["deleted"]:
        raise HTTPException(404, f"key {key} not found on {NODE_ID}")
    return {"key": key, "value": rec["value"], "version": rec["version"], "node_id": NODE_ID}


@app.delete("/internal/data/{key:path}")
def delete_data(key: str, version: int | None = Query(None)):
    applied = store.put(key, None, version or time.time_ns(), deleted=True)
    return {"key": key, "deleted": True, "applied": applied, "node_id": NODE_ID}


@app.get("/internal/data")
def scan(prefix: str = ""):
    return {"items": store.scan(prefix), "node_id": NODE_ID}


@app.get("/internal/snapshot")
def snapshot():
    """Full dump including tombstones; used by recovering peers."""
    return {"items": store.scan("", include_deleted=True), "node_id": NODE_ID}
