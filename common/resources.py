"""Resource capacity operations, done with compare-and-set so that concurrent
matchers (multiple matching-service pods) never over-assign a resource.

A compare-and-set conflict only means another matcher changed the resource first, so the operation re-reads
and retries (with a short random back-off to spread out contending matchers) until it succeeds or the
resource has no capacity left; it never reports "no capacity" just because it lost a race.
"""
import asyncio
import logging
import random

from .models import now_iso, resource_key
from .storage_client import Conflict, NotFound, StorageClient

log = logging.getLogger("resources")


CAS_ATTEMPTS = 60


async def _backoff(attempt: int) -> None:
    await asyncio.sleep(random.uniform(0, min(0.02 * (attempt + 1), 0.25)))


async def try_assign(store: StorageClient, resource_id: str, request_id: str, req_type: str,
                     attempts: int = CAS_ATTEMPTS) -> bool:
    """Atomically take one unit of capacity from a resource. False if it is no longer available."""
    key = resource_key(resource_id)
    for attempt in range(attempts):
        try:
            item = await store.get(key)
        except NotFound:
            return False
        res = item["value"]
        if request_id in res.get("assigned_requests", []):
            return True   # already holds a unit (e.g. redelivered message after a crash): never take two
        if res.get("status") != "available" or res.get("availability", 0) <= 0 or res.get("type") != req_type:
            return False
        res["availability"] -= 1
        res.setdefault("assigned_requests", []).append(request_id)
        if res["availability"] == 0:
            res["status"] = "busy"
        res["updated_at"] = now_iso()
        try:
            await store.put(key, res, if_version=item["version"])
            return True
        except Conflict:
            await _backoff(attempt)   # someone else modified it; re-read and retry
    log.warning("gave up claiming %s for %s after %d conflicting attempts", resource_id, request_id, attempts)
    return False


async def release(store: StorageClient, resource_id: str, request_id: str, attempts: int = CAS_ATTEMPTS) -> bool:
    """Give back the capacity a request held on a resource (idempotent)."""
    key = resource_key(resource_id)
    for attempt in range(attempts):
        try:
            item = await store.get(key)
        except NotFound:
            return False
        res = item["value"]
        assigned = res.get("assigned_requests", [])
        if request_id not in assigned:
            return True
        assigned.remove(request_id)
        res["availability"] = res.get("availability", 0) + 1
        if res.get("status") == "busy":
            res["status"] = "available"
        res["updated_at"] = now_iso()
        try:
            await store.put(key, res, if_version=item["version"])
            log.info("released resource %s from request %s", resource_id, request_id)
            return True
        except Conflict:
            await _backoff(attempt)
    log.error("failed to release resource %s from request %s", resource_id, request_id)
    return False
