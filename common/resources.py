"""Resource capacity operations, done with compare-and-set so that concurrent
matchers (multiple matching-service pods) never over-assign a resource."""
import logging

from .models import now_iso, resource_key
from .storage_client import Conflict, NotFound, StorageClient

log = logging.getLogger("resources")


async def try_assign(store: StorageClient, resource_id: str, request_id: str, req_type: str,
                     attempts: int = 4) -> bool:
    """Atomically take one unit of capacity from a resource. False if it is no longer available."""
    key = resource_key(resource_id)
    for _ in range(attempts):
        try:
            item = await store.get(key)
        except NotFound:
            return False
        res = item["value"]
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
            continue  # someone else modified it; re-read and retry
    return False


async def release(store: StorageClient, resource_id: str, request_id: str, attempts: int = 6) -> bool:
    """Give back the capacity a request held on a resource (idempotent)."""
    key = resource_key(resource_id)
    for _ in range(attempts):
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
            continue
    log.error("failed to release resource %s from request %s", resource_id, request_id)
    return False
