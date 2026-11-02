"""Matching strategy.

Candidates = resources with the same type, status "available", spare capacity
(availability > 0) and within MAX_MATCH_DISTANCE_KM of the request.
They are ranked by distance (closest first), then by spare capacity (more first),
so the nearest suitable helper is assigned. Assignment uses compare-and-set on
the resource, so two matchers can never take the same last unit of capacity.
"""
import logging
import os
import socket

from common.geo import haversine_km
from common.models import MATCHABLE_REQUEST_STATUSES, now_iso, request_key, resource_prefix
from common.resources import release, try_assign
from common.storage_client import Conflict, NotFound, StorageClient

MAX_MATCH_DISTANCE_KM = float(os.getenv("MAX_MATCH_DISTANCE_KM", "50"))
WORKER_ID = os.getenv("HOSTNAME") or socket.gethostname()

log = logging.getLogger("matcher")


def rank_candidates(request: dict, resources: list[dict], max_km: float = MAX_MATCH_DISTANCE_KM) -> list[tuple[dict, float]]:
    loc = request["location"]
    out = []
    for res in resources:
        if res.get("type") != request["type"] or res.get("status") != "available" or res.get("availability", 0) <= 0:
            continue
        d = haversine_km(loc["lat"], loc["lon"], res["location"]["lat"], res["location"]["lon"])
        if d <= max_km:
            out.append((res, round(d, 3)))
    out.sort(key=lambda rd: (rd[1], -rd[0].get("availability", 0), rd[0]["id"]))
    return out


async def match_request(store: StorageClient, request_id: str, source: str) -> dict:
    """Try to assign the best available resource. Returns a result dict with key 'result':
    matched | no_match | skipped | not_found."""
    key = request_key(request_id)
    for _ in range(5):
        try:
            item = await store.get(key)
        except NotFound:
            return {"result": "not_found", "request_id": request_id}
        req, version = item["value"], item["version"]
        if req["status"] not in MATCHABLE_REQUEST_STATUSES:
            return {"result": "skipped", "request_id": request_id, "status": req["status"],
                    "assigned_resource": req.get("assigned_resource")}

        # Only resources of the requested type: resource keys are partitioned by type, so this is an
        # indexed range scan on each shard rather than a scan of every resource in the system.
        listing = await store.list(resource_prefix(req["type"]))
        resources = [i["value"] for i in listing["items"]]
        chosen = None
        # A unit this request already holds (a crash/redelivery after claiming, before marking the
        # request matched) is reused instead of claiming a second one.
        for res in resources:
            if request_id in res.get("assigned_requests", []):
                loc, rloc = req["location"], res["location"]
                chosen = (res, round(haversine_km(loc["lat"], loc["lon"], rloc["lat"], rloc["lon"]), 3))
                break
        candidates = rank_candidates(req, resources)
        for res, dist in ([] if chosen else candidates):
            if await try_assign(store, res["id"], request_id, req["type"]):
                chosen = (res, dist)
                break
        if chosen is None:
            return {"result": "no_match", "request_id": request_id, "candidates": len(candidates),
                    "unavailable_shards": listing["unavailable_shards"]}

        res, dist = chosen
        ts = now_iso()
        req.update({
            "status": "matched",
            "assigned_resource": res["id"],
            "assignment": {"resource_id": res["id"], "resource_name": res.get("name"), "kind": res.get("kind"),
                           "distance_km": dist, "matched_at": ts, "matched_by": WORKER_ID, "via": source},
            "updated_at": ts,
        })
        req.setdefault("history", []).append({"status": "matched", "at": ts, "by": WORKER_ID})
        try:
            await store.put(key, req, if_version=version)
        except Conflict:
            # Request changed under us (e.g. matched by another worker or cancelled): undo and re-evaluate.
            await release(store, res["id"], request_id)
            continue
        log.info("MATCHED %s (%s) -> %s '%s' at %.2f km [via %s]",
                 request_id, req["type"], res["id"], res.get("name"), dist, source)
        return {"result": "matched", "request_id": request_id, "resource_id": res["id"],
                "resource_name": res.get("name"), "distance_km": dist, "matched_by": WORKER_ID}
    return {"result": "no_match", "request_id": request_id, "reason": "contention"}


async def record_attempt(store: StorageClient, request_id: str, attempt: int, give_up: bool, reason: str) -> None:
    """Best-effort bookkeeping of unsuccessful automatic matching attempts."""
    key = request_key(request_id)
    for _ in range(3):
        try:
            item = await store.get(key)
            req = item["value"]
            if req["status"] not in MATCHABLE_REQUEST_STATUSES:
                return
            ts = now_iso()
            req["match_attempts"] = attempt
            req["last_match_error"] = reason
            req["updated_at"] = ts
            if give_up and req["status"] != "unmatched":
                req["status"] = "unmatched"
                req.setdefault("history", []).append({"status": "unmatched", "at": ts, "by": WORKER_ID})
            await store.put(key, req, if_version=item["version"])
            return
        except Conflict:
            continue
        except Exception as e:
            log.warning("could not record attempt for %s: %r", request_id, e)
            return
