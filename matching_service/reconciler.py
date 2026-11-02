"""Capacity reconciler: repairs request <-> resource assignments that got out of step.

Assigning and releasing capacity touch two records (the request and the resource) with
two separate compare-and-set writes. A crash between them leaves them inconsistent:

* orphan claim  - a resource holds a unit for a request that is no longer matched to it
                  (e.g. crash after the request was resolved/cancelled but before release,
                  or after claiming the resource but before marking the request matched).
                  Fix: release the unit.
* missing claim - a request is matched/in_progress to a resource that does not list it.
                  Fix: claim the unit again; if the resource has no capacity left, re-open the
                  request (pending) and re-queue it so it is matched to something else.

An inconsistency is only repaired after it has been seen for RECONCILE_GRACE seconds (a match
in progress is briefly "inconsistent" by design), it is re-checked against fresh reads right
before the fix, and a round is skipped entirely if any shard is unavailable (a partial view
would make healthy assignments look broken). All fixes are compare-and-set based, so several
matching replicas can run the reconciler at the same time.
"""
import logging
import time
from typing import Awaitable, Callable

from common.models import now_iso, request_key, resource_key
from common.resources import release, try_assign
from common.storage_client import Conflict, NotFound, StorageClient

ACTIVE = {"matched", "in_progress"}
log = logging.getLogger("reconciler")


def find_inconsistencies(resources: list[dict], requests: list[dict]) -> set[tuple[str, str, str]]:
    """('orphan_claim', resource_id, request_id) / ('missing_claim', request_id, resource_id)."""
    reqs = {r["id"]: r for r in requests}
    res_by_id = {r["id"]: r for r in resources}
    found = set()
    for res in resources:
        for rid in res.get("assigned_requests", []):
            r = reqs.get(rid)
            if not (r and r.get("status") in ACTIVE and r.get("assigned_resource") == res["id"]):
                found.add(("orphan_claim", res["id"], rid))
    for r in requests:
        if r.get("status") in ACTIVE:
            res = res_by_id.get(r.get("assigned_resource"))
            if not res or r["id"] not in res.get("assigned_requests", []):
                found.add(("missing_claim", r["id"], r.get("assigned_resource") or ""))
    return found


class Reconciler:
    def __init__(self, store: StorageClient, grace: float, requeue: Callable[[str], Awaitable[bool]],
                 worker: str = "reconciler"):
        self.store, self.grace, self.requeue, self.worker = store, grace, requeue, worker
        self.first_seen: dict[tuple, float] = {}

    async def run_once(self, now: float | None = None) -> dict:
        now = time.time() if now is None else now
        reqs = await self.store.list("request:")
        ress = await self.store.list("resource:")
        if reqs["unavailable_shards"] or ress["unavailable_shards"]:
            return {"skipped": "shard unavailable", "fixed": []}
        found = find_inconsistencies([i["value"] for i in ress["items"]], [i["value"] for i in reqs["items"]])
        self.first_seen = {k: self.first_seen.get(k, now) for k in found}
        fixed = []
        for issue, seen in list(self.first_seen.items()):
            if now - seen >= self.grace and await self.fix(issue):
                fixed.append(issue)
                self.first_seen.pop(issue, None)
        return {"inconsistent": len(found), "fixed": fixed}

    async def _get(self, key: str) -> dict | None:
        try:
            return await self.store.get(key)
        except NotFound:
            return None

    async def fix(self, issue: tuple[str, str, str]) -> bool:
        """Re-verify with fresh reads and repair. True = consistent now (fixed or resolved itself)."""
        kind, a, b = issue
        if kind == "orphan_claim":
            res_id, req_id = a, b
            item = await self._get(request_key(req_id))
            req = item["value"] if item else None
            if req and req.get("status") in ACTIVE and req.get("assigned_resource") == res_id:
                return True
            ok = await release(self.store, res_id, req_id)
            if ok:
                log.warning("RECONCILE: released leaked unit of %s held for %s (request %s)",
                            res_id, req_id, req["status"] if req else "missing")
            return ok

        req_id, res_id = a, b
        item = await self._get(request_key(req_id))
        if not item:
            return True
        req = item["value"]
        if req.get("status") not in ACTIVE or req.get("assigned_resource") != res_id:
            return True
        res_item = await self._get(resource_key(res_id)) if res_id else None
        if res_item and req_id in res_item["value"].get("assigned_requests", []):
            return True
        if res_item and await try_assign(self.store, res_id, req_id, req["type"]):
            log.warning("RECONCILE: re-claimed a unit of %s for %s (claim was missing)", res_id, req_id)
            return True
        # The resource is gone or has no capacity left: re-open the request so it is matched again.
        ts = now_iso()
        req.update(status="pending", assigned_resource=None, assignment=None, auto_match=True, updated_at=ts)
        req.setdefault("history", []).append({"status": "pending", "at": ts, "by": self.worker,
                                              "note": f"reconciler: claim on {res_id or 'resource'} was lost"})
        try:
            await self.store.put(request_key(req_id), req, if_version=item["version"])
        except Conflict:
            return False
        await self.requeue(req_id)
        log.warning("RECONCILE: %s lost its claim on %s; re-opened and re-queued", req_id, res_id)
        return True
