"""Request Service: the client-facing REST API.

Stores requests/resources through the storage coordinator (never talks to
shards directly) and publishes new requests to RabbitMQ for asynchronous
matching. POST /match/{id} is forwarded to the Matching Service.
"""
import asyncio
import json
import os
import socket
import uuid
from contextlib import asynccontextmanager
from typing import Literal

import httpx
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field, field_validator

from common import messaging
from common.logging_setup import setup
from common.models import (MATCHABLE_REQUEST_STATUSES, REQUEST_STATUSES, REQUEST_TYPES, RESOURCE_KINDS,
                           RESOURCE_STATUSES, TERMINAL_REQUEST_STATUSES, new_resource_id, now_iso, request_key,
                           resource_key, resource_prefix)
from common.resources import release
from common.storage_client import Conflict, StorageClient, StorageError

MATCHING_URL = os.getenv("MATCHING_SERVICE_URL", "http://matching-service:8000").rstrip("/")
INSTANCE = os.getenv("HOSTNAME") or socket.gethostname()

log = setup(f"request-{INSTANCE}")
store = StorageClient()
broker: dict = {"exchange": None}
http: httpx.AsyncClient | None = None


async def broker_setup(channel) -> None:
    broker["exchange"], _ = await messaging.declare_main(channel)
    log.info("publishing to exchange '%s'", messaging.EXCHANGE)


def broker_lost() -> None:
    broker["exchange"] = None


async def publish(request_id: str, routing_key: str) -> bool:
    if broker["exchange"] is None:
        log.warning("broker not connected; %s not queued", request_id)
        return False
    try:
        await broker["exchange"].publish(
            messaging.make_message(json.dumps({"request_id": request_id, "attempt": 0}).encode()),
            routing_key=routing_key)
        log.info("published %s to RabbitMQ (%s)", request_id, routing_key)
        return True
    except Exception as e:
        log.error("failed to publish %s: %r", request_id, e)
        return False


@asynccontextmanager
async def lifespan(_: FastAPI):
    global http
    http = httpx.AsyncClient(timeout=15)
    task = asyncio.create_task(messaging.maintain_connection(broker_setup, broker_lost))
    yield
    task.cancel()
    await http.aclose()
    await store.close()


app = FastAPI(title="ERMS Request Service", lifespan=lifespan)


@app.exception_handler(StorageError)
async def storage_error_handler(_: Request, exc: StorageError):
    return JSONResponse(status_code=exc.status_code, content={"detail": str(exc)})


# --------------------------------------------------------------------------- documented error responses
class ErrorBody(BaseModel):
    detail: str | dict


def errors(**examples: str) -> dict:
    """OpenAPI `responses` for the error codes an endpoint can return, e.g. errors(e404="...", e503="...")."""
    titles = {400: "Bad Request", 404: "Not Found", 409: "Conflict", 422: "Validation Error",
              503: "Service Unavailable"}
    out = {}
    for name, detail in examples.items():
        code = int(name[1:])
        out[code] = {"model": ErrorBody, "description": titles[code],
                     "content": {"application/json": {"example": {"detail": detail}}}}
    return out


STORAGE_DOWN = "no ready replica available for shard 1"


def clean_id(raw: str) -> str:
    """Ids pasted with surrounding whitespace (a common copy/paste slip) still resolve."""
    return raw.strip()


# --------------------------------------------------------------------------- models
class Location(BaseModel):
    lat: float = Field(ge=-90, le=90)
    lon: float = Field(ge=-180, le=180)
    area: str | None = Field(None, max_length=100)


class CreateRequest(BaseModel):
    type: str
    location: Location
    details: str = Field("", max_length=2000)
    priority: Literal["low", "medium", "high", "critical"] = "medium"
    requester: str | None = Field(None, max_length=100)
    auto_match: bool = True  # false = store only; match later via POST /match/{id}

    @field_validator("type")
    @classmethod
    def valid_type(cls, v: str) -> str:
        v = v.lower().strip()
        if v not in REQUEST_TYPES:
            raise ValueError(f"type must be one of {sorted(REQUEST_TYPES)}")
        return v


class RegisterResource(BaseModel):
    type: str
    location: Location
    availability: int = Field(1, ge=0, le=100_000, description="units of capacity / number of requests it can serve")
    name: str | None = Field(None, max_length=120)
    kind: str = "resource"
    contact: str | None = Field(None, max_length=120)

    @field_validator("type")
    @classmethod
    def valid_type(cls, v: str) -> str:
        v = v.lower().strip()
        if v not in REQUEST_TYPES:
            raise ValueError(f"type must be one of {sorted(REQUEST_TYPES)}")
        return v

    @field_validator("kind")
    @classmethod
    def valid_kind(cls, v: str) -> str:
        if v not in RESOURCE_KINDS:
            raise ValueError(f"kind must be one of {sorted(RESOURCE_KINDS)}")
        return v


class RequestStatusUpdate(BaseModel):
    status: str
    note: str | None = Field(None, max_length=500)

    @field_validator("status")
    @classmethod
    def valid_status(cls, v: str) -> str:
        if v not in REQUEST_STATUSES:
            raise ValueError(f"status must be one of {sorted(REQUEST_STATUSES)}")
        return v


class ResourceStatusUpdate(BaseModel):
    status: str | None = None
    availability: int | None = Field(None, ge=0, le=100_000)

    @field_validator("status")
    @classmethod
    def valid_status(cls, v: str | None) -> str | None:
        if v is not None and v not in RESOURCE_STATUSES:
            raise ValueError(f"status must be one of {sorted(RESOURCE_STATUSES)}")
        return v


def storage_meta(stored: dict) -> dict:
    """Where a write landed. degraded = acknowledged by fewer replicas than the shard has (one is down);
    the write is still durable on the replicas that acked it and is copied to the others when they recover."""
    return {"shard": stored["shard"], "replicas": stored["replicas"], "version": stored["version"],
            "degraded": stored.get("degraded", False)}


def with_meta(item: dict) -> dict:
    return {**item["value"], "_storage": {"shard": item.get("shard"), "served_by": item.get("served_by"),
                                          "version": item.get("version")}}


# --------------------------------------------------------------------------- requests
@app.post("/requests", status_code=201, responses=errors(e503=STORAGE_DOWN))
async def create_request(body: CreateRequest):
    rid = f"req-{uuid.uuid4().hex[:12]}"
    ts = now_iso()
    rec = {"id": rid, "type": body.type, "location": body.location.model_dump(), "details": body.details,
           "priority": body.priority, "requester": body.requester, "status": "pending", "auto_match": body.auto_match,
           "created_at": ts, "updated_at": ts, "assigned_resource": None, "match_attempts": 0,
           "history": [{"status": "pending", "at": ts, "by": INSTANCE}]}
    stored = await store.put(request_key(rid), rec, if_version=0)
    queued = await publish(rid, messaging.RK_CREATED) if body.auto_match else False
    return {**rec, "queued": queued, "_storage": storage_meta(stored)}


@app.get("/requests", responses=errors(e503="storage coordinator unreachable"))
async def list_requests(status: str | None = None, type: str | None = None):
    listing = await store.list("request:")
    items = [i["value"] | {"_shard": i["shard"]} for i in listing["items"]]
    items = [r for r in items if (status is None or r["status"] == status) and (type is None or r["type"] == type)]
    items.sort(key=lambda r: r["created_at"])
    return {"count": len(items), "items": items, "unavailable_shards": listing["unavailable_shards"]}


@app.get("/requests/{request_id}", responses=errors(e404="request:req-87f53d097d08 not found", e503=STORAGE_DOWN))
async def get_request(request_id: str):
    request_id = clean_id(request_id)
    return with_meta(await store.get(request_key(request_id)))


@app.put("/requests/{request_id}/status", responses=errors(
    e404="request:req-87f53d097d08 not found",
    e409="request is already resolved; cannot change to pending", e503=STORAGE_DOWN))
async def update_request_status(request_id: str, body: RequestStatusUpdate):
    request_id = clean_id(request_id)
    key = request_key(request_id)
    for _ in range(5):
        item = await store.get(key)
        req = item["value"]
        old = req["status"]
        if old in TERMINAL_REQUEST_STATUSES and body.status != old:
            raise HTTPException(409, f"request is already {old}; cannot change to {body.status}")
        if body.status in ("matched", "in_progress") and not req.get("assigned_resource"):
            raise HTTPException(409, f"cannot set '{body.status}' without an assigned resource; use POST /match")
        ts = now_iso()
        released = None
        if body.status in TERMINAL_REQUEST_STATUSES | MATCHABLE_REQUEST_STATUSES and req.get("assigned_resource"):
            released = req["assigned_resource"]
            if body.status in MATCHABLE_REQUEST_STATUSES:  # re-opened: drop the assignment
                req["assigned_resource"] = None
                req["assignment"] = None
        req["status"] = body.status
        req["updated_at"] = ts
        if body.status == "pending" and old != "pending":
            req["auto_match"] = True  # re-opened requests go back through the queue
        req.setdefault("history", []).append({"status": body.status, "at": ts, "by": INSTANCE,
                                              **({"note": body.note} if body.note else {})})
        try:
            await store.put(key, req, if_version=item["version"])
        except Conflict:
            continue
        if released:
            await release(store, released, request_id)
        requeued = False
        if body.status == "pending" and old != "pending":
            requeued = await publish(request_id, messaging.RK_REQUEUED)
        return {**req, "previous_status": old, "released_resource": released, "requeued": requeued}
    raise HTTPException(409, "too much contention updating request; retry")


# --------------------------------------------------------------------------- resources
@app.post("/resources", status_code=201, responses=errors(e503=STORAGE_DOWN))
async def register_resource(body: RegisterResource):
    rid = new_resource_id(body.type, uuid.uuid4().hex[:12])
    ts = now_iso()
    rec = {"id": rid, "type": body.type, "kind": body.kind, "name": body.name or rid,
           "location": body.location.model_dump(), "availability": body.availability,
           "status": "available" if body.availability > 0 else "busy", "contact": body.contact,
           "assigned_requests": [], "created_at": ts, "updated_at": ts}
    stored = await store.put(resource_key(rid), rec, if_version=0)
    log.info("registered resource %s (%s, capacity %d) on shard %d", rid, body.type, body.availability, stored["shard"])
    return {**rec, "_storage": storage_meta(stored)}


@app.get("/resources", responses=errors(e422="type must be one of [...]", e503="storage coordinator unreachable"))
async def list_resources(type: str | None = None, status: str | None = None):
    if type is not None and type not in REQUEST_TYPES:
        raise HTTPException(422, f"type must be one of {sorted(REQUEST_TYPES)}")
    listing = await store.list(resource_prefix(type))
    items = [i["value"] | {"_shard": i["shard"]} for i in listing["items"]]
    items = [r for r in items if (type is None or r["type"] == type) and (status is None or r["status"] == status)]
    items.sort(key=lambda r: r["created_at"])
    return {"count": len(items), "items": items, "unavailable_shards": listing["unavailable_shards"]}


@app.get("/resources/{resource_id}", responses=errors(e404="resource:res-water-747383d2ea49 not found", e503=STORAGE_DOWN))
async def get_resource(resource_id: str):
    resource_id = clean_id(resource_id)
    return with_meta(await store.get(resource_key(resource_id)))


@app.put("/resources/{resource_id}/status", responses=errors(
    e400="a resource with availability 0 cannot be 'available'", e404="resource:res-water-747383d2ea49 not found",
    e409="too much contention updating resource; retry", e503=STORAGE_DOWN))
async def update_resource_status(resource_id: str, body: ResourceStatusUpdate):
    resource_id = clean_id(resource_id)
    if body.status is None and body.availability is None:
        raise HTTPException(400, "provide status and/or availability")
    key = resource_key(resource_id)
    for _ in range(5):
        item = await store.get(key)
        res = item["value"]
        if body.availability is not None:
            res["availability"] = body.availability
        if body.status is not None:
            res["status"] = body.status
        elif res["status"] != "offline":
            res["status"] = "available" if res["availability"] > 0 else "busy"
        if res["status"] == "available" and res["availability"] <= 0:
            raise HTTPException(400, "a resource with availability 0 cannot be 'available'")
        res["updated_at"] = now_iso()
        try:
            await store.put(key, res, if_version=item["version"])
            return res
        except Conflict:
            continue
    raise HTTPException(409, "too much contention updating resource; retry")


# --------------------------------------------------------------------------- matching
@app.post("/match/{request_id}", responses=errors(
    e404="request req-87f53d097d08 not found", e409="no suitable available resource",
    e503="matching service unavailable"))
async def match(request_id: str):
    request_id = clean_id(request_id)
    try:
        r = await http.post(f"{MATCHING_URL}/match/{request_id}")
    except httpx.HTTPError as e:
        raise HTTPException(503, f"matching service unavailable: {e!r}")
    try:
        content = r.json()
    except ValueError:   # e.g. a proxy/crash page instead of the matching service's JSON
        content = None
    if r.status_code >= 500 or content is None:
        raise HTTPException(503, f"matching service error (HTTP {r.status_code}): "
                                 f"{content if content is not None else r.text[:200]}")
    return JSONResponse(status_code=r.status_code, content=content)


# --------------------------------------------------------------------------- ops
@app.get("/cluster", responses=errors(e503="coordinator unavailable"))
async def cluster():
    """Convenience passthrough of the coordinator's view of the storage layer."""
    try:
        r = await http.get(f"{store.base_url}/cluster")
        return r.json()
    except httpx.HTTPError as e:
        raise HTTPException(503, f"coordinator unavailable: {e!r}")


@app.get("/health")
async def health():
    return {"status": "ok", "instance": INSTANCE, "broker_connected": broker["exchange"] is not None}
