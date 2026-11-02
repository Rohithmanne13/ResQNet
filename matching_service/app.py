"""Matching service: consumes request messages from RabbitMQ and assigns resources.

Also exposes POST /match/{request_id} for synchronous (on-demand) matching,
which the Request Service proxies for the client API.

Background loops (every replica runs them; all their writes are compare-and-set):
* sweeper    - re-queues pending requests that saw no activity (stored while RabbitMQ was down,
               or message lost) and revives "unmatched" requests once a suitable resource exists.
* reconciler - repairs request/resource assignments left inconsistent by a crash (capacity leaks).
On shutdown (SIGTERM: scale-in, pod deletion) the consumer is cancelled first and in-flight
messages are finished and acknowledged before the process exits.
"""
import asyncio
import json
import os
import time
from collections import Counter
from contextlib import asynccontextmanager
from datetime import datetime, timezone

import aio_pika
from fastapi import FastAPI, HTTPException

from common import messaging
from common.logging_setup import setup
from common.models import TERMINAL_REQUEST_STATUSES, now_iso, resource_prefix
from common.storage_client import Conflict, StorageClient, StorageError

from .matcher import WORKER_ID, match_request, rank_candidates, record_attempt
from .reconciler import Reconciler

PREFETCH = int(os.getenv("PREFETCH", "2"))
RETRY_DELAY_MS = int(os.getenv("RETRY_DELAY_MS", "5000"))
MAX_ATTEMPTS = int(os.getenv("MAX_MATCH_ATTEMPTS", "12"))
MATCH_DELAY_MS = int(os.getenv("MATCH_DELAY_MS", "0"))
STALE_PENDING_SECONDS = float(os.getenv("STALE_PENDING_SECONDS", "60"))
SWEEP_INTERVAL = float(os.getenv("SWEEP_INTERVAL", "30"))
RECONCILE_INTERVAL = float(os.getenv("RECONCILE_INTERVAL", "30"))
RECONCILE_GRACE = float(os.getenv("RECONCILE_GRACE", "60"))
SHUTDOWN_GRACE = float(os.getenv("SHUTDOWN_GRACE", "20"))

log = setup(f"matching-{WORKER_ID}")
store = StorageClient()
counters: Counter = Counter()
broker = {"connected": False, "exchange": None, "retry_exchange": None, "queue": None, "consumer_tag": None}
inflight = {"n": 0, "stopping": False}


async def handle(message: aio_pika.abc.AbstractIncomingMessage) -> None:
    if inflight["stopping"]:
        await message.reject(requeue=True)   # shutting down: hand it back to another replica
        return
    inflight["n"] += 1
    try:
        await _handle(message)
    finally:
        inflight["n"] -= 1


async def _handle(message: aio_pika.abc.AbstractIncomingMessage) -> None:
    try:
        body = json.loads(message.body)
        request_id, attempt = body["request_id"], int(body.get("attempt", 0))
    except (ValueError, KeyError) as e:
        log.error("dropping malformed message %r: %r", message.body[:200], e)
        await message.reject(requeue=False)
        return

    counters["consumed"] += 1
    log.info("consumed %s (routing=%s attempt=%d)", request_id, message.routing_key, attempt)
    if MATCH_DELAY_MS:
        await asyncio.sleep(MATCH_DELAY_MS / 1000)
    reason = None
    try:
        result = await match_request(store, request_id, source="queue")
        counters[result["result"]] += 1
        if result["result"] == "no_match":
            reason = "no suitable resource available"
    except StorageError as e:
        counters["storage_errors"] += 1
        reason = f"storage error: {e}"
        log.warning("storage error matching %s: %s", request_id, e)
    except Exception as e:  # never lose the message on unexpected errors
        counters["errors"] += 1
        reason = f"error: {e!r}"
        log.exception("unexpected error matching %s", request_id)

    if reason:
        next_attempt = attempt + 1
        give_up = next_attempt >= MAX_ATTEMPTS
        await record_attempt(store, request_id, next_attempt, give_up, reason)
        if give_up:
            counters["gave_up"] += 1
            log.warning("giving up auto-matching %s after %d attempts (%s)", request_id, next_attempt, reason)
        else:
            counters["retried"] += 1
            await broker["retry_exchange"].publish(
                messaging.make_message(json.dumps({"request_id": request_id, "attempt": next_attempt}).encode()),
                routing_key=messaging.RK_RETRY)
            log.info("%s -> retry queue (attempt %d/%d in %dms): %s",
                     request_id, next_attempt, MAX_ATTEMPTS, RETRY_DELAY_MS, reason)
    await message.ack()


async def broker_setup(channel) -> None:
    await channel.set_qos(prefetch_count=PREFETCH)
    broker["exchange"], queue = await messaging.declare_main(channel)
    broker["retry_exchange"] = await messaging.declare_retry(channel, RETRY_DELAY_MS)
    broker["queue"], broker["consumer_tag"] = queue, await queue.consume(handle)
    broker["connected"] = True
    log.info("consuming from '%s' (prefetch=%d)", messaging.QUEUE, PREFETCH)


def broker_lost() -> None:
    broker.update(connected=False, exchange=None, retry_exchange=None, queue=None, consumer_tag=None)


async def requeue(request_id: str, attempt: int = 0) -> bool:
    if not broker["connected"]:
        return False   # the sweeper re-queues it once the broker is back
    try:
        await broker["exchange"].publish(
            messaging.make_message(json.dumps({"request_id": request_id, "attempt": attempt}).encode()),
            routing_key=messaging.RK_REQUEUED)
        return True
    except Exception as e:
        log.warning("could not re-queue %s: %r", request_id, e)
        return False


async def sweep_stale_pending() -> None:
    """Safety net: a request stored while RabbitMQ was down (or whose message was lost
    in a broker crash) would otherwise stay 'pending' forever. Re-queue pending requests
    that have seen no activity for STALE_PENDING_SECONDS. Also revive 'unmatched' requests
    (auto-matching gave up) as soon as a suitable resource exists. Duplicate messages are
    harmless because matching is idempotent (compare-and-set on request and resource)."""
    while True:
        await asyncio.sleep(SWEEP_INTERVAL)
        if not broker["connected"]:
            continue
        try:
            await sweep_once()
        except Exception as e:
            log.warning("sweeper failed: %r", e)


async def sweep_once() -> None:
    listing = await store.list("request:")
    now = datetime.now(timezone.utc)
    resources_by_type: dict[str, list[dict]] = {}
    for item in listing["items"]:
        req = item["value"]
        if not req.get("auto_match", True) or req["status"] not in ("pending", "unmatched"):
            continue
        if req["status"] == "pending":
            age = (now - datetime.fromisoformat(req["updated_at"])).total_seconds()
            if age <= STALE_PENDING_SECONDS:
                continue
            why = f"stale pending request (idle {age:.0f}s)"
        else:
            if req["type"] not in resources_by_type:
                res = await store.list(resource_prefix(req["type"]))
                resources_by_type[req["type"]] = [i["value"] for i in res["items"]]
            if not rank_candidates(req, resources_by_type[req["type"]]):
                continue
            why = "unmatched request: a suitable resource is now available"
            ts = now_iso()
            req.update(status="pending", match_attempts=0, last_match_error=None)
            req.setdefault("history", []).append({"status": "pending", "at": ts, "by": WORKER_ID,
                                                  "note": "sweeper: resource now available"})
        # Claim it with compare-and-set so only one matching replica re-queues it.
        req["updated_at"] = now_iso()
        try:
            await store.put(item["key"], req, if_version=item["version"])
        except Conflict:
            continue
        await requeue(req["id"], req.get("match_attempts", 0))
        counters["swept" if "stale" in why else "revived_unmatched"] += 1
        log.warning("sweeper: re-queued %s: %s", req["id"], why)


async def reconcile_loop() -> None:
    reconciler = Reconciler(store, RECONCILE_GRACE, requeue, WORKER_ID)
    while True:
        await asyncio.sleep(RECONCILE_INTERVAL)
        try:
            result = await reconciler.run_once()
            for kind, *_ in result["fixed"]:
                counters[f"reconciled_{kind}"] += 1
        except Exception as e:
            log.warning("reconciler failed: %r", e)


async def drain() -> None:
    """Graceful shutdown: stop taking messages, then let in-flight ones finish and be acked."""
    inflight["stopping"] = True
    if broker["queue"] is not None and broker["consumer_tag"]:
        try:
            await broker["queue"].cancel(broker["consumer_tag"])
        except Exception as e:
            log.warning("could not cancel consumer: %r", e)
    deadline = time.time() + SHUTDOWN_GRACE
    while inflight["n"] and time.time() < deadline:
        await asyncio.sleep(0.1)
    log.info("shutdown: consumer cancelled, %d message(s) still in flight", inflight["n"])


@asynccontextmanager
async def lifespan(_: FastAPI):
    tasks = [asyncio.create_task(messaging.maintain_connection(broker_setup, broker_lost)),
             asyncio.create_task(sweep_stale_pending()), asyncio.create_task(reconcile_loop())]
    yield
    await drain()
    for t in tasks:
        t.cancel()
    await store.close()


app = FastAPI(title="ERMS Matching Service", lifespan=lifespan)


@app.post("/match/{request_id}")
async def match_now(request_id: str):
    try:
        result = await match_request(store, request_id, source="api")
    except StorageError as e:
        raise HTTPException(e.status_code, str(e))
    counters[f"api_{result['result']}"] += 1
    if result["result"] == "not_found":
        raise HTTPException(404, f"request {request_id} not found")
    if result["result"] == "no_match":
        raise HTTPException(409, {"message": "no suitable available resource", **result})
    if result["result"] == "skipped" and result["status"] in TERMINAL_REQUEST_STATUSES:
        raise HTTPException(409, {"message": f"request is already {result['status']}", **result})
    return result


@app.get("/stats")
def get_stats():
    return {"worker": WORKER_ID, "broker_connected": broker["connected"], "counters": dict(counters)}


@app.get("/health")
def health():
    return {"status": "ok", "worker": WORKER_ID, "broker_connected": broker["connected"]}
