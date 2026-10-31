# Distributed Emergency Resource Matching System (ERMS)

Connects people who need emergency help (water, food, medicine, transportation, shelter, rescue) with
available resources / volunteers / NGOs, using sharded + replicated storage, asynchronous matching
over RabbitMQ, Docker and Kubernetes.

```
                         ┌─────────────────────┐   POST /match/{id} (sync)    ┌──────────────────────┐
 client ── REST/JSON ──▶ │   request-service   │ ───────────────────────────▶ │   matching-service   │ (N replicas)
                         │ (N replicas, :8080) │                              │  consumer + /match   │
                         └───┬─────────────┬───┘                              └──┬────────────┬──────┘
                             │ publish     │ storage ops                         │ consume    │ storage ops
                             ▼             ▼                                      │            ▼
                ┌───────────────────────────────┐            ┌──────────────────┴──┐  ┌─────────────────┐
                │ RabbitMQ                      │ ─────────▶ │ matching.requests   │  │   coordinator   │ (1)
                │ erms.requests (topic)         │            │ matching.retry (TTL)│  │ hash(key) mod 3 │
                │ erms.retry    (topic)         │ ◀────────  └─────────────────────┘  │ heartbeats      │
                └───────────────────────────────┘                                     └──┬─────┬─────┬──┘
                                                                     register/heartbeat ▲│     │     │
                                                         ┌───────────────────────────────┘▼     ▼     ▼
                                                         │  shard 0          shard 1          shard 2
                                                         └─ storage-0a/0b   storage-1a/1b    storage-2a/2b   (SQLite each)
```

| Component | What it does |
|---|---|
| **request-service** | Client REST API. Validates input, stores requests/resources through the coordinator, publishes new requests to RabbitMQ, proxies `POST /match` to the matching service. Stateless → horizontally scalable. |
| **matching-service** | Consumes `matching.requests`, finds and assigns the best resource, re-tries unmatched requests through a delayed retry queue. Also serves synchronous `POST /match/{id}`. Stateless → scalable. |
| **coordinator** (exactly 1) | Node registry (register + heartbeat), failure detection, routes every storage operation to the right shard and its replicas. Clients never know where data lives. |
| **storage nodes** (3 shards × 2 replicas) | Versioned key/value store on SQLite. Heartbeat to the coordinator; resync from a peer after (re)joining. |
| **RabbitMQ** | Asynchronous hand-off between request processing and matching. |

## Quick start (Docker Compose)

```bash
docker compose up -d --build            # 12 containers
open http://localhost:8080/docs         # Swagger UI of the client API
./scripts/demo.sh                       # guided walkthrough of every required demonstration (AUTO=1 = no pauses)
```

| URL | |
|---|---|
| http://localhost:8080 | client API (request-service) — `/docs` for Swagger |
| http://localhost:8001/cluster | coordinator: shards, nodes, status, key counts, read stats |
| http://localhost:15672 | RabbitMQ management (guest / guest) |

## Kubernetes

Works with minikube, kind or Docker Desktop Kubernetes.

```bash
minikube start --driver=docker --cpus=4 --memory=4096
minikube addons enable metrics-server                       # only needed for the optional HPA
./k8s/deploy.sh                                             # builds images, loads them into the cluster, applies manifests
./scripts/k8s-demo.sh                                       # pod recovery, scaling, storage failure on k8s
```

On minikube the services are exposed as NodePorts on `$(minikube ip)`: API `:30080` (`/docs`), coordinator
`:30081/cluster`, RabbitMQ UI `:30672`. NodePorts keep working while pods are killed and replaced; `kubectl port-forward`
does not (it binds to one pod). On kind / Docker Desktop use `kubectl -n erms port-forward svc/request-service 8080:8000` etc.

| Manifest | Objects |
|---|---|
| `k8s/10-rabbitmq.yaml` | Deployment + Service |
| `k8s/20-coordinator.yaml` | Deployment (replicas: 1, Recreate) + NodePort Service (30081) |
| `k8s/30-storage.yaml` | 3 StatefulSets `storage-shard-{0,1,2}` × 2 pods + headless Services |
| `k8s/40-matching-service.yaml` | Deployment (2 replicas) + Service |
| `k8s/50-request-service.yaml` | Deployment (2 replicas) + NodePort Service (30080) |
| `k8s/60-hpa-optional.yaml` | optional HPA for matching-service (needs metrics-server) |

All pods have readiness/liveness probes on `/health`. Storage pods take their identity from the downward
API (`NODE_ID` = pod name, `NODE_URL` = pod IP), so a recreated pod re-registers with its new IP.

Useful commands for the demo:
```bash
kubectl -n erms delete pod -l app=request-service --wait=false   # automatic recovery
kubectl -n erms scale deploy/matching-service --replicas=4       # scaling
kubectl -n erms scale sts/storage-shard-1 --replicas=1           # storage node failure (removes storage-shard-1-1)
kubectl -n erms scale sts/storage-shard-1 --replicas=2           # recovery: new pod starts empty, resyncs from peer
kubectl -n erms delete pod storage-shard-0-0                     # also works: recreated empty, full resync
```

## Client REST API

| Method | Endpoint | Purpose | Success |
|---|---|---|---|
| POST | `/requests` | create request (`type`, `location{lat,lon,area?}`, `details`, `priority`, `auto_match`) | 201 |
| GET | `/requests/{id}` | retrieve request (includes `_storage.shard` / `served_by`) | 200 |
| GET | `/requests?status=&type=` | list requests | 200 |
| PUT | `/requests/{id}/status` | update status (`{"status": "...", "note": "..."}`) | 200 |
| POST | `/resources` | register resource/volunteer (`type`, `location`, `availability`, `name`, `kind`) | 201 |
| GET | `/resources?type=&status=` | list resources | 200 |
| GET | `/resources/{id}` | get resource | 200 |
| PUT | `/resources/{id}/status` | update `status` and/or `availability` | 200 |
| POST | `/match/{requestId}` | match now (idempotent: an already matched request returns `result: skipped`) | 200 |

Errors: `422` invalid input, `404` unknown id, `409` invalid state change / no suitable resource /
contention, `503` storage shard or a dependent service unavailable.

```bash
curl -X POST localhost:8080/requests -H 'Content-Type: application/json' -d '{
  "type": "water", "details": "Family of 4 stranded by flooding",
  "location": {"lat": 12.9716, "lon": 77.5946, "area": "Shivajinagar"}, "priority": "high"}'
curl -X POST localhost:8080/resources -H 'Content-Type: application/json' -d '{
  "type": "water", "kind": "volunteer", "name": "Ravi (tanker)", "availability": 2,
  "location": {"lat": 12.978, "lon": 77.599}}'
```

**Request lifecycle:** `pending → matched → in_progress → resolved`; any non-terminal → `cancelled`;
`pending → unmatched` after `MAX_MATCH_ATTEMPTS` failed automatic attempts; setting `pending` again re-opens and
re-queues it. `resolved`/`cancelled` (or re-opening) gives the capacity back to the assigned resource.

**Resource:** `availability` = how many requests it can serve at once. Each assignment takes one unit;
at 0 the status becomes `busy`, and it returns to `available` when a request is released. `offline` resources are never matched.

## Internal API

Coordinator (used by the services) and storage nodes (used by the coordinator) expose the same data interface:

| Method | Endpoint | Purpose |
|---|---|---|
| POST | `/internal/nodes/register` | storage node registers (coordinator replies with peers to sync from) |
| POST | `/internal/nodes/heartbeat` | every 2 s; reply may say `resync: true` |
| PUT | `/internal/data/{key}?if_version=N` | store/update; `if_version` = compare-and-set (0 = create only) |
| GET | `/internal/data/{key}` | retrieve |
| DELETE | `/internal/data/{key}` | delete (tombstone) |
| GET | `/internal/data?prefix=` | scatter-gather scan over all shards |
| GET | `/internal/snapshot` (nodes) | full dump incl. tombstones, used for recovery sync |
| GET | `/cluster`, `/internal/shard-of/{key}` (coordinator) | observability |

## Design decisions (section 12 of the spec)

* **Language/framework:** Python 3.12 + FastAPI (async; good for I/O-bound fan-out to replicas), httpx, aio-pika.
* **Shard storage:** SQLite per node (durable, zero-ops, transactional). Each record keeps a `version` and a `deleted` flag.
* **Partitioning:** `shard = sha256(key) mod 3`. Python's `hash()` is randomised per process, so a stable hash is
  required for every service to agree. Keys are `request:<id>` / `resource:<id>`; ids are random UUID fragments, so load spreads evenly (unit-tested).
* **Replication:** the coordinator writes to **all live replicas** of the shard in parallel; the write succeeds if at least one
  *ready* replica acknowledges. A replica that fails a write is marked down **immediately**, so it is never read while stale.
  Reads go to the first ready replica, failing over to the next.
* **Versioning / consistency:** the coordinator assigns a monotonic version (ns timestamp, never repeating). Nodes apply
  a write only if its version is newer (last-writer-wins). This makes replication, retries and recovery merges idempotent
  and order-independent. Concurrency control is optimistic: `if_version` compare-and-set, serialised per key in the
  (single) coordinator.
* **Failure detection:** heartbeats every 2 s; no heartbeat for 6 s → `down`. Failed requests to a node also mark it down instantly.
* **Recovery / synchronisation:** a node that registers, or heartbeats after being marked down, enters `syncing`:
  it pulls a snapshot (including tombstones) from a ready peer and merges it by version, then reports `ready`. While
  `syncing` it already receives new writes (so it doesn't miss anything) but is not used for reads. This covers both
  a restarted container (stale SQLite file, only the missed updates are applied) and a new pod with an empty disk (full copy).
* **Matching strategy:** candidates must have the same type, `status=available`, spare capacity and be within
  `MAX_MATCH_DISTANCE_KM` (50 km, haversine distance). Ranked nearest first, then by most spare capacity. The resource is claimed
  with compare-and-set **before** the request is marked matched; if the request update then conflicts, the
  claim is rolled back. So multiple matching replicas can never over-allocate a resource (tested with 20 concurrent requests
  against 12 units of capacity).
* **RabbitMQ topology:** topic exchange `erms.requests` → durable queue `matching.requests` (persistent messages,
  manual ack, prefetch 2 per worker → competing consumers load-balance). Unmatched requests go to `erms.retry` →
  `matching.retry`, which has a message TTL (5 s) and dead-letters back into `erms.requests`. This gives a delayed retry
  without blocking a worker. After 12 attempts the request becomes `unmatched`.
* **Broker failures:** services run their own reconnect loop that re-declares the whole topology and consumers on every
  connect. In testing, aio-pika's automatic "robust" restore re-created the exchanges but **not** the queues after a broker
  restart, so we don't rely on it. A request accepted while the broker is down is still stored (`queued: false`). A sweeper in
  the matching service re-queues auto-match requests idle for more than `STALE_PENDING_SECONDS`, claiming each with compare-and-set
  so only one replica publishes it.
* **Microservice boundaries:** client API / matching / storage coordination are separate so the stateless parts scale
  independently of the stateful storage tier.
* **Monitoring / logging:** structured log lines per service (`NODE DOWN`, `SYNC complete`, `MATCHED`, …), `/cluster`
  (node states, key counts per replica, read distribution, failovers), `/stats` on matching workers, RabbitMQ management UI.

Main tunables (env vars): `HEARTBEAT_INTERVAL`, `HEARTBEAT_TIMEOUT`, `MAX_MATCH_DISTANCE_KM`, `RETRY_DELAY_MS`,
`MAX_MATCH_ATTEMPTS`, `PREFETCH`, `STALE_PENDING_SECONDS`, `SWEEP_INTERVAL`, `MATCH_DELAY_MS` (artificial work time
per match, set to 1.5 s in the deployments so queueing is visible in the demo; set to 0 for real use).

## Testing

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt pytest pyyaml
.venv/bin/python -m pytest tests/unit                 # pure logic: hashing, distance, ranking, LWW merge
docker compose up -d --build
.venv/bin/python -m pytest -v tests/integration       # end-to-end incl. node + broker failures (~70 s)
```
(On Python 3.13+ locally, install unpinned packages: `pip install fastapi uvicorn httpx aio-pika pydantic pytest`.)

Against Kubernetes (minikube):
```bash
IP=$(minikube ip); API_URL=http://$IP:30080 COORD_URL=http://$IP:30081 \
FAIL_REPLICA=last STOP_CMD="kubectl -n erms scale sts {statefulset} --replicas=1" \
START_CMD="kubectl -n erms scale sts {statefulset} --replicas=2" \
BROKER_STOP_CMD="kubectl -n erms scale deploy rabbitmq --replicas=0" \
BROKER_START_CMD="kubectl -n erms scale deploy rabbitmq --replicas=1" \
.venv/bin/python -m pytest -v tests/integration
```

| Spec requirement (§11) | Test |
|---|---|
| request creation, retrieval, status updates | `test_create_and_get_request`, `test_status_lifecycle_releases_resource`, `test_validation_and_not_found` |
| resource registration & availability updates | `test_register_and_list_resources`, `test_status_lifecycle_releases_resource` |
| correct matching | `test_manual_match_picks_nearest_resource`, `test_rank_*` (unit), `test_no_match_returns_409` |
| multiple simultaneous requests | `test_many_concurrent_requests_no_overallocation` |
| RabbitMQ delivery & processing | `test_async_matching_through_rabbitmq`, `test_request_waits_in_retry_queue_until_resource_appears`, `test_broker_outage_requests_are_not_lost` |
| shard selection & distribution | `test_data_distributed_across_three_shards`, `test_hash_distribution_roughly_even` |
| replica availability during failure; recovery & sync | `test_storage_node_failure_and_recovery`, `test_store_merge_snapshot_is_idempotent` |
| k8s pod recovery & scaling | `scripts/k8s-demo.sh` |

## Required demonstration → where

| Demo item (§10) | `scripts/demo.sh` step |
|---|---|
| Request creation | 1 |
| Resource registration | 2 |
| Resource matching | 3 |
| Data distribution | 4 (+ `/cluster` key counts) |
| Asynchronous processing | 5 (10 simultaneous requests, queue depths, which worker matched each) |
| Storage failure | 6 (`docker compose stop storage-Xa`; reads fail over, writes continue) |
| Node recovery | 7 (`start`; log shows `SYNC complete … applied N updates`) |
| Kubernetes recovery & scaling | `scripts/k8s-demo.sh` |

## Layout

```
common/            shared code: hashing, storage client, RabbitMQ topology, resource capacity ops, models
coordinator/       coordinator service + Dockerfile
storage_node/      storage node service + Dockerfile
request_service/   client API + Dockerfile
matching_service/  matcher + consumer + Dockerfile
k8s/               manifests + deploy.sh
scripts/           demo.sh, k8s-demo.sh
tests/             unit/ and integration/
```
