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
| **storage nodes** (3 shards × 2 replicas) | Versioned key/value store on SQLite (persistent volume each). Heartbeat to the coordinator; resync from peers after (re)joining. |
| **RabbitMQ** | Asynchronous hand-off between request processing and matching. Durable queues + persistent messages on a persistent volume; credentials from a Secret / `.env`. |

## Quick start (Docker Compose)

```bash
./scripts/init-env.sh                   # once: .env with a random RabbitMQ password (git-ignored)
docker compose up -d --build            # 12 containers
open http://localhost:8080/docs         # Swagger UI of the client API
./scripts/demo.sh                       # guided walkthrough of every required demonstration (AUTO=1 = no pauses)
```

| URL | |
|---|---|
| http://localhost:8080 | client API (request-service) — `/docs` for Swagger |
| http://localhost:8001/cluster | coordinator: shards, nodes, status, key counts, read stats |
| http://localhost:15672 | RabbitMQ management (user `erms`, password: `COMPOSE=1 ./scripts/rabbit-password.sh`) |

## Kubernetes

Works with minikube, kind or Docker Desktop Kubernetes.

```bash
minikube start --driver=docker --cpus=4 --memory=4096
minikube addons enable metrics-server                       # only needed for the optional HPA
./k8s/deploy.sh                                             # builds images, loads them into the cluster, applies manifests
./scripts/k8s-demo.sh                                       # pod recovery, scaling, storage failure on k8s
python3 scripts/expose.py                                   # WSL2: open the NodePorts in the Windows browser as http://localhost:30080/docs etc.
```

On minikube the services are exposed as NodePorts on `$(minikube ip)`: API `:30080` (`/docs`), coordinator
`:30081/cluster`, RabbitMQ UI `:30672` (user `erms`, password: `./scripts/rabbit-password.sh`). NodePorts keep working while pods are killed and replaced; `kubectl port-forward`
does not (it binds to one pod). On kind / Docker Desktop use `kubectl -n erms port-forward svc/request-service 8080:8000` etc.

| Manifest | Objects |
|---|---|
| `k8s/10-rabbitmq.yaml` | StatefulSet (1 replica, persistent volume) + headless Service + NodePort Service (30672); credentials from Secret `rabbitmq-credentials` (created by `deploy.sh`) |
| `k8s/20-coordinator.yaml` | Deployment (replicas: 1, Recreate) + NodePort Service (30081) |
| `k8s/30-storage.yaml` | 3 StatefulSets `storage-shard-{0,1,2}` × 2 pods, one persistent volume (PVC) per pod + headless Services |
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
kubectl -n erms scale sts/storage-shard-1 --replicas=2           # recovery: pod reattaches its volume, catches up from peer
kubectl -n erms delete pod storage-shard-0-0                     # also works: recreated with the same volume, catches up
kubectl -n erms delete pod -l app=coordinator                    # coordinator restart: replicas wait for each other, merge
```

## Client REST API

| Method | Endpoint | Purpose | Success |
|---|---|---|---|
| POST | `/requests` | create request (`type`, `location{lat,lon,area?}`, `details`, `priority`, `auto_match`) | 201 |
| GET | `/requests/{id}` | retrieve request (includes `_storage.shard` / `served_by`) | 200 |
| GET | `/requests?status=&type=` | list requests | 200 |
| PUT | `/requests/{id}/status` | update status (`{"status": "...", "note": "..."}`) | 200 |
| POST | `/resources` | register resource/volunteer (`type`, `location`, `availability`, `name`, `kind`); id = `res-<type>-<hex>` | 201 |
| GET | `/resources?type=&status=` | list resources | 200 |
| GET | `/resources/{id}` | get resource | 200 |
| PUT | `/resources/{id}/status` | update `status` and/or `availability` | 200 |
| POST | `/match/{requestId}` | match now (idempotent: an already matched request returns `result: skipped`) | 200 |

Errors: `422` invalid input, `404` unknown id, `409` invalid state change / no suitable resource /
contention, `503` storage shard or a dependent service unavailable (also when the matching service returns an
error page instead of JSON). Every endpoint documents its codes in Swagger. Ids are trimmed of surrounding spaces.
Write responses include `_storage.degraded: true` when a replica of the shard was down (acknowledged by fewer copies).

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
| POST | `/internal/nodes/register` | storage node registers (reports `max_version`); reply: `syncing` + peers to merge from, or `waiting` |
| POST | `/internal/nodes/heartbeat` | every 2 s (0.3 s while waiting); reply may say `resync: true` + peers |
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
  required for every service to agree. Keys are `request:<id>` / `resource:<type>:<id>`; ids are random UUID fragments,
  so load spreads evenly (unit-tested). Putting the (immutable) type in resource keys lets matching scan only resources of
  the requested type (an indexed key-range scan on each shard) instead of every resource.
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
  a restarted container and a recreated pod (both keep their disk — Compose named volume / Kubernetes PVC — so only the
  missed updates are applied) as well as a brand-new empty replica (full copy).
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
* **Edge cases handled** (each has a test):

  | Edge case | Handling | Tests |
  |---|---|---|
  | Coordinator restarts while a replica is stale (or every replica of a shard was down) | A replica that rejoins a shard with no ready copy is **not trusted alone**: it `waits` until the shard's other replicas are back (`REPLICAS_PER_SHARD`) or `BOOTSTRAP_GRACE` (8 s) passes, then all returning replicas merge each other's snapshots (LWW) before any serves reads | `test_coordinator_restart_never_serves_a_stale_replica` (Compose), `test_coordinator_restart_rebuilds_the_cluster_safely` (k8s), `test_first_returning_replica_is_not_trusted_alone` + 4 more (unit) |
  | Versions after a coordinator restart / clock skew | Nodes report their highest stored version; the coordinator never issues a lower one (LWW would silently drop such writes) | `test_versions_never_go_backwards_after_coordinator_restart` |
  | One-copy writes while a replica is down | Still accepted (availability), flagged `degraded`, fsynced to disk (`synchronous=FULL`), stored on a persistent volume; copied to the other replica when it rejoins. Losing both pods of a shard no longer loses data | `test_degraded_write_survives_losing_both_pods_of_a_shard` (k8s), `test_storage_node_failure_and_recovery` |
  | Capacity leak (crash between the request write and the resource release, or between claim and match) | **Reconciler** in every matching replica: finds orphan claims (released) and lost claims (re-claimed, or the request is re-opened and re-queued). Acts only after `RECONCILE_GRACE`, re-checks with fresh reads, skips rounds with an unavailable shard; all fixes are compare-and-set | `test_capacity_leak_is_reconciled`, `test_lost_claim_is_reconciled`, 5 unit tests |
  | Redelivered message after a matcher crash | Claims are idempotent per request; the matcher reuses a unit it already holds | `test_try_assign_is_idempotent_per_request`, `test_matcher_reuses_a_claim_it_already_holds` |
  | Full scan of all resources per match | Type-partitioned keys: one indexed range scan of the requested type | `test_matcher_scans_only_the_requested_type` |
  | `/match` returned 500 on a non-JSON error from the matching service | Mapped to `503` with the upstream status; 4xx passed through | `test_match_proxy_*` (unit), `test_queued_requests_survive_broker_restart_and_match_503_when_matching_down` |
  | Plain-text `guest:guest` credentials | Random password in a Kubernetes Secret (`secretKeyRef`) / git-ignored `.env`; `guest` is rejected | `test_rabbitmq_credentials_come_from_a_secret` |
  | Broker restart / pod deletion loses queued messages | RabbitMQ on a persistent volume with a stable node name (StatefulSet / fixed hostname); durable queues, persistent messages, publisher confirms. The sweeper remains as a second safety net | `test_queued_requests_survive_broker_restart_…`, `test_broker_outage_requests_are_not_lost` |
  | `unmatched` requests stayed unmatched even after a suitable resource appeared | The sweeper revives them as soon as a candidate exists | `test_unmatched_request_is_revived_when_a_resource_appears` |
  | Matching pod stopped mid-message (scale-in, deletion) | Graceful shutdown: cancel the consumer, finish in-flight messages (`SHUTDOWN_GRACE`), then exit | `test_scale_matching_service_to_4_…` (capacity exact after scale-in) |
  | Ids pasted with spaces; oversized text fields | Ids trimmed; length limits on text fields | `test_create_and_get_request`, `test_match_proxy_unreachable_is_503_and_ids_are_trimmed` |

* **Microservice boundaries:** client API / matching / storage coordination are separate so the stateless parts scale
  independently of the stateful storage tier.
* **Monitoring / logging:** structured log lines per service (`NODE DOWN`, `SYNC complete`, `MATCHED`, …), `/cluster`
  (node states, key counts per replica, read distribution, failovers), `/stats` on matching workers, RabbitMQ management UI.

Main tunables (env vars): `HEARTBEAT_INTERVAL`, `HEARTBEAT_TIMEOUT`, `REPLICAS_PER_SHARD`, `BOOTSTRAP_GRACE`,
`MAX_MATCH_DISTANCE_KM`, `RETRY_DELAY_MS`, `MAX_MATCH_ATTEMPTS`, `PREFETCH`, `STALE_PENDING_SECONDS`, `SWEEP_INTERVAL`,
`RECONCILE_INTERVAL`, `RECONCILE_GRACE`, `SHUTDOWN_GRACE`, `MATCH_DELAY_MS` (artificial work time
per match, set to 1.5 s in the deployments so queueing is visible in the demo; set to 0 for real use).

## Testing

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt pytest pyyaml
.venv/bin/python -m pytest tests/unit                 # logic + edge cases, no Docker needed (25 tests, ~3 s)
./scripts/init-env.sh && docker compose up -d --build
.venv/bin/python -m pytest -v tests/integration       # end-to-end incl. node/broker/coordinator failures (~4 min)
```
(On Python 3.13+ locally, install unpinned packages: `pip install fastapi uvicorn httpx aio-pika pydantic pytest`.)

Against Kubernetes (minikube):
```bash
IP=$(minikube ip)
export API_URL=http://$IP:30080 COORD_URL=http://$IP:30081 RABBIT_URL=http://$IP:30672 \
       RABBIT_USER=erms RABBIT_PASS=$(./scripts/rabbit-password.sh)
FAIL_REPLICA=last STOP_CMD="kubectl -n erms scale sts {statefulset} --replicas=1" \
START_CMD="kubectl -n erms scale sts {statefulset} --replicas=2" \
BROKER_STOP_CMD="kubectl -n erms scale sts rabbitmq --replicas=0" \
BROKER_START_CMD="kubectl -n erms scale sts rabbitmq --replicas=1" \
BROKER_RESTART_CMD="kubectl -n erms delete pod rabbitmq-0" \
MATCHING_STOP_CMD="kubectl -n erms scale deploy matching-service --replicas=0" \
MATCHING_START_CMD="kubectl -n erms scale deploy matching-service --replicas=2" \
.venv/bin/python -m pytest -v tests/integration      # 18 pass; the precise stale-replica test is Compose-only

# Kubernetes-only tests: pod deletion & recovery, scaling with continued availability, Secret credentials,
# losing both pods of a shard, coordinator restart (~2 min)
.venv/bin/python -m pytest -v -s tests/k8s
```
`tests/k8s` is skipped automatically when `kubectl` cannot reach the `erms` namespace.

| Spec requirement (§11) | Test |
|---|---|
| request creation, retrieval, status updates | `test_create_and_get_request`, `test_status_lifecycle_releases_resource`, `test_validation_and_not_found` |
| resource registration & availability updates | `test_register_and_list_resources`, `test_resource_availability_updates`, `test_status_lifecycle_releases_resource` |
| correct matching | `test_manual_match_picks_nearest_resource`, `test_rank_*` (unit), `test_no_match_returns_409` |
| multiple simultaneous requests | `test_many_concurrent_requests_no_overallocation` |
| RabbitMQ delivery & processing | `test_async_matching_through_rabbitmq`, `test_request_waits_in_retry_queue_until_resource_appears`, `test_broker_outage_requests_are_not_lost` |
| shard selection & distribution | `test_data_distributed_across_three_shards`, `test_hash_distribution_roughly_even` |
| replica availability during failure; recovery & sync | `test_storage_node_failure_and_recovery`, `test_store_merge_snapshot_is_idempotent` |
| edge cases (see *Edge cases handled*) | `tests/unit/test_edge_cases.py`, `test_capacity_leak_is_reconciled`, `test_lost_claim_is_reconciled`, `test_unmatched_request_is_revived_…`, `test_queued_requests_survive_broker_restart_…`, `test_coordinator_restart_never_serves_a_stale_replica`, k8s tests below |
| k8s pod recovery & scaling | `tests/k8s/test_k8s.py`: `test_request_service_pod_deleted_is_recreated_and_api_keeps_answering`, `test_storage_pod_deleted_fails_over_then_resyncs`, `test_scale_matching_service_to_4_spreads_work_and_stays_available` (also shown in `scripts/k8s-demo.sh`) |

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
| Kubernetes recovery & scaling | `scripts/k8s-demo.sh` (pod deletion; scaling 2 -> 4 -> 2 with a live availability probe; storage-shard-1 scale-down and `kubectl delete pod`, each with before / during / after reads) |

## Layout

```
common/            shared code: hashing, storage client, RabbitMQ topology, resource capacity ops, models
coordinator/       coordinator service + Dockerfile
storage_node/      storage node service + Dockerfile
request_service/   client API + Dockerfile
matching_service/  matcher + consumer + reconciler + Dockerfile
k8s/               manifests + deploy.sh
scripts/           demo.sh, k8s-demo.sh, init-env.sh, rabbit-password.sh, expose.py
tests/             unit/, integration/ and k8s/
```
