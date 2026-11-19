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
| **Prometheus + Grafana** (extension) | Every service exposes `/metrics`; Prometheus scrapes them every 5 s (plus RabbitMQ's built-in plugin); Grafana shows the live "ERMS overview" dashboard. See [Monitoring](#monitoring-prometheus--grafana). |
| **KEDA** (extension, Kubernetes) | Scales matching-service automatically (2–6 pods) on the number of messages waiting in RabbitMQ. See [Autoscaling](#autoscaling-on-queue-depth-keda). |

## Quick start (Docker Compose)

```bash
./scripts/init-env.sh                   # once: .env with random RabbitMQ / Grafana passwords (git-ignored)
docker compose up -d --build            # 13 containers (11 application + Prometheus + Grafana)
open http://localhost:8080/docs         # Swagger UI of the client API
./scripts/demo.sh                       # guided walkthrough of every required demonstration (AUTO=1 = no pauses)
```

| URL | |
|---|---|
| http://localhost:8080 | client API (request-service) — `/docs` for Swagger |
| http://localhost:8001/cluster | coordinator: shards, nodes, status, key counts, read stats |
| http://localhost:15672 | RabbitMQ management (user `erms`, password: `COMPOSE=1 ./scripts/rabbit-password.sh`) |
| http://localhost:3000 | Grafana, dashboard "ERMS overview" (no login needed to view; admin password: `COMPOSE=1 ./scripts/grafana-password.sh`) |
| http://localhost:9090 | Prometheus (targets: Status → Targets) |

## Kubernetes

Works with minikube, kind or Docker Desktop Kubernetes.

```bash
minikube start --driver=docker --cpus=4 --memory=4096
./k8s/deploy.sh                                             # builds images, installs KEDA, applies manifests (incl. monitoring)
./scripts/k8s-demo.sh                                       # pod recovery, scaling, storage failure on k8s
./scripts/keda-demo.sh                                      # 200 requests -> matching-service autoscales 2 -> 6 -> 2
python3 scripts/expose.py                                   # WSL2: open the NodePorts in the Windows browser as http://localhost:30080/docs etc.
```

On minikube the services are exposed as NodePorts on `$(minikube ip)`: API `:30080` (`/docs`), coordinator
`:30081/cluster`, RabbitMQ UI `:30672` (user `erms`, password: `./scripts/rabbit-password.sh`), Grafana `:30030`
(no login to view; admin password: `./scripts/grafana-password.sh`), Prometheus `:30090`. NodePorts keep working while pods are killed and replaced; `kubectl port-forward`
does not (it binds to one pod). On kind / Docker Desktop use `kubectl -n erms port-forward svc/request-service 8080:8000` etc.

| Manifest | Objects |
|---|---|
| `k8s/10-rabbitmq.yaml` | StatefulSet (1 replica, persistent volume) + headless Service + NodePort Service (30672); credentials from Secret `rabbitmq-credentials` (created by `deploy.sh`) |
| `k8s/20-coordinator.yaml` | Deployment (replicas: 1, Recreate) + NodePort Service (30081) |
| `k8s/30-storage.yaml` | 3 StatefulSets `storage-shard-{0,1,2}` × 2 pods, one persistent volume (PVC) per pod + headless Services |
| `k8s/40-matching-service.yaml` | Deployment (2 replicas; 2–6 when KEDA is installed) + Service |
| `k8s/50-request-service.yaml` | Deployment (2 replicas) + NodePort Service (30080) |
| `k8s/70-monitoring.yaml` | Prometheus (Deployment, ServiceAccount + read-only Role for pod discovery, NodePort 30090) and Grafana (Deployment, NodePort 30030); config from ConfigMaps built by `deploy.sh` out of `monitoring/` |
| `k8s/80-keda-autoscaling.yaml` | KEDA `ScaledObject` (matching-service on queue `matching.requests`, 2–6 pods) + `TriggerAuthentication` (broker URL from Secret `keda-rabbitmq`) |

All application pods carry `prometheus.io/scrape` annotations, so Prometheus finds pods created by scaling or recovery
within seconds. `deploy.sh` installs KEDA v2.21.0 once (namespace `keda`); `SKIP_KEDA=1 ./k8s/deploy.sh` deploys
without it (matching-service then stays at 2 pods). The former CPU-based HPA (`60-hpa-optional.yaml`) was removed: two
autoscalers on one Deployment fight each other.

All pods have readiness/liveness probes on `/health`. Storage pods take their identity from the downward
API (`NODE_ID` = pod name, `NODE_URL` = pod IP), so a recreated pod re-registers with its new IP.

Useful commands for the demo:
```bash
kubectl -n erms delete pod -l app=request-service --wait=false   # automatic recovery
./scripts/matching-replicas.sh 4                                # scaling (pins 4 pods; KEDA-aware)
./scripts/matching-replicas.sh auto                             # back to automatic scaling on queue depth
kubectl -n erms scale sts/storage-shard-1 --replicas=1           # storage node failure (removes storage-shard-1-1)
kubectl -n erms scale sts/storage-shard-1 --replicas=2           # recovery: pod reattaches its volume, catches up from peer
kubectl -n erms delete pod storage-shard-0-0                     # also works: recreated with the same volume, catches up
kubectl -n erms delete pod -l app=coordinator                    # coordinator restart: replicas wait for each other, merge
```

## Monitoring (Prometheus + Grafana)

Extension #39. Runs in both deployments: Compose (Grafana http://localhost:3000, Prometheus :9090) and Kubernetes
(NodePorts 30030 / 30090). Open Grafana and the **ERMS overview** dashboard is the home page; viewing needs no login.

| Dashboard section | Panels |
|---|---|
| Cluster health | healthy shards, storage nodes ready, matching pods, API pods, queue depth, read failovers; **storage node timeline** (green = ready, red = down); ready replicas per shard |
| Messaging and autoscaling | RabbitMQ queue depth (waiting / being processed / delayed retry); **matching pods vs queue depth** (KEDA); matches per second by pod; requests created vs matched |
| Latency and API | **time from request to match** (p50 / p95); API requests per second by HTTP status; API latency p95 by route; storage reads / writes per second |
| Storage failures and recovery | failovers, nodes marked down, degraded writes per minute; **last resync duration per node**; records per node; resyncs and missed writes applied; reconciler / sweeper fixes |

Metrics (all on `/metrics`, Prometheus text format):

| Service | Metrics |
|---|---|
| every service | `erms_http_requests_total{route,method,status}`, `erms_http_request_duration_seconds` (route = template, e.g. `/requests/{request_id}`) |
| request-service | `erms_requests_created_total{type}`, `erms_requests_published_total{result}`, `erms_request_status_changes_total`, `erms_resources_registered_total` |
| matching-service | `erms_matches_total{result,via}`, `erms_time_to_match_seconds` (creation → assignment), `erms_messages_consumed_total`, `erms_match_retries_total`, `erms_messages_inflight`, `erms_reconciler_fixes_total`, `erms_sweeper_requeued_total` |
| coordinator | `erms_storage_node_up{node,shard}`, `erms_storage_node_status`, `erms_shard_ready_replicas`, `erms_shard_healthy`, `erms_storage_node_keys`, `erms_storage_failovers_total`, `erms_storage_node_down_total`, `erms_storage_degraded_writes_total`, `erms_storage_reads_total`, `erms_storage_writes_total` |
| storage node | `erms_storage_sync_duration_seconds`, `erms_storage_last_sync_duration_seconds`, `erms_storage_syncs_total{result}`, `erms_storage_sync_records_applied_total`, `erms_storage_local_keys` |
| RabbitMQ | built-in `rabbitmq_prometheus` plugin: `rabbitmq_detailed_queue_messages_ready / _unacked / _consumers{queue}` |

The dashboard JSON is generated by `monitoring/grafana/build_dashboard.py`; edit the panels there and re-run it.
The test suite runs every panel query through Grafana and fails if any would show "No data".

## Autoscaling on queue depth (KEDA)

Extension #18, Kubernetes. KEDA checks the number of messages waiting in `matching.requests` every 5 s and drives a
HorizontalPodAutoscaler: **desired matching pods = ceil(waiting / 5)**, between 2 and 6. Scale-up is immediate; scale-down
waits 30 s after the queue drained (instead of the default 5 minutes), so a demo burst shows both directions.

```bash
./scripts/keda-demo.sh               # 200 requests: live table of queue depth / ready pods / desired pods, then summary
kubectl -n erms get scaledobject,hpa # the ScaledObject and the HPA KEDA created (keda-hpa-matching-service)
./scripts/matching-replicas.sh 4     # pin 4 pods (KEDA annotation autoscaling.keda.sh/paused-replicas=4)
./scripts/matching-replicas.sh 0     # stop matching completely (failure tests)
./scripts/matching-replicas.sh auto  # automatic again
```

A plain `kubectl scale deploy/matching-service` no longer sticks: KEDA's HPA owns the replica count and corrects it
within seconds (tested). KEDA connects to RabbitMQ with an amqp URL kept in Secret `keda-rabbitmq` (built by
`deploy.sh` from `rabbitmq-credentials`). Removing a pod is safe: matching workers finish their in-flight messages
on SIGTERM (graceful drain), so scale-down loses nothing (tested with 200 requests: all matched, capacity exact).
Docker Compose has no autoscaler; there the replica count is fixed (`deploy.replicas`).

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
  | Simultaneous `POST /match` calls competing for one resource were refused ("no suitable resource", 409) while units were still free: the claim gave up after 4 compare-and-set conflicts | A conflict only means another matcher got there first: re-read and retry with a short random back-off until it succeeds or capacity runs out (claim and release) | `test_contended_matches_never_report_false_no_capacity` (unit), `test_simultaneous_requests_never_double_assign` (spec) |
  | Simultaneous `POST /match` calls of the **same** request: a losing call released the unit the winner had just committed with (request `matched`, resource no longer listing it) | On a conflict the unit is released only if the request is not now using it; after committing, the matcher confirms it still holds the unit | `test_concurrent_matches_of_the_same_request_keep_its_unit` (unit), `test_simultaneous_requests_never_double_assign` (spec) |
  | Docker / host restart (e.g. WSL reboot): RabbitMQ was the only Compose service without a restart policy, so the system came back without a broker | `restart: unless-stopped` on RabbitMQ too (every Compose service now restarts by itself; Kubernetes StatefulSets always did) | tested by killing RabbitMQ inside its container: restarted in 2 s, services reconnected |
  | Ids pasted with spaces; oversized text fields | Ids trimmed; length limits on text fields | `test_create_and_get_request`, `test_match_proxy_unreachable_is_503_and_ids_are_trimmed` |

* **Microservice boundaries:** client API / matching / storage coordination are separate so the stateless parts scale
  independently of the stateful storage tier.
* **Monitoring / logging:** metrics in Prometheus format on `/metrics` of every service, scraped by Prometheus every 5 s
  and shown on a provisioned Grafana dashboard (see [Monitoring](#monitoring-prometheus--grafana)). Metrics are pull-based,
  so a crashed service simply shows as a down target, and Kubernetes pod discovery picks up new pods automatically.
  Plus structured log lines per service (`NODE DOWN`, `SYNC complete`, `MATCHED`, …), `/cluster` (node states, key
  counts per replica, read distribution, failovers), `/stats` on matching workers and the RabbitMQ management UI.
* **Autoscaling:** KEDA scales matching-service on RabbitMQ queue depth instead of CPU. A matching worker spends most of
  its time waiting on I/O, so CPU says little about the backlog; messages waiting per pod measure it directly (see
  [Autoscaling](#autoscaling-on-queue-depth-keda)).

Main tunables (env vars): `HEARTBEAT_INTERVAL`, `HEARTBEAT_TIMEOUT`, `REPLICAS_PER_SHARD`, `BOOTSTRAP_GRACE`,
`MAX_MATCH_DISTANCE_KM`, `RETRY_DELAY_MS`, `MAX_MATCH_ATTEMPTS`, `PREFETCH`, `STALE_PENDING_SECONDS`, `SWEEP_INTERVAL`,
`RECONCILE_INTERVAL`, `RECONCILE_GRACE`, `SHUTDOWN_GRACE`, `MATCH_DELAY_MS` (artificial work time
per match, set to 1.5 s in the deployments so queueing is visible in the demo; set to 0 for real use).

## Testing

```bash
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt pytest pyyaml
.venv/bin/python -m pytest tests/unit                 # logic, edge cases, metrics; no Docker needed (34 tests, ~10 s)
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
MATCHING_STOP_CMD="./scripts/matching-replicas.sh 0" \
MATCHING_START_CMD="./scripts/matching-replicas.sh auto" \
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

### Specification-compliance suite (every requirement of the PDF)

`tests/spec` checks **every mandatory requirement of "Emergency Response Matching.pdf"** against the running system.
`tests/spec/requirements.py` lists 104 numbered requirements (§1–§12). Each test is tagged with the ids it proves,
and a requirement that no test covers is reported as **NOT COVERED**. Every assertion records what was checked and
the value that was observed. At the end the suite prints a report: every check of every test (✔ / ✘), then the
whole PDF requirement by requirement with its verdict.

```bash
./scripts/spec-test.sh compose      # Docker Compose   (~3 min; stops/restarts storage nodes, matching, a whole shard)
./scripts/spec-test.sh k8s          # Kubernetes       (~11 min; also deletes and scales pods, KEDA burst test)
.venv/bin/python tests/spec/report.py   # merge both runs -> tests/spec/reports/spec-report-combined.md
```
Kubernetes-only requirements show as SKIP in the Compose run; the merged report combines both runs. Reports are
saved to `tests/spec/reports/spec-<target>.{json,md}`. The two extensions are tested by the same harness
(`tests/spec/test_x_extensions.py`, requirements `X39.*` / `X18.*`) and reported in their own sections; run only them
with `./scripts/spec-test.sh k8s -k "x39 or x18"`.

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
| Autoscaling (extension) | `scripts/keda-demo.sh` (200 requests: matching-service 2 -> 6 -> 2 by itself, live table of queue depth and pods) |
| Monitoring (extension) | Grafana "ERMS overview" during any of the demos: nodes turn red / green, queue depth, matching pods, time to match |

## Layout

```
common/            shared code: hashing, storage client, RabbitMQ topology, resource capacity ops, models
coordinator/       coordinator service + Dockerfile
storage_node/      storage node service + Dockerfile
request_service/   client API + Dockerfile
matching_service/  matcher + consumer + reconciler + Dockerfile
k8s/               manifests + deploy.sh (incl. monitoring 70-, KEDA 80-)
monitoring/        prometheus/ scrape configs (Compose, Kubernetes); grafana/ provisioning, dashboards, build_dashboard.py
scripts/           demo.sh, k8s-demo.sh, keda-demo.sh, matching-replicas.sh, spec-test.sh, init-env.sh,
                   rabbit-password.sh, grafana-password.sh, expose.py
tests/             unit/, integration/, k8s/ and spec/ (PDF requirements + extensions, with report)
```
