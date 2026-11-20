# ERMS - specification compliance report

* **compose** (API http://localhost:8080  coordinator http://localhost:8001  RabbitMQ http://localhost:15672  Grafana http://localhost:3000), 2026-11-20 16:54:24 -> 2026-11-20 16:59:53: 60 passed, 0 failed (12 Kubernetes-only tests not part of this run)

## Requirement matrix

### §1 Project objective and operations

| Verdict | Id | Requirement (PDF) | Verified by |
|---|---|---|---|
| **PASS** | 1.a | Connect people requiring emergency assistance with available volunteers and resources | `test_flood_victim_needs_drinking_water_end_to_end` [compose] |
| **PASS** | 1.b | Support requests such as food, water, medicine, transportation, or shelter | `test_supported_request_types` [compose] |
| **PASS** | 1.c | Match requests with suitable available resources based on resource type and location | `test_matching_considers_location` [compose]<br>`test_matching_considers_resource_type` [compose]<br>`test_supported_request_types` [compose] |
| **PASS** | 1.d | Demonstrate distributed storage, asynchronous communication, sharding, replication, fault tolerance, containerization and Kubernetes-based deployment | `test_components_run_as_containers` [compose]<br>`test_data_distributed_across_three_shards_and_nodes` [compose]<br>`test_multiple_requests_processed_through_rabbitmq` [compose]<br>`test_storage_node_failure_before_during_after` [compose] |
| **PASS** | 1.op1 | createRequest(type, location, details) - create an emergency request | `test_createRequest` [compose] |
| **PASS** | 1.op2 | registerResource(type, location, availability) - register a resource or volunteer | `test_registerResource` [compose] |
| **PASS** | 1.op3 | matchRequest(requestId) - find and assign a suitable available resource | `test_matchRequest` [compose] |
| **PASS** | 1.op4 | updateStatus(requestId, status) - update the status of a request | `test_updateStatus` [compose] |
| **PASS** | 1.op5 | getRequest(requestId) - retrieve request information | `test_getRequest` [compose] |

### §1.1 Example scenario

| Verdict | Id | Requirement (PDF) | Verified by |
|---|---|---|---|
| **PASS** | 1.1.a | Flood victim needing drinking water: request submitted, processed asynchronously, matched with an available resource, request status updated | `test_flood_victim_needs_drinking_water_end_to_end` [compose] |
| **PASS** | 1.1.b | Flow: User -> Request Service -> RabbitMQ -> Matching Service -> Distributed Storage -> Resource Assigned | `test_flood_victim_needs_drinking_water_end_to_end` [compose] |

### §2 System configuration (Table 1)

| Verdict | Id | Requirement (PDF) | Verified by |
|---|---|---|---|
| **PASS** | 2.coord | Coordinator: 1 | `test_exactly_one_coordinator` [compose] |
| **PASS** | 2.ms | Microservices: 2-3 | `test_two_to_three_microservices` [compose] |
| **PASS** | 2.mq | Message broker: 1 RabbitMQ instance | `test_one_rabbitmq_instance` [compose] |
| **PASS** | 2.shards | Storage shards: 3 | `test_three_shards_each_with_a_replica` [compose] |
| **PASS** | 2.repl | Replication: at least 1 replica per shard | `test_three_shards_each_with_a_replica` [compose] |
| **PASS** | 2.docker | Containerization: Docker - mandatory | `test_components_run_as_containers` [compose]<br>`test_docker_images_are_built` [compose]<br>`test_dockerfiles_for_every_service` [compose] |
| **K8S ONLY** | 2.k8s | Orchestration: Kubernetes - mandatory | - |
| **OPTIONAL** | 2.cloud | Cloud deployment: optional | - |
| **PASS** | 2.clients | Clients: 1 or more | `test_one_or_more_clients` [compose] |

### §3 Request and resource data

| Verdict | Id | Requirement (PDF) | Verified by |
|---|---|---|---|
| **PASS** | 3.maintain | The system maintains information about emergency requests and available resources | `test_system_maintains_requests_and_resources` [compose] |
| **PASS** | 3.1.id | Request has a unique request ID | `test_request_record_fields` [compose] |
| **PASS** | 3.1.type | Request has a request type | `test_request_record_fields` [compose] |
| **PASS** | 3.1.loc | Request has a location | `test_request_record_fields` [compose] |
| **PASS** | 3.1.details | Request has request details | `test_request_record_fields` [compose] |
| **PASS** | 3.1.status | Request has a current status | `test_request_record_fields` [compose] |
| **PASS** | 3.1.created | Request has a creation time | `test_request_record_fields` [compose] |
| **PASS** | 3.1.types | Request types include water, food, medicine, transportation and shelter | `test_supported_request_types` [compose] |
| **PASS** | 3.2.id | Resource has a unique resource ID | `test_resource_record_fields_and_kinds` [compose] |
| **PASS** | 3.2.type | Resource has a resource type | `test_resource_record_fields_and_kinds` [compose] |
| **PASS** | 3.2.loc | Resource has a location | `test_resource_record_fields_and_kinds` [compose] |
| **PASS** | 3.2.avail | Resource has an availability | `test_resource_availability_updates` [compose]<br>`test_resource_record_fields_and_kinds` [compose] |
| **PASS** | 3.2.status | Resource has a current status | `test_resource_availability_updates` [compose]<br>`test_resource_record_fields_and_kinds` [compose] |
| **PASS** | 3.2.kinds | A resource may be a physical resource, volunteer, NGO, or other source of assistance | `test_resource_record_fields_and_kinds` [compose] |

### §4 Distributed storage

| Verdict | Id | Requirement (PDF) | Verified by |
|---|---|---|---|
| **PASS** | 4.a | Requests and resources are stored across multiple storage nodes | `test_data_distributed_across_three_shards_and_nodes` [compose] |
| **PASS** | 4.b | Storage layer: one coordinator, three storage shards, at least one replica for each shard | `test_exactly_one_coordinator` [compose]<br>`test_three_shards_each_with_a_replica` [compose] |
| **PASS** | 4.c | The coordinator determines the appropriate shard and routes storage operations | `test_coordinator_selects_shard_by_hash_mod_3` [compose] |
| **PASS** | 4.d | Hash-based partitioning, e.g. hash(request_id) mod 3 | `test_coordinator_selects_shard_by_hash_mod_3` [compose] |
| **PASS** | 4.e | The client does not need to know the physical storage location of the data | `test_client_does_not_need_the_storage_location` [compose] |

### §5 Client REST API (Table 2)

| Verdict | Id | Requirement (PDF) | Verified by |
|---|---|---|---|
| **PASS** | 5.1 | POST /requests - create request - 201 | `test_client_api_success_codes` [compose]<br>`test_createRequest` [compose] |
| **PASS** | 5.2 | GET /requests/{id} - retrieve request - 200 | `test_client_api_success_codes` [compose]<br>`test_getRequest` [compose] |
| **PASS** | 5.3 | PUT /requests/{id}/status - update status - 200 | `test_client_api_success_codes` [compose]<br>`test_updateStatus` [compose] |
| **PASS** | 5.4 | POST /resources - register resource - 201 | `test_client_api_success_codes` [compose]<br>`test_registerResource` [compose] |
| **PASS** | 5.5 | GET /resources - list resources - 200 | `test_client_api_success_codes` [compose]<br>`test_system_maintains_requests_and_resources` [compose] |
| **PASS** | 5.6 | PUT /resources/{id}/status - update status - 200 | `test_client_api_success_codes` [compose]<br>`test_resource_availability_updates` [compose] |
| **PASS** | 5.7 | POST /match/{requestId} - match resource - 200 | `test_client_api_success_codes` [compose]<br>`test_matchRequest` [compose] |
| **PASS** | 5.json | Structured request and response data use JSON | `test_json_request_and_response_bodies` [compose] |
| **PASS** | 5.ok | Appropriate HTTP status codes for successful requests | `test_client_api_success_codes` [compose] |
| **PASS** | 5.400 | Appropriate HTTP status codes for invalid requests | `test_invalid_requests_get_4xx` [compose] |
| **PASS** | 5.404 | Appropriate HTTP status codes for missing resources | `test_missing_resources_get_404` [compose] |
| **PASS** | 5.503 | Appropriate HTTP status codes for unavailable services | `test_matching_service_down_503_and_queue_buffers` [compose]<br>`test_whole_shard_down_503_then_recovers` [compose] |

### §6 Internal service communication (Table 3)

| Verdict | Id | Requirement (PDF) | Verified by |
|---|---|---|---|
| **PASS** | 6.reg | POST /internal/nodes/register - register storage node | `test_internal_node_register` [compose] |
| **PASS** | 6.hb | POST /internal/nodes/heartbeat - report node availability | `test_internal_heartbeats_are_periodic` [compose] |
| **PASS** | 6.put | PUT /internal/data/{key} - store or update data | `test_internal_data_api` [compose] |
| **PASS** | 6.get | GET /internal/data/{key} - retrieve data | `test_internal_data_api` [compose] |
| **PASS** | 6.del | DELETE /internal/data/{key} - delete data | `test_internal_data_api` [compose] |
| **PASS** | 6.mq | RabbitMQ is used for asynchronous communication between request processing and matching | `test_flood_victim_needs_drinking_water_end_to_end` [compose]<br>`test_matching_service_down_503_and_queue_buffers` [compose]<br>`test_multiple_requests_processed_through_rabbitmq` [compose]<br>`test_one_rabbitmq_instance` [compose]<br>`test_rabbitmq_topology` [compose] |

### §7 Request processing

| Verdict | Id | Requirement (PDF) | Verified by |
|---|---|---|---|
| **PASS** | 7.1 | Step 1: the client submits an emergency request | `test_createRequest` [compose]<br>`test_flood_victim_needs_drinking_water_end_to_end` [compose] |
| **PASS** | 7.2 | Step 2: the Request Service stores the request | `test_flood_victim_needs_drinking_water_end_to_end` [compose] |
| **PASS** | 7.3 | Step 3: the request is published to RabbitMQ | `test_flood_victim_needs_drinking_water_end_to_end` [compose] |
| **PASS** | 7.4 | Step 4: the Matching Service consumes the message | `test_flood_victim_needs_drinking_water_end_to_end` [compose] |
| **PASS** | 7.5 | Step 5: a suitable available resource is identified | `test_flood_victim_needs_drinking_water_end_to_end` [compose]<br>`test_matchRequest` [compose]<br>`test_matching_uses_only_available_resources` [compose] |
| **PASS** | 7.6 | Step 6: the resource is assigned and the corresponding statuses are updated | `test_flood_victim_needs_drinking_water_end_to_end` [compose]<br>`test_matchRequest` [compose] |
| **PASS** | 7.type | The matching considers resource type | `test_matching_considers_resource_type` [compose] |
| **PASS** | 7.loc | The matching considers location | `test_matching_considers_location` [compose] |

### §8 Failure handling

| Verdict | Id | Requirement (PDF) | Verified by |
|---|---|---|---|
| **PASS** | 8.hb | Storage nodes periodically report their availability to the coordinator with heartbeats | `test_internal_heartbeats_are_periodic` [compose] |
| **PASS** | 8.detect | Detect unavailable storage nodes | `test_storage_node_failure_before_during_after` [compose]<br>`test_whole_shard_down_503_then_recovers` [compose] |
| **PASS** | 8.avoid | Avoid routing new requests to failed nodes | `test_storage_node_failure_before_during_after` [compose] |
| **PASS** | 8.replica | Use an available replica when the required data remains accessible | `test_storage_node_failure_before_during_after` [compose] |
| **PASS** | 8.continue | Continue processing when sufficient replicas are available | `test_storage_node_failure_before_during_after` [compose] |
| **PASS** | 8.sync | Synchronize the required data when a failed node recovers | `test_storage_node_failure_before_during_after` [compose] |
| **PASS** | 8.demo | Failure shown by stopping the container / pod and restarting it: before, during and after | `test_storage_node_failure_before_during_after` [compose] |

### §9.1 Docker (mandatory)

| Verdict | Id | Requirement (PDF) | Verified by |
|---|---|---|---|
| **PASS** | 9.1.a | Create Dockerfiles for the required services | `test_dockerfiles_for_every_service` [compose] |
| **PASS** | 9.1.b | Build Docker images | `test_docker_images_are_built` [compose] |
| **PASS** | 9.1.c | Run application components as containers | `test_components_run_as_containers` [compose] |
| **PASS** | 9.1.d | Configure communication between containers | `test_containers_communicate` [compose] |

### §9.2 Kubernetes (mandatory)

| Verdict | Id | Requirement (PDF) | Verified by |
|---|---|---|---|
| **K8S ONLY** | 9.2.a | Kubernetes Deployments | - |
| **K8S ONLY** | 9.2.b | Kubernetes Services | - |
| **K8S ONLY** | 9.2.c | Pod replicas | - |
| **PASS** | 9.2.d | Service-to-service communication | `test_containers_communicate` [compose] |
| **K8S ONLY** | 9.2.e | Microservice scaling | - |
| **K8S ONLY** | 9.2.f | Pod failure and automatic recovery | - |
| **K8S ONLY** | 9.2.local | Deployed locally with Minikube, kind or Docker Desktop Kubernetes (cloud optional) | - |

### §10 Required demonstration

| Verdict | Id | Requirement (PDF) | Verified by |
|---|---|---|---|
| **PASS** | 10.create | Request Creation: create and retrieve an emergency request | `test_createRequest` [compose]<br>`test_getRequest` [compose] |
| **PASS** | 10.register | Resource Registration: register an available resource or volunteer | `test_registerResource` [compose] |
| **PASS** | 10.match | Resource Matching: match a request with an appropriate available resource | `test_flood_victim_needs_drinking_water_end_to_end` [compose]<br>`test_matchRequest` [compose] |
| **PASS** | 10.dist | Data Distribution: distribution across the three storage shards | `test_data_distributed_across_three_shards_and_nodes` [compose] |
| **PASS** | 10.async | Asynchronous Processing: submit multiple requests, processed through RabbitMQ | `test_multiple_requests_processed_through_rabbitmq` [compose] |
| **PASS** | 10.fail | Storage Failure: stop a storage node, continued operation using an available replica | `test_storage_node_failure_before_during_after` [compose] |
| **PASS** | 10.recover | Node Recovery: restart the failed node, restoration / synchronization of the data | `test_storage_node_failure_before_during_after` [compose] |
| **K8S ONLY** | 10.k8srecover | Kubernetes Recovery: terminate a running application pod, automatic recovery | - |
| **K8S ONLY** | 10.k8sscale | Kubernetes Scaling: increase the replicas of a microservice, continued availability | - |

### §11 Testing

| Verdict | Id | Requirement (PDF) | Verified by |
|---|---|---|---|
| **PASS** | 11.cond | The system is tested under both normal and failure conditions | `test_flood_victim_needs_drinking_water_end_to_end` [compose]<br>`test_matching_service_down_503_and_queue_buffers` [compose]<br>`test_storage_node_failure_before_during_after` [compose]<br>`test_whole_shard_down_503_then_recovers` [compose] |
| **PASS** | 11.a | Testing: request creation, retrieval, and status updates | `test_createRequest` [compose]<br>`test_getRequest` [compose]<br>`test_request_record_fields` [compose]<br>`test_updateStatus` [compose] |
| **PASS** | 11.b | Testing: resource registration and availability updates | `test_registerResource` [compose]<br>`test_resource_availability_updates` [compose]<br>`test_resource_record_fields_and_kinds` [compose] |
| **PASS** | 11.c | Testing: correct resource matching | `test_matchRequest` [compose]<br>`test_matching_considers_location` [compose]<br>`test_matching_considers_resource_type` [compose]<br>`test_matching_uses_only_available_resources` [compose]<br>`test_simultaneous_requests_never_double_assign` [compose] |
| **PASS** | 11.d | Testing: multiple simultaneous requests | `test_multiple_requests_processed_through_rabbitmq` [compose]<br>`test_simultaneous_requests_never_double_assign` [compose] |
| **PASS** | 11.e | Testing: RabbitMQ message delivery and processing | `test_flood_victim_needs_drinking_water_end_to_end` [compose]<br>`test_matching_service_down_503_and_queue_buffers` [compose]<br>`test_multiple_requests_processed_through_rabbitmq` [compose]<br>`test_rabbitmq_topology` [compose] |
| **PASS** | 11.f | Testing: correct shard selection and data distribution | `test_coordinator_selects_shard_by_hash_mod_3` [compose]<br>`test_data_distributed_across_three_shards_and_nodes` [compose] |
| **PASS** | 11.g | Testing: replica availability during storage-node failure | `test_storage_node_failure_before_during_after` [compose] |
| **PASS** | 11.h | Testing: recovery and data synchronization after node recovery | `test_storage_node_failure_before_during_after` [compose] |
| **K8S ONLY** | 11.i | Testing: Kubernetes pod failure and automatic recovery | - |
| **K8S ONLY** | 11.j | Testing: Kubernetes-based microservice scaling | - |

### §12 Left to your design

| Verdict | Id | Requirement (PDF) | Verified by |
|---|---|---|---|
| **PASS** | 12 | Design choices (language, storage, sharding, replica sync, matching, RabbitMQ config, service boundaries, Kubernetes config, monitoring/logging) can be justified | `test_design_choices_are_documented` [compose] |

### EXTENSION #39 - Prometheus + Grafana monitoring

| Verdict | Id | Requirement (PDF) | Verified by |
|---|---|---|---|
| **PASS** | X39.metrics | Every service exposes /metrics: request-service, matching-service, coordinator, all storage nodes | `test_x39_every_service_exposes_metrics` [compose] |
| **PASS** | X39.rabbit | RabbitMQ's Prometheus plugin is scraped (queue depth, unacked, consumers) | `test_x39_prometheus_scrapes_every_component` [compose] |
| **PASS** | X39.scrape | Prometheus scrapes every component and every replica; all targets up | `test_x39_prometheus_scrapes_every_component` [compose] |
| **PASS** | X39.grafana | Grafana starts with the Prometheus datasource and the ERMS dashboard provisioned | `test_x39_grafana_is_provisioned_and_secured` [compose] |
| **PASS** | X39.panels | Dashboard shows queue depth, time from request to match, failovers, shard health, node up/down, resync duration, matching pods, API rate/latency/errors | `test_x39_dashboard_shows_everything_promised_with_data` [compose] |
| **PASS** | X39.nodata | Every query of every dashboard panel returns data (no 'No data' panels) | `test_x39_dashboard_shows_everything_promised_with_data` [compose] |
| **PASS** | X39.traffic | Metrics follow real traffic: requests created / published / matched and the time-to-match are recorded correctly | `test_x39_metrics_follow_real_traffic` [compose] |
| **PASS** | X39.failure | Demo: a stopped storage node turns DOWN (red) on the dashboard, failures are counted, it turns READY (green) again after its resync and the resync duration is recorded | `test_x39_killing_the_read_replica_makes_failovers_rise` [compose]<br>`test_x39_storage_failure_is_visible_on_the_dashboard` [compose] |
| **PASS** | X39.secure | Grafana admin password from a Secret / .env; dashboards viewable read-only without login; anonymous users cannot edit | `test_x39_grafana_is_provisioned_and_secured` [compose] |
| **PASS** | X39.both | Monitoring runs on both deployments (Docker Compose and Kubernetes) | `test_x39_monitoring_is_deployed` [compose] |

### EXTENSION #18 - KEDA autoscaling on RabbitMQ queue depth

| Verdict | Id | Requirement (PDF) | Verified by |
|---|---|---|---|
| **K8S ONLY** | X18.install | KEDA is installed in the cluster (operator, metrics API server, admission webhooks) | - |
| **K8S ONLY** | X18.scaledobject | ScaledObject scales Deployment matching-service on queue matching.requests (2-6 pods, 5 waiting messages per pod) and is Ready | - |
| **K8S ONLY** | X18.hpa | KEDA drives a HorizontalPodAutoscaler for matching-service; no conflicting autoscaler | - |
| **K8S ONLY** | X18.secret | KEDA's broker credentials come from a Secret (TriggerAuthentication), not from the manifests | - |
| **K8S ONLY** | X18.up | A burst of requests piling up in RabbitMQ scales matching-service up automatically (2 -> 6) | - |
| **K8S ONLY** | X18.down | When the queue has drained matching-service scales back down to 2 automatically | - |
| **K8S ONLY** | X18.safe | While pods are added and removed no request is lost, none matched twice, capacity exact, and the API stays available | - |
| **K8S ONLY** | X18.visible | The autoscaling is visible in Prometheus / Grafana (matching pods vs queue depth) | - |
| **K8S ONLY** | X18.manual | Manual scaling / stopping still works with KEDA (pin the replica count), then automatic scaling resumes | - |

### EXTENSION - Web UI (frontend for every feature)

| Verdict | Id | Requirement (PDF) | Verified by |
|---|---|---|---|
| **PASS** | UI.serve | The web UI runs as its own container / pod (Compose :8090, Kubernetes NodePort 30088) | `test_ui_is_served_on_this_deployment` [compose] |
| **PASS** | UI.proxy | The UI reaches the API, the coordinator (read-only) and Prometheus through its own origin; storage writes and Prometheus admin paths are blocked; it survives backend restarts | `test_ui_proxies_the_backend_with_the_right_restrictions` [compose] |
| **PASS** | UI.errors | Every tab loads without JavaScript errors or failed calls | `test_ui_every_tab_loads_without_errors` [compose] |
| **PASS** | UI.health | The header shows live health: API, RabbitMQ, shards, storage nodes, matching pods | `test_ui_every_tab_loads_without_errors` [compose] |
| **PASS** | UI.overview | Overview: requests / resources by status, storage nodes, queue depth, matching pods, latest requests - matching the backend's numbers | `test_ui_overview_matches_the_backend` [compose] |
| **PASS** | UI.resources | Resources: register (type, kind, location, availability), list + filters, detail, availability and status updates | `test_ui_register_and_update_a_resource` [compose] |
| **PASS** | UI.requests | Requests: create (type, location, details, priority, auto-match), list + filters, detail with assignment, storage location and history | `test_ui_request_lifecycle_and_match_now` [compose] |
| **PASS** | UI.match | Match now and every status change (in progress, resolved, re-open, cancel) from the UI; errors such as 'no suitable resource' are shown | `test_ui_request_lifecycle_and_match_now` [compose] |
| **PASS** | UI.async | A request created with automatic matching is matched through RabbitMQ and shown so | `test_ui_request_matched_automatically_through_rabbitmq` [compose] |
| **PASS** | UI.map | Map: requests by status, resources, assignment lines; click to create at a location | `test_ui_map` [compose] |
| **PASS** | UI.storage | Storage cluster: shards, nodes, status, records, failovers; shard lookup of any record | `test_ui_storage_cluster_and_shard_lookup` [compose] |
| **PASS** | UI.failure | A storage node failure shows live in the UI (node down, then ready again) | `test_ui_shows_a_storage_failure_live` [compose] |
| **PASS** | UI.messaging | Messaging & autoscaling: queue depth, consumers, matching pods, time to match, charts; burst generator | `test_ui_messaging_charts_and_burst` [compose] |
| **PASS** | UI.monitoring | Monitoring: the Grafana dashboard embedded and live, links to Prometheus / RabbitMQ / Swagger, scrape-target health | `test_ui_monitoring_tab` [compose] |
| **PASS** | UI.both | The UI works on both deployments (Docker Compose and Kubernetes) | `test_ui_is_served_on_this_deployment` [compose] |

## Every check performed

### Target: compose

#### [PASS] `test_exactly_one_coordinator` (0.5s) - covers 2.coord, 4.b

§2 Table 1 'Coordinator: 1' / §4 'one coordinator': exactly one coordinator runs, every storage node is registered with it and every service sends its storage operations to it.

- ✔ exactly one coordinator container/pod is running — `1 running`
- ✔ the coordinator answers GET /health — `{'status': 'ok', 'healthy_shards': [0, 1, 2], 'nodes': 6}`
- ✔ all 6 storage nodes are registered with this one coordinator — `['storage-0a', 'storage-0b', 'storage-1a', 'storage-1b', 'storage-2a', 'storage-2b']`
- ✔ request-service sends every storage operation to the coordinator — `COORDINATOR_URL=http://coordinator:8000`
- ✔ matching-service sends every storage operation to the coordinator — `COORDINATOR_URL=http://coordinator:8000`
- ✔ storage nodes register / heartbeat with the coordinator — `COORDINATOR_URL=http://coordinator:8000`

#### [PASS] `test_two_to_three_microservices` (0.8s) - covers 2.ms

§2 Table 1 'Microservices: 2-3': the application is split into separately deployed microservices.

- ✔ microservice 'request-service' is deployed on its own (client REST API; stores requests; publishes them to RabbitMQ) — `1 instance(s)`
- ✔ microservice 'matching-service' is deployed on its own (consumes RabbitMQ messages; finds and assigns resources) — `2 instance(s)`
- ✔ microservice 'coordinator' is deployed on its own (storage-layer service: shard routing, replication, failure detection) — `1 instance(s)`
- ✔ request-service is healthy — `{'status': 'ok', 'instance': '072775896cc1', 'broker_connected': True}`
- ✔ matching-service is healthy (called from inside the request-service container) — `{"status":"ok","worker":"9dad50fc8c04","broker_connected":true}`
- ✔ 2-3 microservices: 2 application services + the coordinator = 3 — `request-service, matching-service, coordinator; storage nodes are the shards, RabbitMQ the broker`

#### [PASS] `test_one_rabbitmq_instance` (0.5s) - covers 2.mq, 6.mq

§2 Table 1 'Message broker: 1 RabbitMQ instance', used by the request and matching services.

- ✔ exactly one RabbitMQ container/pod is running — `1 running`
- ✔ the broker is a single RabbitMQ node (not a cluster) — `rabbit@rabbitmq`
- · RabbitMQ version — `3.13.7`
- ✔ request-service is configured to use that broker — `RABBITMQ_URL host = rabbitmq:5672`
- ✔ matching-service is configured to use that broker — `RABBITMQ_URL host = rabbitmq:5672`
- ✔ every request-service and matching-service instance is connected to it — `3 AMQP connections (expected >= 3)`

#### [PASS] `test_three_shards_each_with_a_replica` (0.7s) - covers 2.shards, 2.repl, 4.b

§2 'Storage shards: 3', 'Replication: at least 1 replica per shard' / §4 'three storage shards, and at least one replica for each shard'.

- ✔ the storage layer has exactly 3 shards — `num_shards=3`
- ✔ shard 0 = 1 primary copy + at least 1 replica — `['storage-0a', 'storage-0b']`
- ✔ shard 0: every copy is up and ready — `{'storage-0a': 'ready', 'storage-0b': 'ready'}`
- ✔ shard 1 = 1 primary copy + at least 1 replica — `['storage-1a', 'storage-1b']`
- ✔ shard 1: every copy is up and ready — `{'storage-1a': 'ready', 'storage-1b': 'ready'}`
- ✔ shard 2 = 1 primary copy + at least 1 replica — `['storage-2a', 'storage-2b']`
- ✔ shard 2: every copy is up and ready — `{'storage-2a': 'ready', 'storage-2b': 'ready'}`
- ✔ 6 storage-node containers run (3 shards x 2) — `['storage-0a', 'storage-0b', 'storage-1a', 'storage-1b', 'storage-2a', 'storage-2b']`
- ✔ the replicas of each shard hold the same number of records — `key counts per shard: {'0': [1255, 1255], '1': [1259, 1259], '2': [1217, 1217]}`

#### [PASS] `test_one_or_more_clients` (0.4s) - covers 2.clients

§2 Table 1 'Clients: 1 or more': the API is usable by any HTTP/JSON client and by several at the same time.

- ✔ an interactive client (Swagger UI) is served at /docs — `http://localhost:8080/docs -> 200`
- ✔ POST /requests is published in the machine-readable API description — `openapi.json`
- ✔ GET /requests/{request_id} is published in the machine-readable API description — `openapi.json`
- ✔ PUT /requests/{request_id}/status is published in the machine-readable API description — `openapi.json`
- ✔ POST /resources is published in the machine-readable API description — `openapi.json`
- ✔ GET /resources is published in the machine-readable API description — `openapi.json`
- ✔ PUT /resources/{resource_id}/status is published in the machine-readable API description — `openapi.json`
- ✔ POST /match/{request_id} is published in the machine-readable API description — `openapi.json`
- ✔ 5 independent clients working at the same time are all served (create 201, read 200) — `[(201, 200), (201, 200), (201, 200), (201, 200), (201, 200)]`

#### [PASS] `test_createRequest` (0.1s) - covers 1.op1, 5.1, 7.1, 10.create, 11.a

§1 createRequest(type, location, details) - 'Create an emergency request' = POST /requests (201).

- ✔ POST /requests with exactly {type, location, details} -> 201 Created — `201`
- ✔ the response is JSON — `application/json`
- ✔ the new request got an id — `req-1597e92de7a4`
- ✔ 'type' is stored as submitted — `water`
- ✔ 'location' is stored as submitted — `{'lat': -44.930451, 'lon': -24.383099, 'area': 'spec-1339f4'}`
- ✔ 'details' is stored as submitted — `two people stranded on a roof, need drinking water`
- ✔ a new request starts in status 'pending' — `pending`
- ✔ it was handed to RabbitMQ for asynchronous matching — `queued=True`
- ✔ it was written to its shard on both copies — `{'shard': 2, 'replicas': ['storage-2a', 'storage-2b'], 'version': 1795173871413308216, 'degraded': False}`

#### [PASS] `test_getRequest` (0.1s) - covers 1.op5, 5.2, 10.create, 11.a

§1 getRequest(requestId) - 'Retrieve request information' = GET /requests/{id} (200).

- ✔ GET /requests/{id} -> 200 OK — `200`
- ✔ returned 'id' equals what was created — `req-713ef30ca51b`
- ✔ returned 'type' equals what was created — `food`
- ✔ returned 'location' equals what was created — `{'lat': -44.930451, 'lon': -19.883099, 'area': 'spec-1339f4'}`
- ✔ returned 'details' equals what was created — `food for 12 people in a flooded school`
- ✔ returned 'status' equals what was created — `pending`
- ✔ returned 'created_at' equals what was created — `2026-11-20T11:24:31.469+00:00`
- ✔ the read was served by a copy of the request's own shard — `shard 0, served_by storage-0a`
- ✔ reading again returns the same data

#### [PASS] `test_registerResource` (0.1s) - covers 1.op2, 5.4, 10.register, 11.b

§1 registerResource(type, location, availability) - 'Register a resource or volunteer' = POST /resources.

- ✔ POST /resources with exactly {type, location, availability} -> 201 Created — `201`
- ✔ the resource got an id — `res-water-11033ce7e3b8`
- ✔ 'type' is stored as submitted — `water`
- ✔ 'location' is stored as submitted — `{'lat': -44.921468, 'lon': -10.883099, 'area': 'spec-1339f4'}`
- ✔ 'availability' is stored as submitted — `5`
- ✔ a resource with capacity starts 'available' — `available`
- ✔ a volunteer can be registered too (kind=volunteer) -> 201 — `res-transportation-3549412cccb4`
- ✔ the registered resource appears in GET /resources — `319 water resources`
- ✔ and can be read back by id — `200`

#### [PASS] `test_matchRequest` (0.2s) - covers 1.op3, 5.7, 10.match, 11.c, 7.5, 7.6

§1 matchRequest(requestId) - 'Find and assign a suitable available resource' = POST /match/{requestId}.

- ✔ POST /match/{requestId} -> 200 OK — `200`
- ✔ a resource was found and assigned — `Water tanker (res-water-6396360ed757) at 1.998 km`
- ✔ the distance is computed from the two locations — `1.998`
- ✔ the request is now 'matched' to that resource — `{'resource_id': 'res-water-6396360ed757', 'resource_name': 'Water tanker', 'kind': 'resource', 'distance_km': 1.998, 'matched_at': '2026-11-20T11:24:31.689+00:00', 'matched_by': '9dad50fc8c04', 'via': 'api'}`
- ✔ one unit of the resource is assigned to the request — `availability 3 -> 2, assigned_requests=['req-b27f603290d2']`
- ✔ matching an already-matched request again does not take a second unit — `{'result': 'skipped', 'request_id': 'req-b27f603290d2', 'status': 'matched', 'assigned_resource': 'res-water-6396360ed757'}`

#### [PASS] `test_updateStatus` (0.2s) - covers 1.op4, 5.3, 11.a

§1 updateStatus(requestId, status) - 'Update the status of a request' = PUT /requests/{id}/status (200).

- ✔ PUT /requests/{id}/status {'status': 'in_progress'} -> 200 OK — `200`
- ✔ the stored status is now 'in_progress' — `previous_status=matched`
- ✔ PUT /requests/{id}/status {'status': 'resolved'} -> 200 OK — `200`
- ✔ the stored status is now 'resolved' — `previous_status=in_progress`
- ✔ resolving the request released the resource's capacity — `availability back to 1, status available`
- ✔ every status change is recorded in the request history — `['pending', 'matched', 'in_progress', 'resolved']`
- ✔ updated_at moves forward — `2026-11-20T11:24:31.802+00:00 -> 2026-11-20T11:24:31.896+00:00`
- ✔ a pending request can be cancelled -> 200 — `200`

#### [PASS] `test_supported_request_types` (0.6s) - covers 1.b, 3.1.types, 1.c

§1 'requests such as food, water, medicine, transportation, or shelter' / §3.1 example request types.

- ✔ a 'water' request is accepted (201) — `req-3c1d9beba2bf`
- ✔ the 'water' request is matched to the 'water' resource — `matched -> water provider`
- ✔ a 'food' request is accepted (201) — `req-a0aa28542a35`
- ✔ the 'food' request is matched to the 'food' resource — `matched -> food provider`
- ✔ a 'medicine' request is accepted (201) — `req-ae9fb4190e77`
- ✔ the 'medicine' request is matched to the 'medicine' resource — `matched -> medicine provider`
- ✔ a 'transportation' request is accepted (201) — `req-40dbc399c9e1`
- ✔ the 'transportation' request is matched to the 'transportation' resource — `matched -> transportation provider`
- ✔ a 'shelter' request is accepted (201) — `req-342170b6ecbc`
- ✔ the 'shelter' request is matched to the 'shelter' resource — `matched -> shelter provider`
- ✔ an unknown request type is rejected (422) — `Value error, type must be one of ['food', 'medicine', 'other', 'rescue', 'shelter', 'trans`

#### [PASS] `test_request_record_fields` (0.8s) - covers 3.1.id, 3.1.type, 3.1.loc, 3.1.details, 3.1.status, 3.1.created, 11.a

§3.1 'Each request should contain, at minimum: unique request ID, request type, location, request details, current status, creation time'.

- ✔ unique request ID: present — `req-71eae726b6d4`
- ✔ unique request ID: 25 requests created at the same time all got different ids — `25 distinct of 25`
- ✔ request type — `medicine`
- ✔ location (latitude / longitude) — `{'lat': -44.930451, 'lon': 7.116901, 'area': 'spec-1339f4'}`
- ✔ request details — `insulin for a diabetic patient`
- ✔ current status — `pending`
- ✔ creation time (ISO-8601 timestamp with time zone, taken when the request was created) — `2026-11-20T11:24:32.640+00:00`

#### [PASS] `test_resource_record_fields_and_kinds` (0.5s) - covers 3.2.id, 3.2.type, 3.2.loc, 3.2.avail, 3.2.status, 3.2.kinds, 11.b

§3.2 'Each resource should contain, at minimum: unique resource ID, resource type, location, availability, current status' and 'may represent a physical resource, volunteer, NGO, or other available source'.

- ✔ unique resource ID: present — `res-shelter-a34f8f7dd2c2`
- ✔ unique resource ID: 15 resources registered at the same time all got different ids — `15 distinct of 15`
- ✔ resource type — `shelter`
- ✔ location — `{'lat': -44.930451, 'lon': 11.616901, 'area': 'spec-1339f4'}`
- ✔ availability (units of capacity) — `40`
- ✔ current status — `available`
- ✔ a resource of kind 'resource' can be registered — `res-food-e011b4e2e0af`
- ✔ a resource of kind 'volunteer' can be registered — `res-food-d2cf0c3c430a`
- ✔ a resource of kind 'ngo' can be registered — `res-food-303edf4ee37a`
- ✔ a resource of kind 'other' can be registered — `res-food-63c88f6d4839`
- ✔ an unknown kind is rejected (422) — `422`

#### [PASS] `test_system_maintains_requests_and_resources` (0.5s) - covers 3.maintain, 5.5

§3 'The system should maintain information about emergency requests and available resources' and §5 GET /resources 'List resources' (200).

- ✔ GET /resources -> 200 OK with a JSON list — `977 resources across all shards`
- ✔ the list contains the resources just registered (from whichever shard holds them) — `res-water-558d13a1ec66 (shard 2), res-food-37a693e91a79 (shard 2)`
- ✔ GET /resources?type=water lists only water resources — `323 items`
- ✔ GET /resources?status=available lists only available ones — `58 items`
- ✔ no shard was missing from the list — `[]`
- ✔ requests are kept as well (GET /requests) — `1933 water requests`

#### [PASS] `test_resource_availability_updates` (0.3s) - covers 5.6, 11.b, 3.2.avail, 3.2.status

§5 PUT /resources/{id}/status (200) and §11 'Resource registration and availability updates'.

- ✔ PUT /resources/{id}/status {'availability': 5} -> 200 OK — `availability 3 -> 5`
- ✔ PUT {'status': 'offline'} -> 200, resource offline — `offline`
- ✔ an offline resource is not used for matching (409: no suitable resource) — `409`
- ✔ back to 'available' -> the same request is now matched to it — `matched`
- ✔ matching consumed one unit (5 -> 4) — `4`
- ✔ availability 0 -> status becomes 'busy' automatically — `busy`
- ✔ availability 2 -> 'available' again — `available`

#### [PASS] `test_flood_victim_needs_drinking_water_end_to_end` (2.7s) - covers 1.1.a, 1.1.b, 1.a, 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 6.mq, 11.e, 11.cond, 10.match

§1.1 example scenario and §7 steps 1-6: 'a user affected by flooding requires drinking water. The request is submitted, processed asynchronously, matched with an available resource, and the request status is updated.' User -> Request Service -> RabbitMQ -> Matching Service -> Distributed Storage -> Resource Assigned.

- ✔ 1. the client submits the request: POST /requests -> 201 — `201`
- ✔    the API answers at once, before any matching happened (asynchronous): status 'pending' — `answered in 17 ms`
- ✔ 2. the Request Service stored the request in the distributed storage — `shard 1, read from storage-1a`
- ✔    the record is physically on both copies of its shard — `{'storage-1a': 200, 'storage-1b': 200}`
- ✔ 3. the request was published to RabbitMQ (queued: true) — `True`
- ✔    RabbitMQ received it into queue 'matching.requests' — `publish counter 0 -> 2`
- ✔ 4. the Matching Service consumed the message (matched by the queue consumer, not the sync API) — `via=queue`
- ✔    ... in one of the matching-service instances — `9dad50fc8c04 (running now: ['9dad50fc8c04', 'e22c21dddffb'])`
- ✔    the message was acknowledged to RabbitMQ after processing — `ack counter 0 -> 1`
- ✔ 5. a suitable available resource was identified: same type, available, the nearest one — `NGO water tanker at 2.997 km (the other water resource is 20 km away)`
- ✔ 6. the request status was updated to 'matched' — `matched`
- ✔    the resource was assigned: capacity 2 -> 1, request in its assigned list — `availability 1`
- ✔    the assignment is persisted in the distributed storage — `2026-11-20T11:24:36.341+00:00`
- ✔ help delivered: status -> in_progress -> resolved, tanker capacity released (1 -> 2) — `2`
- ✔ the request history shows the whole lifecycle — `['pending', 'matched', 'in_progress', 'resolved']`
- · the path the request took — `User -> request-service (072775896cc1) -> RabbitMQ (erms.requests -> matching.requests) -> matching-service (9dad50fc8c04) -> storage shard 1 ['storage-1a', 'storage-1b'] -> resource assigned (NGO water tanker)`

#### [PASS] `test_matching_considers_resource_type` (0.2s) - covers 7.type, 1.c, 11.c

§7 'The matching mechanism should consider at least resource type' (and location).

- ✔ a medicine request is matched to the medicine resource, not the closer food resource — `pharmacy, 10 km at 9.989 km`
- ✔ the food resource was not touched
- ✔ a shelter request with only food/medicine nearby gets no match (409) — `no suitable available resource`

#### [PASS] `test_matching_considers_location` (0.6s) - covers 7.loc, 1.c, 11.c

§7 'The matching mechanism should consider at least resource type and location'.

- ✔ closest first: 3 requests at the same place get the 5 km, then 15 km, then 25 km resource — `['5 km (4.994 km)', '15 km (14.983 km)', '25 km (24.972 km)']`
- ✔ a resource beyond the matching radius (50 km) is not assigned: 80 km -> 409 — `{'message': 'no suitable available resource', 'result': 'no_match', 'request_id': 'req-b42bd9f14cad', 'candidates': 0, 'unavailable_shards': []}`
- ✔ a resource within 50 km is assigned: 40 km -> 200 — `39.955 km`

#### [PASS] `test_matching_uses_only_available_resources` (0.3s) - covers 7.5, 11.c

§7 step 5 'A suitable AVAILABLE resource is identified': offline or fully-used resources are skipped.

- ✔ a resource with availability 0 is registered as 'busy' — `busy`
- ✔ the request skips the offline (1 km) and busy (2 km) ones and gets the available one (9 km) — `available, 9 km at 8.99 km`
- ✔ after its last unit is taken the resource is 'busy' — `busy`
- ✔ so a further request gets no match (409)

#### [PASS] `test_multiple_requests_processed_through_rabbitmq` (8.6s) - covers 10.async, 11.d, 11.e, 6.mq, 1.d

§10 'Asynchronous Processing: submit multiple requests and demonstrate processing through RabbitMQ' and §11 'RabbitMQ message delivery and processing'.

- ✔ 15 requests submitted at the same time are all accepted (201) and queued — `in 0.10s`
- ✔ RabbitMQ buffered the burst while the matching workers processed it — `peak backlog in matching.requests: 11 messages`
- ✔ all 15 requests were matched — `6.5s after submission`
- ✔ every one of them came through the RabbitMQ consumer
- ✔ the work was shared by the matching-service instances (competing consumers) — `{'e22c21dddffb': 8, '9dad50fc8c04': 7}`
- ✔ RabbitMQ counted >= 15 messages published and acknowledged — `publish +15, ack +16`
- ✔ exactly 15 units were assigned, one per request, none twice — `availability 15 -> 0, status busy`

#### [PASS] `test_simultaneous_requests_never_double_assign` (4.1s) - covers 11.d, 11.c

§11 'Multiple simultaneous requests': many requests created and matched at the same moment never over-assign a resource, and racing matches of the same request take only one unit.

- ✔ 30 simultaneous creations: all 201 with distinct ids — `spread over shards {1: 13, 2: 9, 0: 8}`
- ✔ 30 simultaneous POST /match against 20 units: exactly 20 succeed (200), 10 get 409 — `{200: 20, 409: 10}`
- ✔ the resource is never over-assigned: availability exactly 0, 'busy' — `availability 0, busy`
- ✔ its assigned list is exactly the matched requests (no duplicates, no strays) — `20`
- ✔ 8 simultaneous POST /match of the SAME request all answer 200 — `Counter({'skipped': 7, 'matched': 1})`
- ✔ ... and it holds exactly one unit (5 -> 4), listed once — `['req-a1543e48a023']`
- ✔ the request is matched to that resource — `matched`

#### [PASS] `test_rabbitmq_topology` (0.2s) - covers 6.mq, 11.e

§6 'RabbitMQ should be used for asynchronous communication between the request-processing and matching components' - exchange, queue, binding, consumers and retry path.

- ✔ request-service publishes to exchange 'erms.requests' (topic, durable) — `type=topic durable=True`
- ✔ queue 'matching.requests' is durable (survives broker restarts) — `True`
- ✔ erms.requests is bound to matching.requests with routing key 'request.#' — `[('matching.requests', 'request.#')]`
- ✔ every matching-service instance is a consumer of matching.requests — `2 consumers, 2 matching instances`
- ✔ unmatched requests are retried later via 'matching.retry' (TTL, dead-lettered back to erms.requests) — `ttl=5000 ms`
- ✔ request-service is connected to the broker

#### [PASS] `test_data_distributed_across_three_shards_and_nodes` (3.6s) - covers 4.a, 10.dist, 11.f, 1.d

§4 'Requests and resources must be stored across multiple storage nodes' / §10 'Data Distribution: demonstrate distribution across the three storage shards' - checked on each node's own disk.

- ✔ requests are spread over all 3 shards — `10 requests: {0: 3, 1: 4, 2: 3}`
- ✔ resources are spread over all 3 shards — `7 resources: {0: 2, 1: 3, 2: 2}`
- ✔ every shard holds data — `records per node: {'0': [1299, 1299], '1': [1311, 1311], '2': [1266, 1266]}`
- ✔ request on shard 0 is on the disk of exactly its shard's 2 nodes — `request:req-6b906eb39a86 -> ['storage-0a', 'storage-0b']`
- ✔    both copies are identical (same version) — `{1795173891373364106}`
- ✔ resource on shard 0 is on the disk of exactly its shard's 2 nodes — `resource:food:res-food-b026d453ef9c -> ['storage-0a', 'storage-0b']`
- ✔    both copies are identical (same version) — `{1795173891522795777}`
- ✔ request on shard 1 is on the disk of exactly its shard's 2 nodes — `request:req-984ccb9f13fb -> ['storage-1a', 'storage-1b']`
- ✔    both copies are identical (same version) — `{1795173891360223458}`
- ✔ resource on shard 1 is on the disk of exactly its shard's 2 nodes — `resource:water:res-water-076873ed9099 -> ['storage-1a', 'storage-1b']`
- ✔    both copies are identical (same version) — `{1795173891490776580}`
- ✔ request on shard 2 is on the disk of exactly its shard's 2 nodes — `request:req-bffca1950e01 -> ['storage-2a', 'storage-2b']`
- ✔    both copies are identical (same version) — `{1795173891343014432}`
- ✔ resource on shard 2 is on the disk of exactly its shard's 2 nodes — `resource:medicine:res-medicine-17c2d22df07b -> ['storage-2a', 'storage-2b']`
- ✔    both copies are identical (same version) — `{1795173891537387009}`

#### [PASS] `test_coordinator_selects_shard_by_hash_mod_3` (1.4s) - covers 4.c, 4.d, 11.f

§4 'The coordinator determines the appropriate shard and routes storage operations. A simple hash-based partitioning mechanism may be used. For example: hash(request_id) mod 3'.

- ✔ every record was stored on shard = sha256(key) mod 3 (recomputed by the test) — `45/45 correct`
- · example: request:req-1fafecee2eca — `sha256 mod 3 = 2 -> stored on shard 2 by ['storage-2a', 'storage-2b']`
- · example: request:req-2af35f96ba2d — `sha256 mod 3 = 1 -> stored on shard 1 by ['storage-1a', 'storage-1b']`
- · example: resource:water:res-water-30adcc51118d — `sha256 mod 3 = 2 -> stored on shard 2 by ['storage-2a', 'storage-2b']`
- ✔ the coordinator's own routing table agrees (GET /internal/shard-of/{key}) — `10/10 keys`
- ✔ the shard choice is deterministic (same key -> same shard every time) — `5 keys asked twice`
- ✔ reads are routed to a node of the key's shard — `15/15 reads routed correctly`
- ✔ writes are routed to (all) the replicas of that shard — `45/45 writes acked by both copies`
- · load spread of these 45 keys over the shards — `{0: 16, 1: 15, 2: 14}`

#### [PASS] `test_client_does_not_need_the_storage_location` (0.5s) - covers 4.e

§4 'The client should not need to know the physical storage location of the requested data.'

- ✔ no client endpoint takes a shard / node / replica parameter — `['limit', 'order', 'request_id', 'resource_id', 'status', 'type']`
- ✔ request bodies carry no storage location either — `['auto_match', 'availability', 'contact', 'details', 'kind', 'location', 'name', 'priority', 'requester', 'type']`
- ✔ records on all 3 shards are read with the same URL, only the id is needed — `GET /requests/{id} for ids on shards [0, 1, 2] -> [200, 200, 200]`
- ✔ the request-service knows only the coordinator, not any storage node — `COORDINATOR_URL=http://coordinator:8000`
- ✔ storage nodes are not reachable from outside (no published ports) — `{'storage-0a': [], 'storage-0b': [], 'storage-1a': [], 'storage-1b': [], 'storage-2a': [], 'storage-2b': []}`

#### [PASS] `test_client_api_success_codes` (0.2s) - covers 5.ok, 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7

§5 Table 2: each client endpoint returns the success code listed in the PDF.

- ✔ POST /resources                 -> 201 — `got 201`
- ✔ GET  /resources                 -> 200 — `got 200`
- ✔ PUT  /resources/{id}/status     -> 200 — `got 200`
- ✔ POST /requests                  -> 201 — `got 201`
- ✔ GET  /requests/{id}             -> 200 — `got 200`
- ✔ POST /match/{requestId}         -> 200 — `got 200`
- ✔ PUT  /requests/{id}/status      -> 200 — `got 200`

#### [PASS] `test_json_request_and_response_bodies` (0.1s) - covers 5.json

§5 'Structured request and response data should use JSON.'

- ✔ POST /requests (201): Content-Type application/json with a JSON body — `application/json`
- ✔ GET /requests/{id} (200): Content-Type application/json with a JSON body — `application/json`
- ✔ GET /resources (200): Content-Type application/json with a JSON body — `application/json`
- ✔ POST /match/{id} (409): Content-Type application/json with a JSON body — `application/json`
- ✔ GET /requests/{missing} (404): Content-Type application/json with a JSON body — `application/json`
- ✔ POST /requests invalid (422): Content-Type application/json with a JSON body — `application/json`
- ✔ PUT /resources/{missing}/status (404): Content-Type application/json with a JSON body — `application/json`
- ✔ a non-JSON request body is rejected (422) — `422`
- ✔ malformed JSON is rejected (422) — `json_invalid`

#### [PASS] `test_invalid_requests_get_4xx` (0.2s) - covers 5.400

§5 'appropriate HTTP status codes for ... invalid requests' (422 validation, 400 bad input, 409 conflict).

- ✔ POST /requests without type -> 422 — `got 422: Field required`
- ✔ POST /requests with type 'pizza' -> 422 — `got 422: Value error, type must be one of ['food', 'medicine', 'other', 'rescue`
- ✔ POST /requests without location -> 422 — `got 422: Field required`
- ✔ POST /requests with latitude 123 -> 422 — `got 422: Input should be less than or equal to 90`
- ✔ POST /requests with longitude -200 -> 422 — `got 422: Input should be greater than or equal to -180`
- ✔ POST /requests with 2001-char details -> 422 — `got 422: String should have at most 2000 characters`
- ✔ POST /resources with availability -1 -> 422 — `got 422: Input should be greater than or equal to 0`
- ✔ POST /resources with availability 'lots' -> 422 — `got 422: Input should be a valid integer, unable to parse string as an integer`
- ✔ PUT /requests/{id}/status 'flying' -> 422 — `got 422: Value error, status must be one of ['cancelled', 'in_progress', 'match`
- ✔ PUT /requests/{id}/status without status -> 422 — `got 422: Field required`
- ✔ PUT /resources/{id}/status with an empty body -> 400 — `got 400: provide status and/or availability`
- ✔ PUT /resources/{id}/status availability 0 + 'available' -> 400 — `got 400: a resource with availability 0 cannot be 'available'`
- ✔ PUT /resources/{id}/status 'sleeping' -> 422 — `got 422: Value error, status must be one of ['available', 'busy', 'offline']`
- ✔ GET /resources?type=pizza -> 422 — `got 422: type must be one of ['food', 'medicine', 'other', 'rescue', 'shelter',`
- ✔ PUT /requests/{id}/status 'matched' without a resource -> 409 — `got 409: cannot set 'matched' without an assigned resource; use POST /match`
- ✔ PUT 'pending' on a cancelled request -> 409 — `got 409: request is already cancelled; cannot change to pending`
- ✔ POST /match/{id} with no suitable resource -> 409 — `got 409: no suitable available resource`
- ✔ nothing was stored by the rejected creates (resource still has 1 unit)

#### [PASS] `test_missing_resources_get_404` (0.0s) - covers 5.404

§5 'appropriate HTTP status codes for ... missing resources' (404).

- ✔ GET /requests/{unknown id} -> 404 — `got 404: request:req-000000000000 not found`
- ✔ PUT /requests/{unknown id}/status -> 404 — `got 404: request:req-000000000000 not found`
- ✔ GET /resources/{unknown id} -> 404 — `got 404: resource:water:res-water-000000000000 not found`
- ✔ PUT /resources/{unknown id}/status -> 404 — `got 404: resource:water:res-water-000000000000 not found`
- ✔ PUT /resources/{malformed id}/status -> 404 — `got 404: resource:_invalid:not-an-id not found`
- ✔ POST /match/{unknown id} -> 404 — `got 404: request req-000000000000 not found`
- ✔ GET /no-such-endpoint -> 404 — `got 404: Not Found`

#### [PASS] `test_internal_node_register` (1.2s) - covers 6.reg

§6 POST /internal/nodes/register - 'Register storage node'.

- ✔ a node for a non-existent shard is refused (400) and not added — `{'detail': 'shard_id must be in [0, 3)'}`
- ✔ all storage nodes registered themselves on start-up — `[('storage-0a', 0), ('storage-0b', 0), ('storage-1a', 1), ('storage-1b', 1), ('storage-2a', 2), ('storage-2b', 2)]`
- ✔ POST /internal/nodes/register with a node's own details -> 200 — `{'status': 'syncing', 'resync': True, 'peers': ['http://storage-0a:8000'], 'sync_id': 1795173897477866267, 'num_shards': 3}`
- ✔ a (re)joining node is told to sync from the ready replica of its shard before serving reads — `status=syncing peers=['http://storage-0a:8000'] num_shards=3`
- ✔ the real node reports itself ready again with its next heartbeat — `storage-0b`
- · storage node log — `storage-0b-1  | 2026-11-20 11:23:33,619 [storage-storage-0b] INFO registered with coordinator (shard 0, url http://storage-0b:8000): waiting; peers=[]`

#### [PASS] `test_internal_heartbeats_are_periodic` (5.3s) - covers 6.hb, 8.hb

§6 POST /internal/nodes/heartbeat 'Report node availability' / §8 'Storage nodes should periodically report their availability to the coordinator using heartbeat messages.'

- ✔ a heartbeat from an unknown node -> 404 (it must register first) — `unknown node; register first`
- ✔ storage-0a heartbeats periodically (last beat advanced, never older than the 6 s timeout) — `beat age 0.5s -> 1.5s, advanced 4.0s in 5s`
- ✔ storage-0b heartbeats periodically (last beat advanced, never older than the 6 s timeout) — `beat age 0.2s -> 1.2s, advanced 4.0s in 5s`
- ✔ storage-1a heartbeats periodically (last beat advanced, never older than the 6 s timeout) — `beat age 1.8s -> 0.8s, advanced 6.0s in 5s`
- ✔ storage-1b heartbeats periodically (last beat advanced, never older than the 6 s timeout) — `beat age 2.0s -> 1.0s, advanced 6.0s in 5s`
- ✔ storage-2a heartbeats periodically (last beat advanced, never older than the 6 s timeout) — `beat age 1.7s -> 0.7s, advanced 6.0s in 5s`
- ✔ storage-2b heartbeats periodically (last beat advanced, never older than the 6 s timeout) — `beat age 1.9s -> 0.9s, advanced 6.0s in 5s`
- ✔ POST /internal/nodes/heartbeat from a registered node -> 200 — `{'resync': False}`
- · configuration — `storage HEARTBEAT_INTERVAL=2 (default)s, coordinator HEARTBEAT_TIMEOUT=6s`

#### [PASS] `test_internal_data_api` (3.8s) - covers 6.put, 6.get, 6.del

§6 PUT / GET / DELETE /internal/data/{key} - 'Store or update data', 'Retrieve data', 'Delete data' (both at the coordinator and on the storage nodes it routes to).

- ✔ PUT /internal/data/{key} -> 200, stored on the key's shard — `{'key': 'spec-test:1339f4', 'version': 1795173903917546996, 'shard': 1, 'replicas': ['storage-1a', 'storage-1b'], 'degraded': False}`
- ✔    and written to every replica of that shard — `['storage-1a', 'storage-1b']`
- ✔ GET /internal/data/{key} -> 200 with the stored value — `version 1795173903917546996, served_by storage-1a`
- ✔    the value is on the disk of both replicas with the same version — `['storage-1a', 'storage-1b']`
- ✔ PUT again = update -> 200, new version — `1795173903917546996 -> 1795173903917546997`
- ✔ an update based on an old version is refused (409, optimistic concurrency) — `{'detail': {'message': 'version conflict', 'current_version': 1795173903917546997}}`
- ✔ prefix scan GET /internal/data?prefix= finds it — `[]`
- ✔ DELETE /internal/data/{key} -> 200 — `{'key': 'spec-test:1339f4', 'deleted': True, 'shard': 1, 'replicas': ['storage-1a', 'storage-1b']}`
- ✔    afterwards GET -> 404
- ✔    and it is deleted on both replicas — `{'storage-1a': 404, 'storage-1b': 404}`
- ✔ the storage node itself (storage-1a) serves PUT / GET / DELETE /internal/data/{key} for the coordinator — `PUT 200, GET 200, DELETE 200, GET after delete 404`

#### [PASS] `test_dockerfiles_for_every_service` (0.2s) - covers 9.1.a, 2.docker

§9.1 'Create Dockerfiles for the required services.'

- ✔ request_service/Dockerfile exists and builds the request-service service — `FROM python:3.12-slim ... CMD uvicorn request_service.app:app`
- ✔ matching_service/Dockerfile exists and builds the matching-service service — `FROM python:3.12-slim ... CMD uvicorn matching_service.app:app`
- ✔ coordinator/Dockerfile exists and builds the coordinator service — `FROM python:3.12-slim ... CMD uvicorn coordinator.app:app`
- ✔ storage_node/Dockerfile exists and builds the storage-node service — `FROM python:3.12-slim ... CMD uvicorn storage_node.app:app`
- ✔ frontend/Dockerfile exists and builds the web UI (nginx + the app) — `FROM nginx:1.30.5-alpine`
- ✔ docker-compose builds 'coordinator' from its Dockerfile — `coordinator/Dockerfile`
- ✔ docker-compose builds 'frontend' from its Dockerfile — `frontend/Dockerfile`
- · grafana uses an official image (no Dockerfile needed) — `grafana/grafana:13.2.5`
- ✔ docker-compose builds 'matching-service' from its Dockerfile — `matching_service/Dockerfile`
- · prometheus uses an official image (no Dockerfile needed) — `prom/prometheus:v3.15.1`
- · rabbitmq uses an official image (no Dockerfile needed) — `rabbitmq:3.13-management`
- ✔ docker-compose builds 'request-service' from its Dockerfile — `request_service/Dockerfile`
- ✔ docker-compose builds 'storage-0a' from its Dockerfile — `storage_node/Dockerfile`
- ✔ docker-compose builds 'storage-0b' from its Dockerfile — `storage_node/Dockerfile`
- ✔ docker-compose builds 'storage-1a' from its Dockerfile — `storage_node/Dockerfile`
- ✔ docker-compose builds 'storage-1b' from its Dockerfile — `storage_node/Dockerfile`
- ✔ docker-compose builds 'storage-2a' from its Dockerfile — `storage_node/Dockerfile`
- ✔ docker-compose builds 'storage-2b' from its Dockerfile — `storage_node/Dockerfile`

#### [PASS] `test_docker_images_are_built` (0.1s) - covers 9.1.b, 2.docker

§9.1 'Build Docker images.'

- ✔ image erms/request-service:latest is built — `created 2026-11-20T11:23:30, 165 MB, CMD uvicorn request_service.app:app`
- ✔ image erms/matching-service:latest is built — `created 2026-11-20T11:23:29, 165 MB, CMD uvicorn matching_service.app:app`
- ✔ image erms/coordinator:latest is built — `created 2026-11-20T11:23:28, 165 MB, CMD uvicorn coordinator.app:app`
- ✔ image erms/storage-node:latest is built — `created 2026-11-20T11:23:29, 165 MB, CMD uvicorn storage_node.app:app`
- ✔ image erms/frontend:latest is built — `created 2026-11-20T07:59:53`

#### [PASS] `test_components_run_as_containers` (0.1s) - covers 9.1.c, 2.docker, 1.d

§9.1 'Run application components as containers.'

- ✔ 'rabbitmq' runs as 1 container(s) — `emergency-response-matching-rabbitmq-1 (rabbitmq:3.13-management, Up About a minute (healthy))`
- ✔ 'coordinator' runs as 1 container(s) — `emergency-response-matching-coordinator-1 (erms/coordinator:latest, Up About a minute)`
- ✔ 'request-service' runs as 1 container(s) — `emergency-response-matching-request-service-1 (erms/request-service:latest, Up About a minute)`
- ✔ 'matching-service' runs as 2 container(s) — `emergency-response-matching-matching-service-1 (erms/matching-service:latest, Up About a minute), emergency-response-matching-matching-service-2 (erms/matching-service:latest, Up About a minute)`
- ✔ 'storage-0a' runs as 1 container(s) — `emergency-response-matching-storage-0a-1 (erms/storage-node:latest, Up About a minute)`
- ✔ 'storage-0b' runs as 1 container(s) — `emergency-response-matching-storage-0b-1 (erms/storage-node:latest, Up About a minute)`
- ✔ 'storage-1a' runs as 1 container(s) — `emergency-response-matching-storage-1a-1 (erms/storage-node:latest, Up About a minute)`
- ✔ 'storage-1b' runs as 1 container(s) — `emergency-response-matching-storage-1b-1 (erms/storage-node:latest, Up About a minute)`
- ✔ 'storage-2a' runs as 1 container(s) — `emergency-response-matching-storage-2a-1 (erms/storage-node:latest, Up About a minute)`
- ✔ 'storage-2b' runs as 1 container(s) — `emergency-response-matching-storage-2b-1 (erms/storage-node:latest, Up About a minute)`
- ✔ 'prometheus' runs as 1 container(s) — `emergency-response-matching-prometheus-1 (prom/prometheus:v3.15.1, Up About a minute)`
- ✔ 'grafana' runs as 1 container(s) — `emergency-response-matching-grafana-1 (grafana/grafana:13.2.5, Up About a minute)`
- ✔ 'frontend' runs as 1 container(s) — `emergency-response-matching-frontend-1 (erms/frontend:latest, Up About a minute (healthy))`
- ✔ all 14 containers of the deployment are running — `14 running: coordinator x1, frontend x1, grafana x1, matching-service x2, prometheus x1, rabbitmq x1, request-service x1, storage-0a x1, storage-0b x1, storage-1a x1, storage-1b x1, storage-2a x1, storage-2b x1`

#### [PASS] `test_containers_communicate` (5.0s) - covers 9.1.d, 9.2.d

§9.1 'Configure communication between containers' (service DNS names on the Docker network / Kubernetes Services): each component reaches the ones it depends on from inside its own container.

- ✔ request-service -> coordinator (http://coordinator:8000/health) — `{"status":"ok","healthy_shards":[0,1,2],"nodes":6}`
- ✔ request-service -> matching-service (http://matching-service:8000/health) — `{"status":"ok","worker":"e22c21dddffb","broker_connected":true}`
- ✔ matching-service -> coordinator (http://coordinator:8000/health) — `{"status":"ok","healthy_shards":[0,1,2],"nodes":6}`
- ✔ request-service -> rabbitmq:5672 (AMQP) — `TCP connect`
- ✔ matching-service -> rabbitmq:5672 (AMQP) — `TCP connect`
- ✔ coordinator -> storage node storage-0a (http://storage-0a:8000) — `ready`
- ✔ coordinator -> storage node storage-0b (http://storage-0b:8000) — `ready`
- ✔ coordinator -> storage node storage-1a (http://storage-1a:8000) — `ready`
- ✔ coordinator -> storage node storage-1b (http://storage-1b:8000) — `ready`
- ✔ coordinator -> storage node storage-2a (http://storage-2a:8000) — `ready`
- ✔ coordinator -> storage node storage-2b (http://storage-2b:8000) — `ready`
- ✔ storage node storage-1a -> coordinator (register / heartbeat path) — `{"status":"ok","healthy_shards":[0,1,2],"nodes":6}`
- · how it is configured — `services address each other by name: COORDINATOR_URL=http://coordinator:8000, MATCHING_SERVICE_URL=http://matching-service:8000, RABBITMQ_URL=amqp://...@rabbitmq:5672, storage NODE_URL = compose DNS name`

#### [PASS] `test_design_choices_are_documented` (0.0s) - covers 12

§12 'Students should be able to justify their choices' - the README explains every left-to-design item.

- ✔ README justifies: programming language and web framework — `'Python 3.12 + FastAPI'`
- ✔ README justifies: storage mechanism of the shards — `'SQLite per node'`
- ✔ README justifies: sharding / partitioning strategy — `'sha256(key) mod 3'`
- ✔ README justifies: replica synchronization mechanism — `'Recovery / synchronisation'`
- ✔ README justifies: resource-matching strategy — `'Matching strategy'`
- ✔ README justifies: RabbitMQ exchange / queue / routing — `'RabbitMQ topology'`
- ✔ README justifies: microservice boundaries and internal API — `'Microservice boundaries'`
- ✔ README justifies: monitoring and logging — `'Monitoring / logging'`
- ✔ README justifies: Kubernetes deployment configuration — `'## Kubernetes' section, manifest table`
- ✔ README has the internal API description — `'## Internal API'`

#### [PASS] `test_storage_node_failure_before_during_after` (10.6s) - covers 8.detect, 8.avoid, 8.replica, 8.continue, 8.sync, 8.demo, 10.fail, 10.recover, 11.g, 11.h, 11.cond, 1.d

§8 'A storage-node failure should be demonstrated by stopping the corresponding container or Kubernetes pod and subsequently restarting it ... before, during, and after the failure': detect the failed node, avoid routing to it, use the replica, continue processing, synchronize the data when it recovers.

- · BEFORE: shard 1 = ['storage-1a', 'storage-1b']; the node that will be stopped: storage-1a; its replica: storage-1b
- ✔ before: every storage node is ready — `{'storage-0a': 'ready', 'storage-0b': 'ready', 'storage-1a': 'ready', 'storage-1b': 'ready', 'storage-2a': 'ready', 'storage-2b': 'ready'}`
- ✔ before: a request on shard 1 is written to both copies — `['storage-1a', 'storage-1b']`
- ✔ before: it is readable — `served_by storage-1a`
- · STOP the storage node — `docker compose stop storage-1a`
- ✔ during: the coordinator detected that storage-1a is unavailable — `5.1s after the stop; reason: scan failed: ConnectError('[Errno -2] Name or service not known')`
- ✔ during: shard 1 stays available through its replica storage-1b
- ✔ during: data written before the failure is still readable - served by the replica — `200, served_by storage-1b`
- ✔ during: a new request for shard 1 is accepted (201) and routed only to the live replica — `acked by ['storage-1b']`
- ✔ during: the write is flagged degraded (1 of 2 copies), to be copied on recovery — `{'shard': 1, 'replicas': ['storage-1b'], 'version': 1795173916261362268, 'degraded': True}`
- ✔ during: matching keeps working on shard 1 (POST /match -> 200) — `Shard-1 water point`
- ✔ during: status updates keep working (PUT /requests/{id}/status -> 200) — `in_progress`
- ✔ during: the asynchronous pipeline (RabbitMQ -> matching -> storage) keeps processing — `req-91d6a3ad214b matched by e22c21dddffb`
- ✔ during: no read was sent to the failed node storage-1a — `reads served by storage-1a: 967 before, 967 now`
- ✔ during: listing all requests is still complete (no unavailable shard) — `unavailable_shards = []`
- · RESTART the storage node — `docker compose start storage-1a`
- ✔ after: storage-1a re-registered, synchronized from storage-1b and is ready again — `1.8s after the restart command`
- ✔ after: the request created during the failure is now also on storage-1a's disk — `version 1795173917829685444`
- ✔ after: the updates made during the failure reached storage-1a (request 'in_progress', resource capacity) — `request status in_progress, resource availability 28`
- ✔ after: both copies of shard 1 hold the same number of records — `[1339, 1339]`
- · storage-1a log — `:19,680 [storage-storage-1a] INFO SYNC complete from http://storage-1b:8000: received 1350 records, applied 3 updates; now READY (1339 keys)`
- ✔ after: new writes go to both copies again (not degraded) — `['storage-1a', 'storage-1b']`
- ✔ after: reads are served by the recovered node storage-1a again, including the data it missed — `storage-1a`

#### [PASS] `test_matching_service_down_503_and_queue_buffers` (43.2s) - covers 5.503, 11.e, 11.cond, 6.mq

§5 'appropriate HTTP status codes for ... unavailable services' (503) and §11 RabbitMQ delivery: with the matching microservice stopped, /match answers 503 while new requests are still accepted and wait in RabbitMQ; after the restart they are delivered and processed.

- · STOP the matching microservice — `docker compose stop matching-service`
- ✔ POST /match while the matching service is down -> 503 Service Unavailable (JSON) — `matching service unavailable: ConnectError('[Errno -2] Name or service not known')`
- ✔ the API stays available: new requests are still accepted (201) and queued — `['req-ea5e2e3a44c1', 'req-2c7c931639b1', 'req-dd287ac2ab73']`
- ✔ RabbitMQ holds the messages while no consumer runs — `3 message(s) waiting in matching.requests`
- ✔ nothing is matched while the matching service is down — `all 3 pending`
- · RESTART the matching microservice — `docker compose start matching-service`
- ✔ the matching consumers re-attach to the queue — `2 consumers for 2 matching pods`
- ✔ the buffered messages are delivered and processed: all 3 requests matched — `['e22c21dddffb', 'e22c21dddffb', '9dad50fc8c04']`
- ✔ POST /match works again (200) — `200`

#### [PASS] `test_whole_shard_down_503_then_recovers` (10.6s) - covers 5.503, 8.detect, 11.cond

§5 'unavailable services' at the storage layer: with BOTH copies of a shard stopped, data on that shard answers 503 (not 500 / wrong data) while the other shards keep working; after the restart the data is back.

- · STOP both copies of the shard — `docker compose stop storage-2a storage-2b`
- ✔ the coordinator reports shard 2 unavailable — `{'status': 'degraded', 'healthy_shards': [0, 1], 'nodes': 6}`
- ✔ GET of a request on shard 2 -> 503 Service Unavailable — `no ready replica available for shard 2`
- ✔ creating requests: ids that hash to shard 2 -> 503, the others are still stored (201) — `{201: 12, 503: 3}`
- ✔ listing still answers 200 and names the unavailable shard — `[2]`
- · RESTART both copies of the shard — `docker compose start storage-2a storage-2b`
- ✔ after the restart the shard's data is back (persistent volumes) -> 200 — `200`

#### [PASS] `test_x39_monitoring_is_deployed` (0.6s) - covers X39.both

#39: Prometheus and Grafana run as part of this deployment (Compose services / Kubernetes Deployments).

- ✔ 'prometheus' container is running — `1 running`
- ✔ 'grafana' container is running — `1 running`
- ✔ Prometheus is ready — `http://localhost:9090/-/ready -> 200`
- ✔ Grafana is healthy — `Grafana 13.2.5`

#### [PASS] `test_x39_every_service_exposes_metrics` (3.5s) - covers X39.metrics

#39: every ERMS service serves Prometheus metrics at /metrics.

- ✔ request-service: GET /metrics -> 200 Prometheus text — `text/plain; version=1.0.0; charset=utf-8`
- ✔    ... with erms_http_requests_total — `['erms_http_requests_total{method="GET",route="/health",service="request-service",status="200"} 4.0']`
- ✔    ... with erms_requests_created_total — `['erms_requests_created_total{type="water"} 159.0']`
- ✔    ... with erms_requests_published_total — `['erms_requests_published_total{result="ok",routing_key="request.created"} 21.0']`
- ✔    /metrics is not part of the documented client API — `hidden from /docs`
- ✔ coordinator: /metrics with the live state of all 6 storage nodes — `['erms_storage_node_up{node="storage-0b",shard="0"} 1.0', 'erms_storage_node_up{node="storage-0a",shard="0"} 1.0']`
- ✔    ... with erms_shard_ready_replicas — `['erms_shard_ready_replicas{shard="0"} 2.0']`
- ✔    ... with erms_storage_failovers_total — `['erms_storage_failovers_total 1.0']`
- ✔    ... with erms_storage_node_down_total — `['erms_storage_node_down_total{node="storage-0b",shard="0"} 0.0']`
- ✔    ... with erms_storage_degraded_writes_total — `['erms_storage_degraded_writes_total{shard="0"} 0.0']`
- ✔ matching-service: /metrics (called from inside the cluster) — `2 instance(s)`
- ✔    ... with erms_matches_total — `['erms_matches_total{result="matched",via="queue"} 1.0']`
- ✔    ... with erms_time_to_match_seconds_bucket — `['erms_time_to_match_seconds_bucket{le="0.5",via="queue"} 0.0']`
- ✔    ... with erms_messages_consumed_total — `['erms_messages_consumed_total{routing_key="request.created"} 1.0']`
- ✔    ... with erms_reconciler_fixes_total — `['erms_reconciler_fixes_total{kind="orphan_claim"} 0.0']`
- ✔ storage node storage-0a: /metrics with its own records and resync metrics — `erms_storage_local_keys{node="storage-0a",shard="0"} 1332.0`
- ✔ storage node storage-0b: /metrics with its own records and resync metrics — `erms_storage_local_keys{node="storage-0b",shard="0"} 1332.0`
- ✔ storage node storage-1a: /metrics with its own records and resync metrics — `erms_storage_local_keys{node="storage-1a",shard="1"} 1351.0`
- ✔ storage node storage-1b: /metrics with its own records and resync metrics — `erms_storage_local_keys{node="storage-1b",shard="1"} 1351.0`
- ✔ storage node storage-2a: /metrics with its own records and resync metrics — `erms_storage_local_keys{node="storage-2a",shard="2"} 1291.0`
- ✔ storage node storage-2b: /metrics with its own records and resync metrics — `erms_storage_local_keys{node="storage-2b",shard="2"} 1291.0`

#### [PASS] `test_x39_prometheus_scrapes_every_component` (0.3s) - covers X39.scrape, X39.rabbit

#39: Prometheus scrapes every component and every replica, including RabbitMQ's built-in plugin.

- ✔ job 'coordinator': 1 target(s), all up — `['coordinator']`
- ✔ job 'storage-node': 6 target(s), all up — `['storage-0a', 'storage-0b', 'storage-1a', 'storage-1b', 'storage-2a', 'storage-2b']`
- ✔ job 'request-service': 1 target(s), all up — `['172.18.0.12:8000']`
- ✔ job 'matching-service': 2 target(s), all up — `['172.18.0.13:8000', '172.18.0.14:8000']`
- ✔ job 'rabbitmq': 1 target(s), all up — `['rabbitmq']`
- ✔ RabbitMQ plugin: depth of queue matching.requests is collected — `0 waiting`
- ✔ RabbitMQ plugin: its consumer count is collected (= matching pods) — `2.0`
- ✔ the retry queue matching.retry is collected too — `matching.retry`
- ✔ ERMS metric families stored in Prometheus — `61 erms_* metric names`

#### [PASS] `test_x39_grafana_is_provisioned_and_secured` (0.3s) - covers X39.grafana, X39.secure

#39: Grafana comes up with the Prometheus datasource and the ERMS dashboard, viewable without login.

- ✔ datasource 'Prometheus' provisioned (http://prometheus:9090) — `['Prometheus']`
- ✔    and Grafana can reach Prometheus through it — `Successfully queried the Prometheus API.`
- ✔ dashboard 'ERMS overview' provisioned in folder 'ERMS' — `['ERMS overview']`
- ✔ it is Grafana's home page (opening Grafana shows it directly) — `/d/default-home-dashboard/erms-overview`
- ✔ anyone can VIEW the dashboard without logging in (anonymous viewer) — `GET without login -> 200`
- ✔ ... but nobody can change it without logging in — `POST without login -> 403`
- ✔ the admin login uses the generated password (from .env / Secret grafana-credentials) — `admin / 32-character random password`
- ✔ the default admin/admin is rejected — `admin/admin -> 401`

#### [PASS] `test_x39_dashboard_shows_everything_promised_with_data` (14.9s) - covers X39.panels, X39.nodata

#39: the dashboard has a panel for each promised view (queue depth, time to match, failovers, shard health, node up/down, resync duration, matching pods, API) and every panel query returns data, through Grafana.

- ✔ panel(s) for queue depth — `Queue depth | RabbitMQ queue depth`
- ✔ panel(s) for time from request to match — `Time from request to match`
- ✔ panel(s) for failovers — `Read failovers | Failures (per minute)`
- ✔ panel(s) for shard health — `Healthy shards | Ready replicas per shard`
- ✔ panel(s) for storage node up / down (red / green) — `Storage nodes (green = ready, red = down) | Storage nodes ready`
- ✔ panel(s) for resync duration — `Last resync duration per node`
- ✔ panel(s) for matching pods (autoscaling) — `Matching pods | Autoscaling: matching pods vs queue depth`
- ✔ panel(s) for API rate / latency / errors — `API requests per second, by status | API latency p95, by route`
- · dashboard size — `21 panels in 4 sections, refresh 5 s`
- ✔ all 33 panel queries return data when run through Grafana — `33/33 with data`

#### [PASS] `test_x39_metrics_follow_real_traffic` (7.9s) - covers X39.traffic

#39: creating and matching requests moves the counters by exactly that much, and time-to-match is right.

- ✔ created: +6 after 6 requests — `173 -> 179`
- ✔ published: +6 after 6 requests — `24 -> 30`
- ✔ POST /requests 201: +6 after 6 requests — `173 -> 179`
- ✔ matched via queue: +6 after 6 requests — `6 -> 12`
- ✔ time-to-match samples: +6 after 6 requests — `6 -> 12`
- ✔ the recorded time-to-match equals the real one (created_at -> matched_at of the requests) — `metric average 2.13s, requests' own timestamps 2.10s`

#### [PASS] `test_x39_storage_failure_is_visible_on_the_dashboard` (18.6s) - covers X39.failure

#39 demo moment: stop a storage node -> it turns DOWN (red) on the dashboard and failures are counted; restart it -> READY (green) again, with its resync and resync duration recorded.

- ✔ before: storage-1a shows READY (1, green) — `erms_storage_node_up{node="storage-1a"} = 1`
- · STOP the storage node — `docker compose stop storage-1a`
- ✔ during: the dashboard shows storage-1a DOWN (0, red) — `9s after the stop`
- ✔ during: 'Grafana -> Prometheus' returns the same (the panel's own query) — `erms_storage_node_up = 0`
- ✔ during: 'Ready replicas per shard' drops to 1 for shard 1 — `1 of 2`
- ✔ during: 'Healthy shards' stays 3 (the replica serves the shard) — `3`
- ✔ during: a write with one copy is counted as a degraded write — `6 -> 7`
- ✔ during: the node-down event is counted — `1 -> 2`
- · RESTART the storage node — `docker compose start storage-1a`
- ✔ after: the dashboard shows storage-1a READY (1, green) again — `6s after the restart`
- ✔ the node's timeline over the test shows the red stretch (min 0) and ends green (1) — `min over last 48s = 0, now = 1`
- ✔ after: the restarted node's resync is recorded (count, records applied, duration on the dashboard) — `resyncs 1, missed writes applied 1, resync took 60 ms`

#### [PASS] `test_x39_killing_the_read_replica_makes_failovers_rise` (14.1s) - covers X39.failure

#39 demo moment, the 'failovers rise' part: kill the storage replica that SERVES a shard's reads while reads keep coming -> reads switch to the other replica, the failover counter on the dashboard rises, the node is counted down, and after its restart it is READY (green) again.

- · BEFORE: reads of shard 1 are served by storage-1a
- · KILL the read replica — `docker compose stop storage-1a`
- · RESTART it — `docker compose start storage-1a`
- ✔ reads kept working: none failed while the replica was killed and came back — `35/35 succeeded (100%)`
- ✔ reads switched to the other replica of the shard — `{'storage-1a': 11, 'storage-1b': 24}`
- ✔ the dashboard's failover counter ROSE ('Read failovers' / 'Failures (per minute)' panels) — `erms_storage_failovers_total 1 -> 2`
- ✔    ... the same value through Grafana's own panel query — `sum(erms_storage_failovers_total) = 2`
- ✔ storage-1a was counted DOWN (red on the node timeline) — `node-down events 2 -> 3`
- ✔ after: storage-1a is READY (green) again on the dashboard — `erms_storage_node_up{node="storage-1a"} = 1`

#### [PASS] `test_ui_is_served_on_this_deployment` (0.2s) - covers UI.serve, UI.both

The web UI runs as its own container / pod and serves the app, its assets and its deployment settings.

- ✔ container 'frontend' is running — `docker compose service frontend`
- ✔ GET / -> 200 (text/html) — `text/html`
- ✔ GET /app.js -> 200 (javascript) — `application/javascript`
- ✔ GET /styles.css -> 200 (text/css) — `text/css`
- ✔ GET /vendor/leaflet/leaflet.js -> 200 (javascript) — `application/javascript`
- ✔ GET /healthz -> 200 (text/plain) — `text/plain`
- ✔ /config.json tells the app which deployment it is on and where Grafana etc. are — `{'platform': 'compose', 'grafanaPort': '3000', 'prometheusPort': '9090', 'rabbitPort': '15672', 'apiPort': '8080', 'coordinatorPort': '8001'}`

#### [PASS] `test_ui_proxies_the_backend_with_the_right_restrictions` (13.1s) - covers UI.proxy

The browser only talks to the UI's origin: /api, /coord (read-only), /prom (queries) are forwarded.

- ✔ /api/... reaches the request-service — `{'status': 'ok', 'instance': '072775896cc1', 'broker_connected': True}`
- ✔ /api/summary (dashboard counts) through the UI — `{'cancelled': 1992, 'matched': 470, 'pending': 358, 'resolved': 52, 'unmatched': 94}`
- ✔ /api/requests?order=desc&limit=3 returns the 3 newest — `3 of 2966`
- ✔ /coord/cluster (read) is allowed — `200`
- ✔ /coord PUT (a storage write) is blocked — `403`
- ✔ /coord DELETE (a storage write) is blocked — `403`
- ✔ /coord POST (a storage write) is blocked — `403`
- ✔ /prom/api/v1/query works — `6`
- ✔ /prom/api/v1/admin/tsdb/snapshot (Prometheus admin / config) is blocked — `403`
- ✔ /prom/-/reload (Prometheus admin / config) is blocked — `403`
- ✔ /prom/api/v1/status/config (Prometheus admin / config) is blocked — `403`
- ✔ Grafana may be embedded in the UI (no X-Frame-Options) — `none`
- ✔ after the API is restarted (new container / pods) the UI reaches it again — `docker compose restart request-service: back 2s later`

#### [PASS] `test_ui_every_tab_loads_without_errors` (49.9s) - covers UI.errors, UI.health

Open every tab; no JavaScript error, no failed call; the header shows live health.

- ✔ tab 'overview' renders — `overview`
- ✔ tab 'requests' renders — `requests`
- ✔ tab 'resources' renders — `resources`
- ✔ tab 'map' renders — `map`
- ✔ tab 'storage' renders — `storage`
- ✔ tab 'messaging' renders — `messaging`
- ✔ tab 'monitoring' renders — `monitoring`
- ✔ no JavaScript errors or failed calls on any tab — `none`
- ✔ header: API up — `API up`
- ✔ header: RabbitMQ connected — `RabbitMQ connected`
- ✔ header: all 3 shards healthy — `Shards 3/3`
- ✔ header: all 6 storage nodes ready — `Nodes 6/6 ready`
- ✔ header: matching pods counted (from Prometheus) — `Matching pods 2`

#### [PASS] `test_ui_overview_matches_the_backend` (9.6s) - covers UI.overview

The Overview numbers are the backend's numbers.

- ✔ 8 key figures are shown — `['Requests', 'Waiting for help', 'Being helped', 'Resolved', 'Resources available', 'Storage nodes ready', 'Queue (waiting)', 'Matching pods']`
- ✔ 'Requests' equals the backend total — `UI 2966, API 2966`
- ✔ 'Storage nodes ready' shows 6/6 — `6/6`
- ✔ requests by status: one bar per status (6) — `['pending', 'matched', 'in', 'resolved', 'cancelled', 'unmatched']`
- ✔ latest requests table is filled — `8 rows`
- ✔ the storage nodes are listed with their status — `6 ready`

#### [PASS] `test_ui_register_and_update_a_resource` (22.4s) - covers UI.resources

Register a resource in the form, then change its availability and status from its detail view.

- ✔ registered from the form: detail view opens with the new id — `res-water-bcde27579830`
- ✔ the backend has it as entered — `UI water tanker, ngo, 3 units, available`
- ✔ availability changed in the UI -> backend has 5 units — `5`
- ✔ 'Set offline' -> backend status offline — `offline`
- ✔ 'Set available' -> backend status available — `available`
- ✔ the list (type filter 'water') shows it, and only water resources — `{'water'}`
- ✔ a 'volunteer' can be registered from the form — `res-food-8c6c2b7bb15a`
- ✔ a 'other' can be registered from the form — `res-food-de7ee9e165d8`

#### [PASS] `test_ui_request_lifecycle_and_match_now` (27.4s) - covers UI.requests, UI.match

Create a request in the form, match it with 'Match now', move it through its statuses - all in the UI.

- ✔ created from the form: the backend has type, location, details, priority — `req-a2e46f81265a`
- ✔ detail view: status pending, storage shard and serving node shown — `shard 0`
- ✔ 'Match now' -> matched to the nearest water resource — `Matched to UI tanker (2.00 km)`
- ✔ the assignment (resource, distance, matched by) is shown — `assignment section`
- ✔ the resource lost one unit (2 -> 1) — `1`
- ✔ 'Mark in progress' -> in_progress — `in_progress`
- ✔ 'Mark resolved' -> resolved, unit released (1 -> 2) — `resolved`
- ✔ a resolved request offers no more actions — `terminal`
- ✔ history: pending -> matched -> in progress -> resolved — `['pending', 'matched', 'in progress', 'resolved']`
- ✔ 'Re-open' a matched request -> pending, unit released — `req-0ebafac35956`
- ✔ 'Cancel' -> cancelled — `cancelled`
- ✔ 'Match now' without a suitable resource shows the API's 409 reason — `No match: no suitable available resource`
- ✔ list filter status=resolved applies at once: only resolved requests (incl. ours) — `{'resolved'}`

#### [PASS] `test_ui_request_matched_automatically_through_rabbitmq` (8.4s) - covers UI.async

A request created with 'Match automatically' goes through RabbitMQ and shows up as matched by a worker.

- ✔ the UI reports it stored and queued in RabbitMQ — `Request req-4d85667cede3 created on shard 1 and queued in RabbitMQ`
- ✔ matched through the queue consumer — `e22c21dddffb`
- ✔ its detail view says it was matched by the RabbitMQ consumer — `via queue`

#### [PASS] `test_ui_map` (6.4s) - covers UI.map

The map shows requests and resources; clicking the map pre-fills the forms with that location.

- ✔ requests and resources are drawn on the map — `852 markers / lines`
- ✔ clicking the map -> 'New request here' opens the form with that location — `latitude -79.99717`

#### [PASS] `test_ui_storage_cluster_and_shard_lookup` (3.5s) - covers UI.storage

Shards, nodes, records and the shard lookup of any record.

- ✔ 3 shard cards, each healthy — `3 shards`
- ✔ 6 storage nodes with records and heartbeat age — `1337 records · heartbeat 1.2s ago`
- ✔ records per shard and the read / failover statistics are shown
- ✔ lookup of a request id: the shard the coordinator chose and its replicas — `shard 0, replicas ['storage-0a', 'storage-0b']`
- ✔ the browser computes sha256(key) mod 3 itself and gets the same shard — `request:req-2fc3c95c55e8`
- ✔ the failure-demo commands match this deployment — `docker compose stop storage-1a     # stop the node that serves shard 1's reads`

#### [PASS] `test_ui_shows_a_storage_failure_live` (17.1s) - covers UI.failure

Stop a storage node: the UI shows it down (and 5/6 in the header); restart it: ready again.

- ✔ before: storage-1a shown ready — `ready`
- · STOP — `docker compose stop storage-1a`
- ✔ during: the UI shows storage-1a DOWN — `1362 records · heartbeat 9.9s ago · no heartbeat for 6.5s`
- ✔ during: the header shows Nodes 5/6 ready — `Nodes 5/6 ready`
- · RESTART — `docker compose start storage-1a`
- ✔ after: the UI shows storage-1a ready again and Nodes 6/6 — `Nodes 6/6 ready`

#### [PASS] `test_ui_messaging_charts_and_burst` (5.4s) - covers UI.messaging

Queue / matching figures from Prometheus, 3 charts, and the burst generator sending requests.

- ✔ 8 figures: queue depth, processing, retry, consumers, matching pods, matched, time to match p50 / p95 — `['Waiting in queue', 'Being processed', 'Delayed retry', 'Consumers', 'Matching pods', 'Matched (15 min)', 'Time to match p50', 'Time to match p95']`
- ✔ 'Matching pods' equals the running matching instances — `2`
- ✔ 3 charts drawn from Prometheus (queue vs pods, time to match, created vs matched) — `svg charts`
- ✔ 'Send burst' sends 30 requests (and registers a resource for them) — `30/30 requests accepted in 0.2s and queued in RabbitMQ.`

#### [PASS] `test_ui_monitoring_tab` (10.5s) - covers UI.monitoring

Grafana dashboard embedded and live, links to the other tools, scrape-target health.

- ✔ the Grafana 'ERMS overview' dashboard is embedded (kiosk mode) — `http://localhost:3000/d/erms-overview/erms-overview?orgId=1&kiosk&theme=light&refresh=5s&from=now-15m&to=now`
- ✔ ... and it really renders inside the UI (panel 'Healthy shards' visible) — `Grafana panel text found`
- ✔ links to Grafana, Prometheus, RabbitMQ UI, Swagger, coordinator with this deployment's ports — `['Open Grafana', 'Prometheus', 'RabbitMQ UI', 'API docs (Swagger)', 'Coordinator /cluster']`
- ✔ Prometheus scrape targets listed, all up — `12/12 up`
