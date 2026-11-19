# ERMS - specification compliance report

* **compose** (API http://localhost:8080  coordinator http://localhost:8001  RabbitMQ http://localhost:15672  Grafana http://localhost:3000), 2026-11-19 23:43:36 -> 2026-11-19 23:46:54: 48 passed, 0 failed, 12 skipped

## Requirement matrix

### §1 Project objective and operations

| Verdict | Id | Requirement (PDF) | Verified by |
|---|---|---|---|
| **PASS** | 1.a | Connect people requiring emergency assistance with available volunteers and resources | `test_flood_victim_needs_drinking_water_end_to_end` [compose] |
| **PASS** | 1.b | Support requests such as food, water, medicine, transportation, or shelter | `test_supported_request_types` [compose] |
| **PASS** | 1.c | Match requests with suitable available resources based on resource type and location | `test_matching_considers_location` [compose]<br>`test_matching_considers_resource_type` [compose]<br>`test_supported_request_types` [compose] |
| **PASS** | 1.d | Demonstrate distributed storage, asynchronous communication, sharding, replication, fault tolerance, containerization and Kubernetes-based deployment | `test_components_run_as_containers` [compose]<br>`test_data_distributed_across_three_shards_and_nodes` [compose]<br>`test_k8s_deployments` [compose] (skipped)<br>`test_multiple_requests_processed_through_rabbitmq` [compose]<br>`test_storage_node_failure_before_during_after` [compose] |
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
| **SKIP** | 2.k8s | Orchestration: Kubernetes - mandatory | `test_k8s_deployments` [compose] (skipped) |
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
| **PASS** | 8.replica | Use an available replica when the required data remains accessible | `test_k8s_storage_pod_killed_reads_fail_over_then_resync` [compose] (skipped)<br>`test_storage_node_failure_before_during_after` [compose] |
| **PASS** | 8.continue | Continue processing when sufficient replicas are available | `test_storage_node_failure_before_during_after` [compose] |
| **PASS** | 8.sync | Synchronize the required data when a failed node recovers | `test_k8s_storage_pod_killed_reads_fail_over_then_resync` [compose] (skipped)<br>`test_storage_node_failure_before_during_after` [compose] |
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
| **SKIP** | 9.2.a | Kubernetes Deployments | `test_k8s_deployments` [compose] (skipped) |
| **SKIP** | 9.2.b | Kubernetes Services | `test_k8s_services` [compose] (skipped) |
| **SKIP** | 9.2.c | Pod replicas | `test_k8s_pod_replicas` [compose] (skipped)<br>`test_k8s_scale_request_service_with_continued_availability` [compose] (skipped) |
| **PASS** | 9.2.d | Service-to-service communication | `test_containers_communicate` [compose] |
| **SKIP** | 9.2.e | Microservice scaling | `test_k8s_scale_matching_service_with_continued_availability` [compose] (skipped)<br>`test_k8s_scale_request_service_with_continued_availability` [compose] (skipped) |
| **SKIP** | 9.2.f | Pod failure and automatic recovery | `test_k8s_pod_failure_and_automatic_recovery` [compose] (skipped)<br>`test_k8s_storage_pod_killed_reads_fail_over_then_resync` [compose] (skipped) |
| **SKIP** | 9.2.local | Deployed locally with Minikube, kind or Docker Desktop Kubernetes (cloud optional) | `test_k8s_runs_on_a_local_cluster` [compose] (skipped) |

### §10 Required demonstration

| Verdict | Id | Requirement (PDF) | Verified by |
|---|---|---|---|
| **PASS** | 10.create | Request Creation: create and retrieve an emergency request | `test_createRequest` [compose]<br>`test_getRequest` [compose] |
| **PASS** | 10.register | Resource Registration: register an available resource or volunteer | `test_registerResource` [compose] |
| **PASS** | 10.match | Resource Matching: match a request with an appropriate available resource | `test_flood_victim_needs_drinking_water_end_to_end` [compose]<br>`test_matchRequest` [compose] |
| **PASS** | 10.dist | Data Distribution: distribution across the three storage shards | `test_data_distributed_across_three_shards_and_nodes` [compose] |
| **PASS** | 10.async | Asynchronous Processing: submit multiple requests, processed through RabbitMQ | `test_multiple_requests_processed_through_rabbitmq` [compose] |
| **PASS** | 10.fail | Storage Failure: stop a storage node, continued operation using an available replica | `test_k8s_storage_pod_killed_reads_fail_over_then_resync` [compose] (skipped)<br>`test_storage_node_failure_before_during_after` [compose] |
| **PASS** | 10.recover | Node Recovery: restart the failed node, restoration / synchronization of the data | `test_k8s_storage_pod_killed_reads_fail_over_then_resync` [compose] (skipped)<br>`test_storage_node_failure_before_during_after` [compose] |
| **SKIP** | 10.k8srecover | Kubernetes Recovery: terminate a running application pod, automatic recovery | `test_k8s_pod_failure_and_automatic_recovery` [compose] (skipped) |
| **SKIP** | 10.k8sscale | Kubernetes Scaling: increase the replicas of a microservice, continued availability | `test_k8s_scale_matching_service_with_continued_availability` [compose] (skipped)<br>`test_k8s_scale_request_service_with_continued_availability` [compose] (skipped) |

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
| **PASS** | 11.g | Testing: replica availability during storage-node failure | `test_k8s_storage_pod_killed_reads_fail_over_then_resync` [compose] (skipped)<br>`test_storage_node_failure_before_during_after` [compose] |
| **PASS** | 11.h | Testing: recovery and data synchronization after node recovery | `test_k8s_storage_pod_killed_reads_fail_over_then_resync` [compose] (skipped)<br>`test_storage_node_failure_before_during_after` [compose] |
| **SKIP** | 11.i | Testing: Kubernetes pod failure and automatic recovery | `test_k8s_pod_failure_and_automatic_recovery` [compose] (skipped) |
| **SKIP** | 11.j | Testing: Kubernetes-based microservice scaling | `test_k8s_scale_matching_service_with_continued_availability` [compose] (skipped)<br>`test_k8s_scale_request_service_with_continued_availability` [compose] (skipped) |

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
| **SKIP** | X18.install | KEDA is installed in the cluster (operator, metrics API server, admission webhooks) | `test_x18_keda_is_installed` [compose] (skipped) |
| **SKIP** | X18.scaledobject | ScaledObject scales Deployment matching-service on queue matching.requests (2-6 pods, 5 waiting messages per pod) and is Ready | `test_x18_scaledobject_hpa_and_credentials` [compose] (skipped) |
| **SKIP** | X18.hpa | KEDA drives a HorizontalPodAutoscaler for matching-service; no conflicting autoscaler | `test_x18_scaledobject_hpa_and_credentials` [compose] (skipped) |
| **SKIP** | X18.secret | KEDA's broker credentials come from a Secret (TriggerAuthentication), not from the manifests | `test_x18_scaledobject_hpa_and_credentials` [compose] (skipped) |
| **SKIP** | X18.up | A burst of requests piling up in RabbitMQ scales matching-service up automatically (2 -> 6) | `test_x18_burst_scales_up_then_back_down` [compose] (skipped) |
| **SKIP** | X18.down | When the queue has drained matching-service scales back down to 2 automatically | `test_x18_burst_scales_up_then_back_down` [compose] (skipped) |
| **SKIP** | X18.safe | While pods are added and removed no request is lost, none matched twice, capacity exact, and the API stays available | `test_x18_burst_scales_up_then_back_down` [compose] (skipped) |
| **SKIP** | X18.visible | The autoscaling is visible in Prometheus / Grafana (matching pods vs queue depth) | `test_x18_burst_scales_up_then_back_down` [compose] (skipped) |
| **SKIP** | X18.manual | Manual scaling / stopping still works with KEDA (pin the replica count), then automatic scaling resumes | `test_x18_manual_pin_then_automatic_again` [compose] (skipped) |

## Every check performed

### Target: compose

#### [PASS] `test_exactly_one_coordinator` (0.6s) - covers 2.coord, 4.b

§2 Table 1 'Coordinator: 1' / §4 'one coordinator': exactly one coordinator runs, every storage node is registered with it and every service sends its storage operations to it.

- ✔ exactly one coordinator container/pod is running — `1 running`
- ✔ the coordinator answers GET /health — `{'status': 'ok', 'healthy_shards': [0, 1, 2], 'nodes': 6}`
- ✔ all 6 storage nodes are registered with this one coordinator — `['storage-0a', 'storage-0b', 'storage-1a', 'storage-1b', 'storage-2a', 'storage-2b']`
- ✔ request-service sends every storage operation to the coordinator — `COORDINATOR_URL=http://coordinator:8000`
- ✔ matching-service sends every storage operation to the coordinator — `COORDINATOR_URL=http://coordinator:8000`
- ✔ storage nodes register / heartbeat with the coordinator — `COORDINATOR_URL=http://coordinator:8000`

#### [PASS] `test_two_to_three_microservices` (1.1s) - covers 2.ms

§2 Table 1 'Microservices: 2-3': the application is split into separately deployed microservices.

- ✔ microservice 'request-service' is deployed on its own (client REST API; stores requests; publishes them to RabbitMQ) — `1 instance(s)`
- ✔ microservice 'matching-service' is deployed on its own (consumes RabbitMQ messages; finds and assigns resources) — `2 instance(s)`
- ✔ microservice 'coordinator' is deployed on its own (storage-layer service: shard routing, replication, failure detection) — `1 instance(s)`
- ✔ request-service is healthy — `{'status': 'ok', 'instance': '2c512aac0a69', 'broker_connected': True}`
- ✔ matching-service is healthy (called from inside the request-service container) — `{"status":"ok","worker":"b4a771e441a7","broker_connected":true}`
- ✔ 2-3 microservices: 2 application services + the coordinator = 3 — `request-service, matching-service, coordinator; storage nodes are the shards, RabbitMQ the broker`

#### [PASS] `test_one_rabbitmq_instance` (0.9s) - covers 2.mq, 6.mq

§2 Table 1 'Message broker: 1 RabbitMQ instance', used by the request and matching services.

- ✔ exactly one RabbitMQ container/pod is running — `1 running`
- ✔ the broker is a single RabbitMQ node (not a cluster) — `rabbit@rabbitmq`
- · RabbitMQ version — `3.13.7`
- ✔ request-service is configured to use that broker — `RABBITMQ_URL host = rabbitmq:5672`
- ✔ matching-service is configured to use that broker — `RABBITMQ_URL host = rabbitmq:5672`
- ✔ every request-service and matching-service instance is connected to it — `3 AMQP connections (expected >= 3)`

#### [PASS] `test_three_shards_each_with_a_replica` (1.7s) - covers 2.shards, 2.repl, 4.b

§2 'Storage shards: 3', 'Replication: at least 1 replica per shard' / §4 'three storage shards, and at least one replica for each shard'.

- ✔ the storage layer has exactly 3 shards — `num_shards=3`
- ✔ shard 0 = 1 primary copy + at least 1 replica — `['storage-0a', 'storage-0b']`
- ✔ shard 0: every copy is up and ready — `{'storage-0a': 'ready', 'storage-0b': 'ready'}`
- ✔ shard 1 = 1 primary copy + at least 1 replica — `['storage-1a', 'storage-1b']`
- ✔ shard 1: every copy is up and ready — `{'storage-1a': 'ready', 'storage-1b': 'ready'}`
- ✔ shard 2 = 1 primary copy + at least 1 replica — `['storage-2a', 'storage-2b']`
- ✔ shard 2: every copy is up and ready — `{'storage-2a': 'ready', 'storage-2b': 'ready'}`
- ✔ 6 storage-node containers run (3 shards x 2) — `['storage-0a', 'storage-0b', 'storage-1a', 'storage-1b', 'storage-2a', 'storage-2b']`
- ✔ the replicas of each shard hold the same number of records — `key counts per shard: {'0': [760, 760], '1': [816, 816], '2': [799, 799]}`

#### [PASS] `test_one_or_more_clients` (0.5s) - covers 2.clients

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
- ✔ the new request got an id — `req-3581f35db160`
- ✔ 'type' is stored as submitted — `water`
- ✔ 'location' is stored as submitted — `{'lat': -49.7417, 'lon': -175.231514, 'area': 'spec-b7729d'}`
- ✔ 'details' is stored as submitted — `two people stranded on a roof, need drinking water`
- ✔ a new request starts in status 'pending' — `pending`
- ✔ it was handed to RabbitMQ for asynchronous matching — `queued=True`
- ✔ it was written to its shard on both copies — `{'shard': 0, 'replicas': ['storage-0a', 'storage-0b'], 'version': 1795112025752363159, 'degraded': False}`

#### [PASS] `test_getRequest` (0.1s) - covers 1.op5, 5.2, 10.create, 11.a

§1 getRequest(requestId) - 'Retrieve request information' = GET /requests/{id} (200).

- ✔ GET /requests/{id} -> 200 OK — `200`
- ✔ returned 'id' equals what was created — `req-fb209ef447e8`
- ✔ returned 'type' equals what was created — `food`
- ✔ returned 'location' equals what was created — `{'lat': -49.7417, 'lon': -170.731514, 'area': 'spec-b7729d'}`
- ✔ returned 'details' equals what was created — `food for 12 people in a flooded school`
- ✔ returned 'status' equals what was created — `pending`
- ✔ returned 'created_at' equals what was created — `2026-11-19T18:13:45.809+00:00`
- ✔ the read was served by a copy of the request's own shard — `shard 1, served_by storage-1a`
- ✔ reading again returns the same data

#### [PASS] `test_registerResource` (0.1s) - covers 1.op2, 5.4, 10.register, 11.b

§1 registerResource(type, location, availability) - 'Register a resource or volunteer' = POST /resources.

- ✔ POST /resources with exactly {type, location, availability} -> 201 Created — `201`
- ✔ the resource got an id — `res-water-c79f250e8771`
- ✔ 'type' is stored as submitted — `water`
- ✔ 'location' is stored as submitted — `{'lat': -49.732717, 'lon': -166.231514, 'area': 'spec-b7729d'}`
- ✔ 'availability' is stored as submitted — `5`
- ✔ a resource with capacity starts 'available' — `available`
- ✔ a volunteer can be registered too (kind=volunteer) -> 201 — `res-transportation-c3cc4ec2c3da`
- ✔ the registered resource appears in GET /resources — `209 water resources`
- ✔ and can be read back by id — `200`

#### [PASS] `test_matchRequest` (0.2s) - covers 1.op3, 5.7, 10.match, 11.c, 7.5, 7.6

§1 matchRequest(requestId) - 'Find and assign a suitable available resource' = POST /match/{requestId}.

- ✔ POST /match/{requestId} -> 200 OK — `200`
- ✔ a resource was found and assigned — `Water tanker (res-water-25335af8ddaf) at 1.998 km`
- ✔ the distance is computed from the two locations — `1.998`
- ✔ the request is now 'matched' to that resource — `{'resource_id': 'res-water-25335af8ddaf', 'resource_name': 'Water tanker', 'kind': 'resource', 'distance_km': 1.998, 'matched_at': '2026-11-19T18:13:46.133+00:00', 'matched_by': 'a35e580b12de', 'via': 'api'}`
- ✔ one unit of the resource is assigned to the request — `availability 3 -> 2, assigned_requests=['req-a6c6e237e3c7']`
- ✔ matching an already-matched request again does not take a second unit — `{'result': 'skipped', 'request_id': 'req-a6c6e237e3c7', 'status': 'matched', 'assigned_resource': 'res-water-25335af8ddaf'}`

#### [PASS] `test_updateStatus` (0.3s) - covers 1.op4, 5.3, 11.a

§1 updateStatus(requestId, status) - 'Update the status of a request' = PUT /requests/{id}/status (200).

- ✔ PUT /requests/{id}/status {'status': 'in_progress'} -> 200 OK — `200`
- ✔ the stored status is now 'in_progress' — `previous_status=matched`
- ✔ PUT /requests/{id}/status {'status': 'resolved'} -> 200 OK — `200`
- ✔ the stored status is now 'resolved' — `previous_status=in_progress`
- ✔ resolving the request released the resource's capacity — `availability back to 1, status available`
- ✔ every status change is recorded in the request history — `['pending', 'matched', 'in_progress', 'resolved']`
- ✔ updated_at moves forward — `2026-11-19T18:13:46.285+00:00 -> 2026-11-19T18:13:46.425+00:00`
- ✔ a pending request can be cancelled -> 200 — `200`

#### [PASS] `test_supported_request_types` (0.9s) - covers 1.b, 3.1.types, 1.c

§1 'requests such as food, water, medicine, transportation, or shelter' / §3.1 example request types.

- ✔ a 'water' request is accepted (201) — `req-ab37ce831020`
- ✔ the 'water' request is matched to the 'water' resource — `matched -> water provider`
- ✔ a 'food' request is accepted (201) — `req-5ab3cbcc6cfd`
- ✔ the 'food' request is matched to the 'food' resource — `matched -> food provider`
- ✔ a 'medicine' request is accepted (201) — `req-cee8b9f52941`
- ✔ the 'medicine' request is matched to the 'medicine' resource — `matched -> medicine provider`
- ✔ a 'transportation' request is accepted (201) — `req-5dccaff6c37d`
- ✔ the 'transportation' request is matched to the 'transportation' resource — `matched -> transportation provider`
- ✔ a 'shelter' request is accepted (201) — `req-eff6ac47721f`
- ✔ the 'shelter' request is matched to the 'shelter' resource — `matched -> shelter provider`
- ✔ an unknown request type is rejected (422) — `Value error, type must be one of ['food', 'medicine', 'other', 'rescue', 'shelter', 'trans`

#### [PASS] `test_request_record_fields` (1.2s) - covers 3.1.id, 3.1.type, 3.1.loc, 3.1.details, 3.1.status, 3.1.created, 11.a

§3.1 'Each request should contain, at minimum: unique request ID, request type, location, request details, current status, creation time'.

- ✔ unique request ID: present — `req-47aeeaf67733`
- ✔ unique request ID: 25 requests created at the same time all got different ids — `25 distinct of 25`
- ✔ request type — `medicine`
- ✔ location (latitude / longitude) — `{'lat': -49.7417, 'lon': -148.231514, 'area': 'spec-b7729d'}`
- ✔ request details — `insulin for a diabetic patient`
- ✔ current status — `pending`
- ✔ creation time (ISO-8601 timestamp with time zone, taken when the request was created) — `2026-11-19T18:13:47.470+00:00`

#### [PASS] `test_resource_record_fields_and_kinds` (0.8s) - covers 3.2.id, 3.2.type, 3.2.loc, 3.2.avail, 3.2.status, 3.2.kinds, 11.b

§3.2 'Each resource should contain, at minimum: unique resource ID, resource type, location, availability, current status' and 'may represent a physical resource, volunteer, NGO, or other available source'.

- ✔ unique resource ID: present — `res-shelter-b4af9a4d68ff`
- ✔ unique resource ID: 15 resources registered at the same time all got different ids — `15 distinct of 15`
- ✔ resource type — `shelter`
- ✔ location — `{'lat': -49.7417, 'lon': -143.731514, 'area': 'spec-b7729d'}`
- ✔ availability (units of capacity) — `40`
- ✔ current status — `available`
- ✔ a resource of kind 'resource' can be registered — `res-food-3c3dc862ec23`
- ✔ a resource of kind 'volunteer' can be registered — `res-food-0608ba81f540`
- ✔ a resource of kind 'ngo' can be registered — `res-food-2bee82a7f465`
- ✔ a resource of kind 'other' can be registered — `res-food-296269e0efb9`
- ✔ an unknown kind is rejected (422) — `422`

#### [PASS] `test_system_maintains_requests_and_resources` (1.9s) - covers 3.maintain, 5.5

§3 'The system should maintain information about emergency requests and available resources' and §5 GET /resources 'List resources' (200).

- ✔ GET /resources -> 200 OK with a JSON list — `655 resources across all shards`
- ✔ the list contains the resources just registered (from whichever shard holds them) — `res-water-fa3dd53c9f92 (shard 0), res-food-3c7da47bdbb9 (shard 0)`
- ✔ GET /resources?type=water lists only water resources — `213 items`
- ✔ GET /resources?status=available lists only available ones — `40 items`
- ✔ no shard was missing from the list — `[]`
- ✔ requests are kept as well (GET /requests) — `1229 water requests`

#### [PASS] `test_resource_availability_updates` (0.9s) - covers 5.6, 11.b, 3.2.avail, 3.2.status

§5 PUT /resources/{id}/status (200) and §11 'Resource registration and availability updates'.

- ✔ PUT /resources/{id}/status {'availability': 5} -> 200 OK — `availability 3 -> 5`
- ✔ PUT {'status': 'offline'} -> 200, resource offline — `offline`
- ✔ an offline resource is not used for matching (409: no suitable resource) — `409`
- ✔ back to 'available' -> the same request is now matched to it — `matched`
- ✔ matching consumed one unit (5 -> 4) — `4`
- ✔ availability 0 -> status becomes 'busy' automatically — `busy`
- ✔ availability 2 -> 'available' again — `available`

#### [PASS] `test_flood_victim_needs_drinking_water_end_to_end` (3.6s) - covers 1.1.a, 1.1.b, 1.a, 7.1, 7.2, 7.3, 7.4, 7.5, 7.6, 6.mq, 11.e, 11.cond, 10.match

§1.1 example scenario and §7 steps 1-6: 'a user affected by flooding requires drinking water. The request is submitted, processed asynchronously, matched with an available resource, and the request status is updated.' User -> Request Service -> RabbitMQ -> Matching Service -> Distributed Storage -> Resource Assigned.

- ✔ 1. the client submits the request: POST /requests -> 201 — `201`
- ✔    the API answers at once, before any matching happened (asynchronous): status 'pending' — `answered in 27 ms`
- ✔ 2. the Request Service stored the request in the distributed storage — `shard 2, read from storage-2a`
- ✔    the record is physically on both copies of its shard — `{'storage-2a': 200, 'storage-2b': 200}`
- ✔ 3. the request was published to RabbitMQ (queued: true) — `True`
- ✔    RabbitMQ received it into queue 'matching.requests' — `publish counter 11 -> 12`
- ✔ 4. the Matching Service consumed the message (matched by the queue consumer, not the sync API) — `via=queue`
- ✔    ... in one of the matching-service instances — `a35e580b12de (running now: ['a35e580b12de', 'b4a771e441a7'])`
- ✔    the message was acknowledged to RabbitMQ after processing — `ack counter 70 -> 74`
- ✔ 5. a suitable available resource was identified: same type, available, the nearest one — `NGO water tanker at 2.997 km (the other water resource is 20 km away)`
- ✔ 6. the request status was updated to 'matched' — `matched`
- ✔    the resource was assigned: capacity 2 -> 1, request in its assigned list — `availability 1`
- ✔    the assignment is persisted in the distributed storage — `2026-11-19T18:13:53.053+00:00`
- ✔ help delivered: status -> in_progress -> resolved, tanker capacity released (1 -> 2) — `2`
- ✔ the request history shows the whole lifecycle — `['pending', 'matched', 'in_progress', 'resolved']`
- · the path the request took — `User -> request-service (2c512aac0a69) -> RabbitMQ (erms.requests -> matching.requests) -> matching-service (a35e580b12de) -> storage shard 2 ['storage-2a', 'storage-2b'] -> resource assigned (NGO water tanker)`

#### [PASS] `test_matching_considers_resource_type` (0.4s) - covers 7.type, 1.c, 11.c

§7 'The matching mechanism should consider at least resource type' (and location).

- ✔ a medicine request is matched to the medicine resource, not the closer food resource — `pharmacy, 10 km at 9.989 km`
- ✔ the food resource was not touched
- ✔ a shelter request with only food/medicine nearby gets no match (409) — `no suitable available resource`

#### [PASS] `test_matching_considers_location` (1.0s) - covers 7.loc, 1.c, 11.c

§7 'The matching mechanism should consider at least resource type and location'.

- ✔ closest first: 3 requests at the same place get the 5 km, then 15 km, then 25 km resource — `['5 km (4.994 km)', '15 km (14.983 km)', '25 km (24.972 km)']`
- ✔ a resource beyond the matching radius (50 km) is not assigned: 80 km -> 409 — `{'message': 'no suitable available resource', 'result': 'no_match', 'request_id': 'req-3a672ea8b9ac', 'candidates': 0, 'unavailable_shards': []}`
- ✔ a resource within 50 km is assigned: 40 km -> 200 — `39.955 km`

#### [PASS] `test_matching_uses_only_available_resources` (0.5s) - covers 7.5, 11.c

§7 step 5 'A suitable AVAILABLE resource is identified': offline or fully-used resources are skipped.

- ✔ a resource with availability 0 is registered as 'busy' — `busy`
- ✔ the request skips the offline (1 km) and busy (2 km) ones and gets the available one (9 km) — `available, 9 km at 8.99 km`
- ✔ after its last unit is taken the resource is 'busy' — `busy`
- ✔ so a further request gets no match (409)

#### [PASS] `test_multiple_requests_processed_through_rabbitmq` (13.3s) - covers 10.async, 11.d, 11.e, 6.mq, 1.d

§10 'Asynchronous Processing: submit multiple requests and demonstrate processing through RabbitMQ' and §11 'RabbitMQ message delivery and processing'.

- ✔ 15 requests submitted at the same time are all accepted (201) and queued — `in 0.33s`
- ✔ RabbitMQ buffered the burst while the matching workers processed it — `peak backlog in matching.requests: 16 messages`
- ✔ all 15 requests were matched — `8.4s after submission`
- ✔ every one of them came through the RabbitMQ consumer
- ✔ the work was shared by the matching-service instances (competing consumers) — `{'b4a771e441a7': 8, 'a35e580b12de': 7}`
- ✔ RabbitMQ counted >= 15 messages published and acknowledged — `publish +15, ack +19`
- ✔ exactly 15 units were assigned, one per request, none twice — `availability 15 -> 0, status busy`

#### [PASS] `test_simultaneous_requests_never_double_assign` (9.0s) - covers 11.d, 11.c

§11 'Multiple simultaneous requests': many requests created and matched at the same moment never over-assign a resource, and racing matches of the same request take only one unit.

- ✔ 30 simultaneous creations: all 201 with distinct ids — `spread over shards {0: 16, 2: 6, 1: 8}`
- ✔ 30 simultaneous POST /match against 20 units: exactly 20 succeed (200), 10 get 409 — `{200: 20, 409: 10}`
- ✔ the resource is never over-assigned: availability exactly 0, 'busy' — `availability 0, busy`
- ✔ its assigned list is exactly the matched requests (no duplicates, no strays) — `20`
- ✔ 8 simultaneous POST /match of the SAME request all answer 200 — `Counter({'skipped': 7, 'matched': 1})`
- ✔ ... and it holds exactly one unit (5 -> 4), listed once — `['req-3f46736a7807']`
- ✔ the request is matched to that resource — `matched`

#### [PASS] `test_rabbitmq_topology` (0.5s) - covers 6.mq, 11.e

§6 'RabbitMQ should be used for asynchronous communication between the request-processing and matching components' - exchange, queue, binding, consumers and retry path.

- ✔ request-service publishes to exchange 'erms.requests' (topic, durable) — `type=topic durable=True`
- ✔ queue 'matching.requests' is durable (survives broker restarts) — `True`
- ✔ erms.requests is bound to matching.requests with routing key 'request.#' — `[('matching.requests', 'request.#')]`
- ✔ every matching-service instance is a consumer of matching.requests — `2 consumers, 2 matching instances`
- ✔ unmatched requests are retried later via 'matching.retry' (TTL, dead-lettered back to erms.requests) — `ttl=5000 ms`
- ✔ request-service is connected to the broker

#### [PASS] `test_data_distributed_across_three_shards_and_nodes` (5.6s) - covers 4.a, 10.dist, 11.f, 1.d

§4 'Requests and resources must be stored across multiple storage nodes' / §10 'Data Distribution: demonstrate distribution across the three storage shards' - checked on each node's own disk.

- ✔ requests are spread over all 3 shards — `12 requests: {0: 3, 1: 5, 2: 4}`
- ✔ resources are spread over all 3 shards — `10 resources: {0: 6, 1: 2, 2: 2}`
- ✔ every shard holds data — `records per node: {'0': [822, 823], '1': [869, 867], '2': [835, 835]}`
- ✔ request on shard 0 is on the disk of exactly its shard's 2 nodes — `request:req-3240f5b57a30 -> ['storage-0a', 'storage-0b']`
- ✔    both copies are identical (same version) — `{1795112059185886422}`
- ✔ resource on shard 0 is on the disk of exactly its shard's 2 nodes — `resource:food:res-food-ab383f8e7909 -> ['storage-0a', 'storage-0b']`
- ✔    both copies are identical (same version) — `{1795112059566573596}`
- ✔ request on shard 1 is on the disk of exactly its shard's 2 nodes — `request:req-3fa4d0893d25 -> ['storage-1a', 'storage-1b']`
- ✔    both copies are identical (same version) — `{1795112059235026067}`
- ✔ resource on shard 1 is on the disk of exactly its shard's 2 nodes — `resource:medicine:res-medicine-03c0e139c209 -> ['storage-1a', 'storage-1b']`
- ✔    both copies are identical (same version) — `{1795112059585568443}`
- ✔ request on shard 2 is on the disk of exactly its shard's 2 nodes — `request:req-2013a2fdbcfe -> ['storage-2a', 'storage-2b']`
- ✔    both copies are identical (same version) — `{1795112059302117834}`
- ✔ resource on shard 2 is on the disk of exactly its shard's 2 nodes — `resource:water:res-water-01382ea1d96c -> ['storage-2a', 'storage-2b']`
- ✔    both copies are identical (same version) — `{1795112059550546690}`

#### [PASS] `test_coordinator_selects_shard_by_hash_mod_3` (3.2s) - covers 4.c, 4.d, 11.f

§4 'The coordinator determines the appropriate shard and routes storage operations. A simple hash-based partitioning mechanism may be used. For example: hash(request_id) mod 3'.

- ✔ every record was stored on shard = sha256(key) mod 3 (recomputed by the test) — `45/45 correct`
- · example: request:req-1d3b486997df — `sha256 mod 3 = 2 -> stored on shard 2 by ['storage-2a', 'storage-2b']`
- · example: request:req-e5ef95986236 — `sha256 mod 3 = 2 -> stored on shard 2 by ['storage-2a', 'storage-2b']`
- · example: resource:water:res-water-d822723758a1 — `sha256 mod 3 = 1 -> stored on shard 1 by ['storage-1a', 'storage-1b']`
- ✔ the coordinator's own routing table agrees (GET /internal/shard-of/{key}) — `10/10 keys`
- ✔ the shard choice is deterministic (same key -> same shard every time) — `5 keys asked twice`
- ✔ reads are routed to a node of the key's shard — `15/15 reads routed correctly`
- ✔ writes are routed to (all) the replicas of that shard — `45/45 writes acked by both copies`
- · load spread of these 45 keys over the shards — `{0: 13, 1: 12, 2: 20}`

#### [PASS] `test_client_does_not_need_the_storage_location` (1.0s) - covers 4.e

§4 'The client should not need to know the physical storage location of the requested data.'

- ✔ no client endpoint takes a shard / node / replica parameter — `['request_id', 'resource_id', 'status', 'type']`
- ✔ request bodies carry no storage location either — `['auto_match', 'availability', 'contact', 'details', 'kind', 'location', 'name', 'priority', 'requester', 'type']`
- ✔ records on all 3 shards are read with the same URL, only the id is needed — `GET /requests/{id} for ids on shards [0, 1, 2] -> [200, 200, 200]`
- ✔ the request-service knows only the coordinator, not any storage node — `COORDINATOR_URL=http://coordinator:8000`
- ✔ storage nodes are not reachable from outside (no published ports) — `{'storage-0a': [], 'storage-0b': [], 'storage-1a': [], 'storage-1b': [], 'storage-2a': [], 'storage-2b': []}`

#### [PASS] `test_client_api_success_codes` (0.5s) - covers 5.ok, 5.1, 5.2, 5.3, 5.4, 5.5, 5.6, 5.7

§5 Table 2: each client endpoint returns the success code listed in the PDF.

- ✔ POST /resources                 -> 201 — `got 201`
- ✔ GET  /resources                 -> 200 — `got 200`
- ✔ PUT  /resources/{id}/status     -> 200 — `got 200`
- ✔ POST /requests                  -> 201 — `got 201`
- ✔ GET  /requests/{id}             -> 200 — `got 200`
- ✔ POST /match/{requestId}         -> 200 — `got 200`
- ✔ PUT  /requests/{id}/status      -> 200 — `got 200`

#### [PASS] `test_json_request_and_response_bodies` (0.3s) - covers 5.json

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

#### [PASS] `test_invalid_requests_get_4xx` (0.3s) - covers 5.400

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

#### [PASS] `test_missing_resources_get_404` (0.1s) - covers 5.404

§5 'appropriate HTTP status codes for ... missing resources' (404).

- ✔ GET /requests/{unknown id} -> 404 — `got 404: request:req-000000000000 not found`
- ✔ PUT /requests/{unknown id}/status -> 404 — `got 404: request:req-000000000000 not found`
- ✔ GET /resources/{unknown id} -> 404 — `got 404: resource:water:res-water-000000000000 not found`
- ✔ PUT /resources/{unknown id}/status -> 404 — `got 404: resource:water:res-water-000000000000 not found`
- ✔ PUT /resources/{malformed id}/status -> 404 — `got 404: resource:_invalid:not-an-id not found`
- ✔ POST /match/{unknown id} -> 404 — `got 404: request req-000000000000 not found`
- ✔ GET /no-such-endpoint -> 404 — `got 404: Not Found`

#### [PASS] `test_internal_node_register` (2.4s) - covers 6.reg

§6 POST /internal/nodes/register - 'Register storage node'.

- ✔ a node for a non-existent shard is refused (400) and not added — `{'detail': 'shard_id must be in [0, 3)'}`
- ✔ all storage nodes registered themselves on start-up — `[('storage-0a', 0), ('storage-0b', 0), ('storage-1a', 1), ('storage-1b', 1), ('storage-2a', 2), ('storage-2b', 2)]`
- ✔ POST /internal/nodes/register with a node's own details -> 200 — `{'status': 'syncing', 'resync': True, 'peers': ['http://storage-0a:8000'], 'num_shards': 3}`
- ✔ a (re)joining node is told to sync from the ready replica of its shard before serving reads — `status=syncing peers=['http://storage-0a:8000'] num_shards=3`
- ✔ the real node reports itself ready again with its next heartbeat — `storage-0b`
- · storage node log — `storage-0b-1  | 2026-11-19 17:27:19,358 [storage-storage-0b] INFO registered with coordinator (shard 0, url http://storage-0b:8000): waiting; peers=[]`

#### [PASS] `test_internal_heartbeats_are_periodic` (5.3s) - covers 6.hb, 8.hb

§6 POST /internal/nodes/heartbeat 'Report node availability' / §8 'Storage nodes should periodically report their availability to the coordinator using heartbeat messages.'

- ✔ a heartbeat from an unknown node -> 404 (it must register first) — `unknown node; register first`
- ✔ storage-0a heartbeats periodically (last beat advanced, never older than the 6 s timeout) — `beat age 0.8s -> 1.5s, advanced 4.3s in 5s`
- ✔ storage-0b heartbeats periodically (last beat advanced, never older than the 6 s timeout) — `beat age 0.8s -> 1.6s, advanced 4.2s in 5s`
- ✔ storage-1a heartbeats periodically (last beat advanced, never older than the 6 s timeout) — `beat age 0.8s -> 1.4s, advanced 4.4s in 5s`
- ✔ storage-1b heartbeats periodically (last beat advanced, never older than the 6 s timeout) — `beat age 0.7s -> 1.6s, advanced 4.2s in 5s`
- ✔ storage-2a heartbeats periodically (last beat advanced, never older than the 6 s timeout) — `beat age 0.2s -> 0.9s, advanced 4.3s in 5s`
- ✔ storage-2b heartbeats periodically (last beat advanced, never older than the 6 s timeout) — `beat age 0.1s -> 0.9s, advanced 4.2s in 5s`
- ✔ POST /internal/nodes/heartbeat from a registered node -> 200 — `{'resync': False}`
- · configuration — `storage HEARTBEAT_INTERVAL=2 (default)s, coordinator HEARTBEAT_TIMEOUT=6s`

#### [PASS] `test_internal_data_api` (6.0s) - covers 6.put, 6.get, 6.del

§6 PUT / GET / DELETE /internal/data/{key} - 'Store or update data', 'Retrieve data', 'Delete data' (both at the coordinator and on the storage nodes it routes to).

- ✔ PUT /internal/data/{key} -> 200, stored on the key's shard — `{'key': 'spec-test:b7729d', 'version': 1795112076233230272, 'shard': 1, 'replicas': ['storage-1a', 'storage-1b'], 'degraded': False}`
- ✔    and written to every replica of that shard — `['storage-1a', 'storage-1b']`
- ✔ GET /internal/data/{key} -> 200 with the stored value — `version 1795112076233230272, served_by storage-1a`
- ✔    the value is on the disk of both replicas with the same version — `['storage-1a', 'storage-1b']`
- ✔ PUT again = update -> 200, new version — `1795112076233230272 -> 1795112077867832337`
- ✔ an update based on an old version is refused (409, optimistic concurrency) — `{'detail': {'message': 'version conflict', 'current_version': 1795112077867832337}}`
- ✔ prefix scan GET /internal/data?prefix= finds it — `[]`
- ✔ DELETE /internal/data/{key} -> 200 — `{'key': 'spec-test:b7729d', 'deleted': True, 'shard': 1, 'replicas': ['storage-1a', 'storage-1b']}`
- ✔    afterwards GET -> 404
- ✔    and it is deleted on both replicas — `{'storage-1a': 404, 'storage-1b': 404}`
- ✔ the storage node itself (storage-1a) serves PUT / GET / DELETE /internal/data/{key} for the coordinator — `PUT 200, GET 200, DELETE 200, GET after delete 404`

#### [PASS] `test_dockerfiles_for_every_service` (0.2s) - covers 9.1.a, 2.docker

§9.1 'Create Dockerfiles for the required services.'

- ✔ request_service/Dockerfile exists and builds the request-service service — `FROM python:3.12-slim ... CMD uvicorn request_service.app:app`
- ✔ matching_service/Dockerfile exists and builds the matching-service service — `FROM python:3.12-slim ... CMD uvicorn matching_service.app:app`
- ✔ coordinator/Dockerfile exists and builds the coordinator service — `FROM python:3.12-slim ... CMD uvicorn coordinator.app:app`
- ✔ storage_node/Dockerfile exists and builds the storage-node service — `FROM python:3.12-slim ... CMD uvicorn storage_node.app:app`
- ✔ docker-compose builds 'coordinator' from its Dockerfile — `coordinator/Dockerfile`
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

- ✔ image erms/request-service:latest is built — `created 2026-11-19T15:00:08, 165 MB, CMD uvicorn request_service.app:app`
- ✔ image erms/matching-service:latest is built — `created 2026-11-19T15:00:08, 165 MB, CMD uvicorn matching_service.app:app`
- ✔ image erms/coordinator:latest is built — `created 2026-11-19T17:27:12, 165 MB, CMD uvicorn coordinator.app:app`
- ✔ image erms/storage-node:latest is built — `created 2026-11-19T15:00:08, 165 MB, CMD uvicorn storage_node.app:app`

#### [PASS] `test_components_run_as_containers` (0.1s) - covers 9.1.c, 2.docker, 1.d

§9.1 'Run application components as containers.'

- ✔ 'rabbitmq' runs as 1 container(s) — `emergency-response-matching-rabbitmq-1 (rabbitmq:3.13-management, Up 2 minutes (healthy))`
- ✔ 'coordinator' runs as 1 container(s) — `emergency-response-matching-coordinator-1 (erms/coordinator:latest, Up 5 minutes)`
- ✔ 'request-service' runs as 1 container(s) — `emergency-response-matching-request-service-1 (erms/request-service:latest, Up 5 minutes)`
- ✔ 'matching-service' runs as 2 container(s) — `emergency-response-matching-matching-service-1 (erms/matching-service:latest, Up 5 minutes), emergency-response-matching-matching-service-2 (erms/matching-service:latest, Up 5 minutes)`
- ✔ 'storage-0a' runs as 1 container(s) — `emergency-response-matching-storage-0a-1 (erms/storage-node:latest, Up About a minute)`
- ✔ 'storage-0b' runs as 1 container(s) — `emergency-response-matching-storage-0b-1 (erms/storage-node:latest, Up 5 minutes)`
- ✔ 'storage-1a' runs as 1 container(s) — `emergency-response-matching-storage-1a-1 (erms/storage-node:latest, Up 5 minutes)`
- ✔ 'storage-1b' runs as 1 container(s) — `emergency-response-matching-storage-1b-1 (erms/storage-node:latest, Up 5 minutes)`
- ✔ 'storage-2a' runs as 1 container(s) — `emergency-response-matching-storage-2a-1 (erms/storage-node:latest, Up 5 minutes)`
- ✔ 'storage-2b' runs as 1 container(s) — `emergency-response-matching-storage-2b-1 (erms/storage-node:latest, Up 5 minutes)`
- ✔ 'prometheus' runs as 1 container(s) — `emergency-response-matching-prometheus-1 (prom/prometheus:v3.15.1, Up 5 minutes)`
- ✔ 'grafana' runs as 1 container(s) — `emergency-response-matching-grafana-1 (grafana/grafana:13.2.5, Up 5 minutes)`
- ✔ all 13 containers of the deployment are running — `13 running: coordinator x1, grafana x1, matching-service x2, prometheus x1, rabbitmq x1, request-service x1, storage-0a x1, storage-0b x1, storage-1a x1, storage-1b x1, storage-2a x1, storage-2b x1`

#### [PASS] `test_containers_communicate` (8.7s) - covers 9.1.d, 9.2.d

§9.1 'Configure communication between containers' (service DNS names on the Docker network / Kubernetes Services): each component reaches the ones it depends on from inside its own container.

- ✔ request-service -> coordinator (http://coordinator:8000/health) — `{"status":"ok","healthy_shards":[0,1,2],"nodes":6}`
- ✔ request-service -> matching-service (http://matching-service:8000/health) — `{"status":"ok","worker":"b4a771e441a7","broker_connected":true}`
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

#### [SKIP] `test_k8s_deployments` (0.0s) - covers 9.2.a, 2.k8s, 1.d

§9.2 'Kubernetes Deployments.'

*Skipped: Kubernetes requirement: verified by the run against the minikube deployment (SPEC_TARGET=k8s)*

#### [SKIP] `test_k8s_services` (0.0s) - covers 9.2.b

§9.2 'Kubernetes Services.'

*Skipped: Kubernetes requirement: verified by the run against the minikube deployment (SPEC_TARGET=k8s)*

#### [SKIP] `test_k8s_pod_replicas` (0.0s) - covers 9.2.c

§9.2 'Pod replicas.' Several identical pods per microservice, load-balanced by their Service.

*Skipped: Kubernetes requirement: verified by the run against the minikube deployment (SPEC_TARGET=k8s)*

#### [SKIP] `test_k8s_runs_on_a_local_cluster` (0.0s) - covers 9.2.local

§9.2 'The Kubernetes deployment may be performed locally using Minikube, kind, or Docker Desktop Kubernetes.'

*Skipped: Kubernetes requirement: verified by the run against the minikube deployment (SPEC_TARGET=k8s)*

#### [PASS] `test_storage_node_failure_before_during_after` (11.5s) - covers 8.detect, 8.avoid, 8.replica, 8.continue, 8.sync, 8.demo, 10.fail, 10.recover, 11.g, 11.h, 11.cond, 1.d

§8 'A storage-node failure should be demonstrated by stopping the corresponding container or Kubernetes pod and subsequently restarting it ... before, during, and after the failure': detect the failed node, avoid routing to it, use the replica, continue processing, synchronize the data when it recovers.

- · BEFORE: shard 1 = ['storage-1a', 'storage-1b']; the node that will be stopped: storage-1a; its replica: storage-1b
- ✔ before: every storage node is ready — `{'storage-0a': 'ready', 'storage-0b': 'ready', 'storage-1a': 'ready', 'storage-1b': 'ready', 'storage-2a': 'ready', 'storage-2b': 'ready'}`
- ✔ before: a request on shard 1 is written to both copies — `['storage-1a', 'storage-1b']`
- ✔ before: it is readable — `served_by storage-1a`
- · STOP the storage node — `docker compose stop storage-1a`
- ✔ during: the coordinator detected that storage-1a is unavailable — `-0.6s after the stop; reason: scan failed: ConnectError('All connection attempts failed')`
- ✔ during: shard 1 stays available through its replica storage-1b
- ✔ during: data written before the failure is still readable - served by the replica — `200, served_by storage-1b`
- ✔ during: a new request for shard 1 is accepted (201) and routed only to the live replica — `acked by ['storage-1b']`
- ✔ during: the write is flagged degraded (1 of 2 copies), to be copied on recovery — `{'shard': 1, 'replicas': ['storage-1b'], 'version': 1795112092167179713, 'degraded': True}`
- ✔ during: matching keeps working on shard 1 (POST /match -> 200) — `Shard-1 water point`
- ✔ during: status updates keep working (PUT /requests/{id}/status -> 200) — `in_progress`
- ✔ during: the asynchronous pipeline (RabbitMQ -> matching -> storage) keeps processing — `req-50ef9b53f25f matched by a35e580b12de`
- ✔ during: no read was sent to the failed node storage-1a — `reads served by storage-1a: 1051 before, 1051 now`
- ✔ during: listing all requests is still complete (no unavailable shard) — `unavailable_shards = []`
- · RESTART the storage node — `docker compose start storage-1a`
- ✔ after: storage-1a re-registered, synchronized from storage-1b and is ready again — `2.9s after the restart command`
- ✔ after: the request created during the failure is now also on storage-1a's disk — `version 1795112095477755504`
- ✔ after: the updates made during the failure reached storage-1a (request 'in_progress', resource capacity) — `request status in_progress, resource availability 24`
- ✔ after: both copies of shard 1 hold the same number of records — `[888, 888]`
- · storage-1a log — `14:59,124 [storage-storage-1a] INFO SYNC complete from http://storage-1b:8000: received 895 records, applied 3 updates; now READY (888 keys)`
- ✔ after: new writes go to both copies again (not degraded) — `['storage-1a', 'storage-1b']`
- ✔ after: reads are served by the recovered node storage-1a again, including the data it missed — `storage-1a`

#### [PASS] `test_matching_service_down_503_and_queue_buffers` (39.9s) - covers 5.503, 11.e, 11.cond, 6.mq

§5 'appropriate HTTP status codes for ... unavailable services' (503) and §11 RabbitMQ delivery: with the matching microservice stopped, /match answers 503 while new requests are still accepted and wait in RabbitMQ; after the restart they are delivered and processed.

- · STOP the matching microservice — `docker compose stop matching-service`
- ✔ POST /match while the matching service is down -> 503 Service Unavailable (JSON) — `matching service unavailable: ConnectError('[Errno -2] Name or service not known')`
- ✔ the API stays available: new requests are still accepted (201) and queued — `['req-dff090c71bda', 'req-0ea5241e28f6', 'req-d6b9ec0fcc88']`
- ✔ RabbitMQ holds the messages while no consumer runs — `3 message(s) waiting in matching.requests`
- ✔ nothing is matched while the matching service is down — `all 3 pending`
- · RESTART the matching microservice — `docker compose start matching-service`
- ✔ the matching consumers re-attach to the queue — `2 consumers for 2 matching pods`
- ✔ the buffered messages are delivered and processed: all 3 requests matched — `['b4a771e441a7', 'b4a771e441a7', 'a35e580b12de']`
- ✔ POST /match works again (200) — `200`

#### [PASS] `test_whole_shard_down_503_then_recovers` (11.0s) - covers 5.503, 8.detect, 11.cond

§5 'unavailable services' at the storage layer: with BOTH copies of a shard stopped, data on that shard answers 503 (not 500 / wrong data) while the other shards keep working; after the restart the data is back.

- · STOP both copies of the shard — `docker compose stop storage-2a storage-2b`
- ✔ the coordinator reports shard 2 unavailable — `{'status': 'degraded', 'healthy_shards': [0, 1], 'nodes': 6}`
- ✔ GET of a request on shard 2 -> 503 Service Unavailable — `no ready replica available for shard 2`
- ✔ creating requests: ids that hash to shard 2 -> 503, the others are still stored (201) — `{201: 10, 503: 5}`
- ✔ listing still answers 200 and names the unavailable shard — `[2]`
- · RESTART both copies of the shard — `docker compose start storage-2a storage-2b`
- ✔ after the restart the shard's data is back (persistent volumes) -> 200 — `200`

#### [SKIP] `test_k8s_scale_matching_service_with_continued_availability` (0.0s) - covers 9.2.e, 10.k8sscale, 11.j

§9.2 'Microservice scaling' / §10 'Increase the number of replicas of a microservice and demonstrate continued availability' - matching-service 2 -> 4 -> 2 while traffic keeps flowing.

*Skipped: Kubernetes requirement: verified by the run against the minikube deployment (SPEC_TARGET=k8s)*

#### [SKIP] `test_k8s_scale_request_service_with_continued_availability` (0.0s) - covers 9.2.e, 9.2.c, 10.k8sscale, 11.j

§9.2 'Microservice scaling' for the client-facing microservice: request-service 2 -> 3 -> 2 under traffic.

*Skipped: Kubernetes requirement: verified by the run against the minikube deployment (SPEC_TARGET=k8s)*

#### [SKIP] `test_k8s_pod_failure_and_automatic_recovery` (0.0s) - covers 9.2.f, 10.k8srecover, 11.i

§9.2 'Pod failure and automatic recovery' / §10 'Terminate a running application pod and demonstrate automatic recovery': a request-service pod, a matching-service pod in the middle of work, and the coordinator pod.

*Skipped: Kubernetes requirement: verified by the run against the minikube deployment (SPEC_TARGET=k8s)*

#### [SKIP] `test_k8s_storage_pod_killed_reads_fail_over_then_resync` (0.0s) - covers 8.replica, 8.sync, 9.2.f, 11.g, 11.h, 10.fail, 10.recover

§8 with a Kubernetes pod: delete the storage pod that serves the reads of a shard; reads keep working from the replica, the StatefulSet recreates the pod with the same name and volume, and it catches up.

*Skipped: Kubernetes requirement: verified by the run against the minikube deployment (SPEC_TARGET=k8s)*

#### [PASS] `test_x39_monitoring_is_deployed` (0.6s) - covers X39.both

#39: Prometheus and Grafana run as part of this deployment (Compose services / Kubernetes Deployments).

- ✔ 'prometheus' container is running — `1 running`
- ✔ 'grafana' container is running — `1 running`
- ✔ Prometheus is ready — `http://localhost:9090/-/ready -> 200`
- ✔ Grafana is healthy — `Grafana 13.2.5`

#### [PASS] `test_x39_every_service_exposes_metrics` (5.6s) - covers X39.metrics

#39: every ERMS service serves Prometheus metrics at /metrics.

- ✔ request-service: GET /metrics -> 200 Prometheus text — `text/plain; version=1.0.0; charset=utf-8`
- ✔    ... with erms_http_requests_total — `['erms_http_requests_total{method="POST",route="/requests",service="request-service",status="201"} 235.0']`
- ✔    ... with erms_requests_created_total — `['erms_requests_created_total{type="rescue"} 0.0']`
- ✔    ... with erms_requests_published_total — `['erms_requests_published_total{result="ok",routing_key="request.created"} 35.0']`
- ✔    /metrics is not part of the documented client API — `hidden from /docs`
- ✔ coordinator: /metrics with the live state of all 6 storage nodes — `['erms_storage_node_up{node="storage-1b",shard="1"} 1.0', 'erms_storage_node_up{node="storage-2a",shard="2"} 1.0']`
- ✔    ... with erms_shard_ready_replicas — `['erms_shard_ready_replicas{shard="0"} 2.0']`
- ✔    ... with erms_storage_failovers_total — `['erms_storage_failovers_total 6.0']`
- ✔    ... with erms_storage_node_down_total — `['erms_storage_node_down_total{node="storage-1b",shard="1"} 0.0']`
- ✔    ... with erms_storage_degraded_writes_total — `['erms_storage_degraded_writes_total{shard="0"} 6.0']`
- ✔ matching-service: /metrics (called from inside the cluster) — `2 instance(s)`
- ✔    ... with erms_matches_total — `['erms_matches_total{result="matched",via="queue"} 2.0']`
- ✔    ... with erms_time_to_match_seconds_bucket — `['erms_time_to_match_seconds_bucket{le="0.5",via="queue"} 0.0']`
- ✔    ... with erms_messages_consumed_total — `['erms_messages_consumed_total{routing_key="request.created"} 2.0']`
- ✔    ... with erms_reconciler_fixes_total — `['erms_reconciler_fixes_total{kind="orphan_claim"} 0.0']`
- ✔ storage node storage-0a: /metrics with its own records and resync metrics — `erms_storage_local_keys{node="storage-0a",shard="0"} 872.0`
- ✔ storage node storage-0b: /metrics with its own records and resync metrics — `erms_storage_local_keys{node="storage-0b",shard="0"} 872.0`
- ✔ storage node storage-1a: /metrics with its own records and resync metrics — `erms_storage_local_keys{node="storage-1a",shard="1"} 898.0`
- ✔ storage node storage-1b: /metrics with its own records and resync metrics — `erms_storage_local_keys{node="storage-1b",shard="1"} 898.0`
- ✔ storage node storage-2a: /metrics with its own records and resync metrics — `erms_storage_local_keys{node="storage-2a",shard="2"} 876.0`
- ✔ storage node storage-2b: /metrics with its own records and resync metrics — `erms_storage_local_keys{node="storage-2b",shard="2"} 876.0`

#### [PASS] `test_x39_prometheus_scrapes_every_component` (0.3s) - covers X39.scrape, X39.rabbit

#39: Prometheus scrapes every component and every replica, including RabbitMQ's built-in plugin.

- ✔ job 'coordinator': 1 target(s), all up — `['coordinator']`
- ✔ job 'storage-node': 6 target(s), all up — `['storage-0a', 'storage-0b', 'storage-1a', 'storage-1b', 'storage-2a', 'storage-2b']`
- ✔ job 'request-service': 1 target(s), all up — `['172.18.0.7:8000']`
- ✔ job 'matching-service': 2 target(s), all up — `['172.18.0.11:8000', '172.18.0.6:8000']`
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

#### [PASS] `test_x39_dashboard_shows_everything_promised_with_data` (16.3s) - covers X39.panels, X39.nodata

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

#### [PASS] `test_x39_metrics_follow_real_traffic` (9.0s) - covers X39.traffic

#39: creating and matching requests moves the counters by exactly that much, and time-to-match is right.

- ✔ created: +6 after 6 requests — `238 -> 244`
- ✔ published: +6 after 6 requests — `38 -> 44`
- ✔ POST /requests 201: +6 after 6 requests — `238 -> 244`
- ✔ matched via queue: +6 after 6 requests — `6 -> 12`
- ✔ time-to-match samples: +6 after 6 requests — `6 -> 12`
- ✔ the recorded time-to-match equals the real one (created_at -> matched_at of the requests) — `metric average 2.22s, requests' own timestamps 2.18s`

#### [PASS] `test_x39_storage_failure_is_visible_on_the_dashboard` (22.3s) - covers X39.failure

#39 demo moment: stop a storage node -> it turns DOWN (red) on the dashboard and failures are counted; restart it -> READY (green) again, with its resync and resync duration recorded.

- ✔ before: storage-1a shows READY (1, green) — `erms_storage_node_up{node="storage-1a"} = 1`
- · STOP the storage node — `docker compose stop storage-1a`
- ✔ during: the dashboard shows storage-1a DOWN (0, red) — `10s after the stop`
- ✔ during: 'Grafana -> Prometheus' returns the same (the panel's own query) — `erms_storage_node_up = 0`
- ✔ during: 'Ready replicas per shard' drops to 1 for shard 1 — `1 of 2`
- ✔ during: 'Healthy shards' stays 3 (the replica serves the shard) — `3`
- ✔ during: a write with one copy is counted as a degraded write — `10 -> 11`
- ✔ during: the node-down event is counted — `1 -> 2`
- · RESTART the storage node — `docker compose start storage-1a`
- ✔ after: the dashboard shows storage-1a READY (1, green) again — `5s after the restart`
- ✔ the node's timeline over the test shows the red stretch (min 0) and ends green (1) — `min over last 50s = 0, now = 1`
- ✔ after: the restarted node's resync is recorded (count, records applied, duration on the dashboard) — `resyncs 1, missed writes applied 1, resync took 146 ms`

#### [PASS] `test_x39_killing_the_read_replica_makes_failovers_rise` (14.1s) - covers X39.failure

#39 demo moment, the 'failovers rise' part: kill the storage replica that SERVES a shard's reads while reads keep coming -> reads switch to the other replica, the failover counter on the dashboard rises, the node is counted down, and after its restart it is READY (green) again.

- · BEFORE: reads of shard 1 are served by storage-1a
- · KILL the read replica — `docker compose stop storage-1a`
- · RESTART it — `docker compose start storage-1a`
- ✔ reads kept working: none failed while the replica was killed and came back — `35/35 succeeded (100%)`
- ✔ reads switched to the other replica of the shard — `{'storage-1a': 10, 'storage-1b': 25}`
- ✔ the dashboard's failover counter ROSE ('Read failovers' / 'Failures (per minute)' panels) — `erms_storage_failovers_total 6 -> 7`
- ✔    ... the same value through Grafana's own panel query — `sum(erms_storage_failovers_total) = 7`
- ✔ storage-1a was counted DOWN (red on the node timeline) — `node-down events 2 -> 3`
- ✔ after: storage-1a is READY (green) again on the dashboard — `erms_storage_node_up{node="storage-1a"} = 1`

#### [SKIP] `test_x18_keda_is_installed` (0.0s) - covers X18.install

#18: KEDA's components run in the cluster and serve the external-metrics API used by the HPA.

*Skipped: KEDA runs on Kubernetes only: verified by the run against the minikube deployment (SPEC_TARGET=k8s)*

#### [SKIP] `test_x18_scaledobject_hpa_and_credentials` (0.0s) - covers X18.scaledobject, X18.hpa, X18.secret

#18: the ScaledObject, the HPA KEDA creates from it, and where KEDA's broker credentials come from.

*Skipped: KEDA runs on Kubernetes only: verified by the run against the minikube deployment (SPEC_TARGET=k8s)*

#### [SKIP] `test_x18_burst_scales_up_then_back_down` (0.0s) - covers X18.up, X18.down, X18.safe, X18.visible

#18 demo moment: 200 requests pile up in RabbitMQ -> KEDA scales matching-service 2 -> 6 by itself, the queue drains, and it scales back to 2; nothing is lost, nothing matched twice, the API stays up.

*Skipped: KEDA runs on Kubernetes only: verified by the run against the minikube deployment (SPEC_TARGET=k8s)*

#### [SKIP] `test_x18_manual_pin_then_automatic_again` (0.0s) - covers X18.manual

#18: with KEDA in charge a plain `kubectl scale` is undone; scripts/matching-replicas.sh pins the replica count (paused-replicas) and hands it back to KEDA.

*Skipped: KEDA runs on Kubernetes only: verified by the run against the minikube deployment (SPEC_TARGET=k8s)*
