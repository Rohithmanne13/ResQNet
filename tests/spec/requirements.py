"""Every requirement in 'Emergency Response Matching.pdf', numbered by section.

Each test in test_spec_compliance.py is tagged with the ids it verifies (@pytest.mark.req(...)). The report
lists every entry below with the tests that cover it, so a requirement that no test covers shows up as
NOT COVERED instead of silently passing. `mandatory=False` marks the items the PDF itself calls optional or
leaves to the students' design.
"""
from dataclasses import dataclass


@dataclass(frozen=True)
class Req:
    id: str
    section: str
    text: str
    mandatory: bool = True
    extension: bool = False      # an extension we added on top of the PDF (not a PDF requirement)


SECTIONS = {
    "1": "§1 Project objective and operations",
    "1.1": "§1.1 Example scenario",
    "2": "§2 System configuration (Table 1)",
    "3": "§3 Request and resource data",
    "4": "§4 Distributed storage",
    "5": "§5 Client REST API (Table 2)",
    "6": "§6 Internal service communication (Table 3)",
    "7": "§7 Request processing",
    "8": "§8 Failure handling",
    "9.1": "§9.1 Docker (mandatory)",
    "9.2": "§9.2 Kubernetes (mandatory)",
    "10": "§10 Required demonstration",
    "11": "§11 Testing",
    "12": "§12 Left to your design",
    "X39": "EXTENSION #39 - Prometheus + Grafana monitoring",
    "X18": "EXTENSION #18 - KEDA autoscaling on RabbitMQ queue depth",
}

CATALOG: list[Req] = [
    # §1
    Req("1.a", "1", "Connect people requiring emergency assistance with available volunteers and resources"),
    Req("1.b", "1", "Support requests such as food, water, medicine, transportation, or shelter"),
    Req("1.c", "1", "Match requests with suitable available resources based on resource type and location"),
    Req("1.d", "1", "Demonstrate distributed storage, asynchronous communication, sharding, replication, "
                    "fault tolerance, containerization and Kubernetes-based deployment"),
    Req("1.op1", "1", "createRequest(type, location, details) - create an emergency request"),
    Req("1.op2", "1", "registerResource(type, location, availability) - register a resource or volunteer"),
    Req("1.op3", "1", "matchRequest(requestId) - find and assign a suitable available resource"),
    Req("1.op4", "1", "updateStatus(requestId, status) - update the status of a request"),
    Req("1.op5", "1", "getRequest(requestId) - retrieve request information"),
    # §1.1
    Req("1.1.a", "1.1", "Flood victim needing drinking water: request submitted, processed asynchronously, "
                        "matched with an available resource, request status updated"),
    Req("1.1.b", "1.1", "Flow: User -> Request Service -> RabbitMQ -> Matching Service -> Distributed Storage "
                        "-> Resource Assigned"),
    # §2
    Req("2.coord", "2", "Coordinator: 1"),
    Req("2.ms", "2", "Microservices: 2-3"),
    Req("2.mq", "2", "Message broker: 1 RabbitMQ instance"),
    Req("2.shards", "2", "Storage shards: 3"),
    Req("2.repl", "2", "Replication: at least 1 replica per shard"),
    Req("2.docker", "2", "Containerization: Docker - mandatory"),
    Req("2.k8s", "2", "Orchestration: Kubernetes - mandatory"),
    Req("2.cloud", "2", "Cloud deployment: optional", mandatory=False),
    Req("2.clients", "2", "Clients: 1 or more"),
    # §3
    Req("3.maintain", "3", "The system maintains information about emergency requests and available resources"),
    Req("3.1.id", "3", "Request has a unique request ID"),
    Req("3.1.type", "3", "Request has a request type"),
    Req("3.1.loc", "3", "Request has a location"),
    Req("3.1.details", "3", "Request has request details"),
    Req("3.1.status", "3", "Request has a current status"),
    Req("3.1.created", "3", "Request has a creation time"),
    Req("3.1.types", "3", "Request types include water, food, medicine, transportation and shelter"),
    Req("3.2.id", "3", "Resource has a unique resource ID"),
    Req("3.2.type", "3", "Resource has a resource type"),
    Req("3.2.loc", "3", "Resource has a location"),
    Req("3.2.avail", "3", "Resource has an availability"),
    Req("3.2.status", "3", "Resource has a current status"),
    Req("3.2.kinds", "3", "A resource may be a physical resource, volunteer, NGO, or other source of assistance"),
    # §4
    Req("4.a", "4", "Requests and resources are stored across multiple storage nodes"),
    Req("4.b", "4", "Storage layer: one coordinator, three storage shards, at least one replica for each shard"),
    Req("4.c", "4", "The coordinator determines the appropriate shard and routes storage operations"),
    Req("4.d", "4", "Hash-based partitioning, e.g. hash(request_id) mod 3"),
    Req("4.e", "4", "The client does not need to know the physical storage location of the data"),
    # §5
    Req("5.1", "5", "POST /requests - create request - 201"),
    Req("5.2", "5", "GET /requests/{id} - retrieve request - 200"),
    Req("5.3", "5", "PUT /requests/{id}/status - update status - 200"),
    Req("5.4", "5", "POST /resources - register resource - 201"),
    Req("5.5", "5", "GET /resources - list resources - 200"),
    Req("5.6", "5", "PUT /resources/{id}/status - update status - 200"),
    Req("5.7", "5", "POST /match/{requestId} - match resource - 200"),
    Req("5.json", "5", "Structured request and response data use JSON"),
    Req("5.ok", "5", "Appropriate HTTP status codes for successful requests"),
    Req("5.400", "5", "Appropriate HTTP status codes for invalid requests"),
    Req("5.404", "5", "Appropriate HTTP status codes for missing resources"),
    Req("5.503", "5", "Appropriate HTTP status codes for unavailable services"),
    # §6
    Req("6.reg", "6", "POST /internal/nodes/register - register storage node"),
    Req("6.hb", "6", "POST /internal/nodes/heartbeat - report node availability"),
    Req("6.put", "6", "PUT /internal/data/{key} - store or update data"),
    Req("6.get", "6", "GET /internal/data/{key} - retrieve data"),
    Req("6.del", "6", "DELETE /internal/data/{key} - delete data"),
    Req("6.mq", "6", "RabbitMQ is used for asynchronous communication between request processing and matching"),
    # §7
    Req("7.1", "7", "Step 1: the client submits an emergency request"),
    Req("7.2", "7", "Step 2: the Request Service stores the request"),
    Req("7.3", "7", "Step 3: the request is published to RabbitMQ"),
    Req("7.4", "7", "Step 4: the Matching Service consumes the message"),
    Req("7.5", "7", "Step 5: a suitable available resource is identified"),
    Req("7.6", "7", "Step 6: the resource is assigned and the corresponding statuses are updated"),
    Req("7.type", "7", "The matching considers resource type"),
    Req("7.loc", "7", "The matching considers location"),
    # §8
    Req("8.hb", "8", "Storage nodes periodically report their availability to the coordinator with heartbeats"),
    Req("8.detect", "8", "Detect unavailable storage nodes"),
    Req("8.avoid", "8", "Avoid routing new requests to failed nodes"),
    Req("8.replica", "8", "Use an available replica when the required data remains accessible"),
    Req("8.continue", "8", "Continue processing when sufficient replicas are available"),
    Req("8.sync", "8", "Synchronize the required data when a failed node recovers"),
    Req("8.demo", "8", "Failure shown by stopping the container / pod and restarting it: before, during and after"),
    # §9.1
    Req("9.1.a", "9.1", "Create Dockerfiles for the required services"),
    Req("9.1.b", "9.1", "Build Docker images"),
    Req("9.1.c", "9.1", "Run application components as containers"),
    Req("9.1.d", "9.1", "Configure communication between containers"),
    # §9.2
    Req("9.2.a", "9.2", "Kubernetes Deployments"),
    Req("9.2.b", "9.2", "Kubernetes Services"),
    Req("9.2.c", "9.2", "Pod replicas"),
    Req("9.2.d", "9.2", "Service-to-service communication"),
    Req("9.2.e", "9.2", "Microservice scaling"),
    Req("9.2.f", "9.2", "Pod failure and automatic recovery"),
    Req("9.2.local", "9.2", "Deployed locally with Minikube, kind or Docker Desktop Kubernetes (cloud optional)"),
    # §10
    Req("10.create", "10", "Request Creation: create and retrieve an emergency request"),
    Req("10.register", "10", "Resource Registration: register an available resource or volunteer"),
    Req("10.match", "10", "Resource Matching: match a request with an appropriate available resource"),
    Req("10.dist", "10", "Data Distribution: distribution across the three storage shards"),
    Req("10.async", "10", "Asynchronous Processing: submit multiple requests, processed through RabbitMQ"),
    Req("10.fail", "10", "Storage Failure: stop a storage node, continued operation using an available replica"),
    Req("10.recover", "10", "Node Recovery: restart the failed node, restoration / synchronization of the data"),
    Req("10.k8srecover", "10", "Kubernetes Recovery: terminate a running application pod, automatic recovery"),
    Req("10.k8sscale", "10", "Kubernetes Scaling: increase the replicas of a microservice, continued availability"),
    # §11
    Req("11.cond", "11", "The system is tested under both normal and failure conditions"),
    Req("11.a", "11", "Testing: request creation, retrieval, and status updates"),
    Req("11.b", "11", "Testing: resource registration and availability updates"),
    Req("11.c", "11", "Testing: correct resource matching"),
    Req("11.d", "11", "Testing: multiple simultaneous requests"),
    Req("11.e", "11", "Testing: RabbitMQ message delivery and processing"),
    Req("11.f", "11", "Testing: correct shard selection and data distribution"),
    Req("11.g", "11", "Testing: replica availability during storage-node failure"),
    Req("11.h", "11", "Testing: recovery and data synchronization after node recovery"),
    Req("11.i", "11", "Testing: Kubernetes pod failure and automatic recovery"),
    Req("11.j", "11", "Testing: Kubernetes-based microservice scaling"),
    # §12
    Req("12", "12", "Design choices (language, storage, sharding, replica sync, matching, RabbitMQ config, "
                    "service boundaries, Kubernetes config, monitoring/logging) can be justified", mandatory=False),
]

# ------------------------------------------------------------------------------------------- extensions
_X = dict(mandatory=True, extension=True)
CATALOG += [
    Req("X39.metrics", "X39", "Every service exposes /metrics: request-service, matching-service, coordinator, "
                              "all storage nodes", **_X),
    Req("X39.rabbit", "X39", "RabbitMQ's Prometheus plugin is scraped (queue depth, unacked, consumers)", **_X),
    Req("X39.scrape", "X39", "Prometheus scrapes every component and every replica; all targets up", **_X),
    Req("X39.grafana", "X39", "Grafana starts with the Prometheus datasource and the ERMS dashboard provisioned", **_X),
    Req("X39.panels", "X39", "Dashboard shows queue depth, time from request to match, failovers, shard health, "
                             "node up/down, resync duration, matching pods, API rate/latency/errors", **_X),
    Req("X39.nodata", "X39", "Every query of every dashboard panel returns data (no 'No data' panels)", **_X),
    Req("X39.traffic", "X39", "Metrics follow real traffic: requests created / published / matched and the "
                              "time-to-match are recorded correctly", **_X),
    Req("X39.failure", "X39", "Demo: a stopped storage node turns DOWN (red) on the dashboard, failures are counted, "
                              "it turns READY (green) again after its resync and the resync duration is recorded", **_X),
    Req("X39.secure", "X39", "Grafana admin password from a Secret / .env; dashboards viewable read-only without "
                             "login; anonymous users cannot edit", **_X),
    Req("X39.both", "X39", "Monitoring runs on both deployments (Docker Compose and Kubernetes)", **_X),
    Req("X18.install", "X18", "KEDA is installed in the cluster (operator, metrics API server, admission webhooks)",
        **_X),
    Req("X18.scaledobject", "X18", "ScaledObject scales Deployment matching-service on queue matching.requests "
                                   "(2-6 pods, 5 waiting messages per pod) and is Ready", **_X),
    Req("X18.hpa", "X18", "KEDA drives a HorizontalPodAutoscaler for matching-service; no conflicting autoscaler",
        **_X),
    Req("X18.secret", "X18", "KEDA's broker credentials come from a Secret (TriggerAuthentication), not from "
                             "the manifests", **_X),
    Req("X18.up", "X18", "A burst of requests piling up in RabbitMQ scales matching-service up automatically "
                         "(2 -> 6)", **_X),
    Req("X18.down", "X18", "When the queue has drained matching-service scales back down to 2 automatically", **_X),
    Req("X18.safe", "X18", "While pods are added and removed no request is lost, none matched twice, capacity "
                           "exact, and the API stays available", **_X),
    Req("X18.visible", "X18", "The autoscaling is visible in Prometheus / Grafana (matching pods vs queue depth)",
        **_X),
    Req("X18.manual", "X18", "Manual scaling / stopping still works with KEDA (pin the replica count), then "
                             "automatic scaling resumes", **_X),
]

BY_ID = {r.id: r for r in CATALOG}
assert len(BY_ID) == len(CATALOG), "duplicate requirement id"
