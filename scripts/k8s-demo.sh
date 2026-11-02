#!/usr/bin/env bash
# Kubernetes-specific demonstration: pod self-healing, scaling, storage failure/recovery.
# Prereq: ./k8s/deploy.sh. Services are reached through NodePorts on the minikube IP
# (request-service 30080, coordinator 30081, RabbitMQ UI 30672), which keep working
# while pods are killed. Override with API=/COORD=/RABBIT= (e.g. for port-forwards).
set -euo pipefail
cd "$(dirname "$0")/.."
if IP=$(minikube ip 2>/dev/null); then
  export API=${API:-http://$IP:30080} COORD=${COORD:-http://$IP:30081} RABBIT=${RABBIT:-http://$IP:30672}
fi
RABBIT_PASS=${RABBIT_PASS:-$(kubectl -n erms get secret rabbitmq-credentials -o jsonpath='{.data.password}' | base64 -d)}
source scripts/lib.sh
K="kubectl -n erms"

step "Deployments, StatefulSets, Services, Pods"
$K get deploy,sts,svc; $K get pods -o wide; pause

step "Service-to-service communication: client -> request-service -> coordinator -> storage pods"
R=$(post /requests '{"type":"medicine","location":{"lat":12.97,"lon":77.59},"details":"insulin needed"}')
echo "$R" | show created
cluster_table; pause

step "Kubernetes recovery: kill a request-service pod"
POD=$($K get pod -l app=request-service -o jsonpath='{.items[0].metadata.name}')
info "deleting $POD"; $K delete pod "$POD" --wait=false
for i in 1 2 3 4 5; do
  code=$(curl -s -o /dev/null -w '%{http_code}' "$API/health" || true); echo "  GET /health -> $code"; sleep 1
done
$K get pods -l app=request-service
info "Deployment recreated the pod automatically"; pause

step "Kubernetes recovery: kill the coordinator pod"
POD=$($K get pod -l app=coordinator -o jsonpath='{.items[0].metadata.name}')
$K delete pod "$POD"; $K rollout status deploy/coordinator --timeout=90s
info "storage nodes re-register with the new coordinator; each shard's replicas merge before serving"
for i in $(seq 1 60); do [ "$(curl -s "$COORD/health" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("status"))' 2>/dev/null)" = ok ] && break; sleep 0.5; done
cluster_table; pause

step "Scaling: matching-service 2 -> 4 replicas while traffic keeps flowing"
post /resources '{"type":"rescue","name":"Rescue boats","location":{"lat":12.9,"lon":77.6},"availability":200}' >/dev/null
RESCUE='{"type":"rescue","location":{"lat":12.9,"lon":77.6}}'
info "background probe: GET /health + POST /requests every 0.3 s, running DURING kubectl scale"
probe_start "$RESCUE"
$K scale deploy/matching-service --replicas=4
$K rollout status deploy/matching-service --timeout=120s
for i in $(seq 1 30); do   # until all 4 pods' consumers are attached to matching.requests
  [ "$(curl -s -u "$RABBIT_USER:$RABBIT_PASS" "$RABBIT/api/queues/%2F/matching.requests" | python3 -c 'import json,sys; print(json.load(sys.stdin).get("consumers"))' 2>/dev/null)" = 4 ] && break; sleep 1
done
probe_stop
mapfile -t DURING < "$PROBE_IDS"
info "continued availability: every call above was made while the Deployment was scaling"
queue_depth matching.requests
$K get pods -l app=matching-service
info "burst of 20 requests on the scaled-out Deployment"
IDS=()
for i in $(seq 1 20); do IDS+=("$(field "$(post /requests "$RESCUE")" 'd["id"]')"); done
wait_matched 150 "${DURING[@]}" "${IDS[@]}"
echo "  matches per worker pod (burst):"
for id in "${IDS[@]}"; do get "/requests/$id" | show worker; done | sort | uniq -c
echo "  matches per worker pod (the ${#DURING[@]} requests created during scaling):"
for id in "${DURING[@]}"; do get "/requests/$id" | show worker; done | sort | uniq -c
info "work was spread across the scaled-out matching pods"; pause

step "Scaling back: matching-service 4 -> 2 replicas, still available"
probe_start "$RESCUE"
$K scale deploy/matching-service --replicas=2
$K rollout status deploy/matching-service --timeout=120s
sleep 2
probe_stop
$K get pods -l app=matching-service; pause

SHARD1_REQ='{"type":"water","location":{"lat":13,"lon":77.6},"auto_match":false}'

step "Storage failure on Kubernetes (1/2): scale storage-shard-1 to 1 replica (removes pod storage-shard-1-1)"
info "before: a request stored on shard 1, read from storage-shard-1-0"
R=$(on_shard 1 "$SHARD1_REQ"); BEFORE_ID=$(field "$R" 'd["id"]')
echo "$R" | show created
get "/requests/$BEFORE_ID" | show request
$K scale sts/storage-shard-1 --replicas=1; sleep 9; cluster_table
info "during: the shard-1 key is still readable (served_by=storage-shard-1-0) and writes are still accepted"
get "/requests/$BEFORE_ID" | show request
R=$(on_shard 1 "$SHARD1_REQ"); DURING_ID=$(field "$R" 'd["id"]')
echo "  written during the outage:"; echo "$R" | show created
pause
step "Recovery: scale back to 2; storage-shard-1-1 reattaches its persistent volume and catches up from storage-shard-1-0"
$K scale sts/storage-shard-1 --replicas=2; $K rollout status sts/storage-shard-1 --timeout=90s; sleep 5
cluster_table
$K logs storage-shard-1-1 | grep -E "SYNC|registered" | tail -3
info "after: both replicas hold the same key count (only the missed writes were applied); the request written during the outage is readable"
get "/requests/$DURING_ID" | show request
pause

step "Storage failure on Kubernetes (2/2): kubectl delete pod storage-shard-1-0 (the replica reads normally go to)"
R=$(on_shard 1 "$SHARD1_REQ"); DEL_ID=$(field "$R" 'd["id"]')
info "before:"; get "/requests/$DEL_ID" | show request
OLD_UID=$($K get pod storage-shard-1-0 -o jsonpath='{.metadata.uid}')
$K delete pod storage-shard-1-0 --wait=false
info "during: reads fail over to storage-shard-1-1, writes continue"
for i in $(seq 1 40); do   # read until the coordinator has failed over to storage-shard-1-1
  R=$(get "/requests/$DEL_ID"); echo "$R" | show request
  [ "$(field "$R" 'd.get("_storage", {}).get("served_by")' 2>/dev/null)" = storage-shard-1-1 ] && break; sleep 0.25
done
echo "    storage-shard-1-0 is $(node_status storage-shard-1-0) at the coordinator"
echo "  update during the failure: $(field "$(put "/requests/$DEL_ID/status" '{"status":"cancelled","note":"during pod deletion"}')" 'd.get("status", d)')"
R=$(on_shard 1 "$SHARD1_REQ"); DEL_DURING_ID=$(field "$R" 'd["id"]')
echo "  written during the failure:"; echo "$R" | show created
cluster_table
pause
step "Recovery: the StatefulSet recreates storage-shard-1-0 (same name, same persistent volume); it catches up and serves again"
for i in $(seq 1 60); do
  [ "$($K get pod storage-shard-1-0 -o jsonpath='{.metadata.uid}' 2>/dev/null || true)" != "$OLD_UID" ] && break; sleep 1
done
$K wait --for=condition=Ready pod/storage-shard-1-0 --timeout=120s
for i in $(seq 1 60); do [ "$(node_status storage-shard-1-0)" = ready ] && break; sleep 1; done
$K get pods -l app=storage-node,shard=1
cluster_table
$K logs storage-shard-1-0 | grep -E "SYNC|registered" | tail -3
info "after: storage-shard-1-0 serves reads again, including the update and the write it missed"
get "/requests/$DEL_ID" | show request
get "/requests/$DEL_DURING_ID" | show request
