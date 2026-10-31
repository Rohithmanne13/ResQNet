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
info "storage nodes re-register with the new coordinator on their next heartbeat"
sleep 5; cluster_table; pause

step "Scaling: matching-service 2 -> 4 replicas under load"
$K scale deploy/matching-service --replicas=4
$K rollout status deploy/matching-service --timeout=120s
post /resources '{"type":"rescue","name":"Rescue boats","location":{"lat":12.9,"lon":77.6},"availability":30}' >/dev/null
IDS=()
for i in $(seq 1 20); do IDS+=("$(field "$(post /requests '{"type":"rescue","location":{"lat":12.9,"lon":77.6}}')" 'd["id"]')"); done
sleep 8
for id in "${IDS[@]}"; do get "/requests/$id" | show worker; done | sort | uniq -c
info "work was spread across the scaled-out matching pods"; pause

step "Storage failure on Kubernetes: scale storage-shard-1 to 1 replica (removes pod storage-shard-1-1)"
$K scale sts/storage-shard-1 --replicas=1; sleep 9; cluster_table
R=$(post /requests '{"type":"water","location":{"lat":13,"lon":77.6},"auto_match":false}'); echo "  writes still accepted: $(field "$R" 'd["id"]') shard $(field "$R" 'd["_storage"]["shard"]')"
pause
step "Recovery: scale back to 2; the new pod starts empty and resyncs from storage-shard-1-0"
$K scale sts/storage-shard-1 --replicas=2; $K rollout status sts/storage-shard-1 --timeout=90s; sleep 5
cluster_table
$K logs storage-shard-1-1 | grep -E "SYNC|registered" | tail -3
