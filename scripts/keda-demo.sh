#!/usr/bin/env bash
# KEDA autoscaling demo (extension #18): a burst of requests piles up in RabbitMQ, KEDA scales matching-service
# up (2 -> up to 6 pods) on the queue depth, the queue drains, KEDA scales back down to 2. Watch it live on the
# Grafana dashboard "ERMS overview" (panel "Autoscaling: matching pods vs queue depth").
#
#   ./scripts/keda-demo.sh            # 200 requests        N=300 ./scripts/keda-demo.sh   # bigger burst
# Prereq: ./k8s/deploy.sh (with KEDA). Env: API / COORD / RABBIT like k8s-demo.sh, AUTO=1 = no pauses.
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$HOME/.local/bin:$PATH"
if IP=$(minikube ip 2>/dev/null); then
  export API=${API:-http://$IP:30080} COORD=${COORD:-http://$IP:30081} RABBIT=${RABBIT:-http://$IP:30672}
fi
RABBIT_PASS=${RABBIT_PASS:-$(kubectl -n erms get secret rabbitmq-credentials -o jsonpath='{.data.password}' | base64 -d)}
source scripts/lib.sh
K="kubectl -n erms"
N=${N:-200}

if ! $K get scaledobject matching-service >/dev/null 2>&1; then
  echo "KEDA ScaledObject not found - deploy with ./k8s/deploy.sh (KEDA is installed there)"; exit 1
fi
./scripts/matching-replicas.sh auto >/dev/null       # make sure autoscaling is not paused

waiting() { curl -s -u "$RABBIT_USER:$RABBIT_PASS" "$RABBIT/api/queues/%2F/matching.requests" |
            python3 -c 'import json,sys; print(json.load(sys.stdin).get("messages_ready", 0))' 2>/dev/null || echo "?"; }
pods_ready() { $K get deploy matching-service -o jsonpath='{.status.readyReplicas}'; }
hpa_desired() { $K get hpa keda-hpa-matching-service -o jsonpath='{.status.desiredReplicas}' 2>/dev/null || echo "?"; }
row() { printf "  t=%4ss  waiting in queue: %4s   matching pods ready: %s   KEDA/HPA desired: %s\n" \
          "$1" "$(waiting)" "$(pods_ready)" "$(hpa_desired)"; }

step "KEDA: autoscaling matching-service on RabbitMQ queue depth"
$K get scaledobject matching-service
info "rule: desired pods = ceil(messages waiting / 5), min 2, max 6; checked every 5 s"
$K get hpa keda-hpa-matching-service
$K get pods -l app=matching-service; pause

# a fresh place on the map + one resource with enough capacity for the whole burst
LAT=$(python3 -c 'import random; print(round(random.uniform(-50, -30), 4))'); LON=$(python3 -c 'import random; print(round(random.uniform(-170, 170), 4))')
RES=$(post /resources "{\"type\":\"water\",\"name\":\"KEDA demo water depot\",\"location\":{\"lat\":$LAT,\"lon\":$LON},\"availability\":$((N + 10))}")
RES_ID=$(field "$RES" 'd["id"]')
BODY="{\"type\":\"water\",\"location\":{\"lat\":$LAT,\"lon\":$LON},\"details\":\"keda demo\"}"

step "Burst: $N emergency requests at once"
IDS_FILE=$(mktemp)
seq 1 "$N" | xargs -P 25 -I{} sh -c "curl -s -X POST '$API/requests' -H 'Content-Type: application/json' -d '$BODY' |
  python3 -c 'import json,sys; print(json.load(sys.stdin)[\"id\"])' >> $IDS_FILE"
info "$(wc -l < "$IDS_FILE") requests accepted; they wait in RabbitMQ for the matching workers"

step "Scaling up while the queue is long"
t0=$SECONDS; max=0
while [ $((SECONDS - t0)) -lt 240 ]; do
  row $((SECONDS - t0)); p=$(pods_ready); [ "${p:-0}" -gt "$max" ] && max=$p
  [ "$(waiting)" = 0 ] && [ $((SECONDS - t0)) -gt 10 ] && break
  sleep 3
done
drained=$((SECONDS - t0))
info "queue drained after ~${drained}s; peak matching pods: $max"
$K get pods -l app=matching-service

step "Scaling back down once the queue is empty"
t1=$SECONDS
while [ $((SECONDS - t1)) -lt 240 ]; do
  row $((SECONDS - t0)); [ "$(pods_ready)" = 2 ] && [ "$($K get pods -l app=matching-service --no-headers | wc -l)" = 2 ] && break
  sleep 5
done
$K get pods -l app=matching-service

step "Result"
mapfile -t IDS < "$IDS_FILE"
matched=0; for id in "${IDS[@]}"; do [ "$(get "/requests/$id" | show worker)" != unmatched ] && matched=$((matched + 1)); done
echo "  requests matched:        $matched / ${#IDS[@]}"
echo "  peak matching pods:      $max  (started with 2)"
echo "  back to:                 $(pods_ready) pods"
echo "  work per matching pod:"
for id in "${IDS[@]}"; do get "/requests/$id" | show worker; done | sort | uniq -c | sed 's/^/    /'
echo "  resource capacity left:  $(get "/resources/$RES_ID" | python3 -c 'import json,sys; print(json.load(sys.stdin)["availability"])') of $((N + 10))"
put "/resources/$RES_ID/status" '{"status":"offline"}' >/dev/null
info "Grafana http://${IP:-localhost}:30030 -> 'Autoscaling: matching pods vs queue depth' shows the same run"
rm -f "$IDS_FILE"
