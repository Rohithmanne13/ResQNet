#!/usr/bin/env bash
# Walk through the required demonstration on the Docker Compose deployment.
#   docker compose up -d --build && ./scripts/demo.sh        (AUTO=1 for no pauses)
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f .env ] || ./scripts/init-env.sh
source scripts/lib.sh
STOP=${STOP:-"docker compose stop"}
START=${START:-"docker compose start"}
LOGS=${LOGS:-"docker compose logs --since 2m"}

step "0. Cluster: 1 coordinator, 3 shards x 2 replicas"
cluster_table; pause

step "1. Request creation & retrieval"
info "POST /requests  (flood victim needs drinking water; auto_match=false so we match manually below)"
REQ=$(post /requests '{"type":"water","location":{"lat":12.9716,"lon":77.5946,"area":"Shivajinagar"},"details":"Family of 4 stranded by flooding, need drinking water","priority":"high","auto_match":false}')
echo "$REQ" | pj
RID=$(field "$REQ" 'd["id"]')
info "GET /requests/$RID"
get "/requests/$RID" | pj; pause

step "2. Resource registration"
info "POST /resources  (two water suppliers at different distances)"
FAR=$(post /resources '{"type":"water","kind":"ngo","name":"Whitefield Water NGO","location":{"lat":12.9698,"lon":77.7500},"availability":5}')
NEAR=$(post /resources '{"type":"water","kind":"volunteer","name":"Volunteer Ravi (tanker)","location":{"lat":12.9780,"lon":77.5990},"availability":1}')
echo "$NEAR" | pj
info "GET /resources?type=water"
get "/resources?type=water" | show resources
pause

step "3. Resource matching"
info "POST /match/$RID   (expect the nearest supplier, ~1 km away)"
post "/match/$RID" | pj
info "Request and resource after matching:"
get "/requests/$RID" | show request
NEAR_ID=$(field "$NEAR" 'd["id"]')
get "/resources/$NEAR_ID" | show resource
info "PUT /requests/$RID/status in_progress -> resolved (releases the volunteer)"
put "/requests/$RID/status" '{"status":"in_progress"}' >/dev/null
R=$(put "/requests/$RID/status" '{"status":"resolved","note":"water delivered"}'); echo "  status: $(field "$R" 'd["status"]')  released: $(field "$R" 'd["released_resource"]')"
get "/resources/$NEAR_ID" | show resource
pause

step "4. Data distribution across 3 shards (shard = sha256(key) mod 3)"
for i in $(seq 1 9); do
  R=$(post /requests "{\"type\":\"other\",\"location\":{\"lat\":13.0,\"lon\":77.6},\"details\":\"dist-$i\",\"auto_match\":false}")
  echo "$R" | show created
done
cluster_table; pause

step "5. Asynchronous processing through RabbitMQ"
info "register 2 food kitchens (capacity 4 each), then fire 10 food requests at once"
post /resources '{"type":"food","kind":"ngo","name":"Community Kitchen A","location":{"lat":12.93,"lon":77.62},"availability":4}' >/dev/null
post /resources '{"type":"food","kind":"ngo","name":"Community Kitchen B","location":{"lat":12.95,"lon":77.58},"availability":4}' >/dev/null
IDS=()
for i in $(seq 1 10); do
  R=$(post /requests "{\"type\":\"food\",\"location\":{\"lat\":12.94,\"lon\":77.6$i},\"details\":\"meal request $i\"}")
  IDS+=("$(field "$R" 'd["id"]')")
done
info "all 10 accepted immediately (HTTP 201, queued=true); queue right after submission:"
queue_depth matching.requests
sleep 2; queue_depth matching.requests; queue_depth matching.retry
info "waiting for the matching workers (2 workers x prefetch 2, ~1.5s each)..."; sleep 4; queue_depth matching.requests; sleep 6
for id in "${IDS[@]}"; do
  get "/requests/$id" | show request
done
info "8 matched (capacity 8); the other 2 wait in matching.retry and are re-tried every 5s"
queue_depth matching.retry; pause

step "6. Storage node failure"
KEY="request:${IDS[0]}"
SH=$(curl -s "$COORD/internal/shard-of/$KEY")
VICTIM=$(field "$SH" 'd["replicas"][0]'); SURV=$(field "$SH" 'd["replicas"][1]')
info "${IDS[0]} lives on shard $(field "$SH" 'd["shard"]') -> replicas $(field "$SH" 'd["replicas"]')"
info "before failure:"; get "/requests/${IDS[0]}" | show request
info "stopping $VICTIM"; $STOP "$VICTIM"
info "waiting for heartbeat timeout (6s)..."; sleep 8
cluster_table
info "during failure: GET still works, served by the replica:"; get "/requests/${IDS[0]}" | show request
info "writes continue during the failure:"
R=$(put "/requests/${IDS[0]}/status" '{"status":"cancelled","note":"updated while replica down"}'); echo "  status -> $(field "$R" 'd["status"]')"
for i in 1 2 3 4 5 6; do
  R=$(post /requests "{\"type\":\"water\",\"location\":{\"lat\":13.1,\"lon\":77.5},\"details\":\"during-failure-$i\",\"auto_match\":false}")
  echo "$R" | show created
done
pause

step "7. Node recovery & resynchronisation"
info "starting $VICTIM"; $START "$VICTIM"; sleep 6
cluster_table
info "logs:"
$LOGS coordinator "$VICTIM" 2>&1 | grep -E "DOWN|RE-REGISTERED|RECOVERED|SYNC|READY" | grep -E "$VICTIM" | tail -5
info "after recovery: served by the recovered node again, with the update it missed (status cancelled):"; get "/requests/${IDS[0]}" | show request

step "Done. Kubernetes recovery/scaling: ./scripts/k8s-demo.sh"
