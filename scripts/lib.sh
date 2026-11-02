# Shared helpers for the demo scripts.
API=${API:-http://localhost:8080}
COORD=${COORD:-http://localhost:8001}
RABBIT=${RABBIT:-http://localhost:15672}
AUTO=${AUTO:-0}   # AUTO=1 skips the "press enter" pauses
# RabbitMQ management login: RABBIT_USER/RABBIT_PASS, else the Compose .env file
RABBIT_USER=${RABBIT_USER:-erms}
if [ -z "${RABBIT_PASS:-}" ] && [ -f .env ]; then RABBIT_PASS=$(grep '^RABBITMQ_PASSWORD=' .env | cut -d= -f2-); fi
RABBIT_PASS=${RABBIT_PASS:-}

c_bold=$'\e[1m'; c_cyan=$'\e[36m'; c_yel=$'\e[33m'; c_off=$'\e[0m'
step()  { echo; echo "${c_bold}${c_cyan}==== $* ====${c_off}"; }
info()  { echo "${c_yel}> $*${c_off}"; }
pause() { [ "$AUTO" = 1 ] || read -rp "   [enter to continue] " _; }
pj()    { python3 -m json.tool; }
show()  { python3 scripts/show.py "$1"; }
# field <json> <python-expr on d>
field() { python3 -c "import json,sys; d=json.loads(sys.argv[1]); print($2)" "$1"; }
post()  { curl -s -X POST "$API$1" -H 'Content-Type: application/json' -d "${2:-}"; }
put()   { curl -s -X PUT  "$API$1" -H 'Content-Type: application/json' -d "${2:-}"; }
get()   { curl -s "$API$1"; }

cluster_table() { curl -s "$COORD/cluster" | python3 scripts/show.py cluster; }

queue_depth() { curl -s -u "$RABBIT_USER:$RABBIT_PASS" "$RABBIT/api/queues/%2F/$1" | python3 scripts/show.py queue; }

# node_status <node-id>: the coordinator's view of one storage node (ready / syncing / down / missing)
node_status() {
  curl -s "$COORD/cluster" | python3 -c "import json,sys
d=json.load(sys.stdin); s=[n['status'] for v in d['shards'].values() for n in v['nodes'] if n['node_id']==sys.argv[1]]
print(s[0] if s else 'missing')" "$1"
}

# on_shard <shard> <request-json>: create requests until one lands on <shard>; prints that response
on_shard() {
  local r
  for _ in $(seq 1 60); do
    r=$(post /requests "$2")
    [ "$(field "$r" 'd.get("_storage", {}).get("shard")' 2>/dev/null)" = "$1" ] && { echo "$r"; return 0; }
  done
  echo "could not place a request on shard $1" >&2; return 1
}

# probe_start <request-json>: in the background, every 0.3 s call GET /health and POST /requests,
# logging each HTTP code (ids of created requests go to $PROBE_IDS). probe_stop prints the success rate.
probe_start() {
  PROBE_LOG=$(mktemp); PROBE_IDS=$(mktemp)
  (
    set +e
    while :; do
      h=$(curl -s -o /dev/null -m 3 -w '%{http_code}' "$API/health")
      out=$(curl -s -m 5 -w $'\n%{http_code}' -X POST "$API/requests" -H 'Content-Type: application/json' -d "$1")
      c=${out##*$'\n'}; body=${out%$'\n'*}
      echo "GET/health ${h:-000}" >>"$PROBE_LOG"; echo "POST/requests ${c:-000}" >>"$PROBE_LOG"
      [ "$c" = 201 ] && field "$body" 'd["id"]' >>"$PROBE_IDS"
      sleep 0.3
    done
  ) &
  PROBE_PID=$!
}
probe_stop() {
  kill "$PROBE_PID" 2>/dev/null || true; wait "$PROBE_PID" 2>/dev/null || true
  awk '{n[$1]++; if ($2 ~ /^2/) ok[$1]++}
       END {for (k in n) printf "  %-14s %3d/%-3d succeeded (%.0f%%)\n", k, ok[k], n[k], 100*ok[k]/n[k]}' "$PROBE_LOG"
}

# wait_matched <timeout-s> <id>...: wait until every request has been matched by a worker
wait_matched() {
  local end=$((SECONDS + $1)) left id; shift
  while [ $SECONDS -lt $end ]; do
    left=0
    for id in "$@"; do [ "$(get "/requests/$id" | show worker)" = unmatched ] && left=$((left + 1)); done
    [ $left = 0 ] && return 0
    echo "  ... $left of $# still queued"; sleep 3
  done
  echo "  (timed out: $left still unmatched)"
}
