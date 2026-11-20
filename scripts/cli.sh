# Helpers for exploring ERMS by hand in a terminal (see RUN_AND_TEST_GUIDE.txt).
#
#   source scripts/cli.sh compose      # Docker Compose deployment
#   source scripts/cli.sh k8s          # Kubernetes (minikube) deployment
#
# Sets API / COORD / RABBIT / PROM / GRAFANA (+ RPASS = RabbitMQ password) and defines:
#   call  METHOD PATH [JSON]   request to the client API: prints "HTTP <code>" and the JSON body (kept in $LAST)
#   ccall METHOD PATH [JSON]   the same against the storage coordinator (internal API)
#   last_id                    the "id" of the last response, e.g.  REQ=$(last_id)
#   field EXPR                 a value from the last response, e.g.  field 'd["_storage"]["shard"]'
#   cluster                    coordinator view: shards, storage nodes, status, record counts, read stats
#   queue                      RabbitMQ queue matching.requests: waiting / being processed / consumers
#   prom 'PROMQL'              run a Prometheus query and print every series with its value
# Must be SOURCED (not executed): it changes into the project folder and defines shell functions.

_erms_target=${1:-compose}
cd "$(dirname "${BASH_SOURCE[0]}")/.." || return 1
export PATH="$HOME/.local/bin:$PATH"

if [ "$_erms_target" = k8s ]; then
  _ip=$(minikube ip 2>/dev/null) || { echo "minikube is not running (minikube start)"; return 1; }
  API=http://$_ip:30080 COORD=http://$_ip:30081 RABBIT=http://$_ip:30672
  PROM=http://$_ip:30090 GRAFANA=http://$_ip:30030
  RPASS=$(kubectl -n erms get secret rabbitmq-credentials -o jsonpath='{.data.password}' | base64 -d)
else
  API=http://localhost:8080 COORD=http://localhost:8001 RABBIT=http://localhost:15672
  PROM=http://localhost:9090 GRAFANA=http://localhost:3000
  RPASS=$(grep '^RABBITMQ_PASSWORD=' .env 2>/dev/null | cut -d= -f2-)
fi
export API COORD RABBIT PROM GRAFANA RPASS

_erms_req() {   # base method path [json]
  local base=$1 method=$2 path=$3 data=${4:-} out
  if [ -n "$data" ]; then
    out=$(curl -s -m 20 -w '\n%{http_code}' -X "$method" "$base$path" -H 'Content-Type: application/json' -d "$data")
  else
    out=$(curl -s -m 20 -w '\n%{http_code}' -X "$method" "$base$path")
  fi
  LAST=${out%$'\n'*}
  echo "HTTP ${out##*$'\n'}"
  printf '%s' "$LAST" | python3 -m json.tool 2>/dev/null || printf '%s\n' "$LAST"
}
call()  { _erms_req "$API" "$@"; }
ccall() { _erms_req "$COORD" "$@"; }
last_id() { printf '%s' "$LAST" | python3 -c 'import json,sys; print(json.load(sys.stdin)["id"])'; }
field() { printf '%s' "$LAST" | python3 -c "import json,sys; d=json.load(sys.stdin); print($1)"; }
cluster() { curl -s -m 10 "$COORD/cluster" | python3 scripts/show.py cluster; }
queue() { curl -s -m 10 -u "erms:$RPASS" "$RABBIT/api/queues/%2F/matching.requests" | python3 scripts/show.py queue; }
prom() {
  curl -s -m 20 "$PROM/api/v1/query" --data-urlencode "query=$1" | python3 -c '
import json, sys
for s in json.load(sys.stdin)["data"]["result"]:
    labels = ", ".join(f"{k}={v}" for k, v in sorted(s["metric"].items()) if k not in ("__name__", "job", "instance", "pod"))
    print("  %10s   %s" % (s["value"][1], labels))'
}

echo "ERMS CLI ($_erms_target):  API $API   coordinator $COORD"
echo "  RabbitMQ $RABBIT (user erms)   Prometheus $PROM   Grafana $GRAFANA"
echo "  commands: call, ccall, last_id, field, cluster, queue, prom"
