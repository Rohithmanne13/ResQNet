# Shared helpers for the demo scripts.
API=${API:-http://localhost:8080}
COORD=${COORD:-http://localhost:8001}
RABBIT=${RABBIT:-http://localhost:15672}
AUTO=${AUTO:-0}   # AUTO=1 skips the "press enter" pauses

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

queue_depth() { curl -s -u guest:guest "$RABBIT/api/queues/%2F/$1" | python3 scripts/show.py queue; }
