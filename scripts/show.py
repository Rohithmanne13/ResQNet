"""Pretty-printers for the demo scripts: reads JSON on stdin.  usage: show.py <mode>"""
import json
import sys

mode = sys.argv[1]
try:
    d = json.load(sys.stdin)
except ValueError:
    print("  (no JSON response)")
    sys.exit(0)

if mode == "cluster":
    print(f"  {'shard':<6}{'node':<22}{'status':<9}{'keys':>5}  heartbeat")
    for s, v in d["shards"].items():
        for n in v["nodes"]:
            print(f"  {s:<6}{n['node_id']:<22}{n['status']:<9}{n['key_count']:>5}  {n['heartbeat_age_s']}s ago")
    st = d["stats"]
    print(f"  reads={st['reads']} writes={st['writes']} failovers={st['failovers']} reads_by_node={st['read_by_node']}")
elif mode == "queue":
    if "name" not in d:
        print("  (RabbitMQ management API not reachable)")
    else:
        print(f"  queue {d['name']:<18} ready={d.get('messages_ready', 0)} "
              f"unacked={d.get('messages_unacknowledged', 0)} consumers={d.get('consumers', 0)}")
elif mode == "resources":
    for r in d["items"]:
        print(f"  {r['id']}  {r['name']:<26} avail={r['availability']} status={r['status']} shard={r['_shard']}")
elif mode == "request" and "id" not in d:
    print(f"  error: {d.get('detail', d)}")
elif mode == "request":
    a = d.get("assignment") or {}
    print(f"  {d['id']}  {d['status']:<10} {str(a.get('resource_name', '-')):<22} "
          f"{str(a.get('distance_km', '-')):>7} km  worker={a.get('matched_by', '-')}  attempts={d.get('match_attempts')}  "
          f"served_by={d.get('_storage', {}).get('served_by')}")
elif mode == "resource":
    print(f"  {d['id']}  {d['name']}: status={d['status']} availability={d['availability']} "
          f"assigned={d['assigned_requests']}")
elif mode == "created":
    print(f"  {d['id']} -> shard {d['_storage']['shard']}, acked by {d['_storage']['replicas']}")
elif mode == "worker":
    print((d.get("assignment") or {}).get("matched_by", "unmatched"))
else:
    print(json.dumps(d, indent=2))
