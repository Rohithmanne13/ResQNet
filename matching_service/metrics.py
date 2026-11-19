"""Prometheus metrics of the matching service (every label value is pre-created, so dashboards show 0, not 'no data')."""
from prometheus_client import Counter, Gauge, Histogram

from common.models import REQUEST_TYPES

MATCHES = Counter("erms_matches_total", "Matching attempts by outcome", ["result", "via"])
TIME_TO_MATCH = Histogram("erms_time_to_match_seconds", "Time from request creation to resource assignment",
                          ["via"], buckets=(0.5, 1, 2, 3, 5, 7.5, 10, 15, 20, 30, 45, 60, 120, 300, 600))
MESSAGES = Counter("erms_messages_consumed_total", "Messages consumed from matching.requests", ["routing_key"])
RETRIES = Counter("erms_match_retries_total", "Requests sent to the delayed retry queue")
GAVE_UP = Counter("erms_match_gave_up_total", "Requests marked unmatched after the last automatic attempt")
INFLIGHT = Gauge("erms_messages_inflight", "Messages being processed right now")
RECONCILED = Counter("erms_reconciler_fixes_total", "Inconsistent assignments repaired", ["kind"])
SWEPT = Counter("erms_sweeper_requeued_total", "Requests re-queued by the sweeper", ["reason"])
BROKER_UP = Gauge("erms_broker_connected", "1 = connected to RabbitMQ and consuming")
MATCHED_BY_TYPE = Counter("erms_matched_requests_total", "Requests matched, by request type", ["type"])

for _via in ("queue", "api"):
    TIME_TO_MATCH.labels(_via)
    for _r in ("matched", "no_match", "skipped", "not_found"):
        MATCHES.labels(_r, _via)
for _rk in ("request.created", "request.requeued", "request.retry"):
    MESSAGES.labels(_rk)
for _k in ("orphan_claim", "missing_claim"):
    RECONCILED.labels(_k)
for _w in ("stale_pending", "revived_unmatched"):
    SWEPT.labels(_w)
for _t in REQUEST_TYPES:
    MATCHED_BY_TYPE.labels(_t)
