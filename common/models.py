"""Domain constants shared by the request and matching services."""
from datetime import datetime, timezone

REQUEST_TYPES = {"water", "food", "medicine", "transportation", "shelter", "rescue", "other"}

# Request lifecycle: pending -> matched -> in_progress -> resolved
#                    pending -> unmatched (gave up auto-matching) -> pending (requeued)
#                    any non-terminal -> cancelled
REQUEST_STATUSES = {"pending", "matched", "in_progress", "resolved", "cancelled", "unmatched"}
TERMINAL_REQUEST_STATUSES = {"resolved", "cancelled"}
MATCHABLE_REQUEST_STATUSES = {"pending", "unmatched"}

RESOURCE_STATUSES = {"available", "busy", "offline"}
RESOURCE_KINDS = {"resource", "volunteer", "ngo", "other"}


def request_key(request_id: str) -> str:
    return f"request:{request_id}"


def resource_key(resource_id: str) -> str:
    return f"resource:{resource_id}"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")
