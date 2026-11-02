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


def new_resource_id(resource_type: str, suffix: str) -> str:
    """Resource ids carry their (immutable) type, e.g. res-water-1a2b3c4d5e6f, so the storage key can be
    partitioned by type and matching only scans resources of the requested type."""
    return f"res-{resource_type}-{suffix}"


def resource_type_of(resource_id: str) -> str | None:
    parts = resource_id.split("-")
    return parts[1] if len(parts) == 3 and parts[0] == "res" and parts[1] in REQUEST_TYPES else None


def resource_key(resource_id: str) -> str:
    """resource:<type>:<id>. Malformed ids map to a key that never exists (-> 404)."""
    return f"resource:{resource_type_of(resource_id) or '_invalid'}:{resource_id}"


def resource_prefix(resource_type: str | None = None) -> str:
    """Scan prefix for all resources, or only those of one type (an indexed range scan on every node)."""
    return f"resource:{resource_type}:" if resource_type else "resource:"


def now_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")
