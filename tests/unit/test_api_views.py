"""Unit tests for the list paging options and GET /summary (used by the web UI), no Docker needed."""
import pytest
from fastapi.testclient import TestClient


class ListStore:
    """Only what the list / summary endpoints need: list(prefix) over fixed records."""

    def __init__(self, records: dict[str, dict], unavailable=()):
        self.records, self.unavailable = records, list(unavailable)

    async def list(self, prefix):
        items = [{"key": k, "value": v, "version": 1, "shard": i % 3}
                 for i, (k, v) in enumerate(sorted(self.records.items())) if k.startswith(prefix)]
        return {"items": items, "unavailable_shards": self.unavailable}


@pytest.fixture()
def api(monkeypatch):
    import request_service.app as rs
    records = {}
    for i in range(5):
        records[f"request:req-{i}"] = {"id": f"req-{i}", "type": "water" if i % 2 else "food",
                                       "status": "pending" if i < 3 else "matched", "created_at": f"2026-01-0{i + 1}"}
    records["resource:water:res-water-a"] = {"id": "res-water-a", "type": "water", "kind": "ngo", "status": "available",
                                            "availability": 4, "created_at": "2026-01-01"}
    records["resource:food:res-food-b"] = {"id": "res-food-b", "type": "food", "kind": "volunteer", "status": "offline",
                                          "availability": 9, "created_at": "2026-01-02"}
    monkeypatch.setattr(rs, "store", ListStore(records, unavailable=[2]))
    return TestClient(rs.app)


def test_list_requests_order_and_limit(api):
    all_ = api.get("/requests").json()
    assert [r["id"] for r in all_["items"]] == ["req-0", "req-1", "req-2", "req-3", "req-4"]   # default: oldest first
    assert all_["count"] == all_["total"] == 5
    newest = api.get("/requests", params={"order": "desc", "limit": 2}).json()
    assert [r["id"] for r in newest["items"]] == ["req-4", "req-3"] and newest["count"] == 2 and newest["total"] == 5
    flt = api.get("/requests", params={"status": "pending", "type": "food", "limit": 1}).json()
    assert flt["total"] == 2 and [r["id"] for r in flt["items"]] == ["req-0"]
    assert api.get("/requests", params={"limit": 0}).status_code == 422
    assert api.get("/requests", params={"order": "sideways"}).status_code == 422


def test_list_resources_order_and_limit(api):
    r = api.get("/resources", params={"order": "desc", "limit": 1}).json()
    assert r["total"] == 2 and [x["id"] for x in r["items"]] == ["res-food-b"]


def test_summary_counts(api):
    s = api.get("/summary").json()
    assert s["requests"] == {"total": 5, "by_status": {"matched": 2, "pending": 3}, "by_type": {"food": 3, "water": 2}}
    assert s["resources"]["total"] == 2 and s["resources"]["by_kind"] == {"ngo": 1, "volunteer": 1}
    assert s["resources"]["free_units"] == 4                         # only AVAILABLE resources count
    assert s["unavailable_shards"] == [2]
