from collections import Counter

from common.geo import haversine_km
from common.hashing import shard_for
from matching_service.matcher import rank_candidates
from storage_node.app import Store


def test_shard_for_is_stable_and_in_range():
    assert shard_for("request:req-abc") == shard_for("request:req-abc")
    assert all(0 <= shard_for(f"k{i}") < 3 for i in range(100))


def test_hash_distribution_roughly_even():
    counts = Counter(shard_for(f"request:req-{i:06d}") for i in range(3000))
    assert set(counts) == {0, 1, 2}
    assert all(800 < c < 1200 for c in counts.values()), counts


def test_haversine_known_distance():
    # Bengaluru (MG Road) -> Electronic City is ~ 15 km
    d = haversine_km(12.9756, 77.6050, 12.8452, 77.6602)
    assert 14 < d < 17
    assert haversine_km(1, 1, 1, 1) == 0


def _res(rid, type_, lat, lon, availability=1, status="available"):
    return {"id": rid, "type": type_, "location": {"lat": lat, "lon": lon},
            "availability": availability, "status": status}


def test_rank_filters_type_status_capacity_and_distance():
    req = {"type": "water", "location": {"lat": 12.97, "lon": 77.59}}
    resources = [
        _res("far", "water", 13.97, 77.59),            # ~111 km, outside 50 km
        _res("food", "food", 12.97, 77.59),            # wrong type
        _res("busy", "water", 12.97, 77.59, 0, "busy"),
        _res("offline", "water", 12.97, 77.59, 5, "offline"),
        _res("near", "water", 12.98, 77.59),
        _res("nearest", "water", 12.971, 77.59),
    ]
    ranked = [r["id"] for r, _ in rank_candidates(req, resources, max_km=50)]
    assert ranked == ["nearest", "near"]


def test_rank_tiebreak_prefers_more_capacity():
    req = {"type": "food", "location": {"lat": 0, "lon": 0}}
    ranked = rank_candidates(req, [_res("a", "food", 0, 0, 1), _res("b", "food", 0, 0, 5)])
    assert [r["id"] for r, _ in ranked] == ["b", "a"]


def test_store_last_writer_wins_and_tombstones(tmp_path):
    s = Store(str(tmp_path / "n.db"))
    assert s.put("k", {"v": 1}, version=10)
    assert not s.put("k", {"v": 0}, version=5)       # older write ignored
    assert s.get("k")["value"] == {"v": 1}
    assert s.put("k", None, version=20, deleted=True)
    assert s.get("k")["deleted"] and s.count() == 0
    assert s.scan("") == [] and len(s.scan("", include_deleted=True)) == 1


def test_store_merge_snapshot_is_idempotent(tmp_path):
    a, b = Store(str(tmp_path / "a.db")), Store(str(tmp_path / "b.db"))
    for i in range(5):
        a.put(f"request:{i}", {"i": i}, version=100 + i)
    b.put("request:0", {"i": "stale"}, version=1)
    b.put("request:9", {"i": "newer-on-b"}, version=999)
    snap = a.scan("", include_deleted=True)
    assert b.merge(snap) == 5
    assert b.merge(snap) == 0
    assert b.get("request:0")["value"] == {"i": 0}
    assert b.get("request:9")["value"] == {"i": "newer-on-b"}
    assert b.count() == 6
