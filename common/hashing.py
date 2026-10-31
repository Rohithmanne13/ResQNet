"""Stable hash-based partitioning: shard = hash(key) mod NUM_SHARDS.

Python's built-in hash() is randomised per process, so every service would
disagree about where a key lives. SHA-256 is stable across processes/hosts.
"""
import hashlib
import os

NUM_SHARDS = int(os.getenv("NUM_SHARDS", "3"))


def stable_hash(key: str) -> int:
    return int.from_bytes(hashlib.sha256(key.encode("utf-8")).digest()[:8], "big")


def shard_for(key: str, num_shards: int = NUM_SHARDS) -> int:
    return stable_hash(key) % num_shards
