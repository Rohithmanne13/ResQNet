"""Client used by microservices to talk to the storage coordinator.

Services never know which shard/node holds a key: they only talk to the
coordinator, which hashes the key and routes to the live replicas.
"""
import os
from typing import Any

import httpx


class StorageError(Exception):
    status_code = 502

    def __init__(self, message: str, payload: Any = None):
        super().__init__(message)
        self.payload = payload


class NotFound(StorageError):
    status_code = 404


class Conflict(StorageError):
    status_code = 409


class Unavailable(StorageError):
    status_code = 503


class StorageClient:
    def __init__(self, base_url: str | None = None, timeout: float = 5.0):
        self.base_url = (base_url or os.getenv("COORDINATOR_URL", "http://coordinator:8000")).rstrip("/")
        self._http = httpx.AsyncClient(base_url=self.base_url, timeout=timeout)

    async def close(self) -> None:
        await self._http.aclose()

    async def _request(self, method: str, path: str, **kw) -> dict:
        try:
            r = await self._http.request(method, path, **kw)
        except httpx.HTTPError as e:
            raise Unavailable(f"storage coordinator unreachable: {e!r}") from e
        if r.status_code < 400:
            return r.json()
        try:
            payload = r.json()
        except ValueError:
            payload = {"detail": r.text}
        detail = payload.get("detail", payload) if isinstance(payload, dict) else payload
        msg = detail if isinstance(detail, str) else str(detail)
        if r.status_code == 404:
            raise NotFound(msg, payload)
        if r.status_code == 409:
            raise Conflict(msg, payload)
        if r.status_code == 503:
            raise Unavailable(msg, payload)
        raise StorageError(f"coordinator error {r.status_code}: {msg}", payload)

    async def get(self, key: str) -> dict:
        """Returns {key, value, version, shard, served_by}."""
        return await self._request("GET", f"/internal/data/{key}")

    async def put(self, key: str, value: Any, if_version: int | None = None) -> dict:
        """Store a value. if_version enables compare-and-set (0 = must not exist)."""
        params = {"if_version": if_version} if if_version is not None else None
        return await self._request("PUT", f"/internal/data/{key}", json={"value": value}, params=params)

    async def delete(self, key: str) -> dict:
        return await self._request("DELETE", f"/internal/data/{key}")

    async def list(self, prefix: str) -> dict:
        """Returns {items: [...], unavailable_shards: [...]} across all shards."""
        return await self._request("GET", "/internal/data", params={"prefix": prefix})

    async def health(self) -> dict:
        return await self._request("GET", "/health")
