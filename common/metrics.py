"""Prometheus instrumentation shared by every service.

instrument(app, service) adds to a FastAPI app:
  * GET /metrics            Prometheus text format (scraped every 5 s by Prometheus)
  * erms_http_requests_total{service,method,route,status}       counter
  * erms_http_request_duration_seconds{service,method,route}    histogram
  * erms_service_info{service,instance}                         always 1 (which process is behind a target)

`route` is the route TEMPLATE (/requests/{request_id}), never the raw path, so ids do not create one time
series per request. Each service defines its own domain metrics next to its code (all prefixed erms_).
"""
import os
import socket
import time

from fastapi import FastAPI, Request, Response
from prometheus_client import CONTENT_TYPE_LATEST, Counter, Gauge, Histogram, generate_latest

INSTANCE = os.getenv("NODE_ID") or os.getenv("HOSTNAME") or socket.gethostname()

HTTP_REQUESTS = Counter("erms_http_requests_total", "HTTP requests handled",
                        ["service", "method", "route", "status"])
HTTP_LATENCY = Histogram("erms_http_request_duration_seconds", "HTTP request latency",
                         ["service", "method", "route"],
                         buckets=(0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10))
SERVICE_INFO = Gauge("erms_service_info", "Service process behind this scrape target", ["service", "instance"])


def route_template(request: Request) -> str:
    route = request.scope.get("route")
    return getattr(route, "path", None) or "unmatched"


def instrument(app: FastAPI, service: str) -> None:
    SERVICE_INFO.labels(service, INSTANCE).set(1)

    @app.middleware("http")
    async def _metrics_middleware(request: Request, call_next):
        if request.url.path == "/metrics":
            return await call_next(request)
        start = time.perf_counter()
        status = 500
        try:
            response = await call_next(request)
            status = response.status_code
            return response
        finally:
            route = route_template(request)
            HTTP_REQUESTS.labels(service, request.method, route, str(status)).inc()
            HTTP_LATENCY.labels(service, request.method, route).observe(time.perf_counter() - start)

    async def metrics() -> Response:
        return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

    app.add_api_route("/metrics", metrics, methods=["GET"], include_in_schema=False)
