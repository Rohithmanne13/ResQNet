# ERMS web UI: static single-page app + nginx reverse proxy to the API, coordinator (read-only) and Prometheus.
# Build from the repository root:  docker build -f frontend/Dockerfile -t erms/frontend:latest .
FROM nginx:1.30.5-alpine

# Defaults = Docker Compose. Kubernetes overrides them in k8s/90-frontend.yaml.
ENV NGINX_ENTRYPOINT_LOCAL_RESOLVERS=1 \
    API_UPSTREAM=request-service:8000 \
    COORD_UPSTREAM=coordinator:8000 \
    PROM_UPSTREAM=prometheus:9090 \
    ERMS_PLATFORM=compose \
    GRAFANA_PORT=3000 \
    PROMETHEUS_PORT=9090 \
    RABBIT_PORT=15672 \
    API_PORT=8080 \
    COORD_PORT=8001

COPY frontend/nginx/default.conf.template /etc/nginx/templates/default.conf.template
COPY frontend/public /usr/share/nginx/html
EXPOSE 80
HEALTHCHECK --interval=10s --timeout=3s CMD wget -q -O /dev/null http://127.0.0.1/healthz || exit 1
