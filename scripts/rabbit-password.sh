#!/usr/bin/env bash
# Print the RabbitMQ password: from the Kubernetes Secret (default) or from .env (COMPOSE=1).
set -euo pipefail
cd "$(dirname "$0")/.."
if [ "${COMPOSE:-0}" = 1 ]; then grep '^RABBITMQ_PASSWORD=' .env | cut -d= -f2-
else kubectl -n erms get secret rabbitmq-credentials -o jsonpath='{.data.password}' | base64 -d; echo; fi
