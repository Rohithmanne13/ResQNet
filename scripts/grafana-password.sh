#!/usr/bin/env bash
# Print the Grafana admin password: from the Kubernetes Secret (default) or from .env (COMPOSE=1).
# Viewing the dashboards needs no login; the admin account is only needed to edit them.
set -euo pipefail
cd "$(dirname "$0")/.."
if [ "${COMPOSE:-0}" = 1 ]; then grep '^GRAFANA_ADMIN_PASSWORD=' .env | cut -d= -f2-
else kubectl -n erms get secret grafana-credentials -o jsonpath='{.data.admin-password}' | base64 -d; echo; fi
