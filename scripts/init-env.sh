#!/usr/bin/env bash
# Create / complete .env (git-ignored) with the secrets Docker Compose needs: RabbitMQ credentials and the
# Grafana admin password. Existing values are kept; only missing ones are generated (safe to run again).
set -euo pipefail
cd "$(dirname "$0")/.."
umask 077
touch .env
gen() { python3 -c 'import secrets; print(secrets.token_hex(16))'; }
add() {   # add KEY=VALUE unless KEY is already set
  if ! grep -q "^$1=" .env; then echo "$1=$2" >> .env; echo "  added $1"; fi
}
add RABBITMQ_USER erms
add RABBITMQ_PASSWORD "$(gen)"
add GRAFANA_ADMIN_PASSWORD "$(gen)"
echo ".env ready (RabbitMQ user: $(grep '^RABBITMQ_USER=' .env | cut -d= -f2); Grafana user: admin)"
