#!/usr/bin/env bash
# Create .env (git-ignored) with RabbitMQ credentials for Docker Compose. Keeps an existing .env.
set -euo pipefail
cd "$(dirname "$0")/.."
if [ -f .env ]; then echo ".env already exists (RabbitMQ user: $(grep '^RABBITMQ_USER=' .env | cut -d= -f2))"; exit 0; fi
umask 077
printf 'RABBITMQ_USER=erms\nRABBITMQ_PASSWORD=%s\n' "$(python3 -c 'import secrets; print(secrets.token_hex(16))')" > .env
echo "wrote .env with a random RabbitMQ password (user: erms)"
