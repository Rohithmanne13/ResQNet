#!/usr/bin/env bash
# Specification-compliance test: verifies every requirement of "Emergency Response Matching.pdf" against the
# RUNNING system and prints a report of every check (tests/spec, catalogue in tests/spec/requirements.py).
#
#   ./scripts/spec-test.sh compose         # Docker Compose deployment  (docker compose up -d --build)
#   ./scripts/spec-test.sh k8s             # Kubernetes deployment      (./k8s/deploy.sh on minikube)
#   ./scripts/spec-test.sh                 # auto-detect (Compose if it is running, else Kubernetes)
#   ./scripts/spec-test.sh compose -k shard    # extra arguments go to pytest
#   .venv/bin/python tests/spec/report.py      # merge the Compose + Kubernetes runs into one matrix
#
# The run stops and restarts storage nodes, the matching service and a whole shard (and, on Kubernetes, deletes
# and scales pods); it puts everything back the way it was. The web UI is tested in a headless browser
# (Playwright + Chromium, installed into .venv on first use). Reports: tests/spec/reports/spec-<target>.{json,md}
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$HOME/.local/bin:$PATH"
if [ $# -gt 0 ] && [[ "$1" != -* ]]; then export SPEC_TARGET=$1; shift; fi
if [ ! -x .venv/bin/python ]; then
  python3 -m venv .venv && .venv/bin/pip install -q -r requirements.txt pytest
fi
# the web-UI tests drive a real (headless) browser: install Playwright + Chromium once
if ! .venv/bin/python -c "import playwright" 2>/dev/null; then
  .venv/bin/pip install -q playwright==1.59.0
fi
.venv/bin/python -m playwright install chromium >/dev/null
exec .venv/bin/python -m pytest tests/spec -p no:cacheprovider -q --tb=short "$@"
