#!/usr/bin/env bash
# Set the number of matching-service pods on Kubernetes, KEDA-aware.
#
#   ./scripts/matching-replicas.sh 4       # pin to 4 pods
#   ./scripts/matching-replicas.sh 0       # stop the matching service (e.g. failure tests)
#   ./scripts/matching-replicas.sh auto    # automatic scaling on queue depth again (KEDA)
#
# With KEDA installed the HorizontalPodAutoscaler owns the replica count and would undo a plain `kubectl scale`
# within seconds; KEDA's annotation autoscaling.keda.sh/paused-replicas=N pins the Deployment to N pods and
# pauses autoscaling, removing it resumes. Without KEDA this falls back to `kubectl scale` ("auto" = 2).
# Waits until the requested number of pods is Ready (and the old ones are gone).
set -euo pipefail
cd "$(dirname "$0")/.."
export PATH="$HOME/.local/bin:$PATH"
NS=${K8S_NAMESPACE:-erms}
K="kubectl -n $NS"
want=${1:?usage: matching-replicas.sh <replicas>|auto}
keda=0
$K get scaledobject matching-service >/dev/null 2>&1 && keda=1

if [ "$want" = auto ]; then
  if [ $keda = 1 ]; then
    $K annotate scaledobject matching-service autoscaling.keda.sh/paused-replicas- >/dev/null 2>&1 || true
    echo "matching-service: automatic scaling (KEDA, 2-6 pods on queue depth)"
    target=2
    for _ in $(seq 1 60); do   # KEDA recreates its HPA when un-paused
      $K get hpa keda-hpa-matching-service >/dev/null 2>&1 && break; sleep 1
    done
  else
    $K scale deploy/matching-service --replicas=2 >/dev/null
    echo "matching-service: 2 pods (KEDA not installed)"
    target=2
  fi
else
  if [ $keda = 1 ]; then
    $K annotate scaledobject matching-service autoscaling.keda.sh/paused-replicas="$want" --overwrite >/dev/null
    echo "matching-service: pinned to $want pod(s) (KEDA paused)"
  else
    $K scale deploy/matching-service --replicas="$want" >/dev/null
    echo "matching-service: $want pod(s)"
  fi
  target=$want
fi

# wait for the requested size (for "auto" at least 2 Ready pods)
for _ in $(seq 1 180); do
  ready=$($K get deploy matching-service -o jsonpath='{.status.readyReplicas}'); ready=${ready:-0}
  total=$($K get pods -l app=matching-service --no-headers 2>/dev/null | wc -l)
  if [ "$want" = auto ]; then [ "$ready" -ge 2 ] && break
  elif [ "$ready" -eq "$target" ] && [ "$total" -eq "$target" ]; then break; fi
  sleep 1
done
$K get pods -l app=matching-service --no-headers 2>/dev/null | awk '{print "  " $1, $2, $3}'
