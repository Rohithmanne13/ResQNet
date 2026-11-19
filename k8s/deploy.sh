#!/usr/bin/env bash
# Build images, load them into the local cluster (minikube / kind / Docker Desktop) and deploy, including
# monitoring (Prometheus + Grafana) and KEDA autoscaling of matching-service on RabbitMQ queue depth.
#   SKIP_KEDA=1 ./k8s/deploy.sh      deploy without KEDA (matching-service then stays at a fixed 2 pods)
#   KEDA_VERSION=2.21.0              KEDA release installed when it is not in the cluster yet
set -euo pipefail
cd "$(dirname "$0")/.."
KEDA_VERSION=${KEDA_VERSION:-2.21.0}

IMAGES=(coordinator:coordinator storage_node:storage-node request_service:request-service matching_service:matching-service)
for pair in "${IMAGES[@]}"; do
  dir=${pair%%:*}; name=${pair##*:}
  echo ">> building erms/$name"
  docker build -q -f "$dir/Dockerfile" -t "erms/$name:latest" .
done

ctx=$(kubectl config current-context)
for pair in "${IMAGES[@]}"; do
  name=${pair##*:}
  case "$ctx" in
    minikube*) minikube image load "erms/$name:latest" ;;
    kind-*)    kind load docker-image "erms/$name:latest" --name "${ctx#kind-}" ;;
    *)         : ;;  # docker-desktop shares the local Docker image store
  esac
done

kubectl apply -f k8s/00-namespace.yaml

# RabbitMQ credentials live in a Secret (random password, generated once; never in the manifests).
if ! kubectl -n erms get secret rabbitmq-credentials >/dev/null 2>&1; then
  kubectl -n erms create secret generic rabbitmq-credentials --from-literal=username=erms \
    --from-literal=password="$(python3 -c 'import secrets; print(secrets.token_hex(16))')"
fi

# Grafana admin password (random, generated once). Viewing the dashboards needs no login.
if ! kubectl -n erms get secret grafana-credentials >/dev/null 2>&1; then
  kubectl -n erms create secret generic grafana-credentials \
    --from-literal=admin-password="$(python3 -c 'import secrets; print(secrets.token_hex(16))')"
fi

# Monitoring configuration from monitoring/ -> ConfigMaps (re-applied on every deploy, so edits take effect)
cm() { kubectl -n erms create configmap "$1" "${@:2}" --dry-run=client -o yaml | kubectl apply -f - >/dev/null; }
cm prometheus-config --from-file=prometheus.yml=monitoring/prometheus/prometheus-k8s.yml
cm grafana-datasources --from-file=monitoring/grafana/provisioning/datasources/
cm grafana-dashboard-provider --from-file=monitoring/grafana/provisioning/dashboards/
cm grafana-dashboards --from-file=monitoring/grafana/dashboards/

# KEDA (autoscaling on queue depth). Installed once into namespace "keda"; skipped when offline / SKIP_KEDA=1.
keda=0
if [ "${SKIP_KEDA:-0}" != 1 ]; then
  if ! kubectl get crd scaledobjects.keda.sh >/dev/null 2>&1; then
    echo ">> installing KEDA v$KEDA_VERSION"
    kubectl apply --server-side --force-conflicts \
      -f "https://github.com/kedacore/keda/releases/download/v$KEDA_VERSION/keda-$KEDA_VERSION.yaml" >/dev/null \
      || echo "!! could not install KEDA (offline?) - matching-service keeps a fixed number of pods"
  fi
  if kubectl get crd scaledobjects.keda.sh >/dev/null 2>&1; then
    # operator, metrics API server, admission webhooks (names differ between KEDA versions: wait for all)
    kubectl -n keda wait --for=condition=Available deployment --all --timeout=300s
    keda=1
  fi
fi
if [ $keda = 1 ]; then
  # KEDA's connection to RabbitMQ: an amqp URL in its own Secret, built from rabbitmq-credentials
  RU=$(kubectl -n erms get secret rabbitmq-credentials -o jsonpath='{.data.username}' | base64 -d)
  RP=$(kubectl -n erms get secret rabbitmq-credentials -o jsonpath='{.data.password}' | base64 -d)
  kubectl -n erms create secret generic keda-rabbitmq \
    --from-literal=host="amqp://$RU:$RP@rabbitmq.erms.svc.cluster.local:5672/" \
    --dry-run=client -o yaml | kubectl apply -f - >/dev/null
fi

# The old optional CPU-based HPA would fight KEDA's HPA over the same Deployment
kubectl -n erms delete hpa matching-service --ignore-not-found >/dev/null

# Upgrading an older deployment: RabbitMQ moved from a Deployment to a StatefulSet, and the storage
# StatefulSets gained persistent volumes (volumeClaimTemplates cannot be added in place).
kubectl -n erms delete deployment rabbitmq --ignore-not-found
for s in 0 1 2; do
  if kubectl -n erms get sts "storage-shard-$s" >/dev/null 2>&1 &&
     [ -z "$(kubectl -n erms get sts "storage-shard-$s" -o jsonpath='{.spec.volumeClaimTemplates}')" ]; then
    echo ">> recreating storage-shard-$s with persistent volumes (its old emptyDir data is discarded)"
    kubectl -n erms delete sts "storage-shard-$s" --wait=true
  fi
done

kubectl apply -f k8s/10-rabbitmq.yaml -f k8s/20-coordinator.yaml -f k8s/30-storage.yaml \
              -f k8s/40-matching-service.yaml -f k8s/50-request-service.yaml -f k8s/70-monitoring.yaml
if [ $keda = 1 ]; then kubectl apply -f k8s/80-keda-autoscaling.yaml; fi
# Pods pick up rebuilt :latest images and changed ConfigMaps on redeploy (no-op restart on first deploy is harmless)
kubectl -n erms rollout restart deployment/coordinator deployment/request-service deployment/matching-service statefulset/storage-shard-0 statefulset/storage-shard-1 statefulset/storage-shard-2 deployment/prometheus deployment/grafana >/dev/null
kubectl -n erms rollout status statefulset/rabbitmq --timeout=240s
kubectl -n erms rollout status deployment/coordinator --timeout=120s
for s in 0 1 2; do kubectl -n erms rollout status statefulset/storage-shard-$s --timeout=120s; done
kubectl -n erms rollout status deployment/matching-service --timeout=180s
kubectl -n erms rollout status deployment/request-service --timeout=180s
kubectl -n erms rollout status deployment/prometheus --timeout=180s
kubectl -n erms rollout status deployment/grafana --timeout=180s
kubectl -n erms get pods -o wide
IP=$(minikube ip 2>/dev/null || echo "<node-ip>")
cat <<MSG

Deployed. NodePorts:
  API (Swagger):  http://$IP:30080/docs
  Coordinator:    http://$IP:30081/cluster
  RabbitMQ UI:    http://$IP:30672  (user: erms, password: ./scripts/rabbit-password.sh)
  Grafana:        http://$IP:30030  (dashboard "ERMS overview", no login needed; admin password: ./scripts/grafana-password.sh)
  Prometheus:     http://$IP:30090
  Autoscaling:    $([ $keda = 1 ] && echo "KEDA v$KEDA_VERSION: matching-service 2-6 pods on queue depth (./scripts/keda-demo.sh)" || echo "KEDA not installed: matching-service fixed at 2 pods")
Demo: ./scripts/k8s-demo.sh
(kind / Docker Desktop: use kubectl -n erms port-forward svc/request-service 8080:8000 etc.)
MSG
