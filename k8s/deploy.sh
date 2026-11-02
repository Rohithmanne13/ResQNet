#!/usr/bin/env bash
# Build images, load them into the local cluster (minikube / kind / Docker Desktop) and deploy.
set -euo pipefail
cd "$(dirname "$0")/.."

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
              -f k8s/40-matching-service.yaml -f k8s/50-request-service.yaml
# Pods pick up rebuilt :latest images on redeploy (no-op restart on first deploy is harmless)
kubectl -n erms rollout restart deployment/coordinator deployment/request-service deployment/matching-service statefulset/storage-shard-0 statefulset/storage-shard-1 statefulset/storage-shard-2 >/dev/null
kubectl -n erms rollout status statefulset/rabbitmq --timeout=240s
kubectl -n erms rollout status deployment/coordinator --timeout=120s
for s in 0 1 2; do kubectl -n erms rollout status statefulset/storage-shard-$s --timeout=120s; done
kubectl -n erms rollout status deployment/matching-service --timeout=180s
kubectl -n erms rollout status deployment/request-service --timeout=180s
kubectl -n erms get pods -o wide
IP=$(minikube ip 2>/dev/null || echo "<node-ip>")
cat <<MSG

Deployed. NodePorts:
  API (Swagger):  http://$IP:30080/docs
  Coordinator:    http://$IP:30081/cluster
  RabbitMQ UI:    http://$IP:30672  (user: erms, password: ./scripts/rabbit-password.sh)
Demo: ./scripts/k8s-demo.sh
(kind / Docker Desktop: use kubectl -n erms port-forward svc/request-service 8080:8000 etc.)
MSG
