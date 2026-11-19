"""Deployment target for the spec-compliance suite: Docker Compose or Kubernetes (minikube).

SPEC_TARGET=compose|k8s selects it (default: compose if its request-service container is running, else k8s).
Override URLs with API_URL / COORD_URL / RABBIT_URL and the RabbitMQ login with RABBIT_USER / RABBIT_PASS.

Both platforms expose the same operations, so every test runs unchanged against either deployment:
  exec_py(target, code, *args)   run Python inside a container / pod (to look at a storage node's own disk,
                                 or to call another service by its DNS name from inside the network)
  stop_node / start_node         stop and restart one storage node (container stop/start | StatefulSet scale)
  stop_shard / start_shard       stop / restart every replica of a shard
  stop_matching / start_matching stop / restart the matching microservice
"""
import base64
import functools
import json
import os
import shlex
import subprocess
import time

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
NS = os.getenv("K8S_NAMESPACE", "erms")
_PATH = os.pathsep.join([os.path.expanduser("~/.local/bin"), os.environ.get("PATH", "")])
ENV = {**os.environ, "PATH": _PATH}

# Python run inside a container: an HTTP call with only the standard library (the service images have no curl).
HTTP_SNIPPET = r"""
import json, sys, urllib.request, urllib.error
method, url = sys.argv[1], sys.argv[2]
body = sys.argv[3].encode() if len(sys.argv) > 3 and sys.argv[3] else None
req = urllib.request.Request(url, method=method, data=body, headers={"Content-Type": "application/json"})
try:
    with urllib.request.urlopen(req, timeout=8) as r:
        print(json.dumps({"status": r.status, "body": r.read().decode()}))
except urllib.error.HTTPError as e:
    print(json.dumps({"status": e.code, "body": e.read().decode()}))
except Exception as e:
    print(json.dumps({"status": None, "error": repr(e)}))
"""

TCP_SNIPPET = r"""
import socket, sys
try:
    socket.create_connection((sys.argv[1], int(sys.argv[2])), timeout=5).close(); print("open")
except Exception as e:
    print("closed " + repr(e))
"""

# Many keys checked on a node's local store in one exec: {key: {"status", "version", "value"}}.
NODE_KEYS_SNIPPET = r"""
import json, sys, urllib.request, urllib.error
out = {}
for key in json.loads(sys.argv[1]):
    try:
        with urllib.request.urlopen("http://localhost:8000/internal/data/" + key, timeout=5) as r:
            d = json.loads(r.read()); out[key] = {"status": r.status, "version": d["version"], "value": d["value"]}
    except urllib.error.HTTPError as e:
        out[key] = {"status": e.code}
print(json.dumps(out))
"""


def sh(cmd: list[str] | str, timeout: float = 180, check: bool = True) -> str:
    if isinstance(cmd, str):
        cmd = shlex.split(cmd)
    p = subprocess.run(cmd, cwd=ROOT, env=ENV, capture_output=True, text=True, timeout=timeout)
    if check and p.returncode != 0:
        raise RuntimeError(f"{' '.join(cmd)} -> exit {p.returncode}: {p.stderr.strip()[:500]}")
    return p.stdout


def _ok(cmd: list[str]) -> bool:
    try:
        subprocess.run(cmd, cwd=ROOT, env=ENV, capture_output=True, timeout=20, check=True)
        return True
    except (OSError, subprocess.SubprocessError):
        return False


def _dotenv(name: str) -> str | None:
    path = os.path.join(ROOT, ".env")
    if os.path.exists(path):
        for line in open(path):
            if line.startswith(f"{name}="):
                return line.strip().split("=", 1)[1]
    return None


def detect() -> str:
    t = os.getenv("SPEC_TARGET")
    if t:
        if t not in ("compose", "k8s"):
            raise SystemExit("SPEC_TARGET must be 'compose' or 'k8s'")
        return t
    try:
        if sh(["docker", "compose", "ps", "-q", "--status", "running", "request-service"], timeout=20, check=False).strip():
            return "compose"
    except (OSError, subprocess.SubprocessError):
        pass
    return "k8s" if _ok(["kubectl", "-n", NS, "get", "deploy/request-service"]) else "compose"


TARGET = detect()


class Compose:
    name = "compose"

    def __init__(self):
        self.api = os.getenv("API_URL", "http://localhost:8080")
        self.coord = os.getenv("COORD_URL", "http://localhost:8001")
        self.rabbit = os.getenv("RABBIT_URL", "http://localhost:15672")
        self.rabbit_auth = (os.getenv("RABBIT_USER") or _dotenv("RABBITMQ_USER") or "erms",
                            os.getenv("RABBIT_PASS") or _dotenv("RABBITMQ_PASSWORD") or "")
        self.prometheus = os.getenv("PROMETHEUS_URL", "http://localhost:9090")
        self.grafana = os.getenv("GRAFANA_URL", "http://localhost:3000")
        self.grafana_auth = ("admin", os.getenv("GRAFANA_PASS") or _dotenv("GRAFANA_ADMIN_PASSWORD") or "")

    def describe(self) -> str:
        return f"API {self.api}  coordinator {self.coord}  RabbitMQ {self.rabbit}  Grafana {self.grafana}"

    # ------------------------------------------------------------------ inventory
    def containers(self) -> list[dict]:
        out = sh(["docker", "compose", "ps", "-a", "--format", "json"])
        rows = [json.loads(line) for line in out.splitlines() if line.strip()] if not out.lstrip().startswith("[") \
            else json.loads(out)
        return rows

    def running(self, service: str) -> list[str]:
        """Full container ids of the running containers of a compose service."""
        return sh(["docker", "compose", "ps", "-q", "--status", "running", service]).split()

    def component_count(self, service: str) -> int:
        return len(self.running(service))

    def matching_workers(self) -> set[str]:
        # matched_by = $HOSTNAME of the worker = the 12-character container id
        return {cid[:12] for cid in self.running("matching-service")}

    def is_matching_worker(self, name: str) -> bool:
        return name in self.matching_workers()

    def env_of(self, service: str) -> dict:
        cid = self.running(service)[0]
        env = json.loads(sh(["docker", "inspect", cid, "--format", "{{json .Config.Env}}"]))
        return dict(e.split("=", 1) for e in env)

    # ------------------------------------------------------------------ exec
    def _exec_target(self, target: str) -> str:
        ids = self.running(target)
        if not ids:
            raise RuntimeError(f"no running container for {target}")
        return ids[0]

    def exec_py(self, target: str, code: str, *args: str, timeout: float = 60) -> str:
        return sh(["docker", "exec", "-i", self._exec_target(target), "python", "-c", code, *args], timeout=timeout)

    def node_exec_target(self, node_id: str) -> str:
        return node_id               # compose: node id == service name (storage-0a ...)

    def logs(self, target: str, tail: int = 400) -> str:
        return sh(["docker", "compose", "logs", "--no-color", "--tail", str(tail), target], check=False)

    # ------------------------------------------------------------------ failures
    def failure_node(self, shard: int) -> str:
        return f"storage-{shard}a"    # sorts first -> it is the replica that normally serves the reads

    def stop_node(self, node_id: str) -> str:
        sh(["docker", "compose", "stop", node_id])
        return f"docker compose stop {node_id}"

    def start_node(self, node_id: str) -> str:
        sh(["docker", "compose", "start", node_id])
        return f"docker compose start {node_id}"

    def shard_nodes(self, shard: int) -> list[str]:
        return [f"storage-{shard}a", f"storage-{shard}b"]

    def stop_shard(self, shard: int) -> str:
        sh(["docker", "compose", "stop", *self.shard_nodes(shard)])
        return f"docker compose stop {' '.join(self.shard_nodes(shard))}"

    def start_shard(self, shard: int) -> str:
        sh(["docker", "compose", "start", *self.shard_nodes(shard)])
        return f"docker compose start {' '.join(self.shard_nodes(shard))}"

    def stop_matching(self) -> str:
        sh(["docker", "compose", "stop", "matching-service"], timeout=120)
        return "docker compose stop matching-service"

    def start_matching(self) -> str:
        sh(["docker", "compose", "start", "matching-service"], timeout=120)
        return "docker compose start matching-service"


class K8s:
    name = "k8s"

    def __init__(self):
        ip = os.getenv("MINIKUBE_IP") or (sh(["minikube", "ip"], timeout=30, check=False).strip() or "localhost")
        self.api = os.getenv("API_URL", f"http://{ip}:30080")
        self.coord = os.getenv("COORD_URL", f"http://{ip}:30081")
        self.rabbit = os.getenv("RABBIT_URL", f"http://{ip}:30672")
        self.rabbit_auth = (os.getenv("RABBIT_USER") or self._secret("username"),
                            os.getenv("RABBIT_PASS") or self._secret("password"))
        self.prometheus = os.getenv("PROMETHEUS_URL", f"http://{ip}:30090")
        self.grafana = os.getenv("GRAFANA_URL", f"http://{ip}:30030")
        self.grafana_auth = ("admin", os.getenv("GRAFANA_PASS") or self._secret("admin-password", "grafana-credentials"))

    def describe(self) -> str:
        return f"API {self.api}  coordinator {self.coord}  RabbitMQ {self.rabbit}  Grafana {self.grafana}  namespace {NS}"

    def kubectl(self, *args: str, timeout: float = 180, check: bool = True) -> str:
        return sh(["kubectl", "-n", NS, *args], timeout=timeout, check=check)

    def kjson(self, *args: str) -> dict:
        return json.loads(self.kubectl(*args, "-o", "json"))

    def _secret(self, key: str, secret: str = "rabbitmq-credentials") -> str:
        try:
            out = self.kubectl("get", "secret", secret, "-o", f"jsonpath={{.data.{key}}}", timeout=20)
            return base64.b64decode(out).decode()
        except Exception:
            return ""

    # ------------------------------------------------------------------ inventory
    def pods(self, label: str) -> list[dict]:
        return self.kjson("get", "pods", "-l", label)["items"]

    @staticmethod
    def pod_ready(pod: dict) -> bool:
        if pod["metadata"].get("deletionTimestamp"):
            return False
        return any(c["type"] == "Ready" and c["status"] == "True" for c in pod.get("status", {}).get("conditions", []))

    def ready_pods(self, label: str) -> list[str]:
        return sorted(p["metadata"]["name"] for p in self.pods(label) if self.pod_ready(p))

    def component_count(self, app: str) -> int:
        return len(self.ready_pods(f"app={app}"))

    def matching_workers(self) -> set[str]:
        return set(self.ready_pods("app=matching-service"))   # matched_by = $HOSTNAME = pod name

    def is_matching_worker(self, name: str) -> bool:
        # with autoscaling a pod that did the work may already be gone again: any matching-service pod counts
        return name.startswith("matching-service-")

    def env_of(self, app: str) -> dict:
        kind = "statefulset/storage-shard-0" if app == "storage-node" else f"deploy/{app}"
        spec = self.kjson("get", kind)["spec"]["template"]["spec"]["containers"][0]
        return {e["name"]: e.get("value", "<from " + next(iter(e.get("valueFrom", {"?": 0}))) + ">")
                for e in spec.get("env", [])}

    # ------------------------------------------------------------------ exec
    def _exec_target(self, target: str) -> str:
        if target not in ("request-service", "matching-service", "coordinator", "rabbitmq"):
            return target            # already a pod name (storage-shard-1-0, request-service-5d8f...-abcde)
        pods = self.ready_pods(f"app={target}")
        if not pods:
            raise RuntimeError(f"no ready pod for app={target}")
        return pods[0]

    def exec_py(self, target: str, code: str, *args: str, timeout: float = 60) -> str:
        return self.kubectl("exec", self._exec_target(target), "--", "python", "-c", code, *args, timeout=timeout)

    def node_exec_target(self, node_id: str) -> str:
        return node_id               # k8s: node id == pod name (storage-shard-0-0 ...)

    def logs(self, target: str, tail: int = 400) -> str:
        return self.kubectl("logs", self._exec_target(target), f"--tail={tail}", check=False)

    # ------------------------------------------------------------------ failures
    def failure_node(self, shard: int) -> str:
        # A StatefulSet stops its highest ordinal when scaled down: storage-shard-<s>-1.
        return f"storage-shard-{shard}-1"

    def _wait_pods(self, label: str, n: int, timeout: float = 180) -> None:
        deadline = time.time() + timeout
        while time.time() < deadline:
            pods = self.pods(label)
            if len(pods) == n and all(self.pod_ready(p) for p in pods):
                return
            time.sleep(1)
        raise RuntimeError(f"pods {label} did not reach {n} ready")

    def stop_node(self, node_id: str) -> str:
        sts, _ = node_id.rsplit("-", 1)
        self.kubectl("scale", f"statefulset/{sts}", "--replicas=1")
        self._wait_pods(f"statefulset.kubernetes.io/pod-name={node_id}", 0)
        return f"kubectl -n {NS} scale statefulset/{sts} --replicas=1   (stops pod {node_id})"

    def start_node(self, node_id: str) -> str:
        sts, _ = node_id.rsplit("-", 1)
        self.kubectl("scale", f"statefulset/{sts}", "--replicas=2")
        self._wait_pods(f"statefulset.kubernetes.io/pod-name={node_id}", 1)
        return f"kubectl -n {NS} scale statefulset/{sts} --replicas=2   (restarts pod {node_id})"

    def shard_nodes(self, shard: int) -> list[str]:
        return [f"storage-shard-{shard}-0", f"storage-shard-{shard}-1"]

    def stop_shard(self, shard: int) -> str:
        self.kubectl("scale", f"statefulset/storage-shard-{shard}", "--replicas=0")
        self._wait_pods(f"app=storage-node,shard={shard}", 0)
        return f"kubectl -n {NS} scale statefulset/storage-shard-{shard} --replicas=0"

    def start_shard(self, shard: int) -> str:
        self.kubectl("scale", f"statefulset/storage-shard-{shard}", "--replicas=2")
        self._wait_pods(f"app=storage-node,shard={shard}", 2)
        return f"kubectl -n {NS} scale statefulset/storage-shard-{shard} --replicas=2"

    def matching_replicas(self, n) -> str:
        """KEDA-aware replica control: n pods (pins / pauses autoscaling) or "auto" (KEDA scales again)."""
        sh([os.path.join(ROOT, "scripts", "matching-replicas.sh"), str(n)], timeout=300)
        return f"./scripts/matching-replicas.sh {n}"

    def keda(self) -> bool:
        return bool(self.kubectl("get", "scaledobject", "matching-service", "--ignore-not-found", "-o", "name",
                                 check=False).strip())

    def stop_matching(self) -> str:
        return self.matching_replicas(0) + "   (0 pods; KEDA autoscaling paused)"

    def start_matching(self) -> str:
        return self.matching_replicas("auto") + "   (automatic scaling on queue depth again)"


@functools.cache
def platform() -> Compose | K8s:
    return K8s() if TARGET == "k8s" else Compose()
