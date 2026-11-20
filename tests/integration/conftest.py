"""Integration suite: tests marked @pytest.mark.compose_only need things only Docker Compose offers (stopping
single storage nodes of one shard independently). When the suite runs against Kubernetes (STOP_CMD uses kubectl,
see test_system.py) they are left out of the run entirely - not run, not reported as skipped - and a note says so.
Their Kubernetes counterpart lives in tests/k8s (test_coordinator_restart_rebuilds_the_cluster_safely)."""
import os

LEFT_OUT: list[str] = []


def _kubernetes_target() -> bool:
    return "kubectl" in os.getenv("STOP_CMD", "docker compose stop {node}")


def pytest_configure(config):
    config.addinivalue_line("markers", "compose_only: needs Docker Compose; left out when run against Kubernetes")


def pytest_collection_modifyitems(config, items):
    if not _kubernetes_target():
        return
    keep = []
    for item in items:
        (LEFT_OUT.append(item.name) if item.get_closest_marker("compose_only") else keep.append(item))
    items[:] = keep


def pytest_terminal_summary(terminalreporter):
    if LEFT_OUT:
        terminalreporter.write_line(
            f"{len(LEFT_OUT)} Docker-Compose-only test not part of a Kubernetes run (runs against Compose): "
            + ", ".join(LEFT_OUT))
