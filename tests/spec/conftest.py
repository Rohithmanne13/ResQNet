"""Spec-compliance harness: records every individual check with its evidence and prints the report.

* @pytest.mark.req("5.1", "11.a", ...) tags a test with the PDF requirement ids it verifies (requirements.py).
* The `check(claim, condition, evidence)` fixture asserts AND records the claim, so the report shows exactly
  what was verified and what was observed; `check.note(text)` records an observation (before/during/after...).
* At the end the report is printed and saved to tests/spec/reports/spec-<target>.{json,md}.
"""
import datetime
import json
import os

import pytest

import report
import spec_env
from requirements import BY_ID

RESULTS: dict[str, dict] = {}
DECLARED: set[str] = set()   # requirement ids tagged on ANY collected test (also ones deselected with -k)
STARTED = datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")


def pytest_configure(config):
    config.addinivalue_line("markers", "req(*ids): PDF requirement ids verified by this test (tests/spec/requirements.py)")


@pytest.hookimpl(tryfirst=True)          # before -k / -m deselection, so the full mapping is known
def pytest_collection_modifyitems(config, items):
    for item in items:
        marker = item.get_closest_marker("req")
        if marker is None:
            raise pytest.UsageError(f"{item.nodeid} has no @pytest.mark.req(...) requirement tag")
        unknown = [i for i in marker.args if i not in BY_ID]
        if unknown:
            raise pytest.UsageError(f"{item.nodeid}: unknown requirement ids {unknown}")
        DECLARED.update(marker.args)


def _record(item) -> dict:
    if item.nodeid not in RESULTS:
        doc = (item.function.__doc__ or "").strip()
        RESULTS[item.nodeid] = {"nodeid": item.nodeid, "name": item.name, "title": " ".join(doc.split()),
                                "reqs": list(item.get_closest_marker("req").args), "outcome": "passed",
                                "duration": 0.0, "reason": "", "checks": []}
    return RESULTS[item.nodeid]


@pytest.fixture
def check(request):
    rec = _record(request.node)

    def _check(claim: str, ok, evidence="") -> None:
        rec["checks"].append([bool(ok), claim, str(evidence)])
        assert ok, f"{claim}  =>  {evidence}"

    def note(text: str, evidence="") -> None:
        rec["checks"].append([None, text, str(evidence)])

    _check.note = note
    return _check


@pytest.hookimpl(hookwrapper=True)
def pytest_runtest_makereport(item, call):
    outcome = yield
    rep = outcome.get_result()
    rec = _record(item)
    rec["duration"] += rep.duration
    if rep.skipped and rep.when in ("setup", "call"):
        rec["outcome"] = "skipped"
        rec["reason"] = rep.longrepr[2] if isinstance(rep.longrepr, tuple) else str(rep.longrepr)
        rec["reason"] = rec["reason"].removeprefix("Skipped: ")
    elif rep.failed:
        rec["outcome"] = "failed"
        msg = str(call.excinfo.value) if call.excinfo else str(rep.longrepr)
        rec["reason"] = (rec["reason"] + f" [{rep.when}] " if rec["reason"] else f"[{rep.when}] ") + \
            " ".join(msg.split())[:600]


def pytest_terminal_summary(terminalreporter):
    if not RESULTS:
        return
    run = {"target": spec_env.TARGET, "endpoints": spec_env.platform().describe(), "started": STARTED,
           "finished": datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S"), "declared": sorted(DECLARED),
           "results": list(RESULTS.values())}
    os.makedirs(report.REPORT_DIR, exist_ok=True)
    base = os.path.join(report.REPORT_DIR, f"spec-{spec_env.TARGET}")
    with open(base + ".json", "w") as f:
        json.dump(run, f, indent=1)
    with open(base + ".md", "w") as f:
        f.write(report.render_markdown([run]))
    tr = terminalreporter
    tr.write_line("")
    for text, style in report.render([run]):
        tr.write_line(text, **({style: True} if style else {}))
    tr.write_line(f"saved: {base}.json and {base}.md   (merge targets: .venv/bin/python tests/spec/report.py)")
