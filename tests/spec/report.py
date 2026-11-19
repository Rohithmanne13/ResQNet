"""Renders the specification-compliance report.

Used by conftest.py at the end of a pytest run, and standalone to merge several runs (e.g. one against
Docker Compose and one against Kubernetes) into a single requirement matrix:

    .venv/bin/python tests/spec/report.py                      # merges tests/spec/reports/*.json
    .venv/bin/python tests/spec/report.py a.json b.json        # explicit files
"""
import glob
import json
import os
import sys
import textwrap

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from requirements import BY_ID, CATALOG, SECTIONS  # noqa: E402

REPORT_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)), "reports")
WIDTH = 118
OK, BAD, INFO = "✔", "✘", "·"
STYLE = {"PASS": "green", "FAIL": "red", "SKIP": "yellow", "NOT COVERED": "red", "OPTIONAL": "cyan",
         "NOT RUN": "yellow"}


def verdict(req_id: str, runs: list[dict]) -> tuple[str, list[tuple[str, str, str]]]:
    """(PASS|FAIL|SKIP|NOT RUN|NOT COVERED|OPTIONAL, [(target, test name, outcome)]).
    NOT RUN = a test covers it but was deselected (-k); NOT COVERED = no test covers it at all."""
    hits = [(run["target"], r["name"], r["outcome"]) for run in runs for r in run["results"] if req_id in r["reqs"]]
    if not hits:
        if not BY_ID[req_id].mandatory:
            return "OPTIONAL", hits
        return ("NOT RUN" if any(req_id in run.get("declared", []) for run in runs) else "NOT COVERED"), hits
    outcomes = {o for _, _, o in hits}
    if "failed" in outcomes:
        return "FAIL", hits
    if "passed" in outcomes:
        return "PASS", hits
    return "SKIP", hits


def _wrap(text: str, indent: int, width: int = WIDTH) -> list[str]:
    return textwrap.wrap(text, width=width, initial_indent=" " * indent, subsequent_indent=" " * (indent + 2)) or [""]


def render(runs: list[dict]) -> list[tuple[str, str | None]]:
    """Lines of the report as (text, style) - style is a pytest terminal markup name or None."""
    out: list[tuple[str, str | None]] = []
    add = lambda text="", style=None: out.append((text, style))  # noqa: E731
    bar = "=" * WIDTH

    add(bar, "bold")
    add(" ERMS - SPECIFICATION COMPLIANCE REPORT  (every requirement of 'Emergency Response Matching.pdf')", "bold")
    for run in runs:
        tests = run["results"]
        n = {k: sum(r["outcome"] == k for r in tests) for k in ("passed", "failed", "skipped")}
        checks = [c for r in tests for c in r["checks"] if c[0] is not None]
        add(f" target: {run['target']:<8} {run['endpoints']}")
        add(f"         run {run['started']} -> {run['finished']}   tests: {n['passed']} passed, {n['failed']} failed,"
            f" {n['skipped']} skipped   individual checks: {sum(c[0] for c in checks)} passed,"
            f" {sum(not c[0] for c in checks)} failed")
    add(bar, "bold")

    # ---------------------------------------------------------------- part 1: every check of every test
    add()
    add("PART 1 - EVERY CHECK PERFORMED (per test, in run order; ✔ = check passed, ✘ = failed, · = observation)", "bold")
    for run in runs:
        if len(runs) > 1:
            add()
            add(f"---- target: {run['target']} ----", "bold")
        for r in run["results"]:
            add()
            tag = {"passed": "PASS", "failed": "FAIL", "skipped": "SKIP"}[r["outcome"]]
            add(f"[{tag}] {r['name']}  ({r['duration']:.1f}s)  covers: {', '.join(r['reqs'])}", STYLE[tag])
            for line in _wrap(r["title"], 7):
                add(line)
            if r["outcome"] == "skipped":
                add(f"       skipped: {r['reason']}", "yellow")
                continue
            for ok, claim, evidence in r["checks"]:
                sym = INFO if ok is None else (OK if ok else BAD)
                text = f"{claim}" + (f"  =>  {evidence}" if evidence else "")
                lines = _wrap(text, 9)
                lines[0] = f"     {sym}  " + lines[0].lstrip()
                for i, line in enumerate(lines):
                    add(line, None if ok is not False else "red")
            if r["outcome"] == "failed" and r.get("reason"):
                for line in _wrap("FAILURE: " + r["reason"], 7):
                    add(line, "red")

    # ---------------------------------------------------------------- part 2: requirement matrix
    add()
    add(bar, "bold")
    add("PART 2 - EVERY REQUIREMENT OF THE PDF AND THE TESTS THAT VERIFY IT", "bold")
    add(bar, "bold")
    totals: dict[str, int] = {}
    for sec, title in SECTIONS.items():
        add()
        add(title, "bold")
        for req in (r for r in CATALOG if r.section == sec):
            v, hits = verdict(req.id, runs)
            totals[v] = totals.get(v, 0) + 1
            lines = _wrap(f"{req.id:<11} {req.text}", 0, WIDTH - 14)
            add(f"  {v:<11} {lines[0]}", STYLE[v])
            for line in lines[1:]:
                add(f"  {'':<11} {line}", STYLE[v])
            if hits:
                seen = []
                for target, name, outcome in hits:
                    s = f"{name}[{target}]" + ("" if outcome == "passed" else f"({outcome})")
                    if s not in seen:
                        seen.append(s)
                for line in _wrap("verified by: " + ", ".join(seen), 26, WIDTH):
                    add(line)
            elif v == "OPTIONAL":
                add(" " * 26 + "optional in the PDF - not required")

    add()
    add(bar, "bold")
    order = ("PASS", "FAIL", "SKIP", "NOT RUN", "NOT COVERED", "OPTIONAL")
    for label, reqs in (("PDF REQUIREMENTS", [r for r in CATALOG if not r.extension]),
                        ("EXTENSION REQUIREMENTS", [r for r in CATALOG if r.extension])):
        if not reqs:
            continue
        t: dict[str, int] = {}
        for r in reqs:
            v = verdict(r.id, runs)[0]
            t[v] = t.get(v, 0) + 1
        summary = ", ".join(f"{k} {t[k]}" for k in order if t.get(k))
        add(f" {label}: {len(reqs)} listed ({sum(r.mandatory for r in reqs)} mandatory) -> {summary}",
            "red" if t.get("FAIL") or t.get("NOT COVERED") else "green")
    if totals.get("SKIP"):
        add(" SKIP = only verifiable on the other deployment target; merge both runs with: "
            ".venv/bin/python tests/spec/report.py", "yellow")
    add(bar, "bold")
    return out


def render_markdown(runs: list[dict]) -> str:
    md = ["# ERMS - specification compliance report", ""]
    for run in runs:
        tests = run["results"]
        n = {k: sum(r["outcome"] == k for r in tests) for k in ("passed", "failed", "skipped")}
        md.append(f"* **{run['target']}** ({run['endpoints']}), {run['started']} -> {run['finished']}: "
                  f"{n['passed']} passed, {n['failed']} failed, {n['skipped']} skipped")
    md += ["", "## Requirement matrix", ""]
    for sec, title in SECTIONS.items():
        md += [f"### {title}", "", "| Verdict | Id | Requirement (PDF) | Verified by |", "|---|---|---|---|"]
        for req in (r for r in CATALOG if r.section == sec):
            v, hits = verdict(req.id, runs)
            by = sorted({f"`{name}` [{t}]{'' if o == 'passed' else f' ({o})'}" for t, name, o in hits})
            md.append(f"| **{v}** | {req.id} | {req.text} | {'<br>'.join(by) or '-'} |")
        md.append("")
    md += ["## Every check performed", ""]
    for run in runs:
        md += [f"### Target: {run['target']}", ""]
        for r in run["results"]:
            tag = {"passed": "PASS", "failed": "FAIL", "skipped": "SKIP"}[r["outcome"]]
            md += [f"#### [{tag}] `{r['name']}` ({r['duration']:.1f}s) - covers {', '.join(r['reqs'])}", "",
                   r["title"], ""]
            if r["outcome"] == "skipped":
                md += [f"*Skipped: {r['reason']}*", ""]
                continue
            for ok, claim, evidence in r["checks"]:
                sym = INFO if ok is None else (OK if ok else BAD)
                md.append(f"- {sym} {claim}" + (f" — `{evidence}`" if evidence else ""))
            if r["outcome"] == "failed" and r.get("reason"):
                md.append(f"- **FAILURE:** `{r['reason']}`")
            md.append("")
    return "\n".join(md)


def load(paths: list[str]) -> list[dict]:
    return [json.load(open(p)) for p in paths]


def main(argv: list[str]) -> int:
    paths = argv or sorted(glob.glob(os.path.join(REPORT_DIR, "spec-*.json")))
    if not paths:
        print("no report files found in", REPORT_DIR)
        return 1
    runs = load(paths)
    colors = {"green": "\033[32m", "red": "\033[31m", "yellow": "\033[33m", "cyan": "\033[36m", "bold": "\033[1m"}
    tty = sys.stdout.isatty()
    for text, style in render(runs):
        print(f"{colors[style]}{text}\033[0m" if tty and style else text)
    combined = os.path.join(REPORT_DIR, "spec-report-combined.md")
    with open(combined, "w") as f:
        f.write(render_markdown(runs))
    print(f"\nmarkdown: {combined}")
    failed = any(verdict(r.id, runs)[0] in ("FAIL", "NOT COVERED") for r in CATALOG)
    return 1 if failed else 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
