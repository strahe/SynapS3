#!/usr/bin/env python3
"""Validate the documented S3 matrix against black-box Go test results."""

import argparse
import html
import json
import math
import os
import re
import subprocess
import sys
from collections import Counter
from pathlib import Path
from urllib.parse import quote, urlsplit


MATRIX_ROOT = "TestS3CompatibilityMatrix"
CLIENT_ROOT = "TestS3Clients"
CLIENTS = {"AWS_CLI": ("aws", "--version"), "rclone": ("rclone", "version"), "mc": ("mc", "--version")}
CLIENT_NAMES = {"AWS_CLI": "AWS CLI", "rclone": "rclone", "mc": "MinIO Client"}
RESULT_LABELS = {
    "pass": "✅ Passed",
    "fail": "❌ Failed",
    "skip": "⏭ Skipped",
    "missing": "⚪ Missing",
    "duplicate": "⚠️ Invalid",
}
STATUS = {
    "Supported": "supported",
    "Partial": "partial",
    "Not supported": "unsupported",
    "支持": "supported",
    "部分支持": "partial",
    "不支持": "unsupported",
    "✅": "supported",
    "⚠️": "partial",
    "❌": "unsupported",
}


def matrix_rows(path, heading):
    source = Path(path).read_text(encoding="utf-8")
    marker = f"## {heading}\n"
    if source.count(marker) != 1:
        raise ValueError(f"{path}: expected one {heading} section")
    section = source.split(marker, 1)[1].split("\n## ", 1)[0]
    rows = {}
    for line in section.splitlines():
        if not line.startswith("|"):
            continue
        columns = [column.strip() for column in line.strip().strip("|").split("|")]
        if len(columns) != 4:
            raise ValueError(f"{path}: expected four columns in {line!r}")
        if columns[1] in {"Operation", "操作"} or columns[1].startswith("---"):
            continue
        area, names, raw_status, _ = columns
        if raw_status not in STATUS:
            raise ValueError(f"{path}: unknown status {raw_status!r} for {names}")
        operations = re.findall(r"`([^`]+)`", names)
        if not operations or re.sub(r"`[^`]+`|[,\s]", "", names):
            raise ValueError(f"{path}: cannot parse operations in {names!r}")
        for operation in operations:
            if operation in rows:
                raise ValueError(f"{path}: duplicate operation {operation}")
            rows[operation] = (STATUS[raw_status], area)
    if not rows:
        raise ValueError(f"{path}: operation matrix is empty")
    return rows


def validate_matrices(root):
    en = matrix_rows(root / "docs/en/reference/s3-compatibility.md", "Operation Matrix")
    zh = matrix_rows(root / "docs/zh/reference/s3-compatibility.md", "操作矩阵")
    readme = matrix_rows(root / "README.md", "Core S3 Compatibility")
    errors = []
    for operation in sorted(en.keys() | zh.keys()):
        if operation not in en or operation not in zh:
            errors.append(f"English/Chinese matrix operation differs: {operation}")
        elif en[operation][0] != zh[operation][0]:
            errors.append(f"English/Chinese matrix status differs: {operation}")
    for operation, (status, _) in readme.items():
        if operation not in en:
            errors.append(f"README operation absent from full matrix: {operation}")
        elif status != en[operation][0]:
            errors.append(f"README status differs from full matrix: {operation}")
    promised = {name: value for name, value in en.items() if value[0] in {"supported", "partial"}}
    if not promised:
        errors.append("No supported or partial operations found")
    return promised, errors


def test_results(path):
    results = {}
    package_results = []
    with Path(path).open(encoding="utf-8") as stream:
        for line_number, line in enumerate(stream, 1):
            try:
                event = json.loads(line)
            except json.JSONDecodeError as exc:
                raise ValueError(f"{path}:{line_number}: invalid Go test JSON: {exc}") from exc
            action = event.get("Action")
            if action not in {"pass", "fail", "skip"}:
                continue
            test = event.get("Test")
            if test:
                results.setdefault(test, []).append(event)
            else:
                package_results.append(action)
    if not results and not package_results:
        raise ValueError(f"{path}: no completed Go tests")
    return results, package_results


def result_status(results, test):
    actions = results.get(test, [])
    if len(actions) != 1:
        return "missing" if not actions else "duplicate"
    return actions[0]["Action"]


def duration(results, test):
    events = results.get(test, [])
    if len(events) != 1:
        return "—"
    elapsed = events[0].get("Elapsed")
    if type(elapsed) not in (int, float) or elapsed < 0:
        return "—"
    try:
        if not math.isfinite(elapsed):
            return "—"
        return f"{elapsed:.2f} s"
    except OverflowError:
        return "—"


def display_version(name, version):
    patterns = {
        "AWS_CLI": r"^aws-cli/(\S+)",
        "rclone": r"^rclone v(\S+)",
        "mc": r"^mc version RELEASE\.(\d{4}-\d{2}-\d{2})T",
    }
    match = re.search(patterns[name], version)
    if match:
        return match[1]
    return "Unavailable" if version == "unavailable" else version


def client_versions():
    versions = {}
    for name, command in CLIENTS.items():
        try:
            output = subprocess.run(command, capture_output=True, text=True, timeout=10, check=True)
            versions[name] = (output.stdout or output.stderr).splitlines()[0].strip()
        except (OSError, subprocess.SubprocessError, IndexError):
            versions[name] = "unavailable"
    return versions


def cell(value):
    value = html.escape(str(value).replace("\n", " ").replace("\r", " "), quote=False)
    return re.sub(r"[\\|`*_\[\]~]", lambda match: f"&#{ord(match[0])};", value)


def code(value):
    return f"<code>{cell(value)}</code>"


def source_links(source_sha, github_context):
    commit = f"Commit {code(source_sha[:7])}"
    server = github_context.get("GITHUB_SERVER_URL", "")
    repository = github_context.get("GITHUB_REPOSITORY", "")
    try:
        parsed = urlsplit(server)
    except ValueError:
        return commit
    if (
        not repository or parsed.scheme not in {"https", "http"}
        or not parsed.netloc or parsed.query or parsed.fragment
    ):
        return commit
    base = quote(server.rstrip("/"), safe=":/") + "/" + quote(repository, safe="/")
    commit = f"[{commit}](<{base}/commit/{quote(source_sha, safe='')}>)"
    run_id = github_context.get("GITHUB_RUN_ID", "")
    if run_id:
        return f"[View CI run](<{base}/actions/runs/{quote(run_id, safe='')}>) · {commit}"
    return commit


def report(root, matrix_json, clients_json, source_sha, checkout_sha, versions, github_context=None):
    errors = []
    issues = []
    try:
        promised, matrix_errors = validate_matrices(root)
        errors.extend(matrix_errors)
        issues.extend(("Documentation", error) for error in matrix_errors)
    except (OSError, ValueError) as exc:
        promised = {}
        errors.append(str(exc))
        issues.append(("Documentation", "Operation matrix unavailable. See the CI logs for details."))

    test_sets = {}
    for label, path in (("matrix", matrix_json), ("clients", clients_json)):
        try:
            test_sets[label] = test_results(path)
        except (OSError, ValueError) as exc:
            test_sets[label] = ({}, [])
            errors.append(str(exc))
            issues.append((
                "Operation suite" if label == "matrix" else "Client suite",
                "Test results unavailable. See the CI logs for details.",
            ))

    matrix_tests, matrix_package = test_sets["matrix"]
    client_tests, client_package = test_sets["clients"]
    for label, tests, package, root_test in (
        ("matrix", matrix_tests, matrix_package, MATRIX_ROOT),
        ("clients", client_tests, client_package, CLIENT_ROOT),
    ):
        result = result_status(tests, root_test)
        suite = "Operation suite" if label == "matrix" else "Client suite"
        if result != "pass":
            errors.append(f"{label} root test did not pass: {root_test}")
            issues.append((suite, RESULT_LABELS[result]))
        if package != ["pass"]:
            errors.append(f"{label} Go package did not pass")
            issues.append((suite, "Go package did not pass."))

    operations = {}
    groups = {}
    for operation, (_, area) in promised.items():
        result = result_status(matrix_tests, f"{MATRIX_ROOT}/{operation}")
        operations[operation] = result
        groups.setdefault(area, []).append(operation)
        if result != "pass":
            errors.append(f"{operation} test is {result}")
            issues.append((operation, RESULT_LABELS[result]))

    clients = {}
    for name in CLIENTS:
        result = result_status(client_tests, f"{CLIENT_ROOT}/{name}")
        clients[name] = result
        if result != "pass":
            errors.append(f"{name} client test is {result}")
            issues.append((CLIENT_NAMES[name], RESULT_LABELS[result]))
        if versions[name] == "unavailable":
            errors.append(f"{name} version is unavailable")
            issues.append((CLIENT_NAMES[name], "Version unavailable."))

    counts = Counter((*operations.values(), *clients.values()))
    operation_total = "**Operations unavailable**"
    if operations:
        passed = sum(result == "pass" for result in operations.values())
        operation_total = f"**{passed}/{len(operations)} operations**"
    client_total = f"**{sum(result == 'pass' for result in clients.values())}/{len(clients)} clients**"
    totals = f"Failed **{counts['fail']}** · Skipped **{counts['skip']}** · Missing **{counts['missing']}**"
    if counts["duplicate"]:
        totals += f" · Invalid **{counts['duplicate']}**"
    lines = [
        "# S3 Compatibility",
        "",
        f"{'❌ **Failed**' if errors else '✅ **Passed**'} · {operation_total} · {client_total}",
        "",
        totals,
        "",
        "Verification covers documented supported and partially supported operations.",
    ]
    if issues:
        lines.extend(("", "## Issues", "", "| Check | Finding |", "| :--- | :--- |"))
        lines.extend(f"| {cell(check)} | {cell(finding)} |" for check, finding in issues)

    lines.extend(("", "## S3 operations", ""))
    if groups:
        lines.extend(("| Area | Verified | Result |", "| :--- | ---: | :--- |"))
        for area, names in groups.items():
            group_counts = Counter(operations[name] for name in names)
            result = next((
                state for state in ("fail", "duplicate", "missing", "skip") if group_counts[state]
            ), "pass")
            lines.append(f"| {cell(area)} | {group_counts['pass']}/{len(names)} | {RESULT_LABELS[result]} |")
    else:
        lines.append("Operation matrix unavailable.")
    lines.extend(("", f"**Operation suite:** {duration(matrix_tests, MATRIX_ROOT)}"))

    lines.extend(("", "## Clients", "", "| Client | Version | Result | Duration |", "| :--- | :--- | :--- | ---: |"))
    for name, result in clients.items():
        lines.append(
            f"| {CLIENT_NAMES[name]} | {cell(display_version(name, versions[name]))}"
            f" | {RESULT_LABELS[result]} | {duration(client_tests, f'{CLIENT_ROOT}/{name}')} |"
        )
    lines.extend(("", f"**Client suite:** {duration(client_tests, CLIENT_ROOT)}"))

    details_label = f"Operation details · {len(operations)} operations" if operations else "Operation details"
    lines.extend(("", "<details open>", f"<summary>{html.escape(details_label)}</summary>", ""))
    for area, names in groups.items():
        passed = sum(operations[name] == "pass" for name in names)
        lines.extend((
            f"### {cell(area)} · {passed}/{len(names)} passed", "",
            "| Operation | Support | Result | Duration |", "| :--- | :--- | :--- | ---: |",
        ))
        for name in names:
            lines.append(
                f"| {code(name)} | {promised[name][0].title()}"
                f" | {RESULT_LABELS[operations[name]]} | {duration(matrix_tests, f'{MATRIX_ROOT}/{name}')} |"
            )
        lines.append("")
    if not groups:
        lines.extend(("Operation matrix unavailable.", ""))
    lines.extend((
        "</details>", "", "<details open>", "<summary>Verification details</summary>", "",
        "| Evidence | Value |", "| :--- | :--- |",
        f"| Source commit | {code(source_sha)} |", f"| Tested checkout | {code(checkout_sha)} |", "",
        "### Client versions", "", "| Client | Full version |", "| :--- | :--- |",
    ))
    lines.extend(f"| {CLIENT_NAMES[name]} | {cell(versions[name])} |" for name in CLIENTS)
    lines.extend(("", "</details>", "", source_links(source_sha, github_context or {})))
    return "\n".join(lines) + "\n", errors


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path(__file__).resolve().parents[1])
    parser.add_argument("--matrix-json", type=Path, required=True)
    parser.add_argument("--clients-json", type=Path, required=True)
    parser.add_argument("--summary", type=Path, required=True)
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--checkout-sha", required=True)
    args = parser.parse_args()
    github_context = {
        name: os.environ.get(name, "")
        for name in ("GITHUB_SERVER_URL", "GITHUB_REPOSITORY", "GITHUB_RUN_ID")
    }
    summary, errors = report(
        args.root, args.matrix_json, args.clients_json,
        args.source_sha, args.checkout_sha, client_versions(), github_context,
    )
    with args.summary.open("a", encoding="utf-8") as output:
        output.write(summary)
    print("S3 compatibility report: " + ("failed" if errors else "passed"))
    for error in errors:
        print(error, file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
