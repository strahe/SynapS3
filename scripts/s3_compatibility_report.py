#!/usr/bin/env python3
"""Validate the documented S3 matrix against black-box Go test results."""

import argparse
import json
import re
import subprocess
import sys
from pathlib import Path


MATRIX_ROOT = "TestS3CompatibilityMatrix"
CLIENT_ROOT = "TestS3Clients"
CLIENTS = {"AWS_CLI": ("aws", "--version"), "rclone": ("rclone", "version"), "mc": ("mc", "--version")}
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
                results.setdefault(test, []).append(action)
            else:
                package_results.append(action)
    if not results and not package_results:
        raise ValueError(f"{path}: no completed Go tests")
    return results, package_results


def result_status(results, test):
    actions = results.get(test, [])
    if len(actions) != 1:
        return "missing" if not actions else "duplicate"
    return actions[0]


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
    return str(value).replace("|", "\\|").replace("\n", " ").replace("\r", " ")


def report(root, matrix_json, clients_json, source_sha, checkout_sha, versions):
    errors = []
    try:
        promised, matrix_errors = validate_matrices(root)
        errors.extend(matrix_errors)
    except (OSError, ValueError) as exc:
        promised = {}
        errors.append(str(exc))

    test_sets = {}
    for label, path in (("matrix", matrix_json), ("clients", clients_json)):
        try:
            test_sets[label] = test_results(path)
        except (OSError, ValueError) as exc:
            test_sets[label] = ({}, [])
            errors.append(str(exc))

    matrix_tests, matrix_package = test_sets["matrix"]
    client_tests, client_package = test_sets["clients"]
    for label, tests, package, root_test in (
        ("matrix", matrix_tests, matrix_package, MATRIX_ROOT),
        ("clients", client_tests, client_package, CLIENT_ROOT),
    ):
        if result_status(tests, root_test) != "pass":
            errors.append(f"{label} root test did not pass: {root_test}")
        if package != ["pass"]:
            errors.append(f"{label} Go package did not pass")

    lines = [
        "# S3 Compatibility",
        "",
        f"PR source commit: `{cell(source_sha)}`  ",
        f"Tested checkout: `{cell(checkout_sha)}`",
        "",
        "## S3 operations",
        "",
        "| Operation | Matrix status | Test result |",
        "| --- | --- | --- |",
    ]
    for operation, (status, _) in promised.items():
        result = result_status(matrix_tests, f"{MATRIX_ROOT}/{operation}")
        if result != "pass":
            errors.append(f"{operation} test is {result}")
        lines.append(f"| `{cell(operation)}` | {status} | {result} |")

    lines.extend(("", "## S3 clients", "", "| Client | Version | Test result |", "| --- | --- | --- |"))
    for name in CLIENTS:
        result = result_status(client_tests, f"{CLIENT_ROOT}/{name}")
        if result != "pass":
            errors.append(f"{name} client test is {result}")
        if versions[name] == "unavailable":
            errors.append(f"{name} version is unavailable")
        lines.append(f"| {name} | {cell(versions[name])} | {result} |")

    if errors:
        lines.extend(("", "## Validation errors", ""))
        lines.extend(f"- {cell(error)}" for error in errors)
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
    summary, errors = report(args.root, args.matrix_json, args.clients_json, args.source_sha, args.checkout_sha, client_versions())
    with args.summary.open("a", encoding="utf-8") as output:
        output.write(summary)
    print("S3 compatibility report: " + ("failed" if errors else "passed"))
    for error in errors:
        print(error, file=sys.stderr)
    return 1 if errors else 0


if __name__ == "__main__":
    sys.exit(main())
