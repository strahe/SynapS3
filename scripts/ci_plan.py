"""Select CI work from complete Git diffs and validate scheduled job results."""

import argparse
import json
import os
import re
import subprocess
import sys
from pathlib import Path


FLAGS = (
    "verify", "go_tests", "race", "system", "s3_compatibility",
    "docker_image", "docker_deployment", "docs",
)
CORE = {"verify", "go_tests", "race", "system", "s3_compatibility"}
CORE_JOBS = ("verify", "race", "system", "s3_compatibility")
WRITER_ORDER = ("verify", "system", "race")
S3_DOCS = {
    "README.md",
    "docs/en/reference/s3-compatibility.md",
    "docs/zh/reference/s3-compatibility.md",
}
S3_TESTS = {
    "tests/system/s3_matrix_test.go",
    "tests/system/s3_clients_test.go",
    "tests/system/s3_upgrade_test.go",
}
SHARED_GO_HELPERS = (
    "internal/systemtest/", "internal/testutil/", "internal/testpg/",
    "cmd/synaps3-systemtest/",
)
DEPLOYMENT_FILES = {
    ".env.example", "compose.yaml", "compose.local.yaml",
    "compose.admin-https.yaml", "compose.postgres.yaml",
    "docker/Caddyfile", "docker/deployment.sh", "docker/postgres-init.sql",
    "docker/entrypoint.test.sh", "docker/deployment.test.sh",
}


def selected_flags(path):
    if (
        path.startswith((".github/workflows/", ".github/actions/"))
        or path in {"Makefile", ".golangci.yml", "scripts/ci_plan.py", "scripts/test_ci_plan.py"}
    ):
        return set(FLAGS)
    if path in S3_DOCS:
        return {"s3_compatibility", "docs"}
    if path.startswith("docs/"):
        return {"docs"}
    if path.startswith("ui/") and path.endswith(".go"):
        return CORE | {"docker_image"}
    if path.startswith(SHARED_GO_HELPERS) and path.endswith(".go"):
        return CORE
    if path.startswith(("cmd/", "internal/")) and path.endswith("_test.go"):
        return {"verify", "go_tests", "race"}
    if (
        path.startswith(("cmd/", "internal/")) and path.endswith(".go")
        or path.startswith("internal/db/migrations/") and path.endswith(".sql")
        or path in {"go.mod", "go.sum"}
    ):
        return CORE | {"docker_image"}
    if path in S3_TESTS or path in {"scripts/s3_compatibility_report.py", "scripts/test_s3_compatibility_report.py"}:
        return {"s3_compatibility"}
    if path.startswith(("tests/system/", "tests/testutil/")):
        return {"system", "s3_compatibility"}
    if path.startswith(("ui/e2e/", "ui/test/")) or path == "ui/playwright.config.ts":
        return set()
    if path.startswith("ui/"):
        return {"verify", "docker_image"}
    if path in {"Dockerfile", ".dockerignore"}:
        return {"docker_image"}
    if path == "docker/entrypoint.sh":
        return {"docker_image", "docker_deployment"}
    if path in DEPLOYMENT_FILES:
        return {"docker_deployment"}
    return set(FLAGS)


def plan_for_paths(paths):
    selected = set()
    for path in paths:
        selected.update(selected_flags(path))
    plan = {flag: flag in selected for flag in FLAGS}
    plan["modules_writer"] = next((job for job in WRITER_ORDER if plan[job]), "")
    return plan


def full_plan():
    return plan_for_paths(["Makefile"])


class DiffUnavailable(Exception):
    pass


def git(*args):
    try:
        return subprocess.run(
            ["git", *args], check=True, stdout=subprocess.PIPE,
            stderr=subprocess.PIPE, timeout=30,
        ).stdout
    except (OSError, subprocess.SubprocessError) as error:
        raise DiffUnavailable("Git could not provide a complete diff") from error


def valid_sha(value):
    return isinstance(value, str) and re.fullmatch(r"(?:[0-9a-fA-F]{40}|[0-9a-fA-F]{64})", value) and set(value) != {"0"}


def parse_name_status(data):
    if not data:
        return []
    if not data.endswith(b"\0"):
        raise DiffUnavailable("Git returned an incomplete path list")
    fields = data[:-1].split(b"\0")
    paths = []
    index = 0
    while index < len(fields):
        status = fields[index]
        index += 1
        if re.fullmatch(rb"[RC][0-9]{1,3}", status):
            count = 2
        elif status in (b"A", b"D", b"M", b"T"):
            count = 1
        else:
            raise DiffUnavailable("Git returned an unsupported change status")
        names = fields[index:index + count]
        if len(names) != count or any(not name for name in names):
            raise DiffUnavailable("Git returned an incomplete change record")
        paths.extend(name.decode("utf-8", "surrogateescape") for name in names)
        index += count
    return paths


def changed_paths(event_name, event, sha):
    if not valid_sha(sha) or git("rev-parse", "HEAD").decode().strip() != sha:
        raise DiffUnavailable("The checkout does not match the event commit")
    if event_name == "pull_request":
        header = git("cat-file", "-p", sha).split(b"\n\n", 1)[0]
        parents = [line[7:].decode() for line in header.splitlines() if line.startswith(b"parent ")]
        if len(parents) != 2 or not all(valid_sha(parent) for parent in parents):
            raise DiffUnavailable("The pull request checkout is not a merge commit")
        before = parents[0]
        git("cat-file", "-e", before + "^{commit}")
    elif event_name == "push":
        before = event.get("before")
        if not valid_sha(before) or event.get("after") != sha:
            raise DiffUnavailable("The push does not provide valid before and after commits")
        try:
            git("cat-file", "-e", before + "^{commit}")
        except DiffUnavailable:
            git("fetch", "--no-tags", "--depth=1", "origin", before)
            git("cat-file", "-e", before + "^{commit}")
    else:
        raise DiffUnavailable("This event has no supported diff")
    return parse_name_status(git("diff", "--name-status", "-z", "--find-renames", before, sha, "--"))


def event_plan(event_name, event, ref, sha):
    if event_name == "workflow_dispatch" or event_name == "push" and ref.startswith("refs/tags/"):
        return full_plan()
    try:
        return plan_for_paths(changed_paths(event_name, event, sha))
    except DiffUnavailable as error:
        print(f"CI plan: {error}; running all checks", file=sys.stderr)
        return full_plan()


def validate_plan(plan):
    if not isinstance(plan, dict) or set(plan) != set(FLAGS) | {"modules_writer"}:
        raise ValueError("the plan must contain every flag and modules_writer")
    if any(type(plan[flag]) is not bool for flag in FLAGS):
        raise ValueError("plan flags must be JSON booleans")
    if plan["go_tests"] and not plan["verify"]:
        raise ValueError("Go tests require the verify job")
    writer = next((job for job in WRITER_ORDER if plan[job]), "")
    if plan["modules_writer"] != writer:
        raise ValueError("modules_writer does not match the scheduled jobs")


def check_job_results(plan, results, scope="ci", event_name="pull_request"):
    try:
        validate_plan(plan)
    except ValueError as error:
        return [f"Invalid CI plan: {error}"]
    if not isinstance(results, dict):
        return ["Job results must be a JSON object"]
    if scope not in ("ci", "s3"):
        return [f"Unknown job results scope: {scope}"]

    def result(job):
        value = results.get(job)
        return value.get("result") if isinstance(value, dict) else value

    errors = []
    if result("changes") != "success":
        errors.append("changes must succeed before job results can be accepted")
    jobs = CORE_JOBS if scope == "ci" else ("s3_compatibility",)
    for job in jobs:
        selected = plan[job] and (job != "s3_compatibility" or scope == "s3" or event_name == "pull_request")
        expected = "success" if selected else "skipped"
        actual = result(job)
        if actual != expected:
            errors.append(f"{job}: expected {expected}, got {actual}")
    return errors


def write_outputs(plan, output_path):
    validate_plan(plan)
    payload = json.dumps(plan, separators=(",", ":"))
    if output_path:
        lines = [f"{flag}={str(plan[flag]).lower()}" for flag in FLAGS]
        lines.extend((f"modules_writer={plan['modules_writer']}", f"plan={payload}"))
        with Path(output_path).open("a", encoding="utf-8") as output:
            output.write("\n".join(lines) + "\n")
    return payload


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    select = commands.add_parser("plan")
    select.add_argument("--input-plan", default="")
    select.add_argument("--event-name", default=os.environ.get("GITHUB_EVENT_NAME", ""))
    select.add_argument("--event-path", default=os.environ.get("GITHUB_EVENT_PATH", ""))
    select.add_argument("--ref", default=os.environ.get("GITHUB_REF", ""))
    select.add_argument("--sha", default=os.environ.get("GITHUB_SHA", ""))
    select.add_argument("--output", default=os.environ.get("GITHUB_OUTPUT", ""))
    check = commands.add_parser("check")
    check.add_argument("--scope", choices=("ci", "s3"), default="ci")
    check.add_argument("--event-name", default=os.environ.get("GITHUB_EVENT_NAME") or "pull_request")
    check.add_argument("--plan", required=True)
    check.add_argument("--results", required=True)
    args = parser.parse_args()
    if args.command == "check":
        try:
            errors = check_job_results(json.loads(args.plan), json.loads(args.results), args.scope, args.event_name)
        except (ValueError, TypeError) as error:
            errors = [f"Invalid CI check input: {error}"]
        if errors:
            print("\n".join(errors), file=sys.stderr)
            return 1
        print(f"Job results match the {args.scope} plan")
        return 0
    if args.input_plan:
        try:
            plan = json.loads(args.input_plan)
            validate_plan(plan)
        except (ValueError, TypeError) as error:
            print(f"Invalid input CI plan: {error}", file=sys.stderr)
            return 1
    else:
        try:
            event = json.loads(Path(args.event_path).read_text(encoding="utf-8"))
            if not isinstance(event, dict):
                raise ValueError("event must be an object")
            plan = event_plan(args.event_name, event, args.ref, args.sha)
        except (OSError, ValueError):
            print("CI plan: event data is unavailable; running all checks", file=sys.stderr)
            plan = full_plan()
    print(write_outputs(plan, args.output))
    return 0


if __name__ == "__main__":
    sys.exit(main())
