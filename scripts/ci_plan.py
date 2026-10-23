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
WORKER_CHECKS = {
    "verify": {
        "lint_config": "verify", "fmt": "verify", "lint": "verify", "build": "verify",
        "go_tests": "go_tests", "race": "race", "system": "system", "s3_compatibility": "s3_compatibility",
    },
    "package": {
        "docker_image": "docker_image", "docs": "docs",
        "docker_entrypoint": "docker_deployment", "docker_deployment": "docker_deployment",
    },
}
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
    plan["modules_writer"] = module_writer(plan)
    return plan


def module_writer(plan):
    return "verify" if any(plan[flag] for flag in CORE) else ""


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
    writer = module_writer(plan)
    if plan["modules_writer"] != writer:
        raise ValueError("modules_writer does not match the scheduled jobs")


def check_worker_results(plan, worker, checks, event_name="pull_request"):
    expected = WORKER_CHECKS[worker]
    if not isinstance(checks, dict) or checks.keys() != expected.keys():
        return [f"{worker}: check results must contain every expected check"]
    errors = []
    for check, flag in expected.items():
        actual = checks[check]
        selected = plan[flag] and (check != "docker_image" or event_name == "pull_request")
        if selected:
            if actual != "success":
                errors.append(f"{worker}/{check}: expected success, got {actual}")
        elif actual not in ("skipped", "not_selected"):
            errors.append(f"{worker}/{check}: expected not_selected, got {actual}")
    return errors


def ordered_packages(packages, module):
    if not isinstance(module, str) or not module:
        raise ValueError("ordinary package inventory requires a module identity")
    if not isinstance(packages, list) or not packages or any(not isinstance(package, str) for package in packages):
        raise ValueError("ordinary package inventory must be a nonempty list of package names")
    if len(packages) != len(set(packages)):
        raise ValueError("ordinary package inventory is empty or contains duplicates")
    roots = (module + "/cmd", module + "/internal")
    if any(not isinstance(package, str) or not any(
        package == root or package.startswith(root + "/") for root in roots
    ) for package in packages):
        raise ValueError("ordinary package inventory contains an unrelated package")
    priority = module + "/internal/task"
    if priority not in packages:
        raise ValueError("ordinary package inventory is missing internal/task")
    return [priority, *sorted(package for package in packages if package != priority)]


def check_job_results(plan, results, source_sha, event_name="pull_request"):
    try:
        validate_plan(plan)
    except ValueError as error:
        return [f"Invalid CI plan: {error}"]
    if not isinstance(results, dict):
        return ["Job results must be a JSON object"]
    if not valid_sha(source_sha):
        return ["The expected source SHA is invalid"]
    errors = []
    publish = results.get("publish")
    if event_name == "push":
        if not isinstance(publish, dict) or publish.get("result") != "success":
            actual = publish.get("result") if isinstance(publish, dict) else None
            errors.append(f"publish: expected success, got {actual}")
        else:
            try:
                outputs = publish.get("outputs", {})
                reported_plan = json.loads(outputs.get("plan", ""))
                validate_plan(reported_plan)
                if reported_plan != plan:
                    errors.append("publish: selection plan differs from the final check")
                if outputs.get("source-sha") != source_sha:
                    errors.append("publish: checkout differs from the requested source SHA")
                actual_image = outputs.get("docker-image")
                expected = ("success",) if plan["docker_image"] else ("skipped", "not_selected")
                if actual_image not in expected:
                    errors.append(f"publish/docker_image: expected {expected}, got {actual_image}")
            except (ValueError, TypeError, AttributeError) as error:
                errors.append(f"publish: invalid worker outputs: {error}")
    elif not isinstance(publish, dict) or publish.get("result") != "skipped":
        errors.append("publish: image publishing must be skipped outside a main push")
    for worker in WORKER_CHECKS:
        job = results.get(worker)
        if not isinstance(job, dict) or job.get("result") != "success":
            actual = job.get("result") if isinstance(job, dict) else None
            errors.append(f"{worker}: expected success, got {actual}")
            continue
        outputs = job.get("outputs", {})
        if not isinstance(outputs, dict):
            errors.append(f"{worker}: worker outputs are missing")
            continue
        try:
            reported_plan = json.loads(outputs.get("plan", ""))
            validate_plan(reported_plan)
            if reported_plan != plan:
                errors.append(f"{worker}: selection plan differs from the final check")
            if outputs.get("source-sha") != source_sha:
                errors.append(f"{worker}: checkout differs from the requested source SHA")
            if outputs.get("verified") != "true":
                errors.append(f"{worker}: checks were not verified")
        except (ValueError, TypeError, KeyError) as error:
            errors.append(f"{worker}: invalid worker outputs: {error}")
    return errors


def record_worker(plan, worker, checks, output_path, event_name="pull_request"):
    validate_plan(plan)
    print(f"{worker} check results: {json.dumps(checks, sort_keys=True)}")
    errors = check_worker_results(plan, worker, checks, event_name)
    if errors:
        return errors
    source_sha = git("rev-parse", "HEAD").decode().strip()
    if not valid_sha(source_sha):
        return [f"{worker}: checkout source SHA is invalid"]
    outputs = {
        "plan": json.dumps(plan, separators=(",", ":")),
        "source-sha": source_sha,
        "verified": "true",
    }
    if output_path:
        with Path(output_path).open("a", encoding="utf-8") as output:
            output.write("".join(f"{name}={value}\n" for name, value in outputs.items()))
    return errors


def run_ordinary_tests(worker, parallelism, json_output):
    if worker != "verify":
        raise ValueError("ordinary tests require the verify worker")
    if parallelism < 1:
        raise ValueError("package parallelism must be positive")
    environment = dict(os.environ, CGO_ENABLED="1")
    module = subprocess.check_output(["go", "list", "-m"], env=environment, text=True).strip()
    packages = subprocess.check_output(["go", "list", "./cmd/...", "./internal/..."], env=environment, text=True).splitlines()
    selected = ordered_packages(packages, module)
    print("Ordinary test packages:\n" + "\n".join(selected), file=sys.stderr, flush=True)
    command = ["go", "run", "-trimpath", "./internal/testpg/cmd/testrun", "--", "-trimpath",
               "-p", str(parallelism), "-parallel", "2", "-count=1"]
    if json_output:
        command.append("-json")
    os.execvpe(command[0], [*command, *selected], environment)


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
    check.add_argument("--event-name", default=os.environ.get("GITHUB_EVENT_NAME") or "pull_request")
    check.add_argument("--plan", required=True)
    check.add_argument("--results", required=True)
    check.add_argument("--sha", default=os.environ.get("GITHUB_SHA", ""))
    record = commands.add_parser("record")
    record.add_argument("--worker", choices=tuple(WORKER_CHECKS), required=True)
    record.add_argument("--output", default=os.environ.get("GITHUB_OUTPUT", ""))
    record.add_argument("--event-name", default=os.environ.get("GITHUB_EVENT_NAME") or "pull_request")
    test = commands.add_parser("test")
    test.add_argument("--worker", choices=("verify",), default="verify")
    test.add_argument("--package-parallelism", type=int, default=2)
    test.add_argument("--json", action="store_true")
    args = parser.parse_args()
    if args.command == "test":
        try:
            run_ordinary_tests(args.worker, args.package_parallelism, args.json)
        except (OSError, ValueError, subprocess.SubprocessError) as error:
            print(f"Could not run ordinary tests: {error}", file=sys.stderr)
            return 1
        return 0
    if args.command == "record":
        try:
            errors = record_worker(
                json.loads(os.environ.get("CI_PLAN", "")), args.worker,
                json.loads(os.environ.get("CI_CHECKS", "")), args.output, args.event_name,
            )
        except (OSError, ValueError, TypeError, DiffUnavailable) as error:
            errors = [f"Invalid CI worker input: {error}"]
        if errors:
            print("\n".join(errors), file=sys.stderr)
            return 1
        return 0
    if args.command == "check":
        try:
            errors = check_job_results(json.loads(args.plan), json.loads(args.results), args.sha, args.event_name)
        except (ValueError, TypeError) as error:
            errors = [f"Invalid CI check input: {error}"]
        if errors:
            print("\n".join(errors), file=sys.stderr)
            return 1
        print("Job results match the CI plan")
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
