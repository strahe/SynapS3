"""Regression checks for CI routing, complete diffs, and expected job skips."""

import contextlib
import io
import json
import os
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import ci_plan


class RoutingTest(unittest.TestCase):
    def test_paths_select_the_affected_work(self):
        core = {"verify", "go_tests", "race", "system", "s3_compatibility"}
        cases = (
            ("docs/en/getting-started/overview.md", {"docs"}),
            ("docs/package.json", {"docs"}),
            ("README.md", {"s3_compatibility", "docs"}),
            ("docs/en/reference/s3-compatibility.md", {"s3_compatibility", "docs"}),
            ("docs/zh/reference/s3-compatibility.md", {"s3_compatibility", "docs"}),
            ("ui/src/main.tsx", {"verify", "docker_image"}),
            ("ui/package.json", {"verify", "docker_image"}),
            ("ui/test/tasks.test.ts", set()),
            ("ui/e2e/fixtures.ts", set()),
            ("ui/playwright.config.ts", set()),
            ("ui/embed.go", core | {"docker_image"}),
            ("ui/embed_dev.go", core | {"docker_image"}),
            ("internal/backend/object.go", core | {"docker_image"}),
            ("internal/db/migrations/baseline.sql", core | {"docker_image"}),
            ("go.mod", core | {"docker_image"}),
            ("go.sum", core | {"docker_image"}),
            ("cmd/synaps3/admin_test.go", {"verify", "go_tests", "race"}),
            ("internal/db/repository/object_repo_test.go", {"verify", "go_tests", "race"}),
            ("internal/systemtest/harness.go", core),
            ("internal/systemtest/harness_test.go", core),
            ("internal/testutil/mocks.go", core),
            ("internal/testpg/postgres.go", core),
            ("cmd/synaps3-systemtest/main.go", core),
            ("tests/system/s3_matrix_test.go", {"s3_compatibility"}),
            ("tests/system/s3_clients_test.go", {"s3_compatibility"}),
            ("tests/system/s3_upgrade_test.go", {"s3_compatibility"}),
            ("tests/system/main_test.go", {"system", "s3_compatibility"}),
            ("tests/system/task_lifecycle_test.go", {"system", "s3_compatibility"}),
            ("tests/system/testdata/fixture.json", {"system", "s3_compatibility"}),
            ("tests/testutil/e2e/readiness.go", {"system", "s3_compatibility"}),
            ("scripts/s3_compatibility_report.py", {"s3_compatibility"}),
            ("scripts/test_s3_compatibility_report.py", {"s3_compatibility"}),
            ("Dockerfile", {"docker_image"}),
            (".dockerignore", {"docker_image"}),
            ("docker/entrypoint.sh", {"docker_image", "docker_deployment"}),
            ("docker/entrypoint.test.sh", {"docker_deployment"}),
            ("docker/deployment.test.sh", {"docker_deployment"}),
            (".env.example", {"docker_deployment"}),
            ("compose.postgres.yaml", {"docker_deployment"}),
            ("docker/Caddyfile", {"docker_deployment"}),
            ("docker/deployment.sh", {"docker_deployment"}),
            ("docker/postgres-init.sql", {"docker_deployment"}),
        )
        for path, expected in cases:
            with self.subTest(path=path):
                plan = ci_plan.plan_for_paths([path])
                self.assertEqual({flag for flag in ci_plan.FLAGS if plan[flag]}, expected)

    def test_global_and_unknown_changes_run_everything(self):
        for path in (
            "Makefile", ".golangci.yml", ".github/workflows/ci.yml",
            ".github/workflows/s3-compatibility.yml",
            ".github/actions/setup-go/action.yml", "scripts/ci_plan.py",
            "scripts/test_ci_plan.py", "new-subsystem/input.txt", "docker/new-config.toml",
        ):
            with self.subTest(path=path):
                self.assertEqual(ci_plan.plan_for_paths([path]), ci_plan.full_plan())

    def test_mixed_paths_are_a_union_and_writer_is_scheduled(self):
        plan = ci_plan.plan_for_paths(["docs/index.md", "ui/src/main.tsx"])
        self.assertEqual({flag for flag in ci_plan.FLAGS if plan[flag]}, {"docs", "verify", "docker_image"})
        self.assertEqual(plan["modules_writer"], "verify")
        self.assertEqual(ci_plan.plan_for_paths(["tests/system/main_test.go"])["modules_writer"], "verify")
        self.assertEqual(ci_plan.plan_for_paths(["tests/system/s3_matrix_test.go"])["modules_writer"], "verify")
        self.assertEqual(ci_plan.plan_for_paths(["docs/index.md"])["modules_writer"], "")
        self.assertFalse(any(ci_plan.plan_for_paths([])[flag] for flag in ci_plan.FLAGS))


class DiffTest(unittest.TestCase):
    def test_renames_copies_deletions_and_unusual_names_keep_all_paths(self):
        data = (
            b"R100\0docs/en/old name\n.md\0internal/backend/new.go\0"
            b"D\0docker/entrypoint.sh\0C075\0ui/src/a.tsx\0ui/src/b\t.tsx\0"
        )
        paths = ci_plan.parse_name_status(data)
        self.assertEqual(paths, [
            "docs/en/old name\n.md", "internal/backend/new.go", "docker/entrypoint.sh",
            "ui/src/a.tsx", "ui/src/b\t.tsx",
        ])
        plan = ci_plan.plan_for_paths(paths)
        self.assertTrue(plan["docs"])
        self.assertTrue(plan["go_tests"])
        self.assertTrue(plan["docker_deployment"])

    def test_large_diffs_do_not_drop_late_backend_changes(self):
        data = b"".join(f"M\0docs/page-{index}.md\0".encode() for index in range(4000))
        data += b"M\0internal/backend/object.go\0"
        paths = ci_plan.parse_name_status(data)
        self.assertEqual(len(paths), 4001)
        self.assertTrue(ci_plan.plan_for_paths(paths)["go_tests"])

    def test_incomplete_or_unknown_records_are_rejected(self):
        for data in (b"M\0docs/index.md", b"R100\0old.md\0", b"M\0\0", b"?\0path\0", b"X\0path\0"):
            with self.subTest(data=data), self.assertRaises(ci_plan.DiffUnavailable):
                ci_plan.parse_name_status(data)

    def test_push_fetches_only_a_missing_before_commit(self):
        before, after = "1" * 40, "2" * 40
        fetched = False

        def run(*args):
            nonlocal fetched
            if args == ("rev-parse", "HEAD"):
                return (after + "\n").encode()
            if args == ("cat-file", "-e", before + "^{commit}"):
                if not fetched:
                    raise ci_plan.DiffUnavailable("missing before")
                return b""
            if args == ("fetch", "--no-tags", "--depth=1", "origin", before):
                fetched = True
                return b""
            if args == ("diff", "--name-status", "-z", "--find-renames", before, after, "--"):
                return b"M\0docs/index.md\0"
            self.fail(f"Unexpected Git command: {args}")

        with patch.object(ci_plan, "git", side_effect=run):
            plan = ci_plan.event_plan("push", {"before": before, "after": after}, "refs/heads/main", after)
        self.assertTrue(fetched)
        self.assertEqual(plan, ci_plan.plan_for_paths(["docs/index.md"]))

    def test_missing_diff_or_invalid_before_runs_everything(self):
        sha = "2" * 40
        for event in ({"after": sha}, {"before": "0" * 40, "after": sha}):
            with self.subTest(event=event), patch.object(ci_plan, "git", return_value=(sha + "\n").encode()), contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(ci_plan.event_plan("push", event, "refs/heads/main", sha), ci_plan.full_plan())
        with patch.object(ci_plan, "git", side_effect=ci_plan.DiffUnavailable("fetch failed")), contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(ci_plan.event_plan("pull_request", {}, "refs/pull/1/merge", sha), ci_plan.full_plan())

    def test_failed_before_fetch_cannot_be_treated_as_an_empty_diff(self):
        before, after = "1" * 40, "2" * 40
        with patch.object(ci_plan, "git", side_effect=[
            (after + "\n").encode(), ci_plan.DiffUnavailable("missing before"),
            ci_plan.DiffUnavailable("fetch failed"),
        ]) as run, contextlib.redirect_stderr(io.StringIO()):
            plan = ci_plan.event_plan("push", {"before": before, "after": after}, "refs/heads/main", after)
        self.assertEqual(plan, ci_plan.full_plan())
        self.assertEqual(run.call_args.args, ("fetch", "--no-tags", "--depth=1", "origin", before))

    def test_tags_and_manual_runs_force_all_without_a_diff(self):
        with patch.object(ci_plan, "git", side_effect=AssertionError("Git must not be needed")):
            self.assertEqual(ci_plan.event_plan("push", {}, "refs/tags/v1.0.0", ""), ci_plan.full_plan())
            self.assertEqual(ci_plan.event_plan("workflow_dispatch", {}, "refs/heads/main", ""), ci_plan.full_plan())

    def test_pull_request_diff_uses_the_actual_merge_first_parent(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            env = dict(os.environ, GIT_AUTHOR_NAME="CI plan test", GIT_AUTHOR_EMAIL="ci-plan@example.invalid",
                       GIT_COMMITTER_NAME="CI plan test", GIT_COMMITTER_EMAIL="ci-plan@example.invalid")

            def run(*args, data=None):
                return subprocess.run(["git", *args], cwd=root, env=env, input=data,
                                      check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout.strip()

            run("init", "--quiet")

            def commit(files, parents=()):
                run("read-tree", "--empty")
                for path in files:
                    blob = run("hash-object", "-w", "--stdin", data=path.encode()).decode()
                    run("update-index", "--add", "--cacheinfo", f"100644,{blob},{path}")
                tree = run("write-tree").decode()
                args = ["commit-tree", tree]
                for parent in parents:
                    args.extend(("-p", parent))
                return run(*args, data=b"fixture\n").decode()

            stale_base = commit(["docs/index.md"])
            base = commit(["docs/index.md", "internal/backend/new.go"], [stale_base])
            head = commit(["docs/index.md", "docs/en/new.md"], [stale_base])
            merge = commit(["docs/index.md", "docs/en/new.md", "internal/backend/new.go"], [base, head])
            run("update-ref", "HEAD", merge)

            # Use the real Git tree comparison without changing the process working directory.
            def fixture_git(*args):
                try:
                    return subprocess.run(["git", *args], cwd=root, check=True,
                                          stdout=subprocess.PIPE, stderr=subprocess.PIPE).stdout
                except subprocess.CalledProcessError as error:
                    raise ci_plan.DiffUnavailable("fixture Git failed") from error

            with patch.object(ci_plan, "git", side_effect=fixture_git):
                plan = ci_plan.event_plan("pull_request", {"pull_request": {"base": {"sha": stale_base}, "head": {"sha": head}}}, "refs/pull/1/merge", merge)
            self.assertEqual(plan, ci_plan.plan_for_paths(["docs/en/new.md"]))


MODULE = "example.test/synaps3"
SOURCE_SHA = "1" * 40
PACKAGES = [
    MODULE + "/" + package for package in (
        "cmd/synaps3", "internal/task", "internal/admin", "internal/backend",
        "internal/db/repository", "internal/worker", "internal/empty",
    )
]


def worker_results(plan, source_sha=SOURCE_SHA):
    results = {"publish": {"result": "skipped"}}
    for worker in ci_plan.WORKER_CHECKS:
        outputs = {
            "plan": json.dumps(plan), "source-sha": source_sha,
            "verified": "true",
        }
        results[worker] = {"result": "success", "outputs": outputs}
    return results


class OrdinaryPackagesTest(unittest.TestCase):
    def test_inventory_keeps_all_packages_with_task_first_and_rejects_invalid_input(self):
        packages = PACKAGES + [MODULE + "/internal/new"]
        selected = ci_plan.ordered_packages(list(reversed(packages)), MODULE)
        task = MODULE + "/internal/task"
        self.assertEqual(selected, [task, *sorted(set(packages) - {task})])
        self.assertEqual(len(selected), len(packages))
        self.assertIn(MODULE + "/internal/empty", selected)
        for invalid, module in (
            ([], MODULE), (PACKAGES + [PACKAGES[0]], MODULE),
            (PACKAGES + ["other.test/pkg"], MODULE),
            (PACKAGES + [None], MODULE), (PACKAGES, ""),
            ([package for package in PACKAGES if package != task], MODULE),
        ):
            with self.subTest(packages=invalid, module=module), self.assertRaises(ValueError):
                ci_plan.ordered_packages(invalid, module)


class ResultGateTest(unittest.TestCase):
    def setUp(self):
        self.plan = ci_plan.full_plan()
        self.results = worker_results(self.plan)

    def check(self, results=None, plan=None, event_name="pull_request"):
        return ci_plan.check_job_results(
            self.plan if plan is None else plan, self.results if results is None else results,
            SOURCE_SHA, event_name,
        )

    def test_selected_success_and_explicit_unselected_checks_pass(self):
        self.assertEqual(self.check(), [])
        for paths in (["docs/index.md"], ["tests/system/main_test.go"], ["ui/src/main.tsx"], []):
            plan = ci_plan.plan_for_paths(paths)
            with self.subTest(paths=paths):
                self.assertEqual(self.check(worker_results(plan), plan), [])

    def test_failed_cancelled_skipped_or_missing_workers_never_pass(self):
        for worker in ci_plan.WORKER_CHECKS:
            for result in ("failure", "cancelled", "skipped", None):
                with self.subTest(worker=worker, result=result):
                    results = worker_results(self.plan)
                    results[worker]["result"] = result
                    self.assertTrue(self.check(results))
            results = worker_results(self.plan)
            del results[worker]
            self.assertTrue(self.check(results))

    def test_missing_malformed_or_mismatched_worker_outputs_fail(self):
        for worker in ci_plan.WORKER_CHECKS:
            for field, value in (
                ("plan", ""), ("plan", "null"), ("plan", json.dumps(ci_plan.plan_for_paths(["docs/index.md"]))),
                ("source-sha", ""), ("source-sha", "2" * 40),
                *(("verified", value) for value in (None, "", "false", "True", True, 1)),
            ):
                results = worker_results(self.plan)
                results[worker]["outputs"][field] = value
                with self.subTest(worker=worker, field=field, value=value):
                    self.assertTrue(self.check(results))
            results = worker_results(self.plan)
            del results[worker]["outputs"]
            self.assertTrue(self.check(results))
            results = worker_results(self.plan)
            del results[worker]["outputs"]["verified"]
            self.assertTrue(self.check(results))
        self.assertTrue(ci_plan.check_job_results(self.plan, self.results, ""))

    def test_main_publish_must_match_plan_sha_and_selected_image(self):
        results = worker_results(self.plan)
        results["publish"] = {
            "result": "success",
            "outputs": {"plan": json.dumps(self.plan), "source-sha": SOURCE_SHA, "docker-image": "success"},
        }
        self.assertEqual(self.check(results, event_name="push"), [])
        for field, value in (
            ("plan", "{}"), ("plan", json.dumps(ci_plan.plan_for_paths(["docs/index.md"]))),
            ("source-sha", "2" * 40), ("docker-image", "failure"),
            ("docker-image", "skipped"), ("docker-image", None),
        ):
            broken = json.loads(json.dumps(results))
            broken["publish"]["outputs"][field] = value
            with self.subTest(field=field, value=value):
                self.assertTrue(self.check(broken, event_name="push"))
        for status in ("cancelled", "failure", "skipped", None):
            broken = json.loads(json.dumps(results))
            broken["publish"]["result"] = status
            self.assertTrue(self.check(broken, event_name="push"))
        del results["publish"]
        self.assertTrue(self.check(results, event_name="push"))

    def test_main_without_image_requires_explicit_skip(self):
        plan = ci_plan.plan_for_paths(["README.md"])
        results = worker_results(plan)
        results["publish"] = {
            "result": "success",
            "outputs": {"plan": json.dumps(plan), "source-sha": SOURCE_SHA, "docker-image": "not_selected"},
        }
        self.assertEqual(self.check(results, plan, "push"), [])
        for actual in (None, "success", "failure"):
            results["publish"]["outputs"]["docker-image"] = actual
            with self.subTest(actual=actual):
                self.assertTrue(self.check(results, plan, "push"))

    def test_pr_publish_must_explicitly_skip_and_plans_must_be_complete_booleans(self):
        self.assertEqual(self.check(), [])
        for actual in (None, {}, {"result": "success"}, {"result": "cancelled"}):
            results = worker_results(self.plan)
            results["publish"] = actual
            with self.subTest(publish=actual):
                self.assertTrue(self.check(results))
        results = worker_results(self.plan)
        del results["publish"]
        self.assertTrue(self.check(results))
        for malformed in ({}, {**self.plan, "verify": "false"}, {**self.plan, "modules_writer": ""}):
            with self.subTest(plan=malformed):
                self.assertTrue(self.check(plan=malformed))

    def test_action_outputs_are_strict_booleans_and_complete_json(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "output"
            payload = ci_plan.write_outputs(self.plan, output)
            values = dict(line.split("=", 1) for line in output.read_text().splitlines())
        self.assertEqual(json.loads(payload), self.plan)
        self.assertEqual(json.loads(values["plan"]), self.plan)
        self.assertEqual(values["modules_writer"], "verify")
        self.assertEqual({flag: values[flag] for flag in ci_plan.FLAGS},
                         {flag: str(self.plan[flag]).lower() for flag in ci_plan.FLAGS})

    def record(self, plan, worker, checks, event_name="pull_request", source_sha=SOURCE_SHA):
        with tempfile.TemporaryDirectory() as temp, \
                patch.object(ci_plan, "git", return_value=(source_sha + "\n").encode()), \
                contextlib.redirect_stdout(io.StringIO()) as log:
            output = Path(temp) / "output"
            errors = ci_plan.record_worker(plan, worker, checks, output, event_name)
            values = dict(line.split("=", 1) for line in output.read_text().splitlines()) if output.exists() else {}
        return errors, values, log.getvalue()

    def test_record_logs_actual_checks_and_only_attests_success(self):
        plan = ci_plan.plan_for_paths(["docs/index.md"])
        for worker, expected in ci_plan.WORKER_CHECKS.items():
            checks = {check: "success" if plan[flag] else "skipped" for check, flag in expected.items()}
            with self.subTest(worker=worker):
                errors, values, log = self.record(plan, worker, checks)
                self.assertEqual(errors, [])
                self.assertEqual(values, {"plan": json.dumps(plan, separators=(",", ":")),
                                          "source-sha": SOURCE_SHA, "verified": "true"})
                self.assertIn(json.dumps(checks, sort_keys=True), log)

    def test_record_rejects_failed_missing_and_unexpected_checks_without_outputs(self):
        for worker, expected in ci_plan.WORKER_CHECKS.items():
            successful = {check: "success" for check in expected}
            for check in expected:
                for actual in ("failure", "cancelled", "skipped", None):
                    checks = dict(successful, **{check: actual})
                    with self.subTest(worker=worker, check=check, actual=actual):
                        errors, values, _ = self.record(self.plan, worker, checks)
                        self.assertTrue(errors)
                        self.assertEqual(values, {})
            for malformed in (None, [], {}, {**successful, "unexpected": "success"},
                              {check: actual for check, actual in successful.items() if check != next(iter(expected))}):
                with self.subTest(worker=worker, checks=malformed):
                    errors, values, _ = self.record(self.plan, worker, malformed)
                    self.assertTrue(errors)
                    self.assertEqual(values, {})
        plan = ci_plan.plan_for_paths(["docs/index.md"])
        for worker, expected in ci_plan.WORKER_CHECKS.items():
            checks = {check: "success" if plan[flag] else "not_selected" for check, flag in expected.items()}
            for check, flag in expected.items():
                if plan[flag]:
                    continue
                with self.subTest(worker=worker, unexpected=check):
                    errors, values, _ = self.record(plan, worker, dict(checks, **{check: "success"}))
                    self.assertTrue(errors)
                    self.assertEqual(values, {})

    def test_record_invalid_source_never_attests_success(self):
        checks = {check: "success" for check in ci_plan.WORKER_CHECKS["verify"]}
        for source_sha in ("", "invalid", "0" * 40):
            with self.subTest(source_sha=source_sha):
                errors, values, _ = self.record(self.plan, "verify", checks, source_sha=source_sha)
                self.assertTrue(errors)
                self.assertEqual(values, {})

    def test_record_image_expectations_follow_event_and_plan(self):
        for paths in (["Makefile"], ["docs/index.md"]):
            plan = ci_plan.plan_for_paths(paths)
            for event in ("pull_request", "push", "workflow_dispatch"):
                checks = {check: "success" if plan[flag] else "skipped"
                          for check, flag in ci_plan.WORKER_CHECKS["package"].items()}
                for actual in ("success", "skipped", "not_selected", "failure"):
                    checks["docker_image"] = actual
                    selected = plan["docker_image"] and event == "pull_request"
                    expected = actual == "success" if selected else actual in ("skipped", "not_selected")
                    with self.subTest(paths=paths, event=event, actual=actual):
                        errors, values, _ = self.record(plan, "package", checks, event)
                        self.assertEqual(not errors, expected)
                        self.assertEqual(values.get("verified"), "true" if expected else None)

    def test_record_retains_s3_requirement_on_main(self):
        plan = ci_plan.plan_for_paths(["README.md"])
        checks = {check: "success" if plan[flag] else "skipped"
                  for check, flag in ci_plan.WORKER_CHECKS["verify"].items()}
        checks["s3_compatibility"] = "skipped"
        errors, values, _ = self.record(plan, "verify", checks, "push")
        self.assertTrue(any("s3_compatibility" in error for error in errors))
        self.assertEqual(values, {})

    def test_command_line_consumes_event_environment_and_rejects_cancelled_jobs(self):
        script = Path(ci_plan.__file__).resolve()
        with tempfile.TemporaryDirectory() as temp:
            event = Path(temp) / "event.json"
            event.write_text('{"inputs":{"mode":"build-only"}}', encoding="utf-8")
            output = Path(temp) / "output"
            env = dict(os.environ, GITHUB_EVENT_NAME="workflow_dispatch", GITHUB_EVENT_PATH=str(event),
                       GITHUB_REF="refs/heads/main", GITHUB_SHA="", GITHUB_OUTPUT=str(output))
            for arguments in ([], ["--input-plan", ""]):
                with self.subTest(arguments=arguments):
                    process = subprocess.run([sys.executable, str(script), "plan", *arguments], env=env,
                                             check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                    self.assertEqual(json.loads(process.stdout), ci_plan.full_plan())
                    self.assertIn("docker_image=true\n", output.read_text())
        self.results["verify"]["result"] = "cancelled"
        process = subprocess.run([
            sys.executable, str(script), "check", "--plan", json.dumps(self.plan),
            "--results", json.dumps(self.results), "--sha", SOURCE_SHA, "--event-name", "pull_request",
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.assertEqual(process.returncode, 1)
        self.assertIn("expected success, got cancelled", process.stderr)

    def test_command_line_reuses_parent_plan_without_recomputing_diff(self):
        with tempfile.TemporaryDirectory() as temp:
            event = Path(temp) / "event.json"
            event.write_text("{}", encoding="utf-8")
            output = Path(temp) / "output"
            for parent in (ci_plan.full_plan(), ci_plan.plan_for_paths(["README.md"])):
                for event_path in (event, Path(temp) / "unavailable.json"):
                    arguments = [ci_plan.__file__, "plan", "--input-plan", json.dumps(parent),
                                 "--event-name", "pull_request", "--event-path", str(event_path),
                                 "--output", str(output)]
                    stdout = io.StringIO()
                    with self.subTest(parent=parent, event=event_path.name), patch.object(sys, "argv", arguments), \
                            patch.object(ci_plan, "event_plan") as recompute, contextlib.redirect_stdout(stdout):
                        self.assertEqual(ci_plan.main(), 0)
                    recompute.assert_not_called()
                    self.assertEqual(json.loads(stdout.getvalue()), parent)

    def test_command_line_rejects_invalid_parent_plans_without_outputs(self):
        script = Path(ci_plan.__file__).resolve()
        malformed = ("{", "{}", "null", json.dumps({**self.plan, "s3_compatibility": "false"}),
                     json.dumps({**self.plan, "modules_writer": ""}))
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "output"
            for parent in malformed:
                with self.subTest(parent=parent):
                    process = subprocess.run([
                        sys.executable, str(script), "plan", "--input-plan", parent, "--output", str(output),
                    ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
                    self.assertEqual(process.returncode, 1)
                    self.assertIn("Invalid input CI plan", process.stderr)
                    self.assertEqual(process.stdout, "")
                    self.assertFalse(output.exists())

    def test_check_command_uses_sha_and_event_environment_with_explicit_override(self):
        command = [sys.executable, ci_plan.__file__, "check", "--plan", json.dumps(self.plan),
                   "--results", json.dumps(self.results)]
        for event, expected in (("pull_request", 0), ("push", 1)):
            env = dict(os.environ, GITHUB_EVENT_NAME=event, GITHUB_SHA=SOURCE_SHA)
            process = subprocess.run(command, env=env, capture_output=True, text=True)
            self.assertEqual(process.returncode, expected, process.stderr)
        env = dict(os.environ, GITHUB_EVENT_NAME="push", GITHUB_SHA="2" * 40)
        process = subprocess.run(command + ["--event-name", "pull_request", "--sha", SOURCE_SHA],
                                 env=env, capture_output=True, text=True)
        self.assertEqual(process.returncode, 0, process.stderr)
        env.pop("GITHUB_EVENT_NAME", None)
        env["GITHUB_SHA"] = SOURCE_SHA
        process = subprocess.run(command, env=env, capture_output=True, text=True)
        self.assertEqual(process.returncode, 0, process.stderr)

    def test_record_command_consumes_environment_and_rejects_invalid_json(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "output"
            arguments = [ci_plan.__file__, "record", "--worker", "package", "--output", str(output),
                         "--event-name", "pull_request"]
            checks = {check: "success" for check in ci_plan.WORKER_CHECKS["package"]}
            env = {"CI_PLAN": json.dumps(self.plan), "CI_CHECKS": json.dumps(checks)}
            with patch.object(sys, "argv", arguments), patch.dict(os.environ, env), \
                    patch.object(ci_plan, "git", return_value=(SOURCE_SHA + "\n").encode()), \
                    contextlib.redirect_stdout(io.StringIO()):
                self.assertEqual(ci_plan.main(), 0)
            self.assertEqual(dict(line.split("=", 1) for line in output.read_text().splitlines())["source-sha"], SOURCE_SHA)
            output.unlink()
            with patch.object(sys, "argv", arguments), patch.dict(os.environ, {"CI_PLAN": "{"}), \
                    contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(ci_plan.main(), 1)
            self.assertFalse(output.exists())
            with patch.object(sys, "argv", arguments), patch.dict(os.environ, env), \
                    patch.object(ci_plan, "git", side_effect=ci_plan.DiffUnavailable("checkout unavailable")), \
                    contextlib.redirect_stdout(io.StringIO()), contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(ci_plan.main(), 1)
            self.assertFalse(output.exists())

    def test_test_command_executes_complete_dynamic_inventory(self):
        packages = PACKAGES + [MODULE + "/internal/new"]
        selected = ci_plan.ordered_packages(packages, MODULE)
        for worker_arguments in ([], ["--worker", "verify"]):
            arguments = [ci_plan.__file__, "test", *worker_arguments, "--json"]
            with self.subTest(worker_arguments=worker_arguments), patch.object(sys, "argv", arguments), \
                    patch.object(ci_plan.subprocess, "check_output", side_effect=[MODULE + "\n", "\n".join(packages)]) as go, \
                    patch.object(ci_plan.os, "execvpe") as execute, \
                    contextlib.redirect_stdout(io.StringIO()) as stdout, contextlib.redirect_stderr(io.StringIO()) as stderr:
                self.assertEqual(ci_plan.main(), 0)
            self.assertEqual([call.args[0] for call in go.call_args_list],
                             [["go", "list", "-m"], ["go", "list", "./cmd/...", "./internal/..."]])
            executable, command, environment = execute.call_args.args
            self.assertEqual(executable, "go")
            self.assertEqual(command[:6], ["go", "run", "-trimpath", "./internal/testpg/cmd/testrun", "--", "-trimpath"])
            self.assertEqual(command[6:12], ["-p", "2", "-parallel", "2", "-count=1", "-json"])
            self.assertEqual(command[12:], selected)
            self.assertEqual(environment["CGO_ENABLED"], "1")
            self.assertEqual(stdout.getvalue(), "")
            self.assertIn("\n".join(selected), stderr.getvalue())
        with patch.object(sys, "argv", arguments + ["--package-parallelism", "0"]), \
                patch.object(ci_plan.subprocess, "check_output") as go, contextlib.redirect_stderr(io.StringIO()):
            self.assertEqual(ci_plan.main(), 1)
            go.assert_not_called()

    def test_test_command_does_not_start_tests_when_go_list_fails(self):
        for responses in (
            [subprocess.CalledProcessError(1, ["go", "list", "-m"])],
            [MODULE + "\n", subprocess.CalledProcessError(1, ["go", "list", "./cmd/...", "./internal/..."])],
            [MODULE + "\n", ""],
            [MODULE + "\n", MODULE + "/internal/worker\n"],
        ):
            with self.subTest(responses=responses), patch.object(sys, "argv", [ci_plan.__file__, "test"]), \
                    patch.object(ci_plan.subprocess, "check_output", side_effect=responses), \
                    patch.object(ci_plan.os, "execvpe") as execute, contextlib.redirect_stderr(io.StringIO()):
                self.assertEqual(ci_plan.main(), 1)
            execute.assert_not_called()

    def test_commands_reject_removed_test_worker(self):
        for command in ("test", "record"):
            arguments = [ci_plan.__file__, command, "--worker", "tests"]
            with self.subTest(command=command), patch.object(sys, "argv", arguments), \
                    patch.object(ci_plan.os, "execvpe") as execute, contextlib.redirect_stderr(io.StringIO()), \
                    self.assertRaises(SystemExit) as error:
                ci_plan.main()
            self.assertEqual(error.exception.code, 2)
            execute.assert_not_called()

if __name__ == "__main__":
    unittest.main()
