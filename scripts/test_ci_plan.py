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
        core = ci_plan.CORE
        cases = (
            ("docs/en/getting-started/overview.md", {"docs"}),
            ("docs/package.json", {"docs"}),
            ("README.md", {"system", "docs"}),
            ("docs/en/reference/s3-compatibility.md", {"system", "docs"}),
            ("docs/zh/reference/s3-compatibility.md", {"system", "docs"}),
            ("ui/src/main.tsx", {"verify", "dashboard", "docker_image"}),
            ("ui/package.json", {"verify", "dashboard", "docker_image"}),
            ("ui/test/tasks.test.ts", {"verify"}),
            ("ui/e2e/fixtures.ts", {"verify", "dashboard"}),
            ("ui/playwright.config.ts", {"verify", "dashboard"}),
            ("ui/embed.go", core | {"docker_image"}),
            ("ui/embed_dev.go", core | {"docker_image"}),
            ("internal/backend/object.go", core | {"docker_image"}),
            ("internal/db/migrations/baseline.sql", core | {"docker_image"}),
            ("go.sum", core | {"docker_image"}),
            ("cmd/synaps3/admin_test.go", {"verify", "go_tests", "race"}),
            ("internal/db/repository/object_repo_test.go", {"verify", "go_tests", "race"}),
            ("internal/systemtest/harness.go", core),
            ("internal/systemtest/harness_test.go", core),
            ("internal/testutil/mocks.go", core),
            ("internal/testpg/postgres.go", core),
            ("cmd/synaps3-systemtest/main.go", core),
            ("tests/system/s3_matrix_test.go", {"system"}),
            ("tests/testutil/e2e/readiness.go", {"system"}),
            ("scripts/s3_compatibility_report.py", {"system"}),
            ("scripts/test_s3_compatibility_report.py", {"system"}),
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
            ".github/actions/setup-go/action.yml", "scripts/ci_plan.py",
            "scripts/test_ci_plan.py", "new-subsystem/input.txt", "docker/new-config.toml",
        ):
            with self.subTest(path=path):
                self.assertEqual(ci_plan.plan_for_paths([path]), ci_plan.full_plan())

    def test_mixed_paths_are_a_union_and_writer_is_scheduled(self):
        plan = ci_plan.plan_for_paths(["docs/index.md", "ui/e2e/dashboard.spec.ts"])
        self.assertEqual({flag for flag in ci_plan.FLAGS if plan[flag]}, {"docs", "verify", "dashboard"})
        self.assertEqual(plan["modules_writer"], "verify")
        self.assertEqual(ci_plan.plan_for_paths(["tests/system/main_test.go"])["modules_writer"], "system")
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


class ResultGateTest(unittest.TestCase):
    def setUp(self):
        self.plan = ci_plan.plan_for_paths(["tests/system/main_test.go"])
        self.results = {"changes": {"result": "success"}}
        self.results.update({job: {"result": "success" if self.plan[job] else "skipped"} for job in ci_plan.CORE_JOBS})

    def test_expected_skips_and_selected_success_pass(self):
        self.assertEqual(ci_plan.check_core_results(self.plan, self.results), [])

    def test_selected_failure_cancel_skip_or_missing_never_pass(self):
        for result in ("failure", "cancelled", "skipped", None):
            with self.subTest(result=result):
                self.results["system"] = {"result": result}
                self.assertTrue(ci_plan.check_core_results(self.plan, self.results))

    def test_unplanned_execution_and_unsuccessful_selector_fail(self):
        self.results["verify"] = {"result": "success"}
        self.assertTrue(ci_plan.check_core_results(self.plan, self.results))
        self.results["verify"] = {"result": "skipped"}
        for result in ("failure", "cancelled", "skipped", None):
            with self.subTest(result=result):
                self.results["changes"] = {"result": result}
                self.assertTrue(ci_plan.check_core_results(self.plan, self.results))

    def test_incomplete_or_nonboolean_plans_fail(self):
        for malformed in ({}, {**self.plan, "verify": "false"}, {**self.plan, "modules_writer": "verify"}):
            with self.subTest(plan=malformed):
                self.assertTrue(ci_plan.check_core_results(malformed, self.results))

    def test_action_outputs_are_strict_booleans_and_complete_json(self):
        with tempfile.TemporaryDirectory() as temp:
            output = Path(temp) / "output"
            payload = ci_plan.write_outputs(self.plan, output)
            values = dict(line.split("=", 1) for line in output.read_text().splitlines())
        self.assertEqual(json.loads(payload), self.plan)
        self.assertEqual(json.loads(values["plan"]), self.plan)
        self.assertEqual(values["modules_writer"], "system")
        self.assertEqual({flag: values[flag] for flag in ci_plan.FLAGS},
                         {flag: str(self.plan[flag]).lower() for flag in ci_plan.FLAGS})

    def test_command_line_consumes_event_environment_and_rejects_cancelled_jobs(self):
        script = Path(ci_plan.__file__).resolve()
        with tempfile.TemporaryDirectory() as temp:
            event = Path(temp) / "event.json"
            event.write_text('{"inputs":{"mode":"build-only"}}', encoding="utf-8")
            output = Path(temp) / "output"
            env = dict(os.environ, GITHUB_EVENT_NAME="workflow_dispatch", GITHUB_EVENT_PATH=str(event),
                       GITHUB_REF="refs/heads/main", GITHUB_SHA="", GITHUB_OUTPUT=str(output))
            process = subprocess.run([sys.executable, str(script), "plan"], env=env,
                                     check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
            self.assertEqual(json.loads(process.stdout), ci_plan.full_plan())
            self.assertIn("docker_image=true\n", output.read_text())
        self.results["system"] = {"result": "cancelled"}
        process = subprocess.run([
            sys.executable, str(script), "check", "--plan", json.dumps(self.plan),
            "--results", json.dumps(self.results),
        ], stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        self.assertEqual(process.returncode, 1)
        self.assertIn("expected success, got cancelled", process.stderr)


if __name__ == "__main__":
    unittest.main()
