"""Failure cases for the S3 compatibility evidence gate."""

import html
import io
import json
import tempfile
import unittest
from contextlib import redirect_stderr, redirect_stdout
from pathlib import Path
from unittest import mock

import s3_compatibility_report as report


class CompatibilityReportTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.write_matrices([("Object", "GetObject", "Supported")])
        self.matrix_json = self.root / "matrix.json"
        self.clients_json = self.root / "clients.json"
        self.versions = {name: "test-version" for name in report.CLIENTS}
        self.write_results(self.matrix_json, report.MATRIX_ROOT, ["GetObject"])
        self.write_results(self.clients_json, report.CLIENT_ROOT, list(report.CLIENTS))

    def write_matrices(self, rows):
        for relative, heading, statuses in (
            ("docs/en/reference/s3-compatibility.md", "Operation Matrix", {"Supported": "Supported", "Partial": "Partial", "Not supported": "Not supported"}),
            ("docs/zh/reference/s3-compatibility.md", "操作矩阵", {"Supported": "支持", "Partial": "部分支持", "Not supported": "不支持"}),
            ("README.md", "Core S3 Compatibility", {"Supported": "✅", "Partial": "⚠️", "Not supported": "❌"}),
        ):
            path = self.root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                f"## {heading}\n\n| Area | Operation | Status | Notes |\n"
                "| --- | --- | --- | --- |\n"
                + "".join(f"| {area} | `{name}` | {statuses[status]} | Test scope. |\n" for area, name, status in rows),
                encoding="utf-8",
            )

    @staticmethod
    def write_results(path, root, children, changed=None, package="pass", elapsed=None, root_action="pass"):
        changed = changed or {}
        events = [{"Action": changed.get(child, "pass"), "Test": f"{root}/{child}"} for child in children]
        events += [{"Action": root_action, "Test": root}, {"Action": package}]
        for event in events:
            test = event.get("Test")
            if elapsed is not None and test:
                name = test.removeprefix(root).removeprefix("/")
                if name in elapsed:
                    event["Elapsed"] = elapsed[name]
        path.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")

    def run_report(self, versions=None, github_context=None, source_sha="source-sha", checkout_sha="checkout-sha"):
        return report.report(
            self.root,
            self.matrix_json,
            self.clients_json,
            source_sha,
            checkout_sha,
            self.versions if versions is None else versions,
            github_context,
        )

    def test_complete_results_pass(self):
        summary, errors = self.run_report()
        self.assertEqual(errors, [])
        self.assertIn("✅ **Passed** · **1/1 operations** · **3/3 clients**", summary)
        self.assertIn("Failed **0** · Skipped **0** · Missing **0**", summary)
        self.assertIn("| Object | 1/1 | ✅ Passed |", summary)
        self.assertIn("| <code>GetObject</code> | Supported | ✅ Passed | — |", summary)
        self.assertIn("| AWS CLI | test-version | ✅ Passed | — |", summary)
        self.assertNotIn("## Issues", summary)
        self.assertEqual(summary.count("<details open>"), 2)
        self.assertNotIn("<details>", summary)
        self.assertLess(summary.index("## Clients"), summary.index("<details open>"))

    def test_missing_skipped_and_failed_results_fail(self):
        for action in ("missing", "skip", "fail"):
            with self.subTest(action=action):
                children = [] if action == "missing" else ["GetObject"]
                changed = {} if action == "missing" else {"GetObject": action}
                self.write_results(self.matrix_json, report.MATRIX_ROOT, children, changed)
                summary, errors = self.run_report()
                self.assertTrue(any(f"GetObject test is {action}" in error for error in errors), errors)
                self.assertIn("❌ **Failed**", summary)
                issues = summary.split("## Issues\n", 1)[1].split("## S3 operations\n", 1)[0]
                self.assertIn(f"| GetObject | {report.RESULT_LABELS[action]} |", issues)

    def test_missing_result_file_fails(self):
        for content in (None, "not JSON\n"):
            with self.subTest(content=content):
                self.write_results(self.clients_json, report.CLIENT_ROOT, list(report.CLIENTS))
                if content is None:
                    self.clients_json.unlink()
                else:
                    self.clients_json.write_text(content, encoding="utf-8")
                summary, errors = self.run_report()
                self.assertTrue(any("clients.json" in error for error in errors), errors)
                self.assertIn("**0/3 clients**", summary)
                self.assertIn("Missing **3**", summary)
                self.assertIn("Test results unavailable.", summary)

    def test_missing_client_fails(self):
        self.write_results(self.clients_json, report.CLIENT_ROOT, ["AWS_CLI", "rclone"])
        _, errors = self.run_report()
        self.assertIn("mc client test is missing", errors)

    def test_unavailable_client_version_fails(self):
        summary, errors = self.run_report(versions={**self.versions, "mc": "unavailable"})
        self.assertIn("mc version is unavailable", errors)
        self.assertIn("❌ **Failed** · **1/1 operations** · **3/3 clients**", summary)
        self.assertIn("| MinIO Client | Version unavailable. |", summary)

    def test_bilingual_status_mismatch_fails(self):
        path = self.root / "docs/zh/reference/s3-compatibility.md"
        path.write_text(path.read_text(encoding="utf-8").replace("| 支持 |", "| 部分支持 |"), encoding="utf-8")
        summary, errors = self.run_report()
        self.assertIn("English/Chinese matrix status differs: GetObject", errors)
        self.assertIn("❌ **Failed** · **1/1 operations**", summary)
        self.assertLess(summary.index("English/Chinese matrix status differs: GetObject"), summary.index("<details open>"))

    def test_readme_status_mismatch_fails(self):
        path = self.root / "README.md"
        path.write_text(path.read_text(encoding="utf-8").replace("| ✅ |", "| ⚠️ |"), encoding="utf-8")
        _, errors = self.run_report()
        self.assertIn("README status differs from full matrix: GetObject", errors)

    def test_counts_exclude_parent_nested_and_unsupported_results(self):
        self.write_matrices([
            ("Object", "GetObject", "Supported"),
            ("Object", "PutObject", "Supported"),
            ("Multipart", "UploadPart", "Partial"),
            ("Multipart", "ListParts", "Supported"),
            ("Bucket", "HeadBucket", "Supported"),
            ("Bucket", "DeleteBucket", "Not supported"),
        ])
        self.write_results(
            self.matrix_json, report.MATRIX_ROOT,
            ["GetObject", "GetObject/range", "PutObject", "UploadPart", "HeadBucket", "HeadBucket", "DeleteBucket"],
            {"PutObject": "fail", "UploadPart": "skip"},
        )
        summary, errors = self.run_report()
        self.assertIn("HeadBucket test is duplicate", errors)
        self.assertIn("❌ **Failed** · **1/5 operations** · **3/3 clients**", summary)
        self.assertIn("Failed **1** · Skipped **1** · Missing **1** · Invalid **1**", summary)
        self.assertIn("| Object | 1/2 | ❌ Failed |", summary)
        self.assertIn("| Multipart | 0/2 | ⚪ Missing |", summary)
        self.assertIn("| Bucket | 0/1 | ⚠️ Invalid |", summary)
        self.assertIn("| <code>UploadPart</code> | Partial | ⏭ Skipped | — |", summary)
        self.assertNotIn("DeleteBucket", summary)
        self.assertNotIn("GetObject/range", summary)
        self.assertLess(summary.index("### Object"), summary.index("### Multipart"))
        self.assertLess(summary.index("### Multipart"), summary.index("### Bucket"))

    def test_suite_failures_override_passing_counts(self):
        for root_action, package, duplicate in (("fail", "pass", False), ("pass", "fail", False), ("pass", "pass", True)):
            with self.subTest(root_action=root_action, package=package, duplicate=duplicate):
                self.write_results(self.matrix_json, report.MATRIX_ROOT, ["GetObject"], package=package, root_action=root_action)
                if duplicate:
                    with self.matrix_json.open("a", encoding="utf-8") as output:
                        output.write(json.dumps({"Action": "pass", "Test": report.MATRIX_ROOT}) + "\n")
                summary, errors = self.run_report()
                self.assertTrue(errors)
                self.assertIn("❌ **Failed** · **1/1 operations** · **3/3 clients**", summary)
                issues = summary.split("## Issues\n", 1)[1].split("## S3 operations\n", 1)[0]
                self.assertIn("Operation suite", issues)
                if duplicate:
                    self.assertIn("⚠️ Invalid", issues)

    def test_unavailable_matrix_has_no_zero_denominator(self):
        for unreadable in (False, True):
            with self.subTest(unreadable=unreadable):
                self.write_matrices([("Bucket", "DeleteBucket", "Not supported")])
                if unreadable:
                    (self.root / "docs/en/reference/s3-compatibility.md").unlink()
                summary, errors = self.run_report()
                self.assertTrue(errors)
                self.assertIn("❌ **Failed** · **Operations unavailable** · **3/3 clients**", summary)
                self.assertNotIn("0/0", summary)
                self.assertIn("Operation matrix unavailable.", summary)

    def test_elapsed_uses_each_test_and_suite_event(self):
        self.write_results(self.matrix_json, report.MATRIX_ROOT, ["GetObject", "GetObject/range"], elapsed={"GetObject": 0, "GetObject/range": 99, "": 3.701})
        self.write_results(self.clients_json, report.CLIENT_ROOT, list(report.CLIENTS), elapsed={"AWS_CLI": 1.771, "rclone": 0, "mc": 0.224, "": 4.61})
        summary, errors = self.run_report()
        self.assertEqual(errors, [])
        self.assertIn("| <code>GetObject</code> | Supported | ✅ Passed | 0.00 s |", summary)
        self.assertIn("| AWS CLI | test-version | ✅ Passed | 1.77 s |", summary)
        self.assertIn("| rclone | test-version | ✅ Passed | 0.00 s |", summary)
        self.assertIn("| MinIO Client | test-version | ✅ Passed | 0.22 s |", summary)
        self.assertIn("**Operation suite:** 3.70 s", summary)
        self.assertIn("**Client suite:** 4.61 s", summary)
        self.assertNotIn("99.00", summary)

    def test_invalid_elapsed_does_not_fail_verification(self):
        for elapsed in (None, -1, "1.2", True, float("nan"), float("inf"), 10 ** 400):
            with self.subTest(elapsed=elapsed):
                self.write_results(self.matrix_json, report.MATRIX_ROOT, ["GetObject"], elapsed={"GetObject": elapsed, "": elapsed})
                summary, errors = self.run_report()
                self.assertEqual(errors, [])
                self.assertIn("| <code>GetObject</code> | Supported | ✅ Passed | — |", summary)
                self.assertIn("**Operation suite:** —", summary)

    def test_client_versions_are_compact_with_full_evidence(self):
        versions = {
            "AWS_CLI": "aws-cli/2.31.0 Python/3.13.7 Linux/6.8 exe/x86_64.ubuntu.24",
            "rclone": "rclone v1.71.0",
            "mc": "mc version RELEASE.2025-08-13T08-35-41Z (commit-id=abcdef)",
        }
        summary, errors = self.run_report(versions=versions)
        self.assertEqual(errors, [])
        self.assertIn("| AWS CLI | 2.31.0 | ✅ Passed |", summary)
        self.assertIn("| rclone | 1.71.0 | ✅ Passed |", summary)
        self.assertIn("| MinIO Client | 2025-08-13 | ✅ Passed |", summary)
        details = summary.split("<summary>Verification details</summary>", 1)[1]
        for version in versions.values():
            self.assertIn(version, html.unescape(details))

    def test_source_links_and_local_context(self):
        context = {"GITHUB_SERVER_URL": "https://github.example", "GITHUB_REPOSITORY": "owner/repo", "GITHUB_RUN_ID": "42"}
        summary, errors = self.run_report(github_context=context, source_sha="abcdef1234567890", checkout_sha="123456abcdef")
        self.assertEqual(errors, [])
        self.assertIn("[View CI run](<https://github.example/owner/repo/actions/runs/42>)", summary)
        self.assertIn("[Commit <code>abcdef1</code>](<https://github.example/owner/repo/commit/abcdef1234567890>)", summary)
        self.assertIn("| Source commit | <code>abcdef1234567890</code> |", summary)
        self.assertIn("| Tested checkout | <code>123456abcdef</code> |", summary)
        for context in (None, {"GITHUB_SERVER_URL": "https://github.example", "GITHUB_RUN_ID": "42"}, {**context, "GITHUB_SERVER_URL": "javascript:alert(1)"}):
            with self.subTest(context=context):
                summary, _ = self.run_report(github_context=context)
                self.assertNotIn("[View CI run]", summary)
                self.assertNotIn("[Commit", summary)
                self.assertIn("Commit <code>source-</code>", summary)

    def test_dynamic_content_cannot_break_tables_details_or_links(self):
        self.write_matrices([("Object </summary><script>alert(1)</script>", "GetObject", "Supported")])
        versions = {**self.versions, "mc": "custom|version </details>\n[link](https://example.org) `*_\\"}
        context = {"GITHUB_SERVER_URL": "https://github.example", "GITHUB_REPOSITORY": "owner/repo](other)", "GITHUB_RUN_ID": "42)>"}
        summary, errors = self.run_report(versions=versions, github_context=context, source_sha="abc`[]|<tag>\n")
        self.assertEqual(errors, [])
        self.assertEqual(summary.count("<details open>"), 2)
        self.assertEqual(summary.count("</details>"), 2)
        self.assertEqual(summary.count("</summary>"), 2)
        self.assertNotIn("<script>", summary)
        self.assertNotIn("[link]", summary)
        self.assertNotIn("<tag>", summary)
        self.assertIn("Object &lt;/summary&gt;&lt;script&gt;alert(1)&lt;/script&gt;", summary)
        self.assertIn("custom&#124;version &lt;/details&gt;", summary)
        self.assertIn("https://github.example/owner/repo%5D%28other%29/actions/runs/42%29%3E", summary)
        self.assertIn("/commit/abc%60%5B%5D%7C%3Ctag%3E%0A", summary)

    def test_cli_appends_reports_and_preserves_exit_codes(self):
        summary_path = self.root / "summary.md"
        summary_path.write_text("Existing summary\n", encoding="utf-8")
        argv = ["s3_compatibility_report.py", "--root", str(self.root), "--matrix-json", str(self.matrix_json), "--clients-json", str(self.clients_json), "--summary", str(summary_path), "--source-sha", "source-sha", "--checkout-sha", "checkout-sha"]
        context = {"GITHUB_SERVER_URL": "https://github.example", "GITHUB_REPOSITORY": "owner/repo", "GITHUB_RUN_ID": "42"}
        stdout, stderr = io.StringIO(), io.StringIO()
        with mock.patch.object(report.sys, "argv", argv), mock.patch.object(report, "client_versions", return_value=self.versions), mock.patch.dict(report.os.environ, context, clear=True), redirect_stdout(stdout), redirect_stderr(stderr):
            self.assertEqual(report.main(), 0)
            self.clients_json.unlink()
            self.assertEqual(report.main(), 1)
        summary = summary_path.read_text(encoding="utf-8")
        self.assertTrue(summary.startswith("Existing summary\n"))
        self.assertEqual(summary.count("# S3 Compatibility"), 2)
        self.assertIn("✅ **Passed**", summary)
        self.assertIn("❌ **Failed**", summary)
        self.assertIn("[View CI run](<https://github.example/owner/repo/actions/runs/42>)", summary)
        self.assertEqual(stdout.getvalue().splitlines(), ["S3 compatibility report: passed", "S3 compatibility report: failed"])
        self.assertIn("clients.json", stderr.getvalue())


if __name__ == "__main__":
    unittest.main()
