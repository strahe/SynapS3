"""Failure cases for the S3 compatibility evidence gate."""

import json
import tempfile
import unittest
from pathlib import Path

import s3_compatibility_report as report


class CompatibilityReportTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        for relative, heading, status in (
            ("docs/en/reference/s3-compatibility.md", "Operation Matrix", "Supported"),
            ("docs/zh/reference/s3-compatibility.md", "操作矩阵", "支持"),
            ("README.md", "Core S3 Compatibility", "✅"),
        ):
            path = self.root / relative
            path.parent.mkdir(parents=True, exist_ok=True)
            path.write_text(
                f"## {heading}\n\n| Area | Operation | Status | Notes |\n"
                "| --- | --- | --- | --- |\n"
                f"| Object | `GetObject` | {status} | Reads bytes. |\n",
                encoding="utf-8",
            )
        self.matrix_json = self.root / "matrix.json"
        self.clients_json = self.root / "clients.json"
        self.write_results(self.matrix_json, report.MATRIX_ROOT, ["GetObject"])
        self.write_results(self.clients_json, report.CLIENT_ROOT, list(report.CLIENTS))

    @staticmethod
    def write_results(path, root, children, changed=None, package="pass"):
        changed = changed or {}
        events = [{"Action": changed.get(child, "pass"), "Test": f"{root}/{child}"} for child in children]
        events += [{"Action": "pass", "Test": root}, {"Action": package}]
        path.write_text("".join(json.dumps(event) + "\n" for event in events), encoding="utf-8")

    def run_report(self):
        return report.report(
            self.root,
            self.matrix_json,
            self.clients_json,
            "source-sha",
            "checkout-sha",
            {name: "test-version" for name in report.CLIENTS},
        )

    def test_complete_results_pass(self):
        summary, errors = self.run_report()
        self.assertEqual(errors, [])
        self.assertIn("| `GetObject` | supported | pass |", summary)
        self.assertIn("| AWS_CLI | test-version | pass |", summary)

    def test_missing_skipped_and_failed_results_fail(self):
        for action in ("missing", "skip", "fail"):
            with self.subTest(action=action):
                children = [] if action == "missing" else ["GetObject"]
                changed = {} if action == "missing" else {"GetObject": action}
                self.write_results(self.matrix_json, report.MATRIX_ROOT, children, changed)
                _, errors = self.run_report()
                self.assertTrue(any(f"GetObject test is {action}" in error for error in errors), errors)

    def test_missing_result_file_fails(self):
        self.clients_json.unlink()
        _, errors = self.run_report()
        self.assertTrue(any("clients.json" in error for error in errors), errors)

    def test_missing_client_fails(self):
        self.write_results(self.clients_json, report.CLIENT_ROOT, ["AWS_CLI", "rclone"])
        _, errors = self.run_report()
        self.assertIn("mc client test is missing", errors)

    def test_unavailable_client_version_fails(self):
        _, errors = report.report(
            self.root,
            self.matrix_json,
            self.clients_json,
            "source-sha",
            "checkout-sha",
            {name: "unavailable" if name == "mc" else "test-version" for name in report.CLIENTS},
        )
        self.assertIn("mc version is unavailable", errors)

    def test_bilingual_status_mismatch_fails(self):
        path = self.root / "docs/zh/reference/s3-compatibility.md"
        path.write_text(path.read_text(encoding="utf-8").replace("| 支持 |", "| 部分支持 |"), encoding="utf-8")
        _, errors = self.run_report()
        self.assertIn("English/Chinese matrix status differs: GetObject", errors)

    def test_readme_status_mismatch_fails(self):
        path = self.root / "README.md"
        path.write_text(path.read_text(encoding="utf-8").replace("| ✅ |", "| ⚠️ |"), encoding="utf-8")
        _, errors = self.run_report()
        self.assertIn("README status differs from full matrix: GetObject", errors)


if __name__ == "__main__":
    unittest.main()
