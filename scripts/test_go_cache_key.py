"""Cache reuse and isolation contracts for CI Go compilation."""

import os
import subprocess
import tempfile
import unittest
from pathlib import Path

import go_cache_key as cache


class GoCacheKeyTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        subprocess.run(["git", "init", "-q"], cwd=self.root, check=True)
        self.files = {
            "go.mod": "module example.test/cache\ngo 1.26.3\n",
            "go.sum": "example.test/module v1.0.0 h1:checksum\n",
            "cmd/server/main.go": "package main\n",
            "internal/db/migrations/001.sql": "select 1;\n",
            "internal/server/testdata/example.bin": "binary fixture\x00\n",
            "tests/systemtest/fixture.json": '{"value":1}\n',
            "ui/embed.go": "package ui\n",
            "ui/src/app.tsx": "export const app = 1;\n",
            "ui/public/icon.svg": "<svg/>\n",
            "ui/package.json": '{"private":true}\n',
            "ui/pnpm-lock.yaml": "lockfileVersion: 9\n",
            "ui/pnpm-workspace.yaml": "packages: []\n",
            "ui/vite.config.ts": "export default {};\n",
            "ui/tsconfig.node.json": "{}\n",
            "Makefile": "test:\n\tgo test -trimpath ./...\n",
            ".github/actions/setup-go/action.yml": "name: Setup\n",
            ".github/actions/save-go-cache/action.yml": "name: Save\n",
            ".github/actions/s3-compatibility/action.yml": "name: S3\n",
            ".github/actions/docs-build/action.yml": "name: Docs\n",
            ".github/workflows/ci.yml": "name: CI\n",
            ".github/workflows/s3-compatibility.yml": "name: S3 Compatibility\n",
            ".github/workflows/docker.yml": "name: Docker\n",
            "scripts/ci_plan.py": "# CI execution\n",
            "scripts/go_cache_key.py": "# cache identity\n",
            "README.md": "Project documentation\n",
        }
        for relative, content in self.files.items():
            self.write(relative, content)
        subprocess.run(["git", "add", "."], cwd=self.root, check=True)
        self.go_env = {
            "GOCACHE": "/cache/build", "GOMODCACHE": "/cache/modules",
            "GOVERSION": "go1.26.3", "GOOS": "linux", "GOARCH": "amd64",
        }
        self.environment = {
            "RUNNER_OS": "Linux", "RUNNER_ARCH": "X64", "ImageVersion": "20261001.1",
            "os-release": "ID=ubuntu\nVERSION_ID=24.04\n", "CGO_ENABLED": "1",
            "cc-version": "gcc 13.3.0", "cxx-version": "g++ 13.3.0",
            "GOFLAGS": "", "GOEXPERIMENT": "", "compile-flags": "-trimpath",
        }

    def write(self, relative, content):
        path = self.root / relative
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)

    def keys(self, **overrides):
        arguments = {
            "root": self.root, "go_env": self.go_env, "environment": self.environment,
            "profile": "normal", "writer": "verify",
        }
        arguments.update(overrides)
        return cache.cache_keys(**arguments)

    def test_documentation_and_untracked_outputs_reuse_exact_archive(self):
        original = self.keys()
        self.write("README.md", "Changed documentation\n")
        self.write("internal/generated-output.bin", "untracked output\n")
        self.assertEqual(self.keys(), original)

    def test_all_tracked_backend_resources_change_exact_key(self):
        for relative in (
            "cmd/server/main.go", "internal/db/migrations/001.sql",
            "internal/server/testdata/example.bin", "tests/systemtest/fixture.json",
            "ui/embed.go", "Makefile", ".github/actions/setup-go/action.yml",
            ".github/actions/save-go-cache/action.yml", ".github/actions/s3-compatibility/action.yml",
            ".github/actions/docs-build/action.yml", ".github/workflows/ci.yml",
            ".github/workflows/s3-compatibility.yml", ".github/workflows/docker.yml",
            "scripts/ci_plan.py", "scripts/go_cache_key.py",
        ):
            with self.subTest(path=relative):
                original = self.keys()
                self.write(relative, self.files[relative] + "changed\n")
                changed = self.keys()
                self.assertNotEqual(changed["build-primary-key"], original["build-primary-key"])
                self.assertEqual(changed["build-restore-keys"], original["build-restore-keys"])
                self.assertEqual(changed["modules-primary-key"], original["modules-primary-key"])
                self.write(relative, self.files[relative])

    def test_ui_inputs_only_change_production_ui_keys(self):
        for relative in (
            "ui/src/app.tsx", "ui/public/icon.svg", "ui/package.json", "ui/pnpm-lock.yaml",
            "ui/pnpm-workspace.yaml", "ui/vite.config.ts", "ui/tsconfig.node.json",
        ):
            with self.subTest(path=relative):
                normal, production = self.keys(), self.keys(production_ui=True)
                self.write(relative, self.files[relative] + "changed\n")
                self.assertEqual(self.keys(), normal)
                self.assertNotEqual(self.keys(production_ui=True)["build-primary-key"], production["build-primary-key"])
                self.write(relative, self.files[relative])
        self.assertEqual(self.keys(node_version="26", pnpm_version="12"), self.keys())
        self.assertNotEqual(self.keys(production_ui=True, node_version="26"), self.keys(production_ui=True))
        self.assertNotEqual(self.keys(production_ui=True, pnpm_version="12"), self.keys(production_ui=True))

    def test_writer_fallback_only_crosses_writers_after_same_writer(self):
        verify, system = self.keys(production_ui=True), self.keys(writer="system")
        verify_prefixes = verify["build-restore-keys"].splitlines()
        system_prefixes = system["build-restore-keys"].splitlines()
        self.assertTrue(verify["build-primary-key"].startswith(verify_prefixes[0]))
        self.assertTrue(system["build-primary-key"].startswith(system_prefixes[0]))
        self.assertNotEqual(verify_prefixes[0], system_prefixes[0])
        self.assertEqual(verify_prefixes[1], system_prefixes[1])
        self.assertEqual(verify["modules-primary-key"], system["modules-primary-key"])

    def test_cache_paths_preserve_identity_and_default_race_path(self):
        normal = self.keys(writer="verify")
        race = self.keys(profile="race", writer="race")
        alternate = dict(self.go_env, GOCACHE="/cache/normal-snapshot", GOMODCACHE="/cache/shared-modules")
        moved = self.keys(go_env=alternate)
        self.assertEqual(moved["build-primary-key"], normal["build-primary-key"])
        self.assertEqual(moved["build-restore-keys"], normal["build-restore-keys"])
        self.assertEqual(moved["modules-primary-key"], normal["modules-primary-key"])
        self.assertEqual(normal["gocache"], race["gocache"])
        self.assertEqual(normal["gomodcache"], race["gomodcache"])
        self.assertEqual(race["build-primary-key"].split("-")[-2], "race")

    def test_environment_and_race_cannot_restore_normal_host_archives(self):
        normal = self.keys()
        variants = [self.keys(profile="race")]
        for field, value in (
            ("os-release", "ID=alpine\n"), ("ImageVersion", "20261002.1"),
            ("CGO_ENABLED", "0"), ("cc-version", "gcc 14.2.0"),
            ("cxx-version", "g++ 14.2.0"), ("GOFLAGS", "-tags=custom"),
            ("GOEXPERIMENT", "newinliner"), ("compile-flags", ""),
        ):
            variants.append(self.keys(environment=dict(self.environment, **{field: value})))
        for variant in variants:
            with self.subTest(key=variant["build-primary-key"]):
                for prefix in variant["build-restore-keys"].splitlines():
                    self.assertFalse(normal["build-primary-key"].startswith(prefix))
                self.assertEqual(variant["modules-primary-key"], normal["modules-primary-key"])

    def test_dependencies_target_and_exact_go_version_isolate_both_caches(self):
        original = self.keys()
        for relative in ("go.mod", "go.sum"):
            with self.subTest(path=relative):
                self.write(relative, self.files[relative] + "changed\n")
                self.assertNotEqual(self.keys()["modules-primary-key"], original["modules-primary-key"])
                self.assertNotEqual(self.keys()["build-restore-keys"], original["build-restore-keys"])
                self.write(relative, self.files[relative])
        for field, value in (("GOOS", "darwin"), ("GOARCH", "arm64"), ("GOVERSION", "go1.26.4")):
            with self.subTest(field=field):
                changed_go = dict(self.go_env, **{field: value})
                changed_environment = dict(self.environment, **{field: value})
                variant = self.keys(go_env=changed_go, environment=changed_environment)
                self.assertNotEqual(variant["modules-primary-key"], original["modules-primary-key"])
                self.assertNotEqual(variant["build-restore-keys"], original["build-restore-keys"])

    def test_rename_delete_and_symlink_identity_are_tracked(self):
        original = self.keys()
        path = self.root / "tests/systemtest/fixture.json"
        path.unlink()
        self.assertNotEqual(self.keys()["build-primary-key"], original["build-primary-key"])
        self.write("tests/systemtest/renamed.json", self.files["tests/systemtest/fixture.json"])
        subprocess.run(["git", "add", "-A"], cwd=self.root, check=True)
        self.assertNotEqual(self.keys()["build-primary-key"], original["build-primary-key"])
        link = self.root / "internal/fixture-link"
        os.symlink("../README.md", link)
        subprocess.run(["git", "add", "internal/fixture-link"], cwd=self.root, check=True)
        before = self.keys()
        self.write("README.md", "Documentation changed\n")
        self.assertEqual(self.keys(), before)
        link.unlink()
        os.symlink("../go.mod", link)
        self.assertNotEqual(self.keys()["build-primary-key"], before["build-primary-key"])

    def test_invalid_writer_and_missing_dependencies_fail_closed(self):
        with self.assertRaises(ValueError):
            self.keys(writer="verify\ninjected-key")
        (self.root / "go.sum").unlink()
        with self.assertRaises(ValueError):
            self.keys()

    def test_output_preserves_multiline_restore_keys(self):
        output = self.root / "github-output"
        expected = self.keys()
        cache.write_outputs(output, expected)
        lines = output.read_text().splitlines()
        actual = {}
        while lines:
            name, delimiter = lines.pop(0).split("<<", 1)
            end = lines.index(delimiter)
            actual[name] = "\n".join(lines[:end])
            del lines[:end + 1]
        self.assertEqual(actual, expected)
        self.assertLess(len(expected["build-primary-key"]), 512)


if __name__ == "__main__":
    unittest.main()
