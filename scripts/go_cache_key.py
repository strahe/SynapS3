"""Resolve Go cache identities from tracked inputs and the compilation environment."""

import argparse
import hashlib
import json
import os
import platform
import re
import shlex
import stat
import subprocess
from pathlib import Path


SOURCE_PATHS = (
    "cmd/", "internal/", "tests/", ":(glob)ui/*.go", "Makefile",
    ".github/actions/setup-go/", ".github/actions/save-go-cache/",
    ".github/workflows/ci.yml", "scripts/go_cache_key.py",
)
UI_PATHS = (
    "ui/src/", "ui/public/", "ui/index.html", "ui/package.json",
    "ui/pnpm-lock.yaml", "ui/pnpm-workspace.yaml",
    ":(glob)ui/vite.config.*", ":(glob)ui/tsconfig*.json",
    ":(glob)ui/tsconfig*.jsonc",
)
GO_ENV_NAMES = (
    "GOCACHE", "GOMODCACHE", "GOVERSION", "GOOS", "GOARCH", "GOAMD64",
    "GOARM", "GOARM64", "CGO_ENABLED", "CC", "CXX", "CGO_CFLAGS",
    "CGO_CPPFLAGS", "CGO_CXXFLAGS", "CGO_LDFLAGS", "GOFLAGS", "GOEXPERIMENT",
)


def digest_json(value):
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":")).encode()).hexdigest()


def hash_files(root, paths):
    digest = hashlib.sha256()
    for relative in sorted(paths):
        path = root / relative
        digest.update(os.fsencode(relative) + b"\0")
        if not path.exists() and not path.is_symlink():
            digest.update(b"missing\0")
            continue
        mode = path.lstat().st_mode
        digest.update(str(stat.S_IMODE(mode)).encode() + b"\0")
        if stat.S_ISLNK(mode):
            digest.update(b"symlink\0" + os.fsencode(os.readlink(path)) + b"\0")
        elif stat.S_ISREG(mode):
            digest.update(b"file\0")
            with path.open("rb") as source:
                for chunk in iter(lambda: source.read(1024 * 1024), b""):
                    digest.update(chunk)
            digest.update(b"\0")
        else:
            raise ValueError(f"unsupported tracked cache input: {relative}")
    return digest.hexdigest()


def source_hash(root, production_ui=False):
    pathspecs = SOURCE_PATHS + (UI_PATHS if production_ui else ())
    tracked = subprocess.check_output(["git", "ls-files", "-z", "--", *pathspecs], cwd=root)
    return hash_files(root, {os.fsdecode(path) for path in tracked.split(b"\0") if path})


def compiler_version(command):
    try:
        result = subprocess.run(
            [*shlex.split(command), "--version"], check=True, capture_output=True,
            text=True, timeout=10,
        )
        return result.stdout.strip() or result.stderr.strip()
    except (OSError, ValueError, subprocess.SubprocessError):
        return "unavailable"


def compilation_environment(go_env, runner_env):
    os_release = Path("/etc/os-release")
    environment = {key: value for key, value in go_env.items() if key not in ("GOCACHE", "GOMODCACHE")}
    environment.update({key: runner_env.get(key, "") for key in ("RUNNER_OS", "RUNNER_ARCH", "ImageOS", "ImageVersion")})
    environment["os-release"] = os_release.read_text() if os_release.exists() else platform.platform()
    environment["compile-flags"] = "-trimpath"
    if go_env.get("CGO_ENABLED") == "1":
        environment["cc-version"] = compiler_version(go_env["CC"])
        environment["cxx-version"] = compiler_version(go_env["CXX"])
    return environment


def cache_keys(root, go_env, environment, profile, writer, production_ui=False, node_version="22", pnpm_version="11.1.2"):
    if profile not in ("normal", "race"):
        raise ValueError("profile must be normal or race")
    if not re.fullmatch(r"[A-Za-z0-9_-]{1,100}", writer):
        raise ValueError("writer must be a GitHub job identifier")
    for name in ("go.mod", "go.sum"):
        if not (root / name).is_file():
            raise ValueError(f"missing dependency input: {name}")
    deps = hash_files(root, ("go.mod", "go.sum"))
    build_env = dict(environment, profile=profile)
    build_env.update({key: go_env[key] for key in ("GOVERSION", "GOOS", "GOARCH")})
    sources = source_hash(root, production_ui)
    if production_ui:
        sources = digest_json({"tree": sources, "node-version": node_version, "pnpm-version": pnpm_version})
    env_hash = digest_json(build_env)
    module_env = {key: go_env[key] for key in ("GOOS", "GOARCH", "GOVERSION")}
    module_env.update({key: environment.get(key, "") for key in ("RUNNER_OS", "RUNNER_ARCH")})
    module_prefix = f"go-modules-v2-{digest_json(module_env)}-"
    profile_prefix = f"go-build-v2-{env_hash}-{profile}-{deps}-"
    writer_prefix = f"{profile_prefix}{writer}-"
    return {
        "gocache": go_env["GOCACHE"],
        "gomodcache": go_env["GOMODCACHE"],
        "modules-primary-key": f"{module_prefix}{deps}",
        "build-primary-key": f"{writer_prefix}{sources}",
        "build-restore-keys": f"{writer_prefix}\n{profile_prefix}",
    }


def write_outputs(path, outputs):
    with path.open("a", encoding="utf-8") as destination:
        for name, value in outputs.items():
            delimiter = f"cache_{hashlib.sha256(value.encode()).hexdigest()}"
            destination.write(f"{name}<<{delimiter}\n{value}\n{delimiter}\n")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--root", type=Path, default=Path.cwd())
    parser.add_argument("--profile", choices=("normal", "race"), default="normal")
    parser.add_argument("--writer", required=True)
    parser.add_argument("--production-ui", choices=("true", "false"), default="false")
    parser.add_argument("--node-version", default="22")
    parser.add_argument("--pnpm-version", default="11.1.2")
    parser.add_argument("--github-output", type=Path)
    args = parser.parse_args()
    go_env = json.loads(subprocess.check_output(["go", "env", "-json", *GO_ENV_NAMES], cwd=args.root))
    outputs = cache_keys(
        args.root, go_env, compilation_environment(go_env, os.environ),
        args.profile, args.writer, args.production_ui == "true", args.node_version, args.pnpm_version,
    )
    if args.github_output:
        write_outputs(args.github_output, outputs)
    else:
        print(json.dumps(outputs, indent=2))


if __name__ == "__main__":
    main()
