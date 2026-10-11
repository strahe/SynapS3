import hashlib
import os
from pathlib import Path
import subprocess


def hash_files(paths):
    digest = hashlib.sha256()
    for path in sorted(paths):
        digest.update(os.fsencode(path))
        digest.update(b"\0")
        digest.update(hashlib.sha256(Path(path).read_bytes()).digest())
    return digest.hexdigest()


architecture = os.environ["IMAGE_ARCH"]
if architecture not in {"amd64", "arm64"}:
    raise ValueError("unsupported Docker image architecture")

tracked = subprocess.check_output(
    ["git", "ls-files", "-z", "--", "cmd", "internal", "ui"]
)
sources = [os.fsdecode(path) for path in tracked.split(b"\0") if path]
environment = hash_files(["Dockerfile"])
dependencies = hash_files(["go.mod", "go.sum"])
builder = os.environ["GO_BUILD_DIGEST"].removeprefix("sha256:")
prefix = f"docker-go-v1-linux-{architecture}-musl-{builder}-{environment}-{dependencies}-"
print(f"cache-key={prefix}{hash_files(sources)}")
print(f"restore-key={prefix}")
