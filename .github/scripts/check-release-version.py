#!/usr/bin/env python3
"""Check that every version in the tree matches a release tag.

Usage: check-release-version.py v0.2.0, from the root of the tree to check.

The image is tagged from the git tag and the crate and the npm package from
their own files, so a missed bump would publish parts that name different
versions. This runs before anything is published.
"""

import json
import re
import sys
from pathlib import Path

REPO = Path.cwd()

VERSION = r"[0-9]+\.[0-9]+\.[0-9]+(?:-[0-9A-Za-z]+(?:\.[0-9A-Za-z]+)*?)?"
# Ends a version, so a lazy pre-release suffix is not cut short.
END = r"(?![0-9A-Za-z.-])"

# Image references with a version in the docs.
DOC_PIN = re.compile(rf"ghcr\.io/getfelix/felix-gateway:({VERSION}){END}")
DOC_FILES = ["README.md", "packages/gateway-client/README.md", *sorted(
    str(p.relative_to(REPO)) for p in (REPO / "docs").rglob("*.md"))]


def found_versions() -> list[tuple[str, str | None]]:
    def text(path: str) -> str:
        return (REPO / path).read_text(encoding="utf-8")

    def first(pattern: str, path: str) -> str | None:
        match = re.search(pattern, text(path), re.M | re.S)
        return match.group(1) if match else None

    out: list[tuple[str, str | None]] = []
    out.append(("Cargo.toml workspace.package",
                first(r'^\[workspace\.package\][^\[]*?^version\s*=\s*"([^"]+)"', "Cargo.toml")))
    out.append(("Cargo.lock felix-gateway",
                first(r'^name = "felix-gateway"\nversion = "([^"]+)"', "Cargo.lock")))

    package = "packages/gateway-client"
    out.append((f"{package}/package.json", json.loads(text(f"{package}/package.json")).get("version")))
    lock = json.loads(text("package-lock.json"))["packages"]
    out.append((f"package-lock.json {package}", lock.get(package, {}).get("version")))

    for rel in DOC_FILES:
        for number, line in enumerate(text(rel).splitlines(), 1):
            for match in DOC_PIN.finditer(line):
                out.append((f"{rel}:{number} {match.group(0)}", match.group(1)))
    return out


def main() -> int:
    if len(sys.argv) != 2 or not re.fullmatch(rf"v{VERSION}", sys.argv[1]):
        print("usage: check-release-version.py v<major>.<minor>.<patch>[-<pre>]", file=sys.stderr)
        return 2
    expected = sys.argv[1][1:]

    bad = 0
    for where, version in found_versions():
        if version == expected:
            print(f"ok   {where}: {version}")
        else:
            print(f"FAIL {where}: {version}, expected {expected}")
            bad += 1
    if bad:
        print(f"{bad} version(s) do not match {sys.argv[1]}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
