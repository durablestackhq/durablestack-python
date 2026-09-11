from __future__ import annotations

import argparse
import pathlib
import subprocess
import sys
import tomllib


def _read_version_from_text(text: str) -> str:
    data = tomllib.loads(text)
    return str(data["project"]["version"])


def _read_current_version(pyproject_path: pathlib.Path) -> str:
    return _read_version_from_text(pyproject_path.read_text(encoding="utf-8"))


def _read_version_from_git_ref(git_ref: str, relative_path: str) -> str | None:
    try:
        result = subprocess.run(
            ["git", "show", f"{git_ref}:{relative_path}"],
            check=True,
            capture_output=True,
            text=True,
        )
    except subprocess.CalledProcessError:
        return None
    return _read_version_from_text(result.stdout)


def main() -> int:
    parser = argparse.ArgumentParser(description="Check whether pyproject version changed from a base git ref.")
    parser.add_argument("--base-ref", required=True, help="Base git ref to compare against (e.g. github.event.before)")
    parser.add_argument("--pyproject", default="pyproject.toml", help="Path to pyproject.toml")
    args = parser.parse_args()

    pyproject_path = pathlib.Path(args.pyproject)
    if not pyproject_path.exists():
        print(f"error: pyproject not found at {pyproject_path}", file=sys.stderr)
        return 2

    current_version = _read_current_version(pyproject_path)
    previous_version = _read_version_from_git_ref(args.base_ref, args.pyproject)

    if previous_version is None:
        print(f"version_changed=true current={current_version} previous=<missing>")
        return 0

    changed = previous_version != current_version
    print(
        f"version_changed={'true' if changed else 'false'} current={current_version} previous={previous_version}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
