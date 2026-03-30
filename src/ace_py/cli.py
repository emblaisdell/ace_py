"""
ace-py CLI

  ace-py run <module>                   Run one ACE request against <module>.
  ace-py call <module> <fn> [args...]   Call a @calculation and print results.
  ace-py build <module>                 Write a Containerfile next to <module>
                                        and stream the build context as a .tar
                                        to stdout.
"""

import io
import os
import sys
import tarfile


_CONTAINERFILE_TEMPLATE_BASE = """\
FROM python:3.12-slim

WORKDIR /app

{deps}
# Copy your calculation module(s)
COPY . .

# Run one ACE request per container invocation.
# stdin  → 32-bit big-endian length-prefixed blobs (function name, then args)
# stdout → 32-bit big-endian length-prefixed blobs (results)
# stderr → human-readable errors
ENTRYPOINT ["python", "-m", "ace_py", "{module}"]
"""

_DEPS_WITH_REQUIREMENTS = """\
COPY _vendor/ ./_vendor/
COPY requirements.txt ./
RUN pip install --no-cache-dir --no-index --find-links ./_vendor/ ace-py \
 && pip install --no-cache-dir -r requirements.txt

"""

_DEPS_WITHOUT_REQUIREMENTS = """\
COPY _vendor/ ./_vendor/
RUN pip install --no-cache-dir --no-index --find-links ./_vendor/ ace-py

"""

# Directories / patterns to exclude from the build context tar
_EXCLUDE_DIRS = {".git", ".venv", "venv", "__pycache__", ".mypy_cache", ".ruff_cache", "dist", "build"}
_EXCLUDE_SUFFIXES = {".pyc", ".pyo", ".egg-info"}


def _cmd_run(args):
    if not args:
        print("Usage: ace-py run <module>", file=sys.stderr)
        sys.exit(1)

    import importlib
    if "" not in sys.path:
        sys.path.insert(0, "")
    module_name = args[0]
    try:
        importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        print(f"ace_py: could not import {module_name!r}: {exc}", file=sys.stderr)
        sys.exit(1)

    from ace_py import run
    run()


def _cmd_call(args):
    if len(args) < 2:
        print("Usage: ace-py call <module> <function> [args...]", file=sys.stderr)
        sys.exit(1)

    import importlib, inspect, struct
    from typing import get_type_hints
    from ace_py import _registry, _encode, _decode, _write_blob, _read_blob, run

    module_name, func_name, *raw_args = args

    if "" not in sys.path:
        sys.path.insert(0, "")

    try:
        importlib.import_module(module_name)
    except ModuleNotFoundError as exc:
        print(f"ace_py: could not import {module_name!r}: {exc}", file=sys.stderr)
        sys.exit(1)

    if func_name not in _registry:
        known = ", ".join(sorted(_registry)) or "(none)"
        print(f"ace_py: unknown calculation {func_name!r}; registered: {known}", file=sys.stderr)
        sys.exit(1)

    fn = _registry[func_name]
    hints = get_type_hints(fn)
    params = list(inspect.signature(fn).parameters.values())

    if len(raw_args) != len(params):
        print(
            f"ace_py: {func_name} expects {len(params)} argument(s), got {len(raw_args)}",
            file=sys.stderr,
        )
        sys.exit(1)

    # Encode CLI strings into typed wire blobs using the function's annotations
    def _encode_arg(raw: str, typ: type) -> bytes:
        if typ is int:
            n = int(raw)
            if n == 0:
                return b"\x00"
            return n.to_bytes((n.bit_length() + 8) // 8, byteorder="big", signed=True)
        return raw.encode("utf-8")  # str and unknown types

    stdin_buf = io.BytesIO()
    _write_blob(stdin_buf, func_name.encode("utf-8"))
    for raw, param in zip(raw_args, params):
        typ = hints.get(param.name, str)
        _write_blob(stdin_buf, _encode_arg(raw, typ))
    stdin_buf.seek(0)

    stdout_buf = io.BytesIO()
    run(stdin=stdin_buf, stdout=stdout_buf)

    # Decode and print each result blob
    stdout_buf.seek(0)
    while True:
        blob = _read_blob(stdout_buf)
        if blob is None:
            break
        try:
            text = blob.decode("utf-8")
            if text.isprintable():
                print(text)
                continue
        except UnicodeDecodeError:
            pass
        # Looks like binary — display as a signed big-endian integer
        print(int.from_bytes(blob, byteorder="big", signed=True))


def _build_ace_py_wheel(wheel_dir: str) -> None:
    """Build a wheel for ace-py into *wheel_dir* from its current source."""
    import ace_py as _ace
    import subprocess

    # Walk up from the package file to find the project root (contains pyproject.toml)
    candidate = os.path.dirname(_ace.__file__)
    project_root = None
    for _ in range(5):
        if os.path.isfile(os.path.join(candidate, "pyproject.toml")):
            project_root = candidate
            break
        candidate = os.path.dirname(candidate)

    if project_root is None:
        # Installed from PyPI — download the wheel instead
        subprocess.run(
            [sys.executable, "-m", "pip", "download", "ace-py", "--no-deps", "-d", wheel_dir],
            check=True,
            stdout=subprocess.DEVNULL,
        )
    else:
        subprocess.run(
            [sys.executable, "-m", "pip", "wheel", "--no-deps", "-w", wheel_dir, project_root],
            check=True,
            stdout=subprocess.DEVNULL,
        )


def _resolve_build_root(module_name: str) -> str:
    """
    Return the filesystem directory that should serve as the build context root.

    Resolution order:
    1. A package directory named <module_name>/ in cwd
    2. A file named <module_name>.py in cwd
    3. Fall back to cwd itself (the module may be installed, not local)
    """
    cwd = os.getcwd()
    pkg_dir = os.path.join(cwd, module_name)
    if os.path.isdir(pkg_dir) and os.path.isfile(os.path.join(pkg_dir, "__init__.py")):
        return pkg_dir
    py_file = os.path.join(cwd, module_name + ".py")
    if os.path.isfile(py_file):
        return cwd
    return cwd


def _should_exclude(path: str, root: str) -> bool:
    rel = os.path.relpath(path, root)
    parts = rel.split(os.sep)
    if any(p in _EXCLUDE_DIRS for p in parts):
        return True
    if any(path.endswith(s) for s in _EXCLUDE_SUFFIXES):
        return True
    return False


def _cmd_build(args):
    if not args:
        print("Usage: ace-py build <module>", file=sys.stderr)
        sys.exit(1)

    module_name = args[0]
    build_root = _resolve_build_root(module_name)
    containerfile_path = os.path.join(build_root, "Containerfile")

    # Write container build instructions
    has_requirements = os.path.isfile(os.path.join(build_root, "requirements.txt"))
    deps = _DEPS_WITH_REQUIREMENTS if has_requirements else _DEPS_WITHOUT_REQUIREMENTS
    content = _CONTAINERFILE_TEMPLATE_BASE.format(module=module_name, deps=deps)
    with open(containerfile_path, "w") as f:
        f.write(content)
    print(f"ace-py: wrote {containerfile_path}", file=sys.stderr)

    # Build ace-py wheel to vendor into the tar
    import tempfile
    with tempfile.TemporaryDirectory() as wheel_dir:
        print("ace-py: building wheel...", file=sys.stderr)
        _build_ace_py_wheel(wheel_dir)

        buf = io.BytesIO()
        with tarfile.open(fileobj=buf, mode="w") as tar:
            # User's files
            for dirpath, dirnames, filenames in os.walk(build_root):
                dirnames[:] = [
                    d for d in dirnames
                    if not _should_exclude(os.path.join(dirpath, d), build_root)
                ]
                for filename in filenames:
                    full_path = os.path.join(dirpath, filename)
                    if _should_exclude(full_path, build_root):
                        continue
                    arcname = os.path.relpath(full_path, build_root)
                    tar.add(full_path, arcname=arcname)

            # Vendored ace-py wheel(s)
            for filename in os.listdir(wheel_dir):
                tar.add(os.path.join(wheel_dir, filename), arcname=f"_vendor/{filename}")

    sys.stdout.buffer.write(buf.getvalue())
    sys.stdout.buffer.flush()
    print(f"ace-py: build context written ({buf.tell()} bytes)", file=sys.stderr)


def main():
    argv = sys.argv[1:]

    if not argv:
        print(__doc__.strip(), file=sys.stderr)
        sys.exit(1)

    command, *rest = argv

    if command == "run":
        _cmd_run(rest)
    elif command == "call":
        _cmd_call(rest)
    elif command == "build":
        _cmd_build(rest)
    else:
        print(f"ace-py: unknown command {command!r}", file=sys.stderr)
        print(__doc__.strip(), file=sys.stderr)
        sys.exit(1)
