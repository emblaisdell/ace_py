"""
ace_py — Python FFI for the Ontolog ACE interface.

Usage
-----
    from ace_py import calculation, run

    @calculation
    def add(a: int, b: int) -> int:
        return a + b

    if __name__ == "__main__":
        run()

Wire format (big-endian, on both stdin and stdout)
---------------------------------------------------
Each message is a length-prefixed blob:

    [ uint32 length ][ <length> bytes payload ]

stdin:  blob 0 = UTF-8 function name
        blob 1..N = arguments (encoded per type, see below)

stdout: one blob per output item (result is always treated as a list)

Type encoding
-------------
str  — UTF-8
int  — big-endian two's-complement signed bytes (minimum width)
"""

import inspect
import struct
import sys
from typing import get_type_hints

_registry: dict[str, dict] = {}
# Each entry: {"fn": <callable>, "flags": list[str]}

# ---------------------------------------------------------------------------
# Type codec registry — extend for new types
# ---------------------------------------------------------------------------

def _decode_str(data: bytes) -> str:
    return data.decode("utf-8")

def _decode_int(data: bytes) -> int:
    return int.from_bytes(data, byteorder="big", signed=True)

def _encode_str(value: str) -> bytes:
    return value.encode("utf-8")

def _encode_int(value: int) -> bytes:
    if value == 0:
        return b"\x00"
    byte_length = (value.bit_length() + 8) // 8  # +1 bit for sign, round up
    return value.to_bytes(byte_length, byteorder="big", signed=True)

_DECODERS: dict[type, object] = {
    str: _decode_str,
    int: _decode_int,
}

_ENCODERS: dict[type, object] = {
    str: _encode_str,
    int: _encode_int,
}


def _decode(data: bytes, typ: type) -> object:
    decoder = _DECODERS.get(typ)
    if decoder is None:
        raise TypeError(f"ace_py: no decoder registered for type {typ!r}")
    return decoder(data)


def _encode(value: object) -> bytes:
    encoder = _ENCODERS.get(type(value))
    if encoder is not None:
        return encoder(value)
    # fallback: stringify
    return str(value).encode("utf-8")


# ---------------------------------------------------------------------------
# Wire I/O helpers
# ---------------------------------------------------------------------------

def _read_blob(stream) -> bytes | None:
    header = stream.read(4)
    if not header:
        return None
    if len(header) < 4:
        raise EOFError("ace_py: truncated length header")
    (length,) = struct.unpack(">I", header)
    payload = stream.read(length)
    if len(payload) < length:
        raise EOFError("ace_py: truncated payload")
    return payload


def _write_blob(stream, data: bytes) -> None:
    stream.write(struct.pack(">I", len(data)))
    stream.write(data)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def calculation(*flags):
    """Register a function as a callable ACE calculation.

    May be used with or without flag arguments::

        @calculation
        def add(a: int, b: int) -> int: ...

        @calculation("cast")
        def int2str(a: int) -> str: ...

    Flag strings are recorded in the registry and emitted in .pho output.
    """
    def _register(fn):
        _registry[fn.__name__] = {"fn": fn, "flags": list(flags)}
        return fn

    # @calculation with no parens — flags[0] is the decorated function
    if len(flags) == 1 and callable(flags[0]):
        fn = flags[0]
        _registry[fn.__name__] = {"fn": fn, "flags": []}
        return fn

    # @calculation("cast", ...) — return the actual decorator
    return _register


def run(stdin=None, stdout=None, stderr=None) -> None:
    """
    Read one ACE request from *stdin*, dispatch to the matching @calculation,
    and write the results to *stdout*.

    Defaults to sys.stdin.buffer / sys.stdout.buffer / sys.stderr.
    Exits with a non-zero status on error, writing a message to *stderr*.
    """
    if stdin is None:
        stdin = sys.stdin.buffer
    if stdout is None:
        stdout = sys.stdout.buffer
    if stderr is None:
        stderr = sys.stderr

    def die(msg: str) -> None:
        print(f"ace_py error: {msg}", file=stderr)
        sys.exit(1)

    # --- read function name ---
    name_blob = _read_blob(stdin)
    if name_blob is None:
        die("empty input — expected a function name blob")

    func_name = name_blob.decode("utf-8")

    if func_name not in _registry:
        known = ", ".join(sorted(_registry)) or "(none)"
        die(f"unknown calculation {func_name!r}; registered: {known}")

    fn = _registry[func_name]["fn"]
    hints = get_type_hints(fn)
    params = list(inspect.signature(fn).parameters.values())

    # --- read & coerce arguments ---
    args = []
    for i, param in enumerate(params):
        blob = _read_blob(stdin)
        if blob is None:
            die(
                f"{func_name}: expected {len(params)} argument(s), "
                f"got {i} (missing {param.name!r})"
            )
        typ = hints.get(param.name, str)
        try:
            args.append(_decode(blob, typ))
        except Exception as exc:
            die(f"{func_name}: could not decode argument {param.name!r} as {typ!r}: {exc}")

    # --- call ---
    try:
        result = fn(*args)
    except Exception as exc:
        die(f"{func_name}: raised {type(exc).__name__}: {exc}")

    # --- write output ---
    items = result if isinstance(result, list) else [result]
    for item in items:
        _write_blob(stdout, _encode(item))

    stdout.flush()
