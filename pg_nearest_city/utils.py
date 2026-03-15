"""Generic utility functions shared across the package."""

from __future__ import annotations

import inspect
import re
import shutil
import subprocess
from contextlib import contextmanager
from pathlib import Path
from typing import IO, Any, Generator, Iterable, Mapping, TypeVar

# ---------------------------------------------------------------------------
# Compression module imports
#
# Python 3.14+ provides a unified ``compression`` package.  Try that first,
# then fall back to the individual stdlib modules for older Pythons.
# ---------------------------------------------------------------------------
_HAS_BZ2 = False
_HAS_LZMA = False
_zstd_open = None

try:
    from compression import (  # type: ignore[import-not-found]
        bz2,
        gzip,
        lzma,
        zstd as _zstd_mod,
    )

    _HAS_BZ2 = True
    _HAS_LZMA = True
    _zstd_open = _zstd_mod.open
except ImportError:
    import gzip

    try:
        import bz2

        _HAS_BZ2 = True
    except ImportError:
        pass

    try:
        import lzma

        _HAS_LZMA = True
    except ImportError:
        pass

_HAS_ZSTD_CLI = bool(shutil.which("zstd"))

T = TypeVar("T")

_MISSING = object()


def get_all_subclasses(base: type[T]) -> list[type[T]]:
    """Find all subclasses of `base` defined in its module.

    Non-recursive across modules.
    """
    mod = inspect.getmodule(base)
    if mod is None:
        return []
    return [
        cls
        for _, cls in inspect.getmembers(
            mod,
            lambda c: inspect.isclass(c) and issubclass(c, base) and c is not base,
        )
    ]


def _attr_names_for(obj_or_cls: Any) -> set[str]:
    """Attempts to get attribute names of a given object in multiple ways."""
    try:
        names = set(vars(obj_or_cls))
        # For classes, walk the MRO to include inherited attributes
        if isinstance(obj_or_cls, type):
            for base in obj_or_cls.__mro__:
                names.update(vars(base))
        return names
    except TypeError:
        pass

    try:
        slots = obj_or_cls.__slots__
        if isinstance(slots, str):
            return {slots}
        return set(slots)
    except Exception:
        pass

    if isinstance(obj_or_cls, Mapping):
        return {str(k) for k in obj_or_cls}

    return set()


def _get_value(obj: Any, key: str) -> Any:
    """Get value from an object, defaulting to an empty object."""
    if isinstance(obj, Mapping):
        return obj.get(key, _MISSING)
    return getattr(obj, key, _MISSING)


def _is_sequence_iterable(x: Any) -> bool:
    """Treats sets as an Iterable, which for this purpose is desired."""
    return isinstance(x, Iterable) and not isinstance(
        x, (str, bytes, bytearray, Mapping)
    )


def _eval_simple(val: Any, cond: Any) -> bool:
    """Evaluate a simple condition against val.

    - callable -> cond(val)
    - regex: ("regex", pattern) where pattern is str or re.Pattern
    - membership: list/tuple/set/frozenset => val in cond
    - equality: val == cond
    """
    if callable(cond):
        return bool(cond(val))

    if isinstance(cond, tuple) and len(cond) == 2 and cond[0] == "regex":
        s = "" if val is None else str(val)
        pat = cond[1]
        if isinstance(pat, re.Pattern):
            return bool(pat.search(s))
        return bool(re.search(str(pat), s))

    if isinstance(cond, (list, tuple, set, frozenset)):
        return val in cond

    return val == cond


def _match_obj(obj: Any, cond_map: dict[str, Any]) -> bool:
    """Evaluate all key->condition pairs on `obj`."""
    keys = _attr_names_for(obj)
    if keys.isdisjoint(cond_map):
        return False

    for key, cond in cond_map.items():
        val = _get_value(obj, key)
        if val is _MISSING:
            return False

        if isinstance(cond, dict):
            if _is_sequence_iterable(val):
                if not any(_match_obj(x, cond) for x in val):
                    return False
            else:
                if not _match_obj(val, cond):
                    return False
        else:
            if not _eval_simple(val, cond):
                return False

    return True


def filter_items(
    items: list[T], filters: list[dict[str, Any]] | None = None
) -> list[T]:
    """Generic filter across any objects/classes.

    - filters: list of dicts OR'ed; keys inside a dict are AND'ed.
    - Values:
        * callable -> predicate(value) -> bool
        * ("regex", pattern) -> regex against str(value)
        * list/tuple/set -> membership (value in container)
        * dict -> nested attribute matching (with ANY semantics on sequences)
        * else -> equality
    """
    if not filters:
        return items
    return [obj for obj in items if any(_match_obj(obj, flt) for flt in filters)]


# ---------------------------------------------------------------------------
# Compression helpers
# ---------------------------------------------------------------------------

COMPRESSION_EXTENSIONS: tuple[str, ...] = (".zst", ".xz", ".bz2", ".gz")


def preferred_compression_ext() -> str:
    """Return file extension for the best available compressor."""
    if _zstd_open is not None or _HAS_ZSTD_CLI:
        return ".zst"
    if _HAS_LZMA:
        return ".xz"
    if _HAS_BZ2:
        return ".bz2"
    return ".gz"


@contextmanager
def _zstd_cli_write(path: Path) -> Generator[IO[bytes], None, None]:
    """Write compressed data via the ``zstd`` CLI."""
    proc = subprocess.Popen(
        ["zstd", "-f", "-q", "-o", str(path), "-"],
        stdin=subprocess.PIPE,
    )
    try:
        yield proc.stdin
    finally:
        if proc.stdin:
            proc.stdin.close()
        rc = proc.wait()
        if rc != 0:
            raise subprocess.CalledProcessError(rc, "zstd")


@contextmanager
def _zstd_cli_read(path: Path) -> Generator[IO[bytes], None, None]:
    """Read compressed data via the ``zstd`` CLI."""
    proc = subprocess.Popen(
        ["zstd", "-d", "-q", "-c", str(path)],
        stdout=subprocess.PIPE,
    )
    try:
        yield proc.stdout
    finally:
        if proc.stdout:
            proc.stdout.close()
        proc.wait()


@contextmanager
def open_compressed(path: Path, mode: str = "rb") -> Generator[IO[bytes], None, None]:
    """Open a compressed file, choosing decompressor by suffix.

    Supports ``.gz``, ``.bz2``, ``.xz``, and ``.zst``.
    For ``.zst``, uses ``compression.zstd`` (Python 3.14+) or the
    ``zstd`` CLI tool as a fallback.

    Args:
        path: Path to the compressed file.
        mode: ``'rb'`` for reading, ``'wb'`` for writing.

    Yields:
        A file-like object supporting ``read``/``write``.
    """
    ext = path.suffix.lower()

    if ext == ".zst":
        if _zstd_open is not None:
            with _zstd_open(path, mode) as fh:
                yield fh
        elif _HAS_ZSTD_CLI:
            cm = _zstd_cli_write(path) if "w" in mode else _zstd_cli_read(path)
            with cm as fh:
                yield fh
        else:
            raise ImportError(
                f"Cannot handle {path.name}: zstd is not available. "
                "Use Python 3.14+ or install the zstd CLI tool."
            )
    elif ext == ".xz":
        if not _HAS_LZMA:
            raise ImportError(
                f"Cannot handle {path.name}: lzma module is not available."
            )
        with lzma.open(path, mode) as fh:
            yield fh
    elif ext == ".bz2":
        if not _HAS_BZ2:
            raise ImportError(
                f"Cannot handle {path.name}: bz2 module is not available."
            )
        with bz2.open(path, mode) as fh:
            yield fh
    else:
        with gzip.open(path, mode) as fh:
            yield fh
