"""Generic utility functions shared across the package."""

from __future__ import annotations

import inspect
import re
from typing import Any, Iterable, Mapping, TypeVar

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
        return set(vars(obj_or_cls))
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
