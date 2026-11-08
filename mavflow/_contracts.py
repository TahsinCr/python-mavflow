"""Private immutable-model helpers used by Mavflow."""

from __future__ import annotations

from collections.abc import Mapping
from copy import deepcopy
from types import MappingProxyType
from typing import Any


def _freeze_model_value(value: Any) -> Any:
    """Detach and recursively freeze a value stored by a public model."""

    if isinstance(value, MappingProxyType):
        return value
    if isinstance(value, Mapping):
        return MappingProxyType(
            {deepcopy(key): _freeze_model_value(item) for key, item in value.items()}
        )
    if isinstance(value, tuple):
        frozen = tuple(_freeze_model_value(item) for item in value)
        return value if all(a is b for a, b in zip(value, frozen, strict=False)) else frozen
    if isinstance(value, list):
        return tuple(_freeze_model_value(item) for item in value)
    if isinstance(value, frozenset):
        return frozenset(_freeze_model_value(item) for item in value)
    if isinstance(value, set):
        return frozenset(_freeze_model_value(item) for item in value)
    return deepcopy(value)


def _copy_model_value(value: Any, *, lists: bool = False) -> Any:
    """Return a detached, mutable representation of a frozen model value."""

    if isinstance(value, Mapping):
        return {deepcopy(key): _copy_model_value(item, lists=lists) for key, item in value.items()}
    if isinstance(value, tuple):
        copied = tuple(_copy_model_value(item, lists=lists) for item in value)
        return list(copied) if lists else copied
    if isinstance(value, frozenset):
        return {_copy_model_value(item, lists=lists) for item in value}
    return deepcopy(value)
