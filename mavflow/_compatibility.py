"""Small compatibility types for the supported Python versions."""

from __future__ import annotations

import builtins
from collections.abc import Iterable
from enum import Enum
from typing import cast


class StrEnum(str, Enum):
    """String enum behavior shared by Python 3.10 and later."""

    def __str__(self) -> str:
        return str(self.value)

    def __format__(self, format_spec: str) -> str:
        return format(str(self), format_spec)


class _FallbackExceptionGroup(Exception):
    """Carry multiple ordinary exceptions on Python versions before 3.11."""

    def __init__(self, message: str, exceptions: Iterable[Exception]) -> None:
        self.message = str(message)
        self.exceptions = tuple(exceptions)
        if not self.exceptions:
            raise ValueError("ExceptionGroup requires at least one exception")
        if any(not isinstance(error, Exception) for error in self.exceptions):
            raise TypeError("ExceptionGroup accepts Exception instances only")
        super().__init__(self.message, self.exceptions)

    def __str__(self) -> str:
        count = len(self.exceptions)
        suffix = "s" if count != 1 else ""
        return f"{self.message} ({count} sub-exception{suffix})"


ExceptionGroup = cast(
    type[Exception],
    getattr(builtins, "ExceptionGroup", _FallbackExceptionGroup),
)

__all__ = ["ExceptionGroup", "StrEnum"]
