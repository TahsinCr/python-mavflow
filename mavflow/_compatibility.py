"""Select standard-library types or their Python 3.10 backports."""

from __future__ import annotations

import sys

if sys.version_info >= (3, 11):
    from builtins import ExceptionGroup as ExceptionGroup
    from enum import StrEnum as StrEnum
else:
    from backports.strenum import StrEnum as StrEnum
    from exceptiongroup import ExceptionGroup as ExceptionGroup

__all__ = ["ExceptionGroup", "StrEnum"]
