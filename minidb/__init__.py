"""minidb -- a tiny split-page store with a write-ahead log and recovery."""

from __future__ import annotations

__version__ = "1.0.0"

from .engine import (  # noqa: F401
    DatabaseFull,
    DuplicateKey,
    EngineError,
    KeyNotFound,
    Store,
    Transaction,
    initialise,
)
from .format import PAGE_SIZE, RECORD_SIZE, SLOTS_PER_PAGE  # noqa: F401

__all__ = [
    "DatabaseFull",
    "DuplicateKey",
    "EngineError",
    "KeyNotFound",
    "PAGE_SIZE",
    "RECORD_SIZE",
    "SLOTS_PER_PAGE",
    "Store",
    "Transaction",
    "initialise",
    "__version__",
]
