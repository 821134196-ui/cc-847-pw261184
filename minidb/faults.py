"""Crash injection: stop the process part-way through an I/O write.

The injector is deliberately ignorant of meaning, like a power cut.  It is
armed with a *cumulative byte budget* for the log file and/or the data file.
Writes proceed normally until the budget is exhausted; the write that
crosses the budget is truncated to the remaining bytes, fsynced, and the
process is terminated with ``os._exit`` (or :class:`Crash` is raised, for
fast in-process tests).

Consequences the engine must survive:

* a log frame cut anywhere -- including inside its 8-byte frame header
* a data page cut anywhere -- including inside its header or row area

Because both formats carry CRC32 checksums, neither a torn frame nor a torn
page can be mistaken for valid content.
"""

from __future__ import annotations

import json
import os
import sys
from dataclasses import dataclass, field


class Crash(BaseException):
    """Raised once an injected failure fires (when not exiting the process).

    Derives from ``BaseException`` so ordinary ``except Exception`` handlers
    in the engine cannot swallow a power cut.
    """


@dataclass
class FaultPlan:
    """Where to cut the process short.

    ``wal_bytes`` / ``data_bytes`` are cumulative byte budgets: the first
    write that would cross the budget is truncated.  A budget of 0 kills the
    process before the first write.

    ``wal_write_index`` / ``data_write_index`` target the Nth write of that
    kind (0-based) and kill before it, which is handy for "fail exactly
    before the commit frame" style tests.
    """

    wal_bytes: int | None = None
    data_bytes: int | None = None
    wal_write_index: int | None = None
    data_write_index: int | None = None
    label: str = "crash"

    @classmethod
    def from_json(cls, text: str) -> "FaultPlan":
        return cls(**json.loads(text))

    def as_dict(self) -> dict:
        return {
            "wal_bytes": self.wal_bytes,
            "data_bytes": self.data_bytes,
            "wal_write_index": self.wal_write_index,
            "data_write_index": self.data_write_index,
            "label": self.label,
        }

    def to_json(self) -> str:
        return json.dumps(
            {
                "wal_bytes": self.wal_bytes,
                "data_bytes": self.data_bytes,
                "wal_write_index": self.wal_write_index,
                "data_write_index": self.data_write_index,
                "label": self.label,
            }
        )

    def describe(self) -> str:
        bits = []
        if self.wal_bytes is not None:
            bits.append(f"wal budget {self.wal_bytes} B")
        if self.data_bytes is not None:
            bits.append(f"data budget {self.data_bytes} B")
        if self.wal_write_index is not None:
            bits.append(f"wal write #{self.wal_write_index}")
        if self.data_write_index is not None:
            bits.append(f"data write #{self.data_write_index}")
        return f"{self.label}: " + (", ".join(bits) or "no failure armed")


class Injector:
    """Per-process fault injector with a record of every I/O write."""

    def __init__(self, plan: FaultPlan | None = None, *, exit_process: bool = True):
        self.plan = plan or FaultPlan()
        self.exit_process = exit_process
        self.wal_writes = 0
        self.data_writes = 0
        self.wal_bytes_written = 0
        self.data_bytes_written = 0
        self.fired: str | None = None
        self.events: list[dict] = []

    # -- public ------------------------------------------------------------

    def before_write(self, kind: str, offset: int, length: int, **ctx) -> tuple[int, bool]:
        """Return ``(bytes_to_write, crash_after)`` for one write."""
        if kind == "wal":
            index = self.wal_writes
            self.wal_writes += 1
            budget = self.plan.wal_bytes
            index_target = self.plan.wal_write_index
            written = self.wal_bytes_written
        else:
            index = self.data_writes
            self.data_writes += 1
            budget = self.plan.data_bytes
            index_target = self.plan.data_write_index
            written = self.data_bytes_written

        self.events.append(
            {
                "kind": kind,
                "index": index,
                "offset": offset,
                "length": length,
                "bytes_before": written,
                **{k: v for k, v in ctx.items() if isinstance(v, (int, str))},
            }
        )

        if self.fired is not None:
            return length, False

        if index_target is not None and index == index_target:
            self.fired = f"{kind} write #{index} at offset {offset}"
            return 0, True

        if budget is not None:
            if budget <= written:
                self.fired = f"{kind} budget exhausted before write #{index} at {offset}"
                return 0, True
            if written + length > budget:
                prefix = budget - written
                self.fired = (
                    f"{kind} write #{index} at {offset} cut after {prefix}/{length} B"
                )
                self._account(kind, prefix)
                return prefix, True
            self._account(kind, length)

        return length, False

    def crash_now(self) -> None:
        if self.exit_process:
            # os._exit skips buffered I/O, atexit hooks and finally blocks:
            # the engine must not get a chance to clean up or checkpoint.
            sys.stdout.flush()
            sys.stderr.flush()
            os._exit(90)
        raise Crash(self.fired or "injected crash")

    # -- introspection -----------------------------------------------------

    def _account(self, kind: str, nbytes: int) -> None:
        if kind == "wal":
            self.wal_bytes_written += nbytes
        else:
            self.data_bytes_written += nbytes

    def dump_events(self) -> list[dict]:
        return list(self.events)


# --- module-level write helper ---------------------------------------------


def write_with_fault(fd: int, data: bytes, offset: int, injector, kind: str, **ctx) -> None:
    """``pwrite`` *data* at *offset*, honouring an armed fault."""
    if injector is None:
        _pwrite_all(fd, data, offset)
        return
    limit, crash = injector.before_write(kind, offset, len(data), **ctx)
    if limit:
        _pwrite_all(fd, data[:limit], offset)
        os.fsync(fd)
    if crash:
        injector.crash_now()


def _pwrite_all(fd: int, data: bytes, offset: int) -> None:
    view = memoryview(data)
    while view:
        written = os.pwrite(fd, view, offset)
        if written <= 0:
            raise OSError("short write")
        offset += written
        view = view[written:]
