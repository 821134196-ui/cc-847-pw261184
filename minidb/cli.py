"""``python -m minidb`` -- initialise, run a transaction, crash, recover, inspect."""

from __future__ import annotations

import argparse
import json
import os
import sys

from . import pages as pagefmt
from .engine import (
    DatabaseFull,
    EngineError,
    KeyNotFound,
    Store,
    initialise,
    data_path,
    wal_path,
)
from .faults import FaultPlan, Injector
from .format import PAGE_SIZE
from . import viewer


def _parse_fault(text: str | None) -> FaultPlan | None:
    if not text:
        return None
    if os.path.exists(text):
        with open(text, "r", encoding="utf-8") as fh:
            text = fh.read()
    return FaultPlan.from_json(text)


def _require_db(dbdir: str) -> None:
    if not os.path.exists(data_path(dbdir)):
        raise EngineError(f"no database in {dbdir} (run `init` first)")


# ---------------------------------------------------------------------------
# commands
# ---------------------------------------------------------------------------


def cmd_init(args) -> int:
    initialise(args.dbdir)
    print(f"initialised {args.dbdir}")
    return 0


def cmd_insert(args) -> int:
    _require_db(args.dbdir)
    injector = Injector(_parse_fault(args.fault)) if args.fault else None
    store = Store(args.dbdir, capacity=args.capacity, injector=injector)
    txn = store.begin()
    for i in range(args.count):
        key = args.key_start + i
        txn.insert(key, args.value, args.tag)
    result = txn.commit()
    store.close()
    print(json.dumps(result))
    return 0


def cmd_run(args) -> int:
    """Run a script of operations from a JSON file.

    Script shape::

        {"operations": [
            {"op": "insert", "key": 1, "value": 10, "tag": "a"},
            {"op": "update", "key": 1, "value": 11},
            {"op": "delete", "key": 1},
            {"op": "commit"},
            {"op": "rollback"}
        ]}
    """
    _require_db(args.dbdir)
    with open(args.script, "r", encoding="utf-8") as fh:
        script = json.load(fh)
    injector = (
        Injector(_parse_fault(args.fault), exit_process=not args.in_process)
        if args.fault
        else None
    )
    store = Store(args.dbdir, capacity=args.capacity, injector=injector)
    results = []
    txn = store.begin()
    try:
        for step in script["operations"]:
            op = step["op"]
            if op == "insert":
                txn.insert(step["key"], step.get("value", 0), step.get("tag", ""))
            elif op == "update":
                txn.update(step["key"], step.get("value"), step.get("tag"))
            elif op == "delete":
                txn.delete(step["key"])
            elif op == "commit":
                results.append({"commit": txn.commit()})
                txn = store.begin() if not args.stop_after_commit else None
                if txn is None:
                    break
            elif op == "rollback":
                results.append({"rollback": txn.rollback()})
                txn = store.begin() if not args.stop_after_commit else None
                if txn is None:
                    break
            elif op == "flush":
                store.bp.flush_all(sync=True)
                results.append({"flushed": True})
            elif op == "sleep":
                import time

                time.sleep(step.get("seconds", 0.01))
            else:
                raise EngineError(f"unknown operation {op!r}")
        if txn is not None and not txn.closed:
            results.append({"uncommitted": txn.txn_id})
    finally:
        # A crash injector may have killed us before this point; that is fine.
        pass
    print(json.dumps({"results": results, "injector": injector.dump_events() if injector else []}))
    if injector is not None and injector.fired:
        # The process was supposed to die; if we are here, the injection
        # targeted a write that never happened.
        print(f"WARNING: fault never fired ({injector.plan.describe()})", file=sys.stderr)
    if txn is not None and not txn.closed:
        store.close()
    else:
        store.close()
    return 0


def cmd_crash(args) -> int:
    """Simulate an abrupt process death during a transaction.

    The transaction may be committed or not.  Dirty pages are pushed toward
    disk (the buffer pool is small, so evictions may already have written
    some), and then the process exits without closing the store: no flush of
    remaining pages, no clean marker.  If a fault plan is armed, it cuts one
    of the writes on the way.
    """
    _require_db(args.dbdir)
    plan = _parse_fault(args.fault)
    if plan is None:
        plan = FaultPlan(wal_bytes=args.wal_bytes, data_bytes=args.data_bytes,
                         label="crash")
    injector = Injector(plan, exit_process=not args.in_process)
    store = Store(args.dbdir, capacity=args.capacity, injector=injector)
    txn = store.begin()
    for i in range(args.count):
        txn.insert(args.key_start + i, args.value, args.tag)
    if args.commit:
        txn.commit()
    # Push pages out.  With an armed data fault this is where a torn page
    # can be produced; otherwise it models the steal policy.
    store.bp.flush_all(sync=not args.no_sync)
    if injector.fired:
        print(json.dumps({"fired": injector.fired,
                          "events": injector.dump_events()[-8:]}))
    if args.in_process:
        return 0 if not injector.fired else 0
    # Abrupt death: skip atexit, skip finally, skip close().
    sys.stdout.flush()
    sys.stderr.flush()
    os._exit(97)


def cmd_recover(args) -> int:
    _require_db(args.dbdir)
    # Store() runs restart recovery on open, before any transaction can
    # start.  The report is produced by that run.
    store = Store(args.dbdir, capacity=args.capacity)
    report = store.recovery_report
    store.close()
    print(json.dumps(report, indent=2, sort_keys=True))
    return 0


def cmd_dump(args) -> int:
    _require_db(args.dbdir)
    store = Store(args.dbdir, capacity=4)
    rows = store.rows()
    report = store.recovery_report
    store.close()
    print(json.dumps({"rows": rows, "recovery": report}, indent=2, sort_keys=True))
    return 0


def cmd_inspect(args) -> int:
    print(
        viewer.render_inspection(
            args.dbdir,
            show_rows=not args.no_rows,
            max_rows=args.max_rows,
        )
    )
    return 0


def cmd_pages(args) -> int:
    if args.only:
        page_filter = set(args.only)
        print("\n".join(viewer.render_data_pages(
            args.dbdir, page_filter=page_filter,
            show_rows=not args.no_rows, max_rows=args.max_rows)))
        return 0
    print("\n".join(viewer.render_data_pages(
        args.dbdir, show_rows=not args.no_rows, max_rows=args.max_rows)))
    print()
    print("\n".join(viewer.render_page_crc_table(args.dbdir)))
    return 0


def cmd_wal(args) -> int:
    print("\n".join(viewer.render_wal(args.dbdir, max_frames=args.max_frames)))
    return 0


def cmd_check(args) -> int:
    """Verify checksums; exit non-zero when anything is torn or corrupt."""
    pages, size = viewer._read_all_pages(args.dbdir)
    problems = []
    ok_pages = 0
    for page_no, raw in pages:
        if page_no == pagefmt.DB_HEADER_PAGE:
            try:
                hdr = pagefmt.decode_db_header(raw)
                if not hdr.checksum_ok:
                    problems.append(f"data header CRC mismatch")
            except ValueError as exc:
                problems.append(str(exc))
            continue
        hdr = pagefmt.decode_page_header(raw)
        if hdr.checksum_ok:
            ok_pages += 1
        else:
            problems.append(f"page {page_no}: CRC mismatch")
    from .wal import scan_wal

    scan = scan_wal(wal_path(args.dbdir))
    if scan.header is None or not scan.header.get("checksum_ok"):
        problems.append("WAL header CRC mismatch")
    if scan.torn:
        problems.append(f"WAL torn tail: {scan.tail_bytes} B")
    print(json.dumps({
        "pages_ok": ok_pages,
        "pages_total": max(len(pages) - 1, 0),
        "wal_records": len(scan.records),
        "wal_torn": scan.torn,
        "problems": problems,
    }, indent=2))
    return 1 if problems else 0


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="minidb", description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)

    def add(name, func, help_text):
        p = sub.add_parser(name, help=help_text)
        p.set_defaults(func=func)
        return p

    p = add("init", cmd_init, "create an empty database directory")
    p.add_argument("dbdir")

    p = add("insert", cmd_insert, "insert N rows in one committed transaction")
    p.add_argument("dbdir")
    p.add_argument("--count", type=int, default=1)
    p.add_argument("--key-start", type=int, default=1)
    p.add_argument("--value", type=int, default=0)
    p.add_argument("--tag", default="")
    p.add_argument("--capacity", type=int, default=8)
    p.add_argument("--fault", help="fault plan JSON string or path")

    p = add("run", cmd_run, "run a JSON script of operations")
    p.add_argument("dbdir")
    p.add_argument("script")
    p.add_argument("--capacity", type=int, default=3)
    p.add_argument("--fault", help="fault plan JSON string or path")
    p.add_argument("--in-process", action="store_true",
                   help="raise Crash instead of calling os._exit")
    p.add_argument("--stop-after-commit", action="store_true")

    p = add("crash", cmd_crash, "run inserts then die at an injected fault")
    p.add_argument("dbdir")
    p.add_argument("--count", type=int, default=200)
    p.add_argument("--key-start", type=int, default=1)
    p.add_argument("--value", type=int, default=7)
    p.add_argument("--tag", default="")
    p.add_argument("--commit", action="store_true",
                   help="commit before closing (so the crash hits page flush)")
    p.add_argument("--capacity", type=int, default=2)
    p.add_argument("--fault", help="fault plan JSON string or path")
    p.add_argument("--wal-bytes", type=int, default=None)
    p.add_argument("--data-bytes", type=int, default=None)
    p.add_argument("--in-process", action="store_true",
                   help="raise Crash instead of calling os._exit")
    p.add_argument("--no-sync", action="store_true",
                   help="flush pages without fsyncing the data file")

    p = add("recover", cmd_recover, "run restart recovery and print its report")
    p.add_argument("dbdir")
    p.add_argument("--capacity", type=int, default=8)

    p = add("dump", cmd_dump, "print all live rows as JSON")
    p.add_argument("dbdir")

    p = add("inspect", cmd_inspect, "full read-only view of data, WAL, recovery")
    p.add_argument("dbdir")
    p.add_argument("--no-rows", action="store_true")
    p.add_argument("--max-rows", type=int, default=8)

    p = add("pages", cmd_pages, "page viewer with checksums and rows")
    p.add_argument("dbdir")
    p.add_argument("--only", type=int, nargs="*", help="page numbers to show")
    p.add_argument("--no-rows", action="store_true")
    p.add_argument("--max-rows", type=int, default=8)

    p = add("wal", cmd_wal, "WAL viewer with the valid/torn boundary")
    p.add_argument("dbdir")
    p.add_argument("--max-frames", type=int, default=None)

    p = add("check", cmd_check, "verify page and WAL checksums")
    p.add_argument("dbdir")

    return parser


def main(argv=None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    try:
        return args.func(args)
    except EngineError as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":  # pragma: no cover
    raise SystemExit(main())
