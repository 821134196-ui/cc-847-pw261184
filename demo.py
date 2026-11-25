#!/usr/bin/env python3
"""End-to-end demo: build, crash, recover, inspect.

Runs a fixed narrative and prints the page / WAL evidence at each step.  Use
``--exhaustive`` to run the byte-by-byte crash sweep that the unit tests use.

    python3 demo.py              # quick narrated demo (~2 s)
    python3 demo.py --exhaustive # every WAL byte + every 64th page byte (~1 min)
"""

from __future__ import annotations

import argparse
import os
import shutil
import sys
import tempfile

ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, ROOT)

from minidb import Store, initialise  # noqa: E402
from minidb import pages as pagefmt  # noqa: E402
from minidb import viewer  # noqa: E402
from minidb.faults import Crash, FaultPlan, Injector  # noqa: E402
from minidb.format import PAGE_SIZE, SLOTS_PER_PAGE  # noqa: E402
from minidb.wal import scan_wal  # noqa: E402


def banner(text: str) -> None:
    print()
    print("=" * 78)
    print(text)
    print("=" * 78)


def crash_worker(dbdir: str, plan: FaultPlan, *, insert_from: int, count: int,
                 commit: bool) -> str | None:
    """Run a transaction in-process and kill it at the armed fault.

    Returns the fault description, or ``None`` if the fault never fired.
    """
    injector = Injector(plan, exit_process=False)
    store = Store(dbdir, capacity=2, injector=injector, durable=False)
    try:
        txn = store.begin()
        for i in range(count):
            txn.insert(insert_from + i, 777, "crash")
        if commit:
            txn.commit()
        store.bp.flush_all(sync=False)
    except Crash:
        store.abandon()
        return injector.fired
    else:
        if not txn.closed:
            txn.rollback()
        store.close()
        return None


def show_pages(dbdir: str, *, max_rows: int = 3) -> None:
    print("\n".join(viewer.render_db_header(dbdir)))
    print()
    print("\n".join(viewer.render_data_pages(dbdir, max_rows=max_rows)))
    print()
    print("\n".join(viewer.render_page_crc_table(dbdir)))


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--exhaustive", action="store_true",
                        help="run the full byte sweep instead of one narrative")
    parser.add_argument("--keep", action="store_true", help="keep the work dir")
    args = parser.parse_args(argv)

    if args.exhaustive:
        return exhaustive()

    work = tempfile.mkdtemp(prefix="minidb-demo-")
    dbdir = os.path.join(work, "db")
    print(f"working directory: {dbdir}")
    try:
        banner("1. CREATE + COMMIT A MULTI-PAGE TRANSACTION")
        initialise(dbdir)
        with Store(dbdir, capacity=4, durable=False) as store:
            txn = store.begin()
            for key in range(1, SLOTS_PER_PAGE + 11):
                txn.insert(key, key * 10, "base")
            result = txn.commit()
            print(f"committed {result['inserted']} rows across pages {result['pages']}")
            print(f"begin_lsn={result['begin_lsn']}  commit_lsn={result['commit_lsn']}")
        show_pages(dbdir)

        banner("2. CRASH WHILE WRITING THE NEXT TRANSACTION'S PAGE (torn page)")
        plan = FaultPlan(data_bytes=300, label="torn page write")
        fired = crash_worker(dbdir, plan, insert_from=1000, count=10, commit=False)
        print(f"process killed: {fired}")
        print()
        print("\n".join(viewer.render_page_crc_table(dbdir)))
        print()
        print("\n".join(viewer.render_wal(dbdir, max_frames=12)))
        print("\nThe last WAL frame(s) may also be torn; the tool reports the boundary.")

        banner("3. RESTART: ANALYSIS / REDO / UNDO")
        with Store(dbdir, capacity=8, durable=False) as store:
            print("\n".join(viewer.render_recovery_report(dbdir)))
            keys = store.keys()
        print(f"\nrows after recovery: {len(keys)}  (expected {SLOTS_PER_PAGE + 10})")
        assert keys == list(range(1, SLOTS_PER_PAGE + 11)), "recovery lost data"
        print("the loser's rows are gone; the committed rows are intact")

        banner("4. PAGE CHECKSUMS AND RECORDS AFTER RECOVERY")
        show_pages(dbdir)

        banner("5. BEFORE / AFTER SNAPSHOT COMPARISON")
        print("\n".join(viewer.render_snapshot_compare(dbdir)))

        banner("6. FULL INSPECT OUTPUT (tail)")
        text = viewer.render_inspection(dbdir, max_rows=2)
        print("\n".join(text.splitlines()[-40:]))
        print("\nOK: demo completed")
        return 0
    finally:
        if not args.keep:
            shutil.rmtree(work, ignore_errors=True)
        else:
            print(f"kept {work}")


def exhaustive() -> int:
    """Sweep every byte of a small WAL and every 64th byte of a page write."""
    work = tempfile.mkdtemp(prefix="minidb-exhaustive-")
    template = os.path.join(work, "template")
    initialise(template)
    with Store(template, capacity=8, durable=False) as store:
        txn = store.begin()
        for key in range(1, SLOTS_PER_PAGE + 6):
            txn.insert(key, key, "base")
        txn.commit()

    def fresh(name: str) -> str:
        target = os.path.join(work, name)
        shutil.rmtree(target, ignore_errors=True)
        shutil.copytree(template, target)
        return target

    def body(store, txn):
        for key in range(9000, 9005):
            txn.insert(key, 1, "sweep")
        txn.commit()
        store.bp.flush_all(sync=False)

    def run_once(dbdir: str, plan: FaultPlan) -> str | None:
        injector = Injector(plan, exit_process=False)
        store = Store(dbdir, capacity=2, injector=injector, durable=False)
        try:
            txn = store.begin()
            body(store, txn)
        except Crash:
            store.abandon()
            return injector.fired
        else:
            if not txn.closed:
                txn.rollback()
            store.close()
            return None

    def verify(dbdir: str, expect_new: bool) -> None:
        with Store(dbdir, capacity=8, durable=False) as store:
            keys = set(store.keys())
            for page_no in range(1, pagefmt.page_count(store.data_fd)):
                raw = pagefmt.read_page(store.data_fd, page_no)
                assert pagefmt.page_checksum_ok(raw), f"torn page {page_no}"
        baseline = set(range(1, SLOTS_PER_PAGE + 6))  # rows the template inserted
        assert baseline <= keys, "baseline rows lost"
        new = set(range(9000, 9005))
        assert (keys >= new) if expect_new else (not (keys & new)), (
            "transaction was not atomic"
        )

    try:
        # Measure the WAL footprint once, then sweep the range byte by byte.
        probe = fresh("probe")
        before = os.path.getsize(os.path.join(probe, "wal.log"))
        run_once(probe, FaultPlan())
        after = os.path.getsize(os.path.join(probe, "wal.log"))
        written = after - before
        scan = scan_wal(os.path.join(probe, "wal.log"))
        commit_end = None
        for rec in scan.records:
            if rec.offset >= before and rec.type_name == "COMMIT":
                commit_end = rec.end_offset - before
        if written <= 0:
            print("workload wrote nothing to the WAL; nothing to sweep")
            return 1
        print(f"WAL bytes written by the workload: {written}")
        print(f"COMMIT frame ends at byte: {commit_end}")

        torn_pages = 0
        for cut in range(written + 1):
            dbdir = fresh("work")
            run_once(dbdir, FaultPlan(wal_bytes=cut))
            verify(dbdir, expect_new=(commit_end is not None and cut >= commit_end))
            if cut % 500 == 0:
                print(f"  WAL cut {cut}/{written} ... ok")
        print(f"WAL sweep complete: {written + 1} cut points, all atomic")

        torn_pages = 0
        for cut in range(0, 4097, 64):
            dbdir = fresh("work")
            run_once(dbdir, FaultPlan(data_bytes=cut))
            # Inspect the raw file *before* verify() opens the store, since
            # that open runs recovery and would repair the torn page.  The
            # workload's only dirty page is page 2 (offset 8192).
            with open(os.path.join(dbdir, "data.db"), "rb") as fh:
                fh.seek(2 * PAGE_SIZE)
                raw = fh.read(PAGE_SIZE)
            torn = len(raw) == PAGE_SIZE and not pagefmt.page_checksum_ok(raw)
            if torn:
                torn_pages += 1
            verify(dbdir, expect_new=True)
        print(f"page sweep complete: {len(range(0, 4097, 64))} cut points, "
              f"{torn_pages} produced detectable torn pages (all repaired)")
        print("\nOK: exhaustive sweep passed")
        return 0
    finally:
        shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
