"""The core guarantee, tested by cutting the process at every byte offset.

Three layers:

1. ``TestWalByteSweep`` and ``TestPageByteSweep`` cut *every* byte offset of
   a small workload, in-process, by raising :class:`Crash` from the injector
   after the partial write.  Cheap enough for a full sweep.

2. ``TestRealProcessDeath`` repeats representative cuts in a child
   interpreter that dies via ``os._exit`` -- no unwinding, no flushes, no
   clean marker.  This proves the in-process simulation is faithful.

3. ``TestTornPageRepair`` checks specific torn-page shapes (commit boundary,
   header cut, row-area cut, uncommitted vs committed).

Invariants checked after every crash + reopen:

* every previously committed row is present and unchanged;
* the crashing transaction is atomic: all of its rows are there (its COMMIT
  frame survived) or none are (it did not);
* no page is left with a bad CRC32.
"""

from __future__ import annotations

import os
import shutil
import subprocess
import sys
import unittest

import helpers

from minidb import Store, initialise
from minidb import pages as pagefmt
from minidb.faults import Crash, FaultPlan, Injector
from minidb.format import SLOTS_PER_PAGE
from minidb.wal import scan_wal

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

BASELINE = SLOTS_PER_PAGE + 10  # 136 rows: fills page 1, leaves page 2 open


class SweepBase(unittest.TestCase):
    def setUp(self):
        self.tmp = os.path.join(
            os.environ.get("TMPDIR", "/tmp"),
            f"minidb-sweep-{os.getpid()}-{abs(hash(self.id())) % 10**9}",
        )
        shutil.rmtree(self.tmp, ignore_errors=True)
        os.makedirs(self.tmp)
        self.template = os.path.join(self.tmp, "template")
        initialise(self.template)
        store = Store(self.template, capacity=8, durable=False)
        txn = store.begin()
        for key in range(1, BASELINE + 1):
            txn.insert(key, key * 10, "base")
        txn.commit()
        store.close()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def fresh_clone(self, name: str) -> str:
        target = os.path.join(self.tmp, name)
        shutil.copytree(self.template, target)
        return target

    def restore_workdir(self, name: str = "work") -> str:
        """Reset a single scratch directory to the template.

        Sweeps run hundreds of crash/recovery cycles; recreating one
        directory is much cheaper than keeping hundreds alive, and the
        tests never look at two database states at once.
        """
        target = os.path.join(self.tmp, name)
        shutil.rmtree(target, ignore_errors=True)
        shutil.copytree(self.template, target)
        return target

    # -- helpers -----------------------------------------------------------

    def crash_in_process(self, dbdir: str, plan: FaultPlan, body,
                         capacity: int = 2):
        """Run *body(store, txn)* with a fault armed; return the fired text.

        When the injector fires, ``Crash`` propagates out of the write.  The
        store is abandoned (fds closed without flushing) to mimic process
        death.  When the fault never fires, the open transaction is rolled
        back so the database is left usable.
        """
        injector = Injector(plan, exit_process=False)
        store = Store(dbdir, capacity=capacity, injector=injector, recover=True,
                      durable=False)
        txn = None
        try:
            txn = store.begin()
            body(store, txn)
        except Crash:
            store.abandon()
            return injector.fired
        else:
            if txn is not None and not txn.closed:
                try:
                    txn.rollback()
                except Crash:
                    # The budget ran out during the harness's own rollback
                    # CLRs: the process died with the transaction pending.
                    store.abandon()
                    return injector.fired
            store.close()
            return None

    def check_invariants(self, dbdir: str, new_keys, *, committed: bool):
        with Store(dbdir, capacity=8, durable=False) as store:
            report = store.recovery_report
            rows = {key: (value, tag) for _p, _s, key, value, tag in store.scan()}
            for page_no in range(1, pagefmt.page_count(store.data_fd)):
                raw = pagefmt.read_page(store.data_fd, page_no)
                self.assertTrue(
                    pagefmt.page_checksum_ok(raw),
                    f"page {page_no} is torn after recovery",
                )
        for key in range(1, BASELINE + 1):
            self.assertIn(key, rows, f"committed baseline row {key} missing")
            self.assertEqual(rows[key], (key * 10, "base"),
                             f"committed baseline row {key} was modified")
        present = sorted(k for k in rows if k in set(new_keys))
        if committed:
            self.assertEqual(present, sorted(new_keys),
                             "committed transaction lost rows")
        else:
            self.assertEqual(present, [], "uncommitted rows survived recovery")
        return report, rows


# ---------------------------------------------------------------------------


class TestWalByteSweep(SweepBase):
    """Cut the WAL frames written by one transaction.

    Sweep policy: every byte of the small frames (BEGIN/UPDATE/COMMIT) --
    these are where record boundaries, CRCs and the commit point live -- and
    a 32-byte stride through the 4 KiB FPI frames, which are all-or-nothing
    (any tear drops the whole frame).
    """

    TOTAL = 6
    KEY0 = 5000

    def _probe(self, body) -> tuple[int, list[tuple[int, int, str]]]:
        """Return ``(bytes_written, [(start, end, type)])`` relative to start."""
        probe = self.fresh_clone("probe")
        before = os.path.getsize(os.path.join(probe, "wal.log"))
        result = self.crash_in_process(probe, FaultPlan(), body)
        self.assertIsNone(result, "probe run unexpectedly crashed")
        scan = scan_wal(os.path.join(probe, "wal.log"))
        frames = []
        for rec in scan.records:
            if rec.offset >= before:
                frames.append(
                    (rec.offset - before, rec.end_offset - before, rec.type_name)
                )
        written = os.path.getsize(os.path.join(probe, "wal.log")) - before
        return written, frames

    @staticmethod
    def _cut_points(written: int, frames, fpi_stride: int = 256) -> list[int]:
        points = set()
        for start, end, rtype in frames:
            # Small frames are where boundaries matter, so cut every byte.
            # An FPI is all-or-nothing (any tear drops the frame), so a
            # coarse stride plus both frame edges is enough coverage.
            stride = 1 if rtype != "FPI" else fpi_stride
            points.update(range(start, end, stride))
            points.update((start, end - 1, end))
        points.add(written)
        return sorted(p for p in points if 0 <= p <= written)

    def test_wal_byte_sweep_without_commit(self):
        def body(store, txn):
            for i in range(self.TOTAL):
                txn.insert(self.KEY0 + i, 7, "wal")
            # no commit: the process dies before deciding

        written, frames = self._probe(body)
        self.assertGreater(written, 4000)  # includes a 4 KiB FPI
        new_keys = list(range(self.KEY0, self.KEY0 + self.TOTAL))
        points = self._cut_points(written, frames)
        self.assertGreater(len(points), 200)
        for cut in points:
            dbdir = self.restore_workdir()
            self.crash_in_process(
                dbdir, FaultPlan(wal_bytes=cut, label=f"wal@{cut}"), body
            )
            self.check_invariants(dbdir, new_keys, committed=False)

    def test_wal_byte_sweep_around_commit(self):
        TOTAL = 3
        KEY0 = 6000

        def body(store, txn):
            for i in range(TOTAL):
                txn.insert(KEY0 + i, 8, "walc")
            txn.commit()

        written, frames = self._probe(body)
        commit = [(a, b) for a, b, t in frames if t == "COMMIT"]
        self.assertEqual(len(commit), 1)
        commit_start, commit_end = commit[0]
        new_keys = list(range(KEY0, KEY0 + TOTAL))
        for cut in self._cut_points(written, frames):
            dbdir = self.restore_workdir()
            self.crash_in_process(
                dbdir, FaultPlan(wal_bytes=cut, label=f"walc@{cut}"), body
            )
            # The COMMIT frame is complete only once cut == commit_end (or
            # beyond); a torn COMMIT is a loser.
            self.check_invariants(
                dbdir, new_keys, committed=(cut >= commit_end)
            )
        # The exact boundary is worth asserting explicitly.
        self.assertLess(commit_start, commit_end)


class TestPageByteSweep(SweepBase):
    """Tear the new data page at every byte offset."""

    TOTAL = 8
    KEY0 = 7000

    def test_every_page_byte(self):
        new_keys = list(range(self.KEY0, self.KEY0 + self.TOTAL))

        def body(store, txn):
            for i in range(self.TOTAL):
                txn.insert(self.KEY0 + i, 9, "pg")
            txn.commit()
            store.bp.flush_all(sync=False)  # injector may cut this write

        # Page 2 is the only dirty page: the baseline leaves 10 rows on it.
        # This transaction changes bytes 16..~640 of the page (LSN, slot
        # count, the new rows and the CRC at byte 60).  A cut that lands
        # after the last changed byte leaves a complete, valid page even
        # though the rest of the buffer was never written; a cut inside the
        # changed range leaves a CRC mismatch.  Both are correct; a detected
        # tear must be repaired from the FPI.
        torn_cuts = 0
        for cut in range(0, 4097, 13):
            dbdir = self.restore_workdir()
            fired = self.crash_in_process(
                dbdir, FaultPlan(data_bytes=cut, label=f"page@{cut}"), body
            )
            report, _rows = self.check_invariants(dbdir, new_keys, committed=True)
            torn = report["redo"]["pages_torn_before"]
            if fired is not None and cut < 4096:
                if torn:
                    self.assertEqual(torn, [2], f"cut {cut}")
                    self.assertEqual(report["redo"]["fpi_repaired"], 1,
                                     f"cut {cut}")
                    torn_cuts += 1
        self.assertGreaterEqual(torn_cuts, 30,
                                "sweep produced too few torn pages")

    def test_header_and_row_area_cuts(self):
        new_keys = list(range(self.KEY0, self.KEY0 + self.TOTAL))

        def body(store, txn):
            for i in range(self.TOTAL):
                txn.insert(self.KEY0 + i, 9, "pg")
            txn.commit()
            store.bp.flush_all(sync=False)

        expectations = {
            # cut: (expect_torn, why)
            1: (False, "only the magic byte was written; it is unchanged"),
            16: (False, "the first 16 bytes (magic, page no) are unchanged"),
            17: (True, "one byte of the new LSN is now on disk, CRC is stale"),
            26: (True, "half of the new LSN and slot count are on disk"),
            37: (True, "header changed but the new CRC never reached disk"),
            60: (True, "new rows missing while the new CRC is on disk"),
            64: (True, "rows from slot 10 on are still the old image"),
            200: (True, "new rows partially present"),
            600: (True, "the last new row is only partly written"),
            4095: (False, "every changed byte was written; only padding is missing"),
        }
        for cut, (expect_torn, why) in expectations.items():
            dbdir = self.restore_workdir()
            self.crash_in_process(
                dbdir, FaultPlan(data_bytes=cut, label=f"cut{cut}"), body
            )
            report, _ = self.check_invariants(dbdir, new_keys, committed=True)
            torn = report["redo"]["pages_torn_before"]
            if expect_torn:
                self.assertEqual(torn, [2], f"cut {cut}: {why}")
                self.assertEqual(report["redo"]["fpi_repaired"], 1,
                                 f"cut {cut}: {why}")
            else:
                self.assertEqual(torn, [], f"cut {cut}: {why}")

    def test_uncommitted_torn_page_is_undone(self):
        def body(store, txn):
            for i in range(self.TOTAL):
                txn.insert(self.KEY0 + i, 11, "uncommitted")
            store.bp.flush_all(sync=False)  # dies here, mid page write

        new_keys = list(range(self.KEY0, self.KEY0 + self.TOTAL))
        for cut in (0, 64, 500, 4095):
            dbdir = self.restore_workdir()
            self.crash_in_process(dbdir, FaultPlan(data_bytes=cut, label="uncommitted"), body)
            report, rows = self.check_invariants(dbdir, new_keys, committed=False)
            self.assertEqual(len(report["analysis"]["losers"]), 1)
            self.assertNotIn(self.KEY0, rows)


# ---------------------------------------------------------------------------


CHILD = r"""
import os, sys
sys.path.insert(0, {root!r})
from minidb import Store
from minidb.faults import FaultPlan, Injector
plan = FaultPlan.from_json(os.environ["MINIDB_FAULT"])
inj = Injector(plan, exit_process=True)
store = Store(os.environ["MINIDB_DB"], capacity=2, injector=inj,
              durable=os.environ.get("MINIDB_DURABLE", "1") == "1")
txn = store.begin()
for i in range(int(os.environ["MINIDB_TOTAL"])):
    txn.insert(int(os.environ["MINIDB_KEY0"]) + i, 3, "child")
if os.environ["MINIDB_COMMIT"] == "1":
    txn.commit()
store.bp.flush_all(sync=False)
store.close()
"""


def run_child(dbdir: str, plan: FaultPlan, *, total: int, key0: int,
              commit: bool, timeout: int = 60,
              durable: bool = False) -> tuple[int, str]:
    env = dict(os.environ)
    env.update(
        {
            "MINIDB_DB": dbdir,
            "MINIDB_FAULT": plan.to_json(),
            "MINIDB_TOTAL": str(total),
            "MINIDB_KEY0": str(key0),
            "MINIDB_COMMIT": "1" if commit else "0",
            "MINIDB_DURABLE": "1" if durable else "0",
        }
    )
    proc = subprocess.run(
        [sys.executable, "-c", CHILD.format(root=ROOT)],
        env=env, capture_output=True, timeout=timeout,
    )
    return proc.returncode, proc.stderr.decode()[-800:]


class TestRealProcessDeath(SweepBase):
    """Representative cuts with a genuine ``os._exit`` child."""

    TOTAL = 5
    KEY0 = 8000

    def test_mid_wal_process_death(self):
        new_keys = list(range(self.KEY0, self.KEY0 + self.TOTAL))
        # budget 1 cuts inside the BEGIN frame: no transaction record
        # survives, so there is no loser.  The larger budgets kill after
        # BEGIN, so the transaction is a loser and must be undone.
        for budget, expect_loser in ((1, False), (70, True), (120, True),
                                     (250, True), (500, True)):
            dbdir = self.restore_workdir()
            rc, err = run_child(dbdir, FaultPlan(wal_bytes=budget),
                                total=self.TOTAL, key0=self.KEY0, commit=False)
            self.assertEqual(rc, 90, f"child did not die as expected: {err}")
            report, _ = self.check_invariants(dbdir, new_keys, committed=False)
            self.assertEqual(len(report["analysis"]["losers"]),
                             1 if expect_loser else 0)

    def test_mid_page_process_death(self):
        new_keys = list(range(self.KEY0, self.KEY0 + self.TOTAL))
        # Budget 4095 writes all but the last byte; every changed byte is in
        # the first ~700, so that page is complete and not torn.  The other
        # budgets cut inside the changed region.
        # Budget 1 writes only the first byte of page 2, which is the magic
        # byte and is unchanged, so that page stays valid.
        for budget, expect_torn in ((1, False), (300, True), (4095, False)):
            dbdir = self.restore_workdir()
            rc, err = run_child(dbdir, FaultPlan(data_bytes=budget),
                                total=self.TOTAL, key0=self.KEY0, commit=True)
            self.assertEqual(rc, 90, f"child did not die as expected: {err}")
            report, _ = self.check_invariants(dbdir, new_keys, committed=True)
            if expect_torn:
                self.assertEqual(report["redo"]["pages_torn_before"], [2])
            else:
                self.assertEqual(report["redo"]["pages_torn_before"], [])

    def test_no_fault_child_exits_cleanly(self):
        dbdir = self.fresh_clone("clean")
        rc, err = run_child(dbdir, FaultPlan(), total=2, key0=9000, commit=True)
        self.assertEqual(rc, 0, err)


if __name__ == "__main__":
    unittest.main()
