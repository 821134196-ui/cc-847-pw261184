"""Recovery-specific tests: idempotence, crash during recovery, rollback CLRs,
and the read-only viewer's output."""

from __future__ import annotations

import json
import os
import shutil
import unittest

import helpers

from minidb import Store, initialise
from minidb import pages as pagefmt
from minidb import viewer
from minidb.faults import Crash, FaultPlan, Injector
from minidb.format import SLOTS_PER_PAGE
from minidb.wal import scan_wal

BASELINE = 40


class RecoveryBase(helpers.TempDirMixin):
    def setUp(self):
        super().setUp()
        self.template = os.path.join(self.tmp, "template")
        initialise(self.template)
        store = Store(self.template, capacity=8, durable=False)
        txn = store.begin()
        for key in range(1, BASELINE + 1):
            txn.insert(key, key * 10, "base")
        txn.commit()
        store.close()

    def clone(self, name: str = "work") -> str:
        target = os.path.join(self.tmp, name)
        shutil.rmtree(target, ignore_errors=True)
        shutil.copytree(self.template, target)
        return target

    def crash_txn(self, dbdir: str, *, keys, commit: bool, plan: FaultPlan):
        injector = Injector(plan, exit_process=False)
        store = Store(dbdir, capacity=2, injector=injector, durable=False)
        try:
            txn = store.begin()
            for key in keys:
                txn.insert(key, 900, "crash")
            if commit:
                txn.commit()
            store.bp.flush_all(sync=False)
        except Crash:
            store.abandon()
        else:
            store.close()


class TestRecoveryIdempotence(RecoveryBase):
    def test_second_recovery_is_a_no_op(self):
        dbdir = self.clone()
        self.crash_txn(dbdir, keys=range(1000, 1010), commit=True,
                       plan=FaultPlan(wal_bytes=4000))
        with Store(dbdir, capacity=8, durable=False) as store:
            first = store.recovery_report
            rows_after_first = sorted(store.keys())
            wal_size_after_first = store.wal.size
        with Store(dbdir, capacity=8, durable=False) as store:
            second = store.recovery_report
            rows_after_second = sorted(store.keys())
            wal_size_after_second = store.wal.size
        self.assertEqual(rows_after_first, rows_after_second)
        self.assertEqual(wal_size_after_first, wal_size_after_second)
        self.assertEqual(second["redo"]["applied"], 0,
                         "second recovery redid work that was already done")
        self.assertEqual(second["undo"]["clrs_written"], 0)

    def test_every_page_is_valid_after_recovery(self):
        dbdir = self.clone()
        self.crash_txn(dbdir, keys=range(2000, 2010), commit=True,
                       plan=FaultPlan(data_bytes=300))
        with Store(dbdir, capacity=8, durable=False) as store:
            for page_no in range(1, pagefmt.page_count(store.data_fd)):
                raw = pagefmt.read_page(store.data_fd, page_no)
                self.assertTrue(
                    pagefmt.page_checksum_ok(raw),
                    f"page {page_no} invalid after recovery",
                )

    def test_multiple_losers_are_all_undone(self):
        dbdir = self.clone()
        # Simulate two concurrent open transactions in one process: the
        # engine only allows one at a time, so log the second transaction's
        # records by hand against the same WAL and leave both without COMMIT.
        store = Store(dbdir, capacity=2, durable=False)
        from minidb import format as fmt
        txn_id = 90
        store.wal.next_txn_id = txn_id
        txn = store.begin()
        for key in range(3000, 3005):
            txn.insert(key, 1, "loser-a")
        prev = txn.last_lsn
        other = store.wal.append(fmt.RT_BEGIN, txn_id + 1, 0)
        page = store.bp.fetch(1)
        before = pagefmt.row_at(page, 0)
        after = fmt.pack_row(3100, 2, "loser-b")
        store.wal.append(
            fmt.RT_UPDATE, txn_id + 1, other.lsn,
            data=fmt.encode_update_data(before, after), page_no=1, slot=0,
        )
        pagefmt.put_row(page, 0, after)
        store.abandon()
        with Store(dbdir, capacity=8, durable=False) as reopened:
            report = reopened.recovery_report
            keys = reopened.keys()
        losers = set(report["analysis"]["losers"])
        self.assertEqual(losers, {txn_id, txn_id + 1})
        self.assertEqual(keys, list(range(1, BASELINE + 1)))


class TestCrashDuringRecovery(RecoveryBase):
    def _recover_with_crash(self, dbdir: str, plan: FaultPlan) -> str | None:
        injector = Injector(plan, exit_process=False)
        try:
            store = Store(dbdir, capacity=8, injector=injector, durable=False)
        except Crash:
            return injector.fired
        else:
            store.close()
            return None

    def test_crash_during_undo_then_recover_again(self):
        """Crash while recovery is writing CLRs; a second run finishes the job."""
        dbdir = self.clone()
        self.crash_txn(dbdir, keys=range(4000, 4040), commit=False,
                       plan=FaultPlan(wal_bytes=600))

        # Recovery must append CLRs; cut it in the middle of those writes.
        # The WAL already ends at <some offset>; a small budget reaches the
        # first CLR append quickly.
        fired = self._recover_with_crash(dbdir, FaultPlan(wal_bytes=0))
        self.assertIsNotNone(fired)

        # Second recovery: the loser still has no ABORT, but the CLRs that
        # did land must be followed, not repeated.
        with Store(dbdir, capacity=8, durable=False) as store:
            report = store.recovery_report
            keys = store.keys()
        self.assertEqual(keys, list(range(1, BASELINE + 1)))
        self.assertGreaterEqual(len(report["analysis"]["losers"]), 1)

    def test_crash_during_redo_then_recover(self):
        """Crash during redo of a committed txn; a second run finishes redo."""
        dbdir = self.clone()
        # Tear page 2 mid-write while committing: recovery must repair it
        # from the FPI and then redo the commit's updates, writing pages back.
        self.crash_txn(dbdir, keys=range(5000, 5010), commit=True,
                       plan=FaultPlan(data_bytes=300))
        fired = self._recover_with_crash(dbdir, FaultPlan(data_bytes=0))
        self.assertIsNotNone(fired, "recovery was expected to write a page")
        # The second run finishes redo; the third has nothing left to do.
        with Store(dbdir, capacity=8, durable=False) as store:
            keys = store.keys()
        self.assertIn(5000, keys)
        self.assertIn(5009, keys)
        with Store(dbdir, capacity=8, durable=False) as store:
            self.assertEqual(store.recovery_report["redo"]["applied"], 0)
            self.assertEqual(sorted(store.keys()), keys)


class TestRollbackCLRs(RecoveryBase):
    def test_rollback_logs_clrs_then_abort(self):
        dbdir = self.clone()
        with Store(dbdir, capacity=8, durable=False) as store:
            txn = store.begin()
            for key in range(6000, 6010):
                txn.insert(key, 1, "rb")
            result = txn.rollback()
            self.assertGreater(result["undone"], 0)
            self.assertEqual(store.keys(), list(range(1, BASELINE + 1)))
        scan = scan_wal(os.path.join(dbdir, "wal.log"))
        types = [r.type_name for r in scan.records if r.txn_id == result["txn_id"]]
        self.assertEqual(types[-1], "ABORT")
        self.assertIn("CLR", types)
        # ABORT must be last: recovery treats it as "undo finished".
        self.assertEqual(types.count("ABORT"), 1)

    def test_rolled_back_txn_is_not_reudone(self):
        dbdir = self.clone()
        with Store(dbdir, capacity=8, durable=False) as store:
            txn = store.begin()
            for key in range(7000, 7010):
                txn.insert(key, 1, "rb")
            txn.rollback()
        with Store(dbdir, capacity=8, durable=False) as store:
            report = store.recovery_report
            self.assertEqual(len(report["analysis"]["rolled_back"]), 1)
            self.assertEqual(report["analysis"]["losers"], [])
            self.assertEqual(store.keys(), list(range(1, BASELINE + 1)))


class TestViewer(unittest.TestCase):
    def setUp(self):
        self.tmp = os.path.join(os.environ.get("TMPDIR", "/tmp"),
                                f"minidb-view-{os.getpid()}")
        shutil.rmtree(self.tmp, ignore_errors=True)
        self.dbdir = os.path.join(self.tmp, "db")
        initialise(self.dbdir)
        with Store(self.dbdir, capacity=8, durable=False) as store:
            txn = store.begin()
            for key in range(1, SLOTS_PER_PAGE + 3):
                txn.insert(key, key * 3, "view")
            txn.commit()

    def tearDown(self):
        shutil.rmtree(self.tmp, ignore_errors=True)

    def test_page_viewer_shows_checksums_and_rows(self):
        text = "\n".join(viewer.render_data_pages(self.dbdir))
        self.assertIn("checksum OK", text)
        self.assertIn("page 1", text)
        self.assertIn("key=1", text)
        crc = "\n".join(viewer.render_page_crc_table(self.dbdir))
        self.assertIn("YES", crc)

    def test_page_viewer_reports_torn_page(self):
        path = os.path.join(self.dbdir, "data.db")
        with open(path, "r+b") as fh:
            fh.seek(4096 + 100)
            fh.write(b"\xFF\xFF\xFF")
        text = "\n".join(viewer.render_data_pages(self.dbdir))
        self.assertIn("checksum BAD", text)
        self.assertIn("not decoded", text)
        crc = "\n".join(viewer.render_page_crc_table(self.dbdir))
        self.assertIn("NO", crc)

    def test_wal_viewer_shows_boundary_and_torn_tail(self):
        wal_path = os.path.join(self.dbdir, "wal.log")
        with open(wal_path, "ab") as fh:
            fh.write(b"\x10\x00\x00\x00garbage")
        text = "\n".join(viewer.render_wal(self.dbdir))
        self.assertIn("LOG BOUNDARY", text)
        self.assertIn("torn tail", text)
        self.assertIn("YES", text)

    def test_wal_viewer_lists_record_types(self):
        text = "\n".join(viewer.render_wal(self.dbdir))
        for rtype in ("BEGIN", "FPI", "UPDATE", "COMMIT"):
            self.assertIn(rtype, text)

    def test_recovery_report_view(self):
        with Store(self.dbdir, capacity=8, durable=False):
            pass
        text = "\n".join(viewer.render_recovery_report(self.dbdir))
        self.assertIn("LAST RECOVERY REPORT", text)
        self.assertIn("REDO", text)
        self.assertIn("UNDO", text)

    def test_before_after_snapshot_diff(self):
        # Produce a crash with a loser, then recover and compare snapshots.
        store = Store(self.dbdir, capacity=1, durable=False)
        txn = store.begin()
        for key in range(9000, 9005):
            txn.insert(key, 1, "loser")
        store.abandon()
        with Store(self.dbdir, capacity=8, durable=False):
            pass
        text = "\n".join(viewer.render_snapshot_compare(self.dbdir))
        self.assertIn("RECOVERY BEFORE / AFTER", text)
        self.assertIn("PAGE CHECKSUMS", text)

    def test_inspect_is_read_only(self):
        before = {
            name: os.path.getsize(os.path.join(self.dbdir, name))
            for name in ("data.db", "wal.log")
        }
        viewer.render_inspection(self.dbdir)
        after = {
            name: os.path.getsize(os.path.join(self.dbdir, name))
            for name in ("data.db", "wal.log")
        }
        self.assertEqual(before, after)


if __name__ == "__main__":
    unittest.main()
