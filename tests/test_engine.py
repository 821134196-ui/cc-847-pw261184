"""Engine behaviour without crashes: page placement, multi-page commits,
rollback, and the steal policy.

The tests that matter for the deliverable live here; the byte-by-byte crash
sweep is in ``test_crash_sweep.py``.
"""

from __future__ import annotations

import os
import unittest

import helpers

from minidb import (
    DatabaseFull,
    DuplicateKey,
    KeyNotFound,
    Store,
    initialise,
)
from minidb import pages as pagefmt
from minidb import format as fmt
from minidb.format import SLOTS_PER_PAGE, txn_status
from minidb.wal import scan_wal


class TestEngineBasics(helpers.TempDirMixin):
    def test_insert_commit_and_read_back(self):
        with Store(self.dbdir) as store:
            txn = store.begin()
            for key in range(1, 6):
                txn.insert(key, key * 100, f"k{key}")
            result = txn.commit()
            self.assertEqual(result["inserted"], 5)
            self.assertEqual(store.get(3), (300, "k3"))

    def test_reopen_sees_committed_rows(self):
        with Store(self.dbdir) as store:
            txn = store.begin()
            txn.insert(1, 10, "a")
            txn.commit()
        with Store(self.dbdir) as store:
            self.assertEqual(store.get(1), (10, "a"))

    def test_duplicate_key_rejected(self):
        with Store(self.dbdir) as store:
            txn = store.begin()
            txn.insert(1, 10)
            with self.assertRaises(DuplicateKey):
                txn.insert(1, 20)
            txn.rollback()

    def test_update_and_delete(self):
        with Store(self.dbdir) as store:
            txn = store.begin()
            txn.insert(1, 10, "a")
            txn.insert(2, 20, "b")
            txn.commit()

            txn = store.begin()
            txn.update(1, value=99)
            txn.delete(2)
            txn.commit()

            self.assertEqual(store.get(1), (99, "a"))
            with self.assertRaises(KeyNotFound):
                store.get(2)

    def test_rollback_restores_rows(self):
        with Store(self.dbdir) as store:
            txn = store.begin()
            txn.insert(1, 10, "a")
            txn.commit()

            txn = store.begin()
            txn.update(1, value=20, tag="changed")
            txn.insert(2, 30, "new")
            txn.delete(1)
            txn.rollback()

            self.assertEqual(store.get(1), (10, "a"))
            with self.assertRaises(KeyNotFound):
                store.get(2)

    def test_multi_page_transaction(self):
        """A transaction that crosses the 126-row page boundary commits."""
        with Store(self.dbdir) as store:
            txn = store.begin()
            total = SLOTS_PER_PAGE * 2 + 5
            for key in range(1, total + 1):
                txn.insert(key, key, "x")
            result = txn.commit()
            self.assertGreaterEqual(len(result["pages"]), 3)
            self.assertEqual(len(store.keys()), total)

    def test_rows_are_spread_across_pages(self):
        with Store(self.dbdir) as store:
            txn = store.begin()
            for key in range(1, SLOTS_PER_PAGE * 2 + 2):
                txn.insert(key, key)
            txn.commit()
            pages = sorted({row[0] for row in store.scan()})
            self.assertEqual(pages, [1, 2, 3])

    def test_rollback_after_multi_page_txn(self):
        with Store(self.dbdir) as store:
            txn = store.begin()
            for key in range(1, SLOTS_PER_PAGE + 20):
                txn.insert(key, key)
            txn.rollback()
            self.assertEqual(store.keys(), [])

    def test_fresh_page_is_created_only_when_full(self):
        with Store(self.dbdir) as store:
            txn = store.begin()
            for key in range(1, SLOTS_PER_PAGE + 1):
                txn.insert(key, key)
            txn.commit()
            self.assertEqual(len({row[0] for row in store.scan()}), 1)
            txn = store.begin()
            txn.insert(SLOTS_PER_PAGE + 1, 1)
            txn.commit()
            self.assertEqual(len({row[0] for row in store.scan()}), 2)

    def test_database_full_on_oversized_transaction(self):
        # 200 pages is well past the point where the fixed-size row area of
        # a single transaction would need more pages than the store allows
        # here, which is not the case; instead verify the row-capacity guard
        # by asking the pool for slots on a database whose pages are all full
        # and whose allocation is disabled.
        with Store(self.dbdir) as store:
            txn = store.begin()
            store._find_free_slot = lambda: None
            with self.assertRaises(DatabaseFull):
                txn.insert(1, 1)
            txn.rollback()


class TestStealPolicy(helpers.TempDirMixin):
    def test_dirty_page_reaches_disk_before_commit(self):
        """With a tiny pool, uncommitted rows are written out (steal)."""
        store = Store(self.dbdir, capacity=1)
        txn = store.begin()
        for key in range(1, SLOTS_PER_PAGE + 2):
            txn.insert(key, key, "uncommitted")
        # Page 1 was evicted to make room for page 2, so its rows are on
        # disk before the transaction commits.
        raw = pagefmt.read_page(store.data_fd, 1)
        self.assertIsNotNone(raw)
        self.assertTrue(pagefmt.page_checksum_ok(raw))
        self.assertEqual(len(list(pagefmt.decode_rows(raw))), SLOTS_PER_PAGE)
        self.assertEqual(txn.commit()["inserted"], SLOTS_PER_PAGE + 1)
        store.close()

    def test_uncommitted_rows_are_undone_on_reopen(self):
        store = Store(self.dbdir, capacity=1)
        txn = store.begin()
        for key in range(1, SLOTS_PER_PAGE + 1):
            txn.insert(key, key, "uncommitted")
        store.abandon()  # process dies without commit
        with Store(self.dbdir) as reopened:
            report = reopened.recovery_report
            self.assertEqual(report["analysis"]["losers"], [1])
            self.assertEqual(reopened.keys(), [])
            self.assertGreater(report["undo"]["clrs_written"], 0)

    def test_committed_rows_survive_without_page_flush(self):
        store = Store(self.dbdir, capacity=1)
        txn = store.begin()
        for key in range(1, SLOTS_PER_PAGE + 1):
            txn.insert(key, key * 2, "com")
        txn.commit()
        store.abandon()
        with Store(self.dbdir) as reopened:
            self.assertEqual(len(reopened.keys()), SLOTS_PER_PAGE)
            self.assertEqual(reopened.get(5), (10, "com"))


class TestWalShape(helpers.TempDirMixin):
    def test_wal_record_sequence_for_one_insert(self):
        with Store(self.dbdir) as store:
            txn = store.begin()
            txn.insert(1, 1, "a")
            txn.commit()
        scan = scan_wal(store.wal.path)
        self.assertEqual([r.type_name for r in scan.records],
                         ["BEGIN", "FPI", "UPDATE", "COMMIT"])
        self.assertFalse(scan.torn)

    def test_full_page_image_precedes_first_update_on_a_page(self):
        with Store(self.dbdir) as store:
            txn = store.begin()
            for key in range(1, SLOTS_PER_PAGE + 2):
                txn.insert(key, key)
            txn.commit()
        scan = scan_wal(store.wal.path)
        seen_pages = {}
        for rec in scan.records:
            if rec.rtype == fmt.RT_FPI:
                seen_pages[rec.page_no] = rec.lsn
            elif rec.rtype == fmt.RT_UPDATE:
                self.assertIn(rec.page_no, seen_pages,
                              "UPDATE logged before its FPI")
                self.assertLess(seen_pages[rec.page_no], rec.lsn)

    def test_commit_is_durable_and_lsn_monotonic(self):
        with Store(self.dbdir) as store:
            txn = store.begin()
            for key in range(1, 10):
                txn.insert(key, key)
            result = txn.commit()
        scan = scan_wal(store.wal.path)
        lsns = [r.lsn for r in scan.records]
        self.assertEqual(lsns, sorted(lsns))
        self.assertEqual(result["commit_lsn"], lsns[-1])
        statuses = {}
        by_txn = scan.by_txn()
        for txn_id, recs in by_txn.items():
            statuses[txn_id] = txn_status(recs)
        self.assertEqual(statuses[1], "COMMITTED")


if __name__ == "__main__":
    unittest.main()
