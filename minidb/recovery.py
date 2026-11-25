"""ARIES-style restart recovery: analysis, redo, undo.

Analysis
    Read the WAL, group records by transaction, classify each as COMMITTED
    (has a COMMIT record), ROLLED_BACK (has an ABORT record -- rollback logs
    ABORT only after all CLRs are durable), or a LOSER (anything else: the
    process died mid-transaction or mid-commit).

Redo
    Replay FPI, UPDATE and CLR records in LSN order, applying each only to
    pages whose stored page-LSN is older.  Redo is idempotent, so a crash
    during redo can simply be followed by another recovery run.  FPIs are
    full page images logged on a transaction's first touch of a page; they
    are what heal a page that was torn by a crash mid-write.

Undo
    Walk each loser backwards along its ``prev_lsn`` chain, restoring before
    images and logging a CLR per step.  Undo is restartable: CLRs written by
    an earlier attempt are followed (their ``undo_next`` says where to
    continue) instead of being repeated.

Committed transactions survive because their COMMIT record is durable and
redo reapplies their rows.  Losers are erased by undo.  Uncommitted rows
that reached disk via buffer-pool eviction are restored to their
before-images.
"""

from __future__ import annotations

import json
import os
import shutil
import time

from . import pages as pagefmt
from .format import (
    PAGE_FLAG_REBUILT,
    RECORD_SIZE,
    RECOVERY_DIR,
    RT_ABORT,
    RT_CLR,
    RT_FPI,
    RT_UPDATE,
    TXN_COMMITTED,
    TXN_LOSER,
    TXN_ROLLED_BACK,
    ZERO_ROW,
    decode_clr_data,
    decode_update_data,
    txn_status,
)
from .faults import write_with_fault
from .wal import scan_wal


class RecoveryError(RuntimeError):
    pass


# ---------------------------------------------------------------------------
# snapshot of the raw files, for the before/after viewer
# ---------------------------------------------------------------------------


def snapshot_files(dbdir: str) -> str | None:
    rdir = os.path.join(dbdir, RECOVERY_DIR)
    os.makedirs(rdir, exist_ok=True)
    stamp = time.strftime("%Y%m%d-%H%M%S", time.localtime())
    target = os.path.join(rdir, f"before-{stamp}-{time.time_ns() % 1_000_000:06d}")
    os.makedirs(target, exist_ok=True)
    for name in ("data.db", "wal.log"):
        src = os.path.join(dbdir, name)
        if os.path.exists(src):
            shutil.copy2(src, os.path.join(target, name))
    return target


# ---------------------------------------------------------------------------
# page images
# ---------------------------------------------------------------------------


def load_page_images(dbdir: str) -> tuple[dict[int, bytearray], dict[int, bool]]:
    """Return ``(images, valid)`` for every page in the data file.

    ``valid[p]`` records whether the on-disk image passed its CRC32 at load
    time.  It is deliberately *not* recomputed after in-memory edits: the
    CRC only becomes meaningful again when the page is resealed for write-
    back.  Treating it as live would reject every page after its first redo.
    """
    path = os.path.join(dbdir, "data.db")
    images: dict[int, bytearray] = {}
    valid: dict[int, bool] = {}
    fd = os.open(path, os.O_RDONLY)
    try:
        size = os.fstat(fd).st_size
        count = size // pagefmt.PAGE_SIZE + (1 if size % pagefmt.PAGE_SIZE else 0)
        for page_no in range(count):
            raw = pagefmt.read_page_lenient(fd, page_no)
            if raw is None:
                continue
            images[page_no] = bytearray(raw)
            valid[page_no] = (
                page_no != pagefmt.DB_HEADER_PAGE and pagefmt.page_checksum_ok(raw)
            )
    finally:
        os.close(fd)
    return images, valid


# ---------------------------------------------------------------------------
# recovery
# ---------------------------------------------------------------------------


def recover(store, *, was_clean: bool = False) -> dict:
    """Recover an open :class:`minidb.engine.Store` in place.

    ``was_clean`` says the previous process closed the store normally, so the
    data file matches the log.  Recovery still runs -- redo is idempotent and
    cheap, and it is the simplest way to guarantee consistency -- but no
    snapshot is taken because nothing can change.
    """
    dbdir = store.dbdir
    before_snapshot = None if was_clean else snapshot_files(dbdir)

    # Describe the log as the crashed process left it.  ``Wal.__init__``
    # already truncated a torn tail, so a fresh scan here would report a
    # clean boundary and hide the evidence from the report and the viewer.
    # The opening scan lists every complete record, which is all analysis
    # needs; only its ``valid_end``/``tail`` describe the pre-truncation file.
    scan = getattr(store.wal, "opening_scan", None) or scan_wal(store.wal.path)
    txns = scan.by_txn()
    statuses = {txn_id: txn_status(recs) for txn_id, recs in txns.items()}

    committed = sorted(t for t, s in statuses.items() if s == TXN_COMMITTED)
    rolled_back = sorted(t for t, s in statuses.items() if s == TXN_ROLLED_BACK)
    losers = sorted(t for t, s in statuses.items() if s == TXN_LOSER)

    analysis = {
        "wal_file_size": scan.file_size,
        "wal_valid_end": scan.valid_end,
        "torn_tail_bytes": scan.tail_bytes,
        "torn_tail_hex": scan.tail_hex,
        "tail_errors": list(scan.errors),
        "records": len(scan.records),
        "txn_status": {str(k): v for k, v in sorted(statuses.items())},
        "committed": committed,
        "rolled_back": rolled_back,
        "losers": losers,
    }

    # Drop a torn log tail before anything is appended to the log.
    if scan.torn:
        store.wal.reopen_after_truncate()
        scan = scan_wal(store.wal.path)

    images, valid = load_page_images(dbdir)
    changed: set[int] = set()
    max_page = max(images, default=0)

    def page_for(page_no: int) -> bytearray:
        """Return a trusted page image, or raise if it cannot be trusted."""
        nonlocal max_page
        page = images.get(page_no)
        if page is None:
            page = bytearray(pagefmt.new_page_image(page_no, store.rec_format))
            images[page_no] = page
            valid[page_no] = True
            max_page = max(max_page, page_no)
        if not valid[page_no]:
            raise RecoveryError(
                f"page {page_no} failed its CRC32 and no full page image in the "
                f"log precedes it; the data file cannot be repaired"
            )
        return page

    # -- redo ------------------------------------------------------------
    redo_applied = 0
    redo_skipped = 0
    fpi_repaired = 0
    torn_before = sorted(p for p, ok in valid.items() if not ok and p != 0)

    for rec in scan.records:
        if rec.rtype == RT_FPI:
            if len(rec.data) != pagefmt.PAGE_SIZE:
                continue
            page = images.get(rec.page_no)
            if page is None:
                images[rec.page_no] = bytearray(rec.data)
                valid[rec.page_no] = True
                max_page = max(max_page, rec.page_no)
                changed.add(rec.page_no)
                redo_applied += 1
                continue
            was_valid = valid[rec.page_no]
            page_lsn = pagefmt.get_page_lsn(page) if was_valid else 0
            if not was_valid or page_lsn < rec.lsn:
                images[rec.page_no] = bytearray(rec.data)
                valid[rec.page_no] = True
                if not was_valid:
                    fpi_repaired += 1
                changed.add(rec.page_no)
                redo_applied += 1
            else:
                redo_skipped += 1
        elif rec.rtype == RT_UPDATE:
            page = page_for(rec.page_no)
            if pagefmt.get_page_lsn(page) >= rec.lsn:
                redo_skipped += 1
                continue
            _before, after = decode_update_data(rec.data)
            pagefmt.put_row(page, rec.slot, after)
            if after != ZERO_ROW:
                pagefmt.bump_slot_count(page, rec.slot)
            else:
                pagefmt.recompute_slot_count(page)
            pagefmt.set_page_lsn(page, rec.lsn)
            changed.add(rec.page_no)
            redo_applied += 1
        elif rec.rtype == RT_CLR:
            page = page_for(rec.page_no)
            if pagefmt.get_page_lsn(page) >= rec.lsn:
                redo_skipped += 1
                continue
            restored, _undo_next = decode_clr_data(rec.data)
            pagefmt.put_row(page, rec.slot, restored)
            if restored != ZERO_ROW:
                pagefmt.bump_slot_count(page, rec.slot)
            else:
                pagefmt.recompute_slot_count(page)
            pagefmt.set_page_lsn(page, rec.lsn)
            changed.add(rec.page_no)
            redo_applied += 1

    redo = {
        "pages_loaded": len(images),
        "pages_torn_before": torn_before,
        "applied": redo_applied,
        "skipped": redo_skipped,
        "fpi_repaired": fpi_repaired,
    }

    # -- undo ------------------------------------------------------------
    undo_actions: list[dict] = []
    clrs_written = 0
    for txn_id in losers:
        result = _undo_loser(store, images, valid, txn_id, txns[txn_id],
                             undo_actions, changed)
        clrs_written += result["clrs"]
        # ABORT after undo: a second crash during undo leaves the transaction
        # a loser again, and its CLRs make the next attempt resume correctly.
        store.wal.append(RT_ABORT, txn_id, store.wal.last_lsn)
        store.wal.flush()

    undo = {
        "losers": losers,
        "actions": undo_actions,
        "clrs_written": clrs_written,
    }

    # -- write repaired images and make them durable ---------------------
    # Only pages that actually changed are written.  On a clean reopen this
    # is nothing at all, which keeps opening cheap and keeps the crash
    # injector's byte budget focused on the workload rather than on recovery.
    if changed:
        fd = os.open(os.path.join(dbdir, "data.db"), os.O_RDWR)
        try:
            for page_no in sorted(changed):
                image = images[page_no]
                pagefmt.seal_page(image)
                # Route through the fault injector: a crash *during recovery*
                # must be survivable, and the next run must finish the job.
                write_with_fault(
                    fd, bytes(image), page_no * pagefmt.PAGE_SIZE,
                    store.injector, "data", page_no=page_no,
                    during_recovery=True,
                )
            if store.durable:
                pagefmt.fsync_fd(fd)
        finally:
            os.close(fd)

    store.bp.discard_all()
    store.wal.reopen_after_truncate()
    store.bp.next_page_no = max(pagefmt.page_count(store.data_fd), 1)
    mark_clean_after_recovery(dbdir, store)

    report = {
        "clean_shutdown": was_clean,
        "analysis": analysis,
        "redo": redo,
        "undo": undo,
        "before_snapshot": before_snapshot,
    }
    _write_report(dbdir, report)
    return report


def mark_clean_after_recovery(dbdir: str, store) -> None:
    from .engine import mark_clean

    mark_clean(dbdir, lsn=store.wal.last_lsn, note="recovered",
               durable=store.durable)


def _undo_loser(store, images, valid, txn_id: int, recs, actions: list[dict],
                changed: set[int]) -> dict:
    """Undo one loser from its latest record back to its BEGIN.

    If the loser already has CLRs (a previous recovery or rollback crashed
    part-way), those CLRs are followed and their effects are not repeated.
    """
    wal = store.wal
    clrs = 0
    updates = 0
    lsn = recs[-1].lsn
    while lsn:
        rec = wal.find(lsn)
        if rec is None or rec.txn_id != txn_id:
            break
        if rec.rtype == RT_UPDATE:
            before, _after = decode_update_data(rec.data)
            page = _loser_page(images, valid, rec.page_no, store.rec_format)
            pagefmt.put_row(page, rec.slot, before)
            pagefmt.recompute_slot_count(page)
            clr = wal.append(
                RT_CLR, txn_id, wal.last_lsn,
                data=_encode_clr(before, rec.prev_lsn),
                page_no=rec.page_no, slot=rec.slot,
            )
            pagefmt.set_page_lsn(page, clr.lsn)
            changed.add(rec.page_no)
            updates += 1
            clrs += 1
            actions.append(
                {
                    "txn_id": txn_id,
                    "undo_lsn": rec.lsn,
                    "page": rec.page_no,
                    "slot": rec.slot,
                    "restored": before.hex(),
                }
            )
            lsn = rec.prev_lsn
        elif rec.rtype == RT_CLR:
            restored, undo_next = decode_clr_data(rec.data)
            page = _loser_page(images, valid, rec.page_no, store.rec_format)
            pagefmt.put_row(page, rec.slot, restored)
            pagefmt.recompute_slot_count(page)
            pagefmt.set_page_lsn(page, rec.lsn)
            changed.add(rec.page_no)
            lsn = undo_next
        else:
            lsn = rec.prev_lsn
    wal.flush()
    return {"clrs": clrs, "updates": updates}


def _loser_page(images, valid, page_no, rec_format):
    page = images.get(page_no)
    if page is None:
        page = bytearray(pagefmt.new_page_image(page_no, rec_format))
        images[page_no] = page
        valid[page_no] = True
    if not valid.get(page_no, False):
        # After redo every page a loser touches should be trusted: redo would
        # have repaired it from an FPI or raised.  Guard anyway.
        raise RecoveryError(f"page {page_no} is unreadable during undo")
    return page


def _encode_clr(after: bytes, undo_next: int) -> bytes:
    from .format import encode_clr_data

    return encode_clr_data(after, undo_next)


def _write_report(dbdir: str, report: dict) -> None:
    latest = os.path.join(dbdir, RECOVERY_DIR, "latest.json")
    os.makedirs(os.path.dirname(latest), exist_ok=True)
    tmp = latest + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(report, fh, indent=2, sort_keys=True)
        fh.flush()
        os.fsync(fh.fileno())
    os.replace(tmp, latest)
