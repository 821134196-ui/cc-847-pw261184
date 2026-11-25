"""Engine: tiny transactions over a split-page store with an append WAL.

A row is 32 bytes (key, value, tag) and 126 rows fit on a 4096-byte page.
A transaction may touch many pages; all changes are logged before they are
applied, and the WAL is forced at commit.

Public surface::

    initialise(dbdir)                 create an empty database
    Store(dbdir, capacity=8)          open + crash-recover
    txn = store.begin()               ... insert/update/delete/read ...
    txn.commit() / txn.rollback()

Placement policy is deliberately dumb: rows fill the first free slot in page
order, and a fresh page is appended when every page is full.  Searching is a
linear scan.  This is a teaching store, not a fast one.
"""

from __future__ import annotations

import json
import os
import time
import uuid as uuidlib

from . import pages as pagefmt
from .bufferpool import BufferPool, PageError
from .faults import Injector, write_with_fault
from .format import (
    CLEAN_FILE,
    DATA_FILE,
    DEFAULT_REC_FORMAT,
    FILE_UUID_SIZE,
    LOCK_FILE,
    PAGE_FLAG_REBUILT,
    PAGE_SIZE,
    RECOVERY_DIR,
    RECORD_SIZE,
    RT_BEGIN,
    RT_CLR,
    RT_COMMIT,
    RT_FPI,
    RT_UPDATE,
    RT_ABORT,
    SLOTS_PER_PAGE,
    UPDATE_FLAG_DELETE,
    UPDATE_FLAG_INSERT,
    WAL_FILE,
    ZERO_ROW,
    encode_clr_data,
    encode_update_data,
    pack_row,
    row_key,
    unpack_row,
)
from .format import txn_status
from .wal import RTYPE_NAMES, LogRecord, Wal, WalCorrupt, scan_wal


class EngineError(RuntimeError):
    pass


class KeyNotFound(EngineError):
    pass


class DuplicateKey(EngineError):
    pass


class DatabaseFull(EngineError):
    pass


class TransactionClosed(EngineError):
    pass


# ---------------------------------------------------------------------------
# Paths / creation
# ---------------------------------------------------------------------------


def data_path(dbdir: str) -> str:
    return os.path.join(dbdir, DATA_FILE)


def wal_path(dbdir: str) -> str:
    return os.path.join(dbdir, WAL_FILE)


def recovery_dir(dbdir: str) -> str:
    return os.path.join(dbdir, RECOVERY_DIR)


def clean_marker(dbdir: str) -> str:
    return os.path.join(dbdir, CLEAN_FILE)


def initialise(dbdir: str, *, rec_format: bytes = DEFAULT_REC_FORMAT) -> None:
    """Create ``data.db`` (+ empty WAL, lock file) in *dbdir*."""
    if os.path.exists(data_path(dbdir)):
        raise EngineError(f"{dbdir} already contains a data.db")
    os.makedirs(dbdir, exist_ok=True)
    os.makedirs(recovery_dir(dbdir), exist_ok=True)
    file_uuid = uuidlib.uuid4().bytes

    db_file = data_path(dbdir)
    tmp = db_file + ".tmp"
    fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
    try:
        os.write(fd, pagefmt.encode_db_header(rec_format, file_uuid, int(time.time())))
        os.fsync(fd)
    finally:
        os.close(fd)
    os.replace(tmp, db_file)

    Wal(wal_path(dbdir), rec_format, create=True).close()
    pagefmt.fsync_dir(dbdir)

    with open(os.path.join(dbdir, LOCK_FILE), "w", encoding="utf-8") as fh:
        fh.write(f"minidb uuid={file_uuid.hex()}\ncreated={int(time.time())}\n")
    mark_clean(dbdir, lsn=0, note="initialised")


def mark_clean(dbdir: str, *, lsn: int, note: str = "",
               durable: bool = True) -> None:
    payload = {"clean": True, "lsn": lsn, "at": time.time(), "note": note}
    path = clean_marker(dbdir)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as fh:
        json.dump(payload, fh)
        fh.flush()
        if durable:
            os.fsync(fh.fileno())
    os.replace(tmp, path)
    if durable:
        pagefmt.fsync_dir(dbdir)


def clear_clean(dbdir: str) -> None:
    try:
        os.unlink(clean_marker(dbdir))
    except FileNotFoundError:
        pass


def read_clean(dbdir: str) -> dict | None:
    try:
        with open(clean_marker(dbdir), "r", encoding="utf-8") as fh:
            return json.load(fh)
    except (FileNotFoundError, json.JSONDecodeError):
        return None


# ---------------------------------------------------------------------------
# Transactions
# ---------------------------------------------------------------------------


class Transaction:
    """One in-flight transaction.

    Changes are applied to buffer-pool pages as they are made (steal), and
    each change appends an UPDATE record with before and after row images.
    """

    def __init__(self, store: "Store", txn_id: int):
        self.store = store
        self.wal: Wal = store.wal
        self.bp: BufferPool = store.bp
        self.txn_id = txn_id
        self.last_lsn = 0
        self.records = 0
        self.pages_touched: list[int] = []
        self.fpi_pages: set[int] = set()
        self.inserted = 0
        self.updated = 0
        self.deleted = 0
        self.closed = False
        begin = self._append(RT_BEGIN)
        self.begin_lsn = begin.lsn
        # Force BEGIN immediately.  Without it, a crash right after the first
        # UPDATE could reach the data file before the BEGIN did, leaving a
        # page change with no transaction record in the log.
        self.wal.flush()

    # -- logging -----------------------------------------------------------

    def _append(self, rtype: int, *, data: bytes = b"", page_no: int = 0,
                slot: int = 0, flags: int = 0) -> LogRecord:
        rec = self.wal.append(
            rtype, self.txn_id, self.last_lsn,
            data=data, page_no=page_no, slot=slot, flags=flags,
        )
        self.last_lsn = rec.lsn
        self.records += 1
        return rec

    def _ensure_fpi(self, page_no: int) -> None:
        """Log a full page image the first time this txn touches a page."""
        if page_no in self.fpi_pages:
            return
        page = self.bp.fetch(page_no)
        image = bytearray(page)
        pagefmt.seal_page(image)  # a clean image, checksum valid
        self._append(RT_FPI, data=bytes(image), page_no=page_no)
        self.fpi_pages.add(page_no)
        self._note_page(page_no)

    def _note_page(self, page_no: int) -> None:
        if page_no not in self.pages_touched:
            self.pages_touched.append(page_no)

    def _apply(self, page_no: int, slot: int, before: bytes, after: bytes,
               flags: int = 0) -> None:
        self._ensure_fpi(page_no)
        rec = self._append(
            RT_UPDATE, data=encode_update_data(before, after),
            page_no=page_no, slot=slot, flags=flags,
        )
        page = self.bp.fetch(page_no)
        pagefmt.put_row(page, slot, after)
        if after != ZERO_ROW:
            pagefmt.bump_slot_count(page, slot)
        else:
            pagefmt.recompute_slot_count(page)
        self.bp.mark_dirty(page_no, rec.lsn)
        self._note_page(page_no)

    # -- operations --------------------------------------------------------

    def _check_open(self) -> None:
        if self.closed:
            raise TransactionClosed("transaction has already finished")

    def _locate(self, key: int) -> tuple[int, int] | None:
        for page_no in self.store._data_page_numbers():
            page = self.bp.try_fetch(page_no)
            if page is None:
                continue
            for slot, row in pagefmt.decode_rows(page):
                if row_key(row) == key:
                    return page_no, slot
        return None

    def insert(self, key: int, value: int, tag: str = "") -> None:
        self._check_open()
        if key <= 0:
            raise EngineError("keys must be positive integers")
        if self._locate(key) is not None:
            raise DuplicateKey(f"key {key} already exists")
        target = self.store._find_free_slot()
        if target is None:
            raise DatabaseFull("store is out of row slots")
        page_no, slot = target
        self._apply(page_no, slot, ZERO_ROW, pack_row(key, value, tag),
                    UPDATE_FLAG_INSERT)
        self.inserted += 1

    def update(self, key: int, value: int | None = None,
               tag: str | None = None) -> None:
        self._check_open()
        loc = self._locate(key)
        if loc is None:
            raise KeyNotFound(f"key {key} not found")
        page_no, slot = loc
        before = pagefmt.row_at(self.bp.fetch(page_no), slot)
        _, old_value, old_tag = unpack_row(before)
        after = pack_row(
            key,
            value if value is not None else old_value,
            tag if tag is not None else old_tag,
        )
        self._apply(page_no, slot, before, after)
        self.updated += 1

    def delete(self, key: int) -> None:
        self._check_open()
        loc = self._locate(key)
        if loc is None:
            raise KeyNotFound(f"key {key} not found")
        page_no, slot = loc
        before = pagefmt.row_at(self.bp.fetch(page_no), slot)
        self._apply(page_no, slot, before, ZERO_ROW, UPDATE_FLAG_DELETE)
        self.deleted += 1

    def read(self, key: int) -> tuple[int, str]:
        self._check_open()
        loc = self._locate(key)
        if loc is None:
            raise KeyNotFound(f"key {key} not found")
        page = self.bp.fetch(loc[0])
        _, value, tag = unpack_row(pagefmt.row_at(page, loc[1]))
        return value, tag

    # -- finish ------------------------------------------------------------

    def commit(self) -> dict:
        self._check_open()
        rec = self._append(RT_COMMIT)
        # Durability point: the COMMIT record must be on stable storage
        # before the caller hears "committed".  Pages may still be dirty.
        self.wal.flush()
        self.closed = True
        self.store._current = None
        return {
            "txn_id": self.txn_id,
            "begin_lsn": self.begin_lsn,
            "commit_lsn": rec.lsn,
            "inserted": self.inserted,
            "updated": self.updated,
            "deleted": self.deleted,
            "pages": list(self.pages_touched),
            "records": self.records,
        }

    def rollback(self) -> dict:
        self._check_open()
        undone = self.store.undo_loser(self.txn_id, self.last_lsn)
        # ABORT goes last: recovery treats its presence as "undo finished",
        # so it must not be visible until every CLR is durable.
        self.wal.flush()
        abort = self._append(RT_ABORT)
        self.wal.flush()
        self.closed = True
        self.store._current = None
        return {
            "txn_id": self.txn_id,
            "undone": undone,
            "abort_lsn": abort.lsn,
            "pages": list(self.pages_touched),
        }


# ---------------------------------------------------------------------------
# Store
# ---------------------------------------------------------------------------


class Store:
    def __init__(self, dbdir: str, *, capacity: int = 8,
                 injector: Injector | None = None, recover: bool = True,
                 durable: bool = True):
        self.dbdir = dbdir
        self.injector = injector
        self._lock_fd = None
        self._current: Transaction | None = None
        self._txn_seq = 0
        self.recovery_report = None
        self._acquire_lock()
        try:
            self.data_fd = os.open(data_path(dbdir), os.O_RDWR)
            header_page = pagefmt.read_page(self.data_fd, pagefmt.DB_HEADER_PAGE)
            self.header = pagefmt.validate_db_header(header_page)
            self.rec_format = self.header.rec_format
            self.durable = durable
            self.wal = Wal(
                wal_path(dbdir), self.rec_format, injector=injector,
                durable=durable,
            )
            self.bp = BufferPool(
                self.data_fd, capacity, self.wal, injector,
                rec_format=self.rec_format,
            )
            # A clean marker from a previous run means the data file is
            # already consistent with the log.  The marker is removed as soon
            # as this process starts mutating, and restored on clean close.
            marker = read_clean(dbdir)
            was_clean = (
                marker is not None
                and marker.get("lsn") == self.wal.last_lsn
                and self.wal.truncated_tail_bytes == 0
            )
            clear_clean(dbdir)
            if recover:
                from .recovery import recover

                self.recovery_report = recover(self, was_clean=was_clean)
            else:
                self.bp.next_page_no = max(pagefmt.page_count(self.data_fd), 1)
        except BaseException:
            self._release_lock()
            raise

    # -- lock --------------------------------------------------------------

    def _acquire_lock(self) -> None:
        path = os.path.join(self.dbdir, LOCK_FILE)
        fd = os.open(path, os.O_RDWR | os.O_CREAT, 0o644)
        try:
            import fcntl

            fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except OSError:
            os.close(fd)
            raise EngineError(f"{self.dbdir} is locked by another process")
        except ImportError:  # pragma: no cover - Windows
            pass
        self._lock_fd = fd

    def _release_lock(self) -> None:
        if self._lock_fd is not None:
            try:
                import fcntl

                fcntl.flock(self._lock_fd, fcntl.LOCK_UN)
            except ImportError:  # pragma: no cover - Windows
                pass
            finally:
                os.close(self._lock_fd)
                self._lock_fd = None

    # -- lifecycle ---------------------------------------------------------

    def close(self) -> None:
        if self._current is not None:
            raise EngineError("cannot close with an open transaction")
        # Clean shutdown: force all dirty pages, then record that the data
        # file is consistent with the log.  After this the next open can
        # still replay (redo is idempotent) but will find no losers.
        self.bp.flush_all(sync=True)
        self.wal.flush()
        lsn = self.wal.last_lsn
        mark_clean(self.dbdir, lsn=lsn, note="clean shutdown",
                   durable=self.durable)
        self.wal.close()
        os.close(self.data_fd)
        self._release_lock()

    def abandon(self) -> None:
        """Close descriptors *without* flushing: simulates process death.

        Only used by in-process crash tests.  Nothing dirty is written and no
        clean marker is produced.
        """
        try:
            self._release_lock()
        finally:
            if getattr(self, "wal", None) is not None:
                try:
                    os.close(self.wal.fd)
                except OSError:
                    pass
            if getattr(self, "data_fd", None) is not None:
                try:
                    os.close(self.data_fd)
                except OSError:
                    pass

    def __enter__(self) -> "Store":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        if self._current is not None:
            try:
                self._current.rollback()
            except Exception:
                pass
        self.close()

    # -- transactions ------------------------------------------------------

    def begin(self) -> Transaction:
        if self._current is not None:
            raise EngineError("a transaction is already open in this process")
        # Continue the WAL's txn numbering so ids are unique for the file.
        txn_id = self.wal.next_txn_id
        self.wal.next_txn_id += 1
        txn = Transaction(self, txn_id)
        self._current = txn
        return txn

    def transaction(self):
        store = self
        txn = store.begin()

        class _Ctx:
            def __enter__(self_inner):
                return txn

            def __exit__(self_inner, exc_type, exc, tb):
                if exc_type is None:
                    txn.commit()
                elif not txn.closed:
                    txn.rollback()
                return False

        return _Ctx()

    # -- page helpers ------------------------------------------------------

    def _data_page_numbers(self):
        # Include pages allocated by this process that are still only dirty
        # in the buffer pool and have not been written to the file yet.
        return range(1, max(self.bp.next_page_no, 1))

    def _find_free_slot(self) -> tuple[int, int] | None:
        for page_no in self._data_page_numbers():
            page = self.bp.try_fetch(page_no)
            if page is None:
                continue
            rows = pagefmt.page_rows(page)
            for slot in range(SLOTS_PER_PAGE):
                if rows[slot] is None:
                    return page_no, slot
        page_no, _image = self.bp.new_page()
        # A page created in memory lives only in the pool until it is dirty
        # (the first insert below) and later flushed.  The pool knows its
        # number, but scans and the WAL's buffer-pool reads must see it too.
        return page_no, 0

    # -- undo --------------------------------------------------------------

    def undo_loser(self, txn_id: int, undo_from_lsn: int,
                   *, until_before: int = 0) -> int:
        """Undo a transaction's UPDATEs back to (not including) *until_before*.

        CLRs are appended for each undo so a crash halfway through undo is
        resumable.  Returns the number of UPDATE records undone.
        """
        undone = 0
        lsn = undo_from_lsn
        txn_last = self._txn_last_lsn(txn_id)
        while lsn and lsn != until_before:
            rec = self.wal.find(lsn)
            if rec is None:
                raise EngineError(f"undo chain broken: LSN {lsn} not in WAL")
            if rec.txn_id != txn_id:
                raise EngineError(
                    f"undo chain crossed into txn {rec.txn_id} at LSN {lsn}"
                )
            if rec.rtype == RT_UPDATE:
                before, _after = _decode_update(rec.data)
                page = self.bp.try_fetch(rec.page_no)
                if page is None:
                    page = bytearray(
                        pagefmt.read_page_lenient(self.data_fd, rec.page_no)
                        or pagefmt.new_page_image(rec.page_no, self.rec_format)
                    )
                pagefmt.put_row(page, rec.slot, before)
                pagefmt.recompute_slot_count(page)
                clr = self.wal.append(
                    RT_CLR, txn_id, txn_last,
                    data=encode_clr_data(before, rec.prev_lsn),
                    page_no=rec.page_no, slot=rec.slot,
                )
                self.bp.install(rec.page_no, page, dirty=True)
                self.bp.mark_dirty(rec.page_no, clr.lsn)
                txn_last = clr.lsn
                undone += 1
                lsn = rec.prev_lsn
            elif rec.rtype == RT_CLR:
                lsn = _decode_clr_undo_next(rec.data)
            elif rec.rtype == RT_BEGIN:
                break
            else:
                lsn = rec.prev_lsn
        return undone

    def _txn_last_lsn(self, txn_id: int) -> int:
        for rec in reversed(self.wal.records):
            if rec.txn_id == txn_id:
                return rec.lsn
        return 0

    # -- reads -------------------------------------------------------------

    def scan(self) -> list[tuple[int, int, int, int, str]]:
        """``(page, slot, key, value, tag)`` for every live row.

        Pages still cached in the buffer pool are read from memory so rows
        the current process has written but not yet flushed are visible.
        Every other page is read from the data file.
        """
        out = []
        for page_no in self._data_page_numbers():
            cached = self.bp.cached_image(page_no)
            raw = cached if cached is not None else pagefmt.read_page(self.data_fd, page_no)
            if raw is None:
                continue
            if not pagefmt.page_checksum_ok(raw):
                continue
            for slot, row in pagefmt.decode_rows(raw):
                key, value, tag = unpack_row(row)
                out.append((page_no, slot, key, value, tag))
        return out

    def get(self, key: int) -> tuple[int, str]:
        for _page, _slot, k, value, tag in self.scan():
            if k == key:
                return value, tag
        raise KeyNotFound(f"key {key} not found")

    def keys(self) -> list[int]:
        return sorted(row[2] for row in self.scan())

    def rows(self) -> list[dict]:
        return [
            {"page": page, "slot": slot, "key": key, "value": value, "tag": tag}
            for page, slot, key, value, tag in sorted(self.scan(), key=lambda r: r[2])
        ]


def _decode_update(data: bytes):
    from .format import decode_update_data

    return decode_update_data(data)


def _decode_clr_undo_next(data: bytes) -> int:
    from .format import decode_clr_data

    return decode_clr_data(data)[1]
