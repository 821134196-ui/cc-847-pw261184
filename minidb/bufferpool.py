"""Buffer pool: a small LRU cache of 4096-byte pages over the split data file.

Policy: *steal / no-force*.

* Steal -- a dirty page may be evicted before its transaction commits, so an
  uncommitted transaction's rows can be on disk.  Rollback and crash
  recovery therefore need before-images, which the WAL keeps.
* No-force -- commit does not write pages.  Only the log is forced at
  commit, which is why redo must be able to rebuild pages from the log.

Page writes go through :func:`minidb.faults.write_with_fault`, so the crash
injector can cut a page in half.  Before any page with a page-LSN reaches
disk, the WAL is flushed through that LSN (the write-ahead rule).
"""

from __future__ import annotations

from collections import OrderedDict

from . import pages as pagefmt
from .faults import write_with_fault
from .format import PAGE_SIZE, PAGE_FLAG_REBUILT, RECORD_SIZE, SLOTS_PER_PAGE


class PageError(RuntimeError):
    """A page could not be read back as valid (torn CRC32, short file)."""


class BufferPool:
    def __init__(self, fd: int, capacity: int, wal, injector=None,
                 rec_format: bytes = b"demo32"):
        if capacity < 1:
            raise ValueError("buffer pool capacity must be at least 1")
        self.fd = fd
        self.capacity = capacity
        self.wal = wal
        self.injector = injector
        self.rec_format = rec_format
        self._frames: OrderedDict[int, bytearray] = OrderedDict()
        self._dirty: set[int] = set()
        self.next_page_no = max(pagefmt.page_count(fd), 1)
        self.page_faults = 0
        self.evictions = 0
        self.bytes_written = 0

    # -- fetch / allocate --------------------------------------------------

    def _read(self, page_no: int) -> bytearray:
        raw = pagefmt.read_page(self.fd, page_no)
        if raw is None:
            raise PageError(
                f"page {page_no} is past the end of the data file "
                f"(file has {pagefmt.page_count(self.fd)} complete pages)"
            )
        if not pagefmt.page_checksum_ok(raw):
            raise PageError(
                f"page {page_no} failed its CRC32 check; the page is torn or corrupt"
            )
        return bytearray(raw)

    def fetch(self, page_no: int) -> bytearray:
        cached = self._frames.get(page_no)
        if cached is not None:
            self._frames.move_to_end(page_no)
            return cached
        image = self._read(page_no)
        self.page_faults += 1
        self._make_room()
        self._frames[page_no] = image
        return image

    def try_fetch(self, page_no: int) -> bytearray | None:
        """Like :meth:`fetch`, but returns ``None`` for an invalid page."""
        try:
            return self.fetch(page_no)
        except PageError:
            return None

    def new_page(self) -> tuple[int, bytearray]:
        page_no = self.next_page_no
        self.next_page_no += 1
        self._make_room()
        image = pagefmt.new_page_image(page_no, self.rec_format)
        self._frames[page_no] = image
        self._dirty.add(page_no)
        return page_no, image

    def allocate_if_needed(self, page_no: int) -> bytearray:
        """Return a page, creating it on disk-in-memory if it is new."""
        if page_no in self._frames:
            self._frames.move_to_end(page_no)
            return self._frames[page_no]
        if page_no >= self.next_page_no:
            raise PageError(
                f"page {page_no} was never allocated (next page is {self.next_page_no})"
            )
        return self.fetch(page_no)

    def _make_room(self) -> None:
        while len(self._frames) >= self.capacity:
            victim = next(iter(self._frames))
            if victim in self._dirty:
                self.flush_page(victim)
            self._frames.pop(victim, None)
            self.evictions += 1

    # -- mutation ----------------------------------------------------------

    def mark_dirty(self, page_no: int, lsn: int) -> None:
        if page_no not in self._frames:
            raise PageError(f"cannot dirty page {page_no}: it is not in the pool")
        self._dirty.add(page_no)
        pagefmt.set_page_lsn(self._frames[page_no], lsn)

    def mark_rebuilt(self, page_no: int) -> None:
        image = self._frames[page_no]
        pagefmt.set_page_flags(image, pagefmt.get_page_flags(image) | PAGE_FLAG_REBUILT)
        self._dirty.add(page_no)

    # -- writeback ---------------------------------------------------------

    def flush_page(self, page_no: int) -> None:
        if page_no not in self._dirty:
            return
        image = self._frames[page_no]
        page_lsn = pagefmt.get_page_lsn(image)
        if page_lsn:
            self.wal.ensure_flushed(page_lsn)
        pagefmt.seal_page(image)
        write_with_fault(
            self.fd, bytes(image), page_no * PAGE_SIZE, self.injector, "data",
            page_no=page_no, page_lsn=page_lsn,
        )
        self.bytes_written += PAGE_SIZE
        self._dirty.discard(page_no)

    def flush_all(self, *, sync: bool = True) -> list[int]:
        flushed = sorted(self._dirty)
        for page_no in flushed:
            self.flush_page(page_no)
        if sync and self.wal.durable:
            pagefmt.fsync_fd(self.fd)
        return flushed

    def discard_all(self) -> None:
        """Throw the cache away without writing anything (recovery reloads)."""
        self._frames.clear()
        self._dirty.clear()
        self.next_page_no = max(pagefmt.page_count(self.fd), 1)

    def invalidate(self, page_no: int) -> None:
        self._frames.pop(page_no, None)
        self._dirty.discard(page_no)

    def install(self, page_no: int, image: bytes | bytearray, *, dirty: bool) -> None:
        """Put a page image into the pool directly (recovery redo/undo)."""
        self._make_room()
        copy = bytearray(image)
        self._frames[page_no] = copy
        self._frames.move_to_end(page_no)
        if dirty:
            self._dirty.add(page_no)
        if page_no >= self.next_page_no:
            self.next_page_no = page_no + 1

    # -- introspection -----------------------------------------------------

    def cached_image(self, page_no: int) -> bytes | None:
        """A checksum-valid copy of *page_no*, if it is resident.

        In-memory pages carry stale header CRCs between modifications; they
        are only resealed on writeback.  Callers that validate pages (scans,
        the viewer) need a sealed copy, hence the copy.
        """
        image = self._frames.get(page_no)
        if image is None:
            return None
        snapshot = bytearray(image)
        pagefmt.seal_page(snapshot)
        return bytes(snapshot)

    @property
    def dirty_pages(self) -> list[int]:
        return sorted(self._dirty)

    @property
    def cached_pages(self) -> list[int]:
        return list(self._frames)
