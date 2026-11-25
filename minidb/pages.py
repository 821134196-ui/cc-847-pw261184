"""Split-file page encoding and raw page I/O.

``data.db`` is an array of independent 4096-byte pages addressed by page
number (``offset == page_no * PAGE_SIZE``).  Page 0 is the database header;
pages 1.. are row pages.  This module only formats pages and reads/writes
whole pages; durability policy lives in :mod:`minidb.bufferpool`.

A data page's CRC32 is stored at byte 60 and covers bytes ``[0:60]`` plus
``[64:4096]``.  The 4-byte gap holds the CRC itself.  The database header's
CRC32 sits at byte 4092 and covers ``[0:4092]``.
"""

from __future__ import annotations

import os
import struct

from .format import (
    DB_HDR_CRC_OFF,
    DB_HDR_CREATED_OFF,
    DB_HDR_FLAGS_OFF,
    DB_HDR_MAGIC_OFF,
    DB_HDR_PAGE_SIZE_OFF,
    DB_HDR_REC_FORMAT_OFF,
    DB_HDR_REC_SIZE_OFF,
    DB_HDR_UUID_OFF,
    DB_HDR_VERSION_OFF,
    DB_MAGIC,
    DEFAULT_REC_FORMAT,
    FILE_UUID_SIZE,
    PAGE_HEADER_STRUCT,
    PAGE_HDR_CRC_OFF,
    PAGE_HDR_FLAGS_OFF,
    PAGE_HDR_LSN_OFF,
    PAGE_HDR_RESERVED2_OFF,
    PAGE_HDR_RESERVED_OFF,
    PAGE_HDR_SLOT_COUNT_OFF,
    PAGE_MAGIC,
    PAGE_ROWS_OFF,
    PAGE_SIZE,
    REC_FORMAT_SIZE,
    RECORD_SIZE,
    SLOTS_PER_PAGE,
    VERSION,
    ZERO_ROW,
    db_header_crc,
    page_crc,
    row_key,
    unpack_row,
)

DB_HEADER_PAGE = 0
FIRST_DATA_PAGE = 1


# --- data pages -------------------------------------------------------------


class PageHeader:
    __slots__ = (
        "magic",
        "version",
        "page_no",
        "lsn",
        "slot_count",
        "flags",
        "rec_size",
        "rec_format",
        "stored_crc",
        "computed_crc",
    )

    def __init__(self, **kw):
        for name in self.__slots__:
            setattr(self, name, kw.get(name))

    @property
    def checksum_ok(self) -> bool:
        return self.magic == PAGE_MAGIC and self.stored_crc == self.computed_crc

    @property
    def row_format(self) -> str:
        return (
            self.rec_format.rstrip(b"\x00").decode("ascii", "replace")
            if self.rec_format
            else ""
        )

    def as_dict(self) -> dict:
        d = {
            "magic": self.magic.decode("latin-1") if self.magic else "",
            "version": self.version,
            "page_no": self.page_no,
            "page_lsn": self.lsn,
            "row_count": self.slot_count,
            "flags": self.flags,
            "row_size": self.rec_size,
            "row_format": self.row_format,
            "stored_crc": self.stored_crc,
            "computed_crc": self.computed_crc,
            "checksum_ok": self.checksum_ok,
        }
        return d


def decode_page_header(page: bytes) -> PageHeader:
    if page is None or len(page) < PAGE_HEADER_STRUCT.size:
        return PageHeader(
            magic=b"", version=0, page_no=0, lsn=0, slot_count=0, flags=0,
            rec_size=0, rec_format=b"", stored_crc=0, computed_crc=0,
        )
    (magic, version, page_no, lsn, slot_count, flags, rec_size, _reserved,
     rec_format, _reserved2, stored_crc) = PAGE_HEADER_STRUCT.unpack_from(page, 0)
    return PageHeader(
        magic=magic,
        version=version,
        page_no=page_no,
        lsn=lsn,
        slot_count=slot_count,
        flags=flags,
        rec_size=rec_size,
        rec_format=rec_format,
        stored_crc=stored_crc,
        computed_crc=page_crc(page),
    )


def page_checksum_ok(page: bytes) -> bool:
    if page is None or len(page) != PAGE_SIZE:
        return False
    return decode_page_header(page).checksum_ok


def encode_page(
    page_no: int,
    lsn: int,
    rec_format: bytes,
    rows,
    flags: int = 0,
) -> bytes:
    """Serialize a page from a slot list (``bytes | None`` per slot)."""
    if len(rows) > SLOTS_PER_PAGE:
        raise ValueError(f"page {page_no}: {len(rows)} rows > {SLOTS_PER_PAGE}")
    page = bytearray(PAGE_SIZE)
    high = 0
    for slot, row in enumerate(rows):
        if row is None or row == ZERO_ROW:
            continue
        off = PAGE_ROWS_OFF + slot * RECORD_SIZE
        page[off : off + RECORD_SIZE] = row
        high = slot + 1
    PAGE_HEADER_STRUCT.pack_into(
        page, 0, PAGE_MAGIC, VERSION, page_no, lsn, high, flags,
        RECORD_SIZE, 0, rec_format.ljust(REC_FORMAT_SIZE, b"\x00"),
        b"\x00" * 12, 0,
    )
    struct.pack_into("<I", page, PAGE_HDR_CRC_OFF, page_crc(page))
    return bytes(page)


def decode_rows(page: bytes):
    """Yield ``(slot, row)`` for each populated slot of a page image."""
    if page is None or len(page) < PAGE_SIZE:
        return
    count = min(decode_page_header(page).slot_count, SLOTS_PER_PAGE)
    for slot in range(count):
        off = PAGE_ROWS_OFF + slot * RECORD_SIZE
        row = bytes(page[off : off + RECORD_SIZE])
        if row != ZERO_ROW:
            yield slot, row


def page_rows(page: bytes) -> list[bytes | None]:
    rows: list[bytes | None] = [None] * SLOTS_PER_PAGE
    for slot, row in decode_rows(page):
        rows[slot] = row
    return rows


def row_at(page: bytes, slot: int) -> bytes:
    if not 0 <= slot < SLOTS_PER_PAGE:
        raise ValueError(f"slot {slot} out of range")
    off = PAGE_ROWS_OFF + slot * RECORD_SIZE
    return bytes(page[off : off + RECORD_SIZE])


def put_row(page: bytearray, slot: int, row: bytes) -> None:
    if len(row) != RECORD_SIZE:
        raise ValueError("row image must be exactly 32 bytes")
    if not 0 <= slot < SLOTS_PER_PAGE:
        raise ValueError(f"slot {slot} out of range")
    off = PAGE_ROWS_OFF + slot * RECORD_SIZE
    page[off : off + RECORD_SIZE] = row


def bump_slot_count(page: bytearray, slot: int) -> None:
    current = struct.unpack_from("<H", page, PAGE_HDR_SLOT_COUNT_OFF)[0]
    if slot + 1 > current:
        struct.pack_into("<H", page, PAGE_HDR_SLOT_COUNT_OFF, slot + 1)


def recompute_slot_count(page: bytearray) -> None:
    high = 0
    for slot in range(SLOTS_PER_PAGE):
        off = PAGE_ROWS_OFF + slot * RECORD_SIZE
        if bytes(page[off : off + RECORD_SIZE]) != ZERO_ROW:
            high = slot + 1
    struct.pack_into("<H", page, PAGE_HDR_SLOT_COUNT_OFF, high)


def get_page_lsn(page: bytes) -> int:
    return struct.unpack_from("<Q", page, PAGE_HDR_LSN_OFF)[0]


def set_page_lsn(page: bytearray, lsn: int) -> None:
    struct.pack_into("<Q", page, PAGE_HDR_LSN_OFF, lsn)


def get_page_flags(page: bytes) -> int:
    return struct.unpack_from("<H", page, PAGE_HDR_FLAGS_OFF)[0]


def set_page_flags(page: bytearray, flags: int) -> None:
    struct.pack_into("<H", page, PAGE_HDR_FLAGS_OFF, flags)


def seal_page(page: bytearray) -> None:
    struct.pack_into("<I", page, PAGE_HDR_CRC_OFF, page_crc(page))


def new_page_image(page_no: int, rec_format: bytes) -> bytearray:
    return bytearray(encode_page(page_no, 0, rec_format, []))


# --- database header --------------------------------------------------------


class DatabaseFileHeader:
    __slots__ = ("magic", "version", "page_size", "rec_size", "flags", "uuid",
                 "rec_format", "created", "stored_crc", "computed_crc")

    def __init__(self, **kw):
        for name in self.__slots__:
            setattr(self, name, kw.get(name))

    @property
    def checksum_ok(self) -> bool:
        return self.magic == DB_MAGIC and self.stored_crc == self.computed_crc

    @property
    def row_format(self) -> str:
        return self.rec_format.rstrip(b"\x00").decode("ascii", "replace")

    def as_dict(self) -> dict:
        return {
            "magic": self.magic.decode("latin-1"),
            "version": self.version,
            "page_size": self.page_size,
            "rec_size": self.rec_size,
            "flags": self.flags,
            "uuid": self.uuid.hex(),
            "row_format": self.row_format,
            "created": self.created,
            "stored_crc": self.stored_crc,
            "computed_crc": self.computed_crc,
            "checksum_ok": self.checksum_ok,
        }


def encode_db_header(rec_format: bytes, file_uuid: bytes, created: int) -> bytes:
    if len(file_uuid) != FILE_UUID_SIZE:
        raise ValueError("file uuid must be 16 bytes")
    page = bytearray(PAGE_SIZE)
    page[DB_HDR_MAGIC_OFF : DB_HDR_MAGIC_OFF + 8] = DB_MAGIC
    struct.pack_into("<I", page, DB_HDR_VERSION_OFF, VERSION)
    struct.pack_into("<I", page, DB_HDR_PAGE_SIZE_OFF, PAGE_SIZE)
    struct.pack_into("<I", page, DB_HDR_REC_SIZE_OFF, RECORD_SIZE)
    struct.pack_into("<I", page, DB_HDR_FLAGS_OFF, 0)
    page[DB_HDR_UUID_OFF : DB_HDR_UUID_OFF + FILE_UUID_SIZE] = file_uuid
    page[DB_HDR_REC_FORMAT_OFF : DB_HDR_REC_FORMAT_OFF + REC_FORMAT_SIZE] = (
        rec_format.ljust(REC_FORMAT_SIZE, b"\x00")
    )
    struct.pack_into("<Q", page, DB_HDR_CREATED_OFF, created)
    struct.pack_into("<I", page, DB_HDR_CRC_OFF, db_header_crc(page))
    return bytes(page)


def decode_db_header(page: bytes) -> DatabaseFileHeader:
    if page is None or len(page) < PAGE_SIZE:
        raise ValueError("data file is missing its header page")
    (magic,) = struct.unpack_from("<8s", page, DB_HDR_MAGIC_OFF)
    version, page_size, rec_size, flags = struct.unpack_from(
        "<IIII", page, DB_HDR_VERSION_OFF
    )
    return DatabaseFileHeader(
        magic=magic,
        version=version,
        page_size=page_size,
        rec_size=rec_size,
        flags=flags,
        uuid=bytes(page[DB_HDR_UUID_OFF : DB_HDR_UUID_OFF + FILE_UUID_SIZE]),
        rec_format=bytes(
            page[DB_HDR_REC_FORMAT_OFF : DB_HDR_REC_FORMAT_OFF + REC_FORMAT_SIZE]
        ),
        created=struct.unpack_from("<Q", page, DB_HDR_CREATED_OFF)[0],
        stored_crc=struct.unpack_from("<I", page, DB_HDR_CRC_OFF)[0],
        computed_crc=db_header_crc(page),
    )


def validate_db_header(page: bytes) -> DatabaseFileHeader:
    hdr = decode_db_header(page)
    if hdr.magic != DB_MAGIC:
        raise ValueError("not a minidb data file (bad magic)")
    if hdr.version != VERSION:
        raise ValueError(f"unsupported data file version {hdr.version}")
    if hdr.page_size != PAGE_SIZE or hdr.rec_size != RECORD_SIZE:
        raise ValueError("data file page/record size mismatch")
    if not hdr.checksum_ok:
        raise ValueError("data file header checksum mismatch")
    return hdr


def default_rec_format() -> bytes:
    return DEFAULT_REC_FORMAT


# --- raw split-file I/O -----------------------------------------------------


def _pread_all(fd: int, length: int, offset: int) -> bytes:
    chunks = []
    while length > 0:
        chunk = os.pread(fd, length, offset)
        if not chunk:
            break
        chunks.append(chunk)
        length -= len(chunk)
        offset += len(chunk)
    return b"".join(chunks)


def read_page(fd: int, page_no: int) -> bytes | None:
    data = _pread_all(fd, PAGE_SIZE, page_no * PAGE_SIZE)
    return data if len(data) == PAGE_SIZE else None


def read_page_lenient(fd: int, page_no: int) -> bytes | None:
    """Read a page, returning a zero-padded image for a short (torn) tail."""
    data = _pread_all(fd, PAGE_SIZE, page_no * PAGE_SIZE)
    if not data:
        return None
    if len(data) < PAGE_SIZE:
        return data + b"\x00" * (PAGE_SIZE - len(data))
    return data


def write_page(fd: int, page_no: int, page: bytes) -> None:
    if len(page) != PAGE_SIZE:
        raise ValueError(f"page {page_no}: expected {PAGE_SIZE} bytes, got {len(page)}")
    pwrite_all(fd, page, page_no * PAGE_SIZE)


def pwrite_all(fd: int, data: bytes, offset: int) -> None:
    view = memoryview(data)
    while view:
        written = os.pwrite(fd, view, offset)
        if written <= 0:
            raise OSError("short write")
        offset += written
        view = view[written:]


def page_count(fd: int) -> int:
    """Number of *complete* pages in the data file."""
    return os.fstat(fd).st_size // PAGE_SIZE


def file_size(fd: int) -> int:
    return os.fstat(fd).st_size


def fsync_fd(fd: int) -> None:
    os.fsync(fd)


def fsync_dir(path: str) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


__all__ = [
    "DB_HEADER_PAGE",
    "FIRST_DATA_PAGE",
    "DatabaseFileHeader",
    "PageHeader",
    "bump_slot_count",
    "decode_db_header",
    "decode_page_header",
    "decode_rows",
    "default_rec_format",
    "encode_db_header",
    "encode_page",
    "file_size",
    "fsync_dir",
    "fsync_fd",
    "get_page_flags",
    "get_page_lsn",
    "new_page_image",
    "page_checksum_ok",
    "page_count",
    "page_crc",
    "page_rows",
    "pwrite_all",
    "put_row",
    "read_page",
    "read_page_lenient",
    "recompute_slot_count",
    "row_at",
    "seal_page",
    "set_page_flags",
    "set_page_lsn",
    "validate_db_header",
    "write_page",
]
