"""On-disk format for the tiny page store.

Layout at a glance
------------------

``data.db`` is a split array of 4096-byte pages::

    page 0         database header (magic, uuid, row format, created)
    page 1..N      fixed-size rows, 126 slots per page, 32 bytes per row

Every page carries a CRC32 at byte 60.  A data page's CRC covers the first
60 header bytes plus all 4032 row bytes, so a torn (half-written) page fails
validation and is never trusted as data.  The database header's CRC sits at
byte 4092 and covers bytes ``[0:4092]``.

Row layout (32 bytes)::

    u32 key      positive; 0 marks an empty slot
    i32 value
    char tag[8]  ASCII, NUL padded
    u8  pad[12]

``wal.log`` is a single append-only WAL::

    header  64 B   magic, version, sizes, created, row format, CRC32
    frame   u32 length | u32 CRC32 | 32-byte record header | payload

Frames have their own CRC, so a half-written frame at the tail is detected
and dropped.  Record types: BEGIN, UPDATE (before + after row image), FPI
(full page image, logged the first time a transaction touches a page), CLR
(undo after-image + undo-next LSN), COMMIT, ABORT.

LSNs are 64-bit wall-clock nanoseconds forced to be strictly increasing, so
they remain comparable across process restarts.

Everything is little-endian.
"""

from __future__ import annotations

import struct
import time
import zlib

# --- global -----------------------------------------------------------------

VERSION = 1
PAGE_SIZE = 4096
PAGE_HEADER_SIZE = 64
PAGE_ROWS_OFF = PAGE_HEADER_SIZE
RECORD_SIZE = 32
SLOTS_PER_PAGE = (PAGE_SIZE - PAGE_HEADER_SIZE) // RECORD_SIZE  # 126
FILE_UUID_SIZE = 16
REC_FORMAT_SIZE = 16
DEFAULT_REC_FORMAT = b"demo32"

DB_MAGIC = b"MINIDB01"
PAGE_MAGIC = b"MINIPG01"
WAL_MAGIC = b"MINILOG1"

DATA_FILE = "data.db"
WAL_FILE = "wal.log"
LOCK_FILE = "LOCK"
CLEAN_FILE = "CLEAN"
RECOVERY_DIR = "recovery"

DB_HDR_CRC_OFF = 4092
PAGE_HDR_CRC_OFF = 60
WAL_HDR_CRC_OFF = 60


def crc32(data: bytes) -> int:
    return zlib.crc32(data) & 0xFFFFFFFF


# --- fixed-size row ---------------------------------------------------------

ROW_STRUCT = struct.Struct("<Ii8s16s")  # key, value, tag, padding
assert ROW_STRUCT.size == RECORD_SIZE

ZERO_ROW = b"\x00" * RECORD_SIZE


def pack_row(key: int, value: int, tag: str = "") -> bytes:
    if key <= 0:
        raise ValueError("keys must be positive (key 0 marks an empty slot)")
    raw_tag = tag.encode("ascii", "replace")[:8].ljust(8, b"\x00")
    return ROW_STRUCT.pack(key, int(value), raw_tag, b"\x00" * 16)


def unpack_row(row: bytes):
    key, value, raw_tag, _pad = ROW_STRUCT.unpack(row)
    return key, value, raw_tag.rstrip(b"\x00").decode("ascii", "replace")


def format_row(row: bytes | None) -> str:
    if row is None or row == ZERO_ROW:
        return "-"
    key, value, tag = unpack_row(row)
    return f"key={key} value={value} tag={tag!r}"


def row_key(row: bytes) -> int:
    return ROW_STRUCT.unpack_from(row, 0)[0]


# --- database header page (page 0) -----------------------------------------

DB_HDR_MAGIC_OFF = 0
DB_HDR_VERSION_OFF = 8
DB_HDR_PAGE_SIZE_OFF = 12
DB_HDR_REC_SIZE_OFF = 16
DB_HDR_FLAGS_OFF = 20
DB_HDR_UUID_OFF = 24
DB_HDR_REC_FORMAT_OFF = 40
DB_HDR_CREATED_OFF = 56


def db_header_crc(page: bytes) -> int:
    return crc32(bytes(page[:DB_HDR_CRC_OFF]))


# --- data page header (64 B) ------------------------------------------------

PAGE_HEADER_STRUCT = struct.Struct("<8sIIQHHHH16s12sI")
assert PAGE_HEADER_STRUCT.size == PAGE_HEADER_SIZE

PAGE_HDR_MAGIC_OFF = 0
PAGE_HDR_VERSION_OFF = 8
PAGE_HDR_PAGE_NO_OFF = 12
PAGE_HDR_LSN_OFF = 16
PAGE_HDR_SLOT_COUNT_OFF = 24
PAGE_HDR_FLAGS_OFF = 26
PAGE_HDR_REC_SIZE_OFF = 28
PAGE_HDR_RESERVED_OFF = 30
PAGE_HDR_REC_FORMAT_OFF = 32
PAGE_HDR_RESERVED2_OFF = 48

PAGE_FLAG_REBUILT = 0x0001


def page_crc(page: bytes) -> int:
    return crc32(bytes(page[:PAGE_HDR_CRC_OFF]) + bytes(page[PAGE_ROWS_OFF:]))


# --- WAL header (64 B) ------------------------------------------------------

WAL_HEADER_SIZE = 64
WAL_HEADER_STRUCT = struct.Struct("<8sIIIIQ16s12sI")
assert WAL_HEADER_STRUCT.size == WAL_HEADER_SIZE

WAL_HDR_MAGIC_OFF = 0
WAL_HDR_VERSION_OFF = 8
WAL_HDR_PAGE_SIZE_OFF = 12
WAL_HDR_REC_SIZE_OFF = 16
WAL_HDR_FLAGS_OFF = 20
WAL_HDR_CREATED_OFF = 24
WAL_HDR_REC_FORMAT_OFF = 32
WAL_HDR_RESERVED_OFF = 48
WAL_HDR_CRC_OFF = 60


def wal_header_crc(header: bytes) -> int:
    return crc32(bytes(header[:WAL_HDR_CRC_OFF]))


# --- WAL frames and records -------------------------------------------------

FRAME_HEADER_STRUCT = struct.Struct("<II")  # payload length, CRC32 of payload
FRAME_HEADER_SIZE = FRAME_HEADER_STRUCT.size  # 8

# type, flags, data_len, page_no, slot, reserved, txn_id, prev_lsn, lsn
LOG_REC_HEADER_STRUCT = struct.Struct("<BBHIHHIQQ")
LOG_REC_HEADER_SIZE = LOG_REC_HEADER_STRUCT.size  # 32
assert LOG_REC_HEADER_SIZE == 32

LOG_REC_MAX_DATA = PAGE_SIZE  # an FPI carries a whole page

RT_BEGIN = 1
RT_UPDATE = 2
RT_FPI = 3
RT_CLR = 4
RT_COMMIT = 5
RT_ABORT = 6

RTYPE_NAMES = {
    RT_BEGIN: "BEGIN",
    RT_UPDATE: "UPDATE",
    RT_FPI: "FPI",
    RT_CLR: "CLR",
    RT_COMMIT: "COMMIT",
    RT_ABORT: "ABORT",
}
RTYPES = frozenset(RTYPE_NAMES)
RTYPE_BY_NAME = {name: value for value, name in RTYPE_NAMES.items()}

UPDATE_FLAG_INSERT = 0x01
UPDATE_FLAG_DELETE = 0x02

UPDATE_DATA_SIZE = RECORD_SIZE * 2          # before + after
CLR_UNDO_NEXT_STRUCT = struct.Struct("<Q")
CLR_DATA_SIZE = RECORD_SIZE + CLR_UNDO_NEXT_STRUCT.size  # after + undo_next


def encode_update_data(before: bytes, after: bytes) -> bytes:
    if len(before) != RECORD_SIZE or len(after) != RECORD_SIZE:
        raise ValueError("row images must be exactly 32 bytes")
    return before + after


def decode_update_data(data: bytes):
    if len(data) != UPDATE_DATA_SIZE:
        raise ValueError(f"bad UPDATE payload size {len(data)}")
    return data[:RECORD_SIZE], data[RECORD_SIZE:]


def encode_clr_data(after: bytes, undo_next_lsn: int) -> bytes:
    if len(after) != RECORD_SIZE:
        raise ValueError("row image must be exactly 32 bytes")
    return after + CLR_UNDO_NEXT_STRUCT.pack(undo_next_lsn)


def decode_clr_data(data: bytes):
    if len(data) != CLR_DATA_SIZE:
        raise ValueError(f"bad CLR payload size {len(data)}")
    return data[:RECORD_SIZE], CLR_UNDO_NEXT_STRUCT.unpack_from(data, RECORD_SIZE)[0]


# --- transaction status labels ----------------------------------------------

TXN_COMMITTED = "COMMITTED"
TXN_ROLLED_BACK = "ROLLED_BACK"
TXN_LOSER = "LOSER"


def txn_status(records) -> str:
    """Classify one transaction from its WAL records (LSN order)."""
    types = {rec.rtype for rec in records}
    if RT_COMMIT in types:
        return TXN_COMMITTED
    if RT_ABORT in types:
        # ABORT is appended only after undo has finished, so this transaction
        # is resolved: its CLRs are in the log and will be redone.
        return TXN_ROLLED_BACK
    return TXN_LOSER


# --- LSN generation ---------------------------------------------------------


class LSNGenerator:
    """Monotonically increasing 64-bit LSN source.

    Values are wall-clock nanoseconds, forced strictly increasing.  They stay
    comparable across process restarts, which recovery relies on when it
    compares a page's stored LSN against log records.
    """

    def __init__(self, start: int | None = None):
        self._last = int(start if start is not None else time.time_ns())

    def next(self) -> int:
        now = time.time_ns()
        self._last = now if now > self._last else self._last + 1
        return self._last

    @property
    def last(self) -> int:
        return self._last
