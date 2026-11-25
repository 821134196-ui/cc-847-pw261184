"""Write-ahead log: framing, append path, and crash-tolerant scanning.

The WAL is one append-only file.  The 64-byte header is followed by frames::

    +-------------+-------------+---------------------------+
    | u32 length  | u32 crc32   | length bytes              |
    +-------------+-------------+---------------------------+
                                | 32 B record header        |
                                | payload                   |

``length`` covers record header + payload.  ``crc32`` covers those same
bytes.  A scan stops at the first frame that is short, has an implausible
length, or fails its CRC.  Everything from that point on is an uncommitted
tail and is truncated away on the next open, so a torn record can never be
replayed and new records never follow garbage.

The writer only ever appends; it never rewrites the header.  Therefore the
header's CRC stays valid even when the tail is torn.
"""

from __future__ import annotations

import os
import struct

from .faults import write_with_fault
from .format import (
    FRAME_HEADER_SIZE,
    FRAME_HEADER_STRUCT,
    LOG_REC_HEADER_SIZE,
    LOG_REC_HEADER_STRUCT,
    LOG_REC_MAX_DATA,
    PAGE_SIZE,
    REC_FORMAT_SIZE,
    RECORD_SIZE,
    RTYPES,
    VERSION,
    WAL_HDR_CRC_OFF,
    WAL_HDR_CREATED_OFF,
    WAL_HDR_FLAGS_OFF,
    WAL_HDR_MAGIC_OFF,
    WAL_HDR_PAGE_SIZE_OFF,
    WAL_HDR_REC_FORMAT_OFF,
    WAL_HDR_REC_SIZE_OFF,
    WAL_HDR_VERSION_OFF,
    WAL_HEADER_SIZE,
    WAL_HEADER_STRUCT,
    WAL_MAGIC,
    LSNGenerator,
    crc32,
    wal_header_crc,
)
from .format import RTYPE_NAMES


class WalCorrupt(RuntimeError):
    """The WAL header is unreadable; the file is not usable."""


class LogRecord:
    """One decoded WAL frame."""

    __slots__ = (
        "offset",
        "frame_len",
        "rtype",
        "flags",
        "data",
        "page_no",
        "slot",
        "txn_id",
        "prev_lsn",
        "lsn",
    )

    def __init__(self, **kw):
        for name in self.__slots__:
            setattr(self, name, kw.get(name))

    @property
    def type_name(self) -> str:
        return RTYPE_NAMES.get(self.rtype, f"?{self.rtype}")

    @property
    def end_offset(self) -> int:
        return self.offset + self.frame_len

    def __repr__(self) -> str:  # pragma: no cover - debugging aid
        return (
            f"<LogRecord {self.type_name} lsn={self.lsn} txn={self.txn_id} "
            f"page={self.page_no} slot={self.slot} src={self.prev_lsn}>"
        )


class ScanResult:
    __slots__ = ("header", "records", "file_size", "valid_end", "torn", "tail_hex", "errors")

    def __init__(self, header, records, file_size, valid_end, torn, tail_hex, errors):
        self.header = header
        self.records = records
        self.file_size = file_size
        self.valid_end = valid_end
        self.torn = torn
        self.tail_hex = tail_hex
        self.errors = errors

    @property
    def tail_bytes(self) -> int:
        return max(0, self.file_size - self.valid_end)

    def by_txn(self) -> dict[int, list[LogRecord]]:
        out: dict[int, list[LogRecord]] = {}
        for rec in self.records:
            out.setdefault(rec.txn_id, []).append(rec)
        return out


# --- header -----------------------------------------------------------------


def encode_wal_header(rec_format: bytes, created: int) -> bytes:
    header = bytearray(WAL_HEADER_SIZE)
    header[WAL_HDR_MAGIC_OFF : WAL_HDR_MAGIC_OFF + 8] = WAL_MAGIC
    struct.pack_into("<I", header, WAL_HDR_VERSION_OFF, VERSION)
    struct.pack_into("<I", header, WAL_HDR_PAGE_SIZE_OFF, PAGE_SIZE)
    struct.pack_into("<I", header, WAL_HDR_REC_SIZE_OFF, RECORD_SIZE)
    struct.pack_into("<I", header, WAL_HDR_FLAGS_OFF, 0)
    struct.pack_into("<Q", header, WAL_HDR_CREATED_OFF, created)
    header[WAL_HDR_REC_FORMAT_OFF : WAL_HDR_REC_FORMAT_OFF + REC_FORMAT_SIZE] = (
        rec_format.ljust(REC_FORMAT_SIZE, b"\x00")
    )
    struct.pack_into("<I", header, WAL_HDR_CRC_OFF, wal_header_crc(header))
    return bytes(header)


def decode_wal_header(raw: bytes) -> dict:
    if len(raw) < WAL_HEADER_SIZE:
        raise WalCorrupt("WAL is shorter than its 64-byte header")
    (magic, version, page_size, rec_size, flags, created, rec_format,
     _reserved, stored_crc) = WAL_HEADER_STRUCT.unpack_from(raw, 0)
    info = {
        "magic": magic,
        "version": version,
        "page_size": page_size,
        "rec_size": rec_size,
        "flags": flags,
        "created": created,
        "rec_format": rec_format.rstrip(b"\x00").decode("ascii", "replace"),
        "stored_crc": stored_crc,
        "computed_crc": wal_header_crc(raw),
    }
    info["checksum_ok"] = magic == WAL_MAGIC and stored_crc == info["computed_crc"]
    if magic != WAL_MAGIC:
        raise WalCorrupt("WAL magic mismatch")
    if version != VERSION:
        raise WalCorrupt(f"unsupported WAL version {version}")
    if not info["checksum_ok"]:
        raise WalCorrupt("WAL header CRC mismatch")
    return info


# --- frames -----------------------------------------------------------------


def encode_frame(
    rtype: int,
    txn_id: int,
    lsn: int,
    prev_lsn: int = 0,
    data: bytes = b"",
    page_no: int = 0,
    slot: int = 0,
    flags: int = 0,
) -> bytes:
    if len(data) > LOG_REC_MAX_DATA:
        raise ValueError(f"log payload too large: {len(data)} > {LOG_REC_MAX_DATA}")
    rec = LOG_REC_HEADER_STRUCT.pack(
        rtype, flags, len(data), page_no, slot, 0, txn_id, prev_lsn, lsn
    ) + data
    return FRAME_HEADER_STRUCT.pack(len(rec), crc32(rec)) + rec


def decode_frame(frame: bytes, offset: int = 0) -> LogRecord:
    length, stored_crc = FRAME_HEADER_STRUCT.unpack_from(frame, 0)
    payload = bytes(frame[FRAME_HEADER_SIZE : FRAME_HEADER_SIZE + length])
    if len(payload) != length:
        raise WalCorrupt("frame payload is truncated")
    if crc32(payload) != stored_crc:
        raise WalCorrupt("frame CRC mismatch")
    (rtype, flags, data_len, page_no, slot, _res, txn_id, prev_lsn,
     lsn) = LOG_REC_HEADER_STRUCT.unpack_from(payload, 0)
    data = payload[LOG_REC_HEADER_SIZE:]
    if len(data) != data_len:
        raise WalCorrupt("record data length mismatch")
    if rtype not in RTYPES:
        raise WalCorrupt(f"unknown record type {rtype}")
    if lsn <= 0:
        raise WalCorrupt("record has an invalid LSN")
    return LogRecord(
        offset=offset,
        frame_len=FRAME_HEADER_SIZE + length,
        rtype=rtype,
        flags=flags,
        data=data,
        page_no=page_no,
        slot=slot,
        txn_id=txn_id,
        prev_lsn=prev_lsn,
        lsn=lsn,
    )


def scan_wal(path: str) -> ScanResult:
    """Parse a WAL without modifying it."""
    if not os.path.exists(path):
        return ScanResult(None, [], 0, 0, False, "", ["WAL file does not exist"])
    with open(path, "rb") as fh:
        raw = fh.read()
    file_size = len(raw)
    if file_size < WAL_HEADER_SIZE:
        return ScanResult(None, [], file_size, 0, True, raw.hex(),
                          ["WAL is shorter than its header (created then torn)"])
    try:
        header = decode_wal_header(raw[:WAL_HEADER_SIZE])
    except WalCorrupt as exc:
        return ScanResult(None, [], file_size, 0, True, raw.hex(), [str(exc)])

    records: list[LogRecord] = []
    errors: list[str] = []
    pos = WAL_HEADER_SIZE
    torn = False
    while pos < file_size:
        remaining = file_size - pos
        if remaining < FRAME_HEADER_SIZE:
            torn = True
            errors.append(f"tail at {pos}: {remaining} B is shorter than a frame header")
            break
        length, _crc = FRAME_HEADER_STRUCT.unpack_from(raw, pos)
        if length < LOG_REC_HEADER_SIZE or length > LOG_REC_HEADER_SIZE + LOG_REC_MAX_DATA:
            torn = True
            errors.append(f"tail at {pos}: implausible frame length {length}")
            break
        if remaining < FRAME_HEADER_SIZE + length:
            torn = True
            errors.append(
                f"tail at {pos}: frame claims {length} B but only "
                f"{remaining - FRAME_HEADER_SIZE} B are present"
            )
            break
        try:
            rec = decode_frame(raw[pos : pos + FRAME_HEADER_SIZE + length], pos)
        except WalCorrupt as exc:
            torn = True
            errors.append(f"tail at {pos}: {exc}")
            break
        records.append(rec)
        pos += rec.frame_len
    valid_end = pos
    tail_hex = raw[valid_end:].hex()
    return ScanResult(header, records, file_size, valid_end, torn, tail_hex, errors)


# --- writer -----------------------------------------------------------------


class Wal:
    """Append-only WAL writer.

    Opening an existing WAL truncates a torn tail and resumes appending at
    the end of the last complete frame.  Open is *not* the place where
    recovery runs; see :mod:`minidb.recovery`.
    """

    def __init__(
        self,
        path: str,
        rec_format: bytes,
        *,
        create: bool = False,
        injector=None,
        truncate_torn: bool = True,
        durable: bool = True,
    ):
        self.path = path
        self.injector = injector
        # ``durable=False`` keeps data in the kernel page cache instead of
        # calling fsync.  That is sufficient for *process* death (the crash
        # injector's model: the kernel still owns the written pages) but not
        # for power loss.  Tests use it to sweep thousands of cut points;
        # the default for real use is ``True``.
        self.durable = durable
        if create:
            self._create(rec_format)
        elif not os.path.exists(path):
            raise WalCorrupt(f"missing WAL file {path}")

        self.fd = os.open(path, os.O_RDWR | os.O_APPEND)
        self.scan = scan_wal(path)
        if self.scan.header is None:
            os.close(self.fd)
            raise WalCorrupt(f"cannot open WAL: {'; '.join(self.scan.errors)}")
        self.torn_tail_bytes = self.scan.tail_bytes
        self.torn_tail_hex = self.scan.tail_hex
        # Keep the scan taken *before* truncation: recovery's report should
        # describe the file as the crashed process left it, not as this
        # constructor cleaned it up.  ``opening_scan`` still lists every
        # complete record; only the torn tail differs from the live scan.
        self.opening_scan = self.scan
        self.opening_tail_bytes = self.scan.tail_bytes
        self.opening_tail_hex = self.scan.tail_hex
        if self.scan.torn and truncate_torn:
            os.ftruncate(self.fd, self.scan.valid_end)
            if durable:
                os.fsync(self.fd)
            self.truncated_tail_bytes = self.scan.tail_bytes
            self.scan = scan_wal(path)
        else:
            self.truncated_tail_bytes = 0
        self.records: list[LogRecord] = list(self.scan.records)
        self.append_offset = self.scan.valid_end
        self.last_lsn = max((r.lsn for r in self.records), default=0)
        self._by_lsn = {rec.lsn: rec for rec in self.records}
        self.lsn_gen = LSNGenerator(
            max(self.last_lsn, int(self.scan.header["created"]) * 1_000_000_000) + 1
        )
        self.next_txn_id = max((r.txn_id for r in self.records), default=0) + 1
        # Anything already in the file survived a previous process, so it is
        # durable.  Establish that watermark explicitly.
        if durable:
            os.fsync(self.fd)
        self.flushed_lsn = self.last_lsn

    def _create(self, rec_format: bytes) -> None:
        durable = self.durable
        import time

        os.makedirs(os.path.dirname(self.path) or ".", exist_ok=True)
        tmp = self.path + ".tmp"
        fd = os.open(tmp, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o644)
        try:
            os.write(fd, encode_wal_header(rec_format, int(time.time())))
            os.fsync(fd)
        finally:
            os.close(fd)
        os.replace(tmp, self.path)
        if durable:
            _fsync_dir(os.path.dirname(self.path) or ".")

    # -- append ------------------------------------------------------------

    def append(
        self,
        rtype: int,
        txn_id: int,
        prev_lsn: int = 0,
        *,
        data: bytes = b"",
        page_no: int = 0,
        slot: int = 0,
        flags: int = 0,
        sync: bool = False,
    ) -> LogRecord:
        lsn = self.lsn_gen.next()
        frame = encode_frame(rtype, txn_id, lsn, prev_lsn, data, page_no, slot, flags)
        offset = self.append_offset
        write_with_fault(
            self.fd, frame, offset, self.injector, "wal", rtype=rtype, txn_id=txn_id
        )
        self.append_offset += len(frame)
        rec = decode_frame(frame, offset)
        self.records.append(rec)
        if hasattr(self, "_by_lsn"):
            self._by_lsn[rec.lsn] = rec
        self.last_lsn = lsn
        if sync:
            self.flush()
        return rec

    def flush(self) -> None:
        if self.durable:
            os.fsync(self.fd)
        self.flushed_lsn = self.last_lsn

    def ensure_flushed(self, lsn: int) -> None:
        """WAL rule: a page carrying ``lsn`` may not reach disk before it."""
        if lsn > self.flushed_lsn:
            self.flush()

    def find(self, lsn: int) -> LogRecord | None:
        if not hasattr(self, "_by_lsn"):
            self._by_lsn = {rec.lsn: rec for rec in self.records}
        return self._by_lsn.get(lsn)

    @property
    def size(self) -> int:
        return self.append_offset

    def close(self) -> None:
        try:
            os.fsync(self.fd)
        finally:
            os.close(self.fd)

    def reopen_after_truncate(self) -> None:
        """Re-read the file after recovery rewrote its tail."""
        os.close(self.fd)
        self.fd = os.open(self.path, os.O_RDWR | os.O_APPEND)
        self.scan = scan_wal(self.path)
        self.records = list(self.scan.records)
        self.append_offset = self.scan.valid_end
        self.last_lsn = max((r.lsn for r in self.records), default=0)
        self._by_lsn = {rec.lsn: rec for rec in self.records}
        self.flushed_lsn = self.last_lsn


def _fsync_dir(path: str) -> None:
    fd = os.open(path, os.O_RDONLY)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)
