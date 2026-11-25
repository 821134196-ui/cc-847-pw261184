"""Format-level tests: page layout, checksums, WAL framing, tail scanning."""

from __future__ import annotations

import os
import struct
import unittest

import helpers  # noqa: F401  (adds the project root to sys.path)

from minidb import format as fmt
from minidb import pages as pagefmt
from minidb.wal import (
    WalCorrupt,
    decode_frame,
    decode_wal_header,
    encode_frame,
    encode_wal_header,
    scan_wal,
)


class TestRowFormat(unittest.TestCase):
    def test_row_is_32_bytes(self):
        self.assertEqual(fmt.ROW_STRUCT.size, 32)
        self.assertEqual(len(fmt.pack_row(1, 2, "abcd")), 32)

    def test_roundtrip(self):
        raw = fmt.pack_row(42, -7, "tag")
        self.assertEqual(fmt.unpack_row(raw), (42, -7, "tag"))

    def test_rejects_zero_and_negative_keys(self):
        with self.assertRaises(ValueError):
            fmt.pack_row(0, 1, "")
        with self.assertRaises(ValueError):
            fmt.pack_row(-5, 1, "")

    def test_long_tag_is_truncated_to_eight_bytes(self):
        raw = fmt.pack_row(1, 0, "0123456789")
        self.assertEqual(fmt.unpack_row(raw)[2], "01234567")


class TestPageFormat(unittest.TestCase):
    def test_slots_per_page(self):
        self.assertEqual(fmt.SLOTS_PER_PAGE, 126)
        self.assertEqual(fmt.PAGE_ROWS_OFF + 126 * 32 <= 4096, True)

    def test_encode_decode_roundtrip(self):
        rows = [fmt.pack_row(i, i * 10, "r") for i in range(1, 4)]
        raw = pagefmt.encode_page(7, 1234, fmt.DEFAULT_REC_FORMAT, rows)
        self.assertEqual(len(raw), 4096)
        self.assertTrue(pagefmt.page_checksum_ok(raw))
        hdr = pagefmt.decode_page_header(raw)
        self.assertEqual(hdr.page_no, 7)
        self.assertEqual(hdr.lsn, 1234)
        self.assertEqual(hdr.slot_count, 3)
        decoded = [(slot, fmt.unpack_row(row)) for slot, row in pagefmt.decode_rows(raw)]
        self.assertEqual(decoded, [(0, (1, 10, "r")), (1, (2, 20, "r")), (2, (3, 30, "r"))])

    def test_every_single_byte_flip_breaks_the_checksum(self):
        rows = [fmt.pack_row(1, 1, "a"), fmt.pack_row(2, 2, "b")]
        raw = bytearray(pagefmt.encode_page(1, 0, fmt.DEFAULT_REC_FORMAT, rows))
        # Skip the four CRC bytes themselves; flipping those also invalidates
        # the page, but the point here is the covered region.
        for offset in list(range(0, 60)) + list(range(64, 4096)):
            mutated = bytearray(raw)
            mutated[offset] ^= 0xFF
            self.assertFalse(
                pagefmt.page_checksum_ok(bytes(mutated)),
                f"byte {offset} was not covered by the page CRC",
            )

    def test_torn_page_fails_checksum(self):
        rows = [fmt.pack_row(1, 1, "a")]
        raw = pagefmt.encode_page(1, 0, fmt.DEFAULT_REC_FORMAT, rows)
        self.assertTrue(pagefmt.page_checksum_ok(raw))
        # Model a write cut at byte 100: the header and first row survive,
        # but the CRC recorded in the header now describes a longer buffer.
        torn = raw[:100] + bytes([0xAA]) * (4096 - 100)
        self.assertFalse(pagefmt.page_checksum_ok(torn))

    def test_db_header_roundtrip(self):
        raw = pagefmt.encode_db_header(fmt.DEFAULT_REC_FORMAT, b"u" * 16, 123)
        hdr = pagefmt.validate_db_header(raw)
        self.assertEqual(hdr.page_size, 4096)
        self.assertEqual(hdr.rec_size, 32)
        self.assertEqual(hdr.uuid, b"u" * 16)
        self.assertEqual(hdr.row_format, "demo32")

    def test_db_header_crc_covers_first_4092_bytes(self):
        raw = bytearray(pagefmt.encode_db_header(fmt.DEFAULT_REC_FORMAT, b"u" * 16, 1))
        raw[100] ^= 0xFF
        with self.assertRaises(ValueError):
            pagefmt.validate_db_header(bytes(raw))


class TestWalFormat(unittest.TestCase):
    def test_wal_header_roundtrip(self):
        raw = encode_wal_header(fmt.DEFAULT_REC_FORMAT, 999)
        hdr = decode_wal_header(raw)
        self.assertTrue(hdr["checksum_ok"])
        self.assertEqual(hdr["rec_format"], "demo32")

    def test_wal_header_detects_corruption(self):
        raw = bytearray(encode_wal_header(fmt.DEFAULT_REC_FORMAT, 999))
        raw[20] ^= 0x01
        with self.assertRaises(WalCorrupt):
            decode_wal_header(bytes(raw))

    def test_frame_roundtrip(self):
        frame = encode_frame(
            fmt.RT_UPDATE, txn_id=3, lsn=111, prev_lsn=100,
            data=fmt.encode_update_data(b"\x00" * 32, fmt.pack_row(9, 9, "x")),
            page_no=2, slot=5, flags=fmt.UPDATE_FLAG_INSERT,
        )
        rec = decode_frame(frame)
        self.assertEqual(rec.type_name, "UPDATE")
        self.assertEqual(rec.txn_id, 3)
        self.assertEqual(rec.lsn, 111)
        self.assertEqual(rec.prev_lsn, 100)
        self.assertEqual(rec.page_no, 2)
        self.assertEqual(rec.slot, 5)
        self.assertEqual(rec.flags, fmt.UPDATE_FLAG_INSERT)

    def test_frame_crc_detects_single_bit(self):
        frame = bytearray(encode_frame(fmt.RT_BEGIN, 1, 55))
        frame[-1] ^= 0x01
        with self.assertRaises(WalCorrupt):
            decode_frame(bytes(frame))

    def test_scan_stops_at_torn_frame(self):
        import tempfile

        tmp = tempfile.mkdtemp()
        path = os.path.join(tmp, "wal.log")
        with open(path, "wb") as fh:
            fh.write(encode_wal_header(fmt.DEFAULT_REC_FORMAT, 1))
            good = encode_frame(fmt.RT_BEGIN, 1, 100)
            good2 = encode_frame(fmt.RT_COMMIT, 1, 101, prev_lsn=100)
            bad = encode_frame(fmt.RT_UPDATE, 1, 102, prev_lsn=101,
                               data=b"\x00" * 64, page_no=1, slot=0)
            fh.write(good)
            fh.write(good2)
            fh.write(bad[: len(bad) // 2])   # torn
            fh.flush()
        result = scan_wal(path)
        self.assertTrue(result.torn)
        self.assertEqual(len(result.records), 2)
        self.assertEqual(result.valid_end, 64 + len(good) + len(good2))
        self.assertGreater(result.tail_bytes, 0)
        self.assertTrue(result.tail_hex)

    def test_scan_stops_at_short_frame_header(self):
        import tempfile

        tmp = tempfile.mkdtemp()
        path = os.path.join(tmp, "wal.log")
        with open(path, "wb") as fh:
            fh.write(encode_wal_header(fmt.DEFAULT_REC_FORMAT, 1))
            fh.write(b"\x01\x02\x03")  # 3 bytes, less than an 8-byte frame header
        result = scan_wal(path)
        self.assertTrue(result.torn)
        self.assertEqual(result.records, [])
        self.assertEqual(result.valid_end, 64)


if __name__ == "__main__":
    unittest.main()
