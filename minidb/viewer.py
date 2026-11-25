"""Human-readable views of the data file, the WAL, and a recovery run.

Everything here is read-only: the viewer opens files directly instead of
going through :class:`minidb.engine.Store`, so it can show torn pages, bad
checksums, and uncommitted log tails *before* recovery touches them.
"""

from __future__ import annotations

import json
import os

from . import pages as pagefmt
from .format import (
    DATA_FILE,
    PAGE_SIZE,
    RECOVERY_DIR,
    RTYPE_NAMES,
    SLOTS_PER_PAGE,
    WAL_FILE,
    format_row,
    row_key,
    unpack_row,
)
from .wal import decode_wal_header, scan_wal


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _rule(char: str = "-", width: int = 78) -> str:
    return char * width


def _kv(label: str, value) -> str:
    return f"  {label:<22} {value}"


def _crc_line(ok: bool, stored: int, computed: int) -> str:
    return (
        f"checksum {'OK' if ok else 'BAD'}  "
        f"stored={stored:#010x}  computed={computed:#010x}"
    )


def _read_all_pages(dbdir: str):
    """Return ``(list_of_pages, file_size)``; short last page is padded."""
    path = os.path.join(dbdir, DATA_FILE)
    if not os.path.exists(path):
        return [], 0
    size = os.path.getsize(path)
    pages = []
    fd = os.open(path, os.O_RDONLY)
    try:
        count = size // PAGE_SIZE + (1 if size % PAGE_SIZE else 0)
        for page_no in range(count):
            raw = pagefmt.read_page_lenient(fd, page_no)
            if raw is not None:
                pages.append((page_no, raw))
    finally:
        os.close(fd)
    return pages, size


# ---------------------------------------------------------------------------
# data file
# ---------------------------------------------------------------------------


def render_db_header(dbdir: str) -> list[str]:
    pages, size = _read_all_pages(dbdir)
    lines = [_rule("="), "DATA FILE", _rule("=")]
    path = os.path.join(dbdir, DATA_FILE)
    if not pages:
        lines.append(f"  missing: {path}")
        return lines
    lines.append(_kv("path", path))
    lines.append(_kv("size", f"{size} B  ({size / PAGE_SIZE:.2f} pages)"))
    try:
        hdr = pagefmt.validate_db_header(pages[0][1])
    except ValueError as exc:
        lines.append(_kv("header", f"INVALID: {exc}"))
        return lines
    d = hdr.as_dict()
    lines.append(_kv("uuid", d["uuid"]))
    lines.append(_kv("row format", d["row_format"]))
    lines.append(_kv("page size", d["page_size"]))
    lines.append(_kv("row size", d["rec_size"]))
    lines.append(_kv("created", d["created"]))
    lines.append(
        _crc_line(d["checksum_ok"], d["stored_crc"], d["computed_crc"])
    )
    return lines


def render_data_pages(dbdir: str, *, page_filter=None, show_rows: bool = True,
                      max_rows: int | None = None) -> list[str]:
    pages, _size = _read_all_pages(dbdir)
    lines = [_rule("="), "DATA PAGES", _rule("=")]
    if not pages:
        lines.append("  no data file")
        return lines
    for page_no, raw in pages:
        if page_no == pagefmt.DB_HEADER_PAGE:
            continue
        if page_filter is not None and page_no not in page_filter:
            continue
        hdr = pagefmt.decode_page_header(raw)
        ok = hdr.checksum_ok
        lines.append("")
        lines.append(f"page {page_no}   {_crc_line(ok, hdr.stored_crc, hdr.computed_crc)}")
        lines.append(
            _kv("magic/version", f"{hdr.magic.decode('latin-1')} / {hdr.version}")
        )
        lines.append(_kv("page LSN", hdr.lsn))
        lines.append(_kv("slot count", hdr.slot_count))
        lines.append(_kv("flags", f"{hdr.flags:#06x}"))
        lines.append(_kv("row size", hdr.rec_size))
        if not ok:
            lines.append(
                _kv("rows", "not decoded: page failed its checksum (torn write?)")
            )
            continue
        rows = list(pagefmt.decode_rows(raw))
        lines.append(_kv("live rows", len(rows)))
        if show_rows:
            shown = 0
            for slot, row in rows:
                if max_rows is not None and shown >= max_rows:
                    lines.append(f"    ... {len(rows) - shown} more rows")
                    break
                key, value, tag = unpack_row(row)
                lines.append(
                    f"    slot {slot:>3}  key={key:<8} value={value:<12} tag={tag!r}"
                )
                shown += 1
    return lines


def render_row_diff(before_dir: str, after_dir: str) -> list[str]:
    """Compare rows of two directories (snapshot vs recovered)."""
    def rows_of(dbdir):
        out = {}
        pages, _ = _read_all_pages(dbdir)
        for page_no, raw in pages:
            if page_no == pagefmt.DB_HEADER_PAGE:
                continue
            if not pagefmt.page_checksum_ok(raw):
                out[f"page{page_no}"] = ("TORN", None)
                continue
            for slot, row in pagefmt.decode_rows(raw):
                key, value, tag = unpack_row(row)
                out[key] = (value, tag, page_no, slot)
        return out

    before = rows_of(before_dir)
    after = rows_of(after_dir)
    lines = [_rule("="), "RECORD DIFF  (before snapshot -> after recovery)", _rule("=")]
    keys = sorted(k for k in set(before) | set(after) if isinstance(k, int))
    any_diff = False
    for key in keys:
        b = before.get(key)
        a = after.get(key)
        if b == a:
            continue
        any_diff = True
        if b is None:
            lines.append(f"  + key {key:<6} added by recovery   -> {a[0]}, {a[1]!r}")
        elif a is None:
            lines.append(f"  - key {key:<6} removed by recovery <- was {b[0]}, {b[1]!r}")
        else:
            lines.append(
                f"  ~ key {key:<6} {b[0]},{b[1]!r}  ->  {a[0]},{a[1]!r}"
            )
    torn_before = [k for k, v in before.items() if isinstance(k, str)]
    torn_after = [k for k, v in after.items() if isinstance(k, str)]
    if torn_before:
        lines.append(f"  torn pages before recovery: {', '.join(sorted(torn_before))}")
    if torn_after:
        lines.append(f"  torn pages after recovery:  {', '.join(sorted(torn_after))}")
    if not any_diff and not torn_before and not torn_after:
        lines.append("  no row-level differences")
    return lines


def render_page_crc_table(dbdir: str) -> list[str]:
    pages, _ = _read_all_pages(dbdir)
    lines = [
        _rule("="),
        "PAGE CHECKSUMS",
        _rule("="),
        f"  {'page':>5}  {'page_lsn':>20}  {'rows':>5}  {'stored':>10}  "
        f"{'computed':>10}  ok",
    ]
    for page_no, raw in pages:
        if page_no == pagefmt.DB_HEADER_PAGE:
            try:
                hdr = pagefmt.decode_db_header(raw)
                ok = hdr.checksum_ok
                lines.append(
                    f"  {'0*':>5}  {'-':>20}  {'-':>5}  {hdr.stored_crc:#010x}  "
                    f"{hdr.computed_crc:#010x}  {'YES' if ok else 'NO'}   (db header)"
                )
            except ValueError:
                lines.append(f"  {'0*':>5}  {'-':>20}  {'-':>5}  {'-':>10}  "
                             f"{'-':>10}  NO   (db header unreadable)")
            continue
        hdr = pagefmt.decode_page_header(raw)
        ok = hdr.checksum_ok
        rows = len(list(pagefmt.decode_rows(raw))) if ok else -1
        lines.append(
            f"  {page_no:>5}  {hdr.lsn:>20}  {rows if rows >= 0 else '???':>5}  "
            f"{hdr.stored_crc:#010x}  {hdr.computed_crc:#010x}  {'YES' if ok else 'NO'}"
        )
    return lines


# ---------------------------------------------------------------------------
# WAL
# ---------------------------------------------------------------------------


def render_wal(dbdir: str, *, max_frames: int | None = None) -> list[str]:
    path = os.path.join(dbdir, WAL_FILE)
    lines = [_rule("="), "WRITE-AHEAD LOG", _rule("=")]
    if not os.path.exists(path):
        lines.append(f"  missing: {path}")
        return lines
    scan = scan_wal(path)
    lines.append(_kv("path", path))
    lines.append(_kv("file size", f"{scan.file_size} B"))
    if scan.header is None:
        lines.append(_kv("header", "UNREADABLE"))
        for err in scan.errors:
            lines.append(f"  ! {err}")
        return lines
    h = scan.header
    lines.append(_kv("magic/version", f"{h['magic'].decode('latin-1')} / {h['version']}"))
    lines.append(_kv("page/row size", f"{h['page_size']} / {h['rec_size']}"))
    lines.append(_kv("row format", h["rec_format"]))
    lines.append(
        _crc_line(h["checksum_ok"], h["stored_crc"], h["computed_crc"])
    )
    lines.append("")
    lines.append(
        f"  {'offset':>8}  {'len':>5}  {'type':<9}  {'txn':>4}  {'lsn':>20}  "
        f"{'page':>4} {'slot':>4}  prev_lsn"
    )
    frames = scan.records
    for idx, rec in enumerate(frames):
        if max_frames is not None and idx >= max_frames:
            lines.append(f"  ... {len(frames) - idx} more frames")
            break
        lines.append(
            f"  {rec.offset:>8}  {rec.frame_len:>5}  {rec.type_name:<9}  "
            f"{rec.txn_id:>4}  {rec.lsn:>20}  {rec.page_no:>4} {rec.slot:>4}  "
            f"{rec.prev_lsn}"
        )
    lines.append("")
    lines.append("LOG BOUNDARY")
    lines.append(_kv("frames", len(frames)))
    lines.append(_kv("valid end", f"{scan.valid_end} B"))
    lines.append(_kv("file size", f"{scan.file_size} B"))
    lines.append(_kv("tail bytes", f"{scan.tail_bytes} B"))
    lines.append(_kv("torn tail", "YES" if scan.torn else "NO"))
    for err in scan.errors:
        lines.append(f"  ! {err}")
    if scan.tail_hex:
        tail = scan.tail_hex
        shown = tail[:160]
        lines.append(_kv("tail hex", shown + ("..." if len(tail) > 160 else "")))
    return lines


# ---------------------------------------------------------------------------
# recovery report / snapshots
# ---------------------------------------------------------------------------


def load_recovery_report(dbdir: str) -> dict | None:
    path = os.path.join(dbdir, RECOVERY_DIR, "latest.json")
    if not os.path.exists(path):
        return None
    with open(path, "r", encoding="utf-8") as fh:
        return json.load(fh)


def render_recovery_report(dbdir: str) -> list[str]:
    report = load_recovery_report(dbdir)
    lines = [_rule("="), "LAST RECOVERY REPORT", _rule("=")]
    if report is None:
        lines.append("  no recovery has run in this directory")
        return lines
    a = report["analysis"]
    lines.append("ANALYSIS")
    lines.append(_kv("wal file/valid", f"{a['wal_file_size']} / {a['wal_valid_end']} B"))
    lines.append(_kv("torn tail", f"{a['torn_tail_bytes']} B"))
    lines.append(_kv("log records", a["records"]))
    lines.append(_kv("committed", a["committed"] or "-"))
    lines.append(_kv("rolled back", a["rolled_back"] or "-"))
    lines.append(_kv("losers (undone)", a["losers"] or "-"))
    if a["txn_status"]:
        for txn, status in sorted(a["txn_status"].items(), key=lambda kv: int(kv[0])):
            lines.append(f"    txn {txn:>4}  {status}")
    for err in a["tail_errors"]:
        lines.append(f"  ! {err}")
    r = report["redo"]
    lines.append("")
    lines.append("REDO")
    lines.append(_kv("pages loaded", r["pages_loaded"]))
    lines.append(_kv("records applied", r["applied"]))
    lines.append(_kv("records skipped", r["skipped"]))
    lines.append(_kv("pages healed by FPI", r["fpi_repaired"]))
    u = report["undo"]
    lines.append("")
    lines.append("UNDO")
    lines.append(_kv("losers", u["losers"] or "-"))
    lines.append(_kv("CLRs written", u["clrs_written"]))
    for action in u["actions"]:
        lines.append(
            f"    txn {action['txn_id']} undo lsn {action['undo_lsn']} "
            f"page {action['page']} slot {action['slot']} "
            f"restore {action['restored'][:32]}..."
        )
    if report.get("before_snapshot"):
        lines.append("")
        lines.append(_kv("before snapshot", report["before_snapshot"]))
    return lines


def list_snapshots(dbdir: str) -> list[str]:
    rdir = os.path.join(dbdir, RECOVERY_DIR)
    if not os.path.isdir(rdir):
        return []
    return sorted(
        os.path.join(rdir, name)
        for name in os.listdir(rdir)
        if name.startswith("before-") and os.path.isdir(os.path.join(rdir, name))
    )


def render_snapshot_compare(dbdir: str, snapshot: str | None = None) -> list[str]:
    snaps = list_snapshots(dbdir)
    lines = [_rule("="), "RECOVERY BEFORE / AFTER", _rule("=")]
    if not snaps:
        lines.append("  no snapshots recorded")
        return lines
    snap = snapshot or snaps[-1]
    lines.append(_kv("snapshot", snap))
    lines.extend(render_row_diff(snap, dbdir))
    lines.append("")
    lines.extend(render_page_crc_table(snap))
    lines.append("")
    lines.append("AFTER RECOVERY")
    lines.extend(render_page_crc_table(dbdir))
    return lines


def render_inspection(dbdir: str, *, show_rows: bool = True,
                      max_rows: int | None = 8) -> str:
    lines = []
    lines.extend(render_db_header(dbdir))
    lines.append("")
    lines.extend(render_data_pages(dbdir, show_rows=show_rows, max_rows=max_rows))
    lines.append("")
    lines.extend(render_wal(dbdir))
    lines.append("")
    lines.extend(render_recovery_report(dbdir))
    lines.append("")
    lines.extend(render_snapshot_compare(dbdir))
    return "\n".join(lines)
