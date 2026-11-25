# minidb

A tiny page-oriented database, built from scratch. Fixed-size rows, a
write-ahead log, ARIES-style restart recovery, and a read-only page viewer
that lets you compare log boundaries, page checksums, and rows before and
after recovery.

```bash
python3 demo.py              # narrated crash → recover → inspect
python3 demo.py --exhaustive # cut every WAL byte and every 64th page byte
python3 -m unittest discover -s tests -p 'test_*.py'
```

Python 3.10+. No dependencies beyond the standard library.

---

## What it does

| Area | Behaviour |
| --- | --- |
| Storage | `data.db` is a split array of 4096-byte pages. Page 0 is the database header; pages 1..N hold 126 fixed 32-byte rows each. |
| Records | `<u32 key, i32 value, char tag[8], pad[16]>`. Key 0 means "empty slot"; keys must be positive. |
| Transactions | `insert`, `update`, `delete`, `read`, `commit`, `rollback`. One transaction at a time per process. A transaction may touch any number of pages. |
| WAL | Single append-only `wal.log`. Every change is logged before it is applied. The log is forced at commit and before any page carrying its records reaches disk. |
| Buffer pool | Small LRU (default 8 pages), **steal / no-force**: dirty pages may be evicted before commit, and commit does not write pages. |
| Faults | A cumulative byte budget on the WAL and/or data file can kill the process (real `os._exit`) part-way through *any* write — including inside a log frame header or a page header. |
| Recovery | Analysis → redo → undo on every open. Committed transactions are rolled forward, uncommitted ones are rolled back. Redo and undo are both restartable, so a crash *during recovery* is survivable. |
| Viewer | Read-only renderers for the data file, WAL boundary, recovery report, and before/after snapshots. Never goes through the engine, so it can show a torn page *before* recovery fixes it. |

---

## Layout

```
data.db    page 0: header (magic MINIDB01, uuid, row format, created, CRC32 @ 4092)
           page N: 64-byte header + 126 × 32-byte rows, CRC32 @ 60
wal.log    64-byte header + frames: u32 len | u32 CRC32 | 32-byte record header | payload
LOCK       flock() target, so two processes cannot open the same directory
CLEAN      JSON marker written on clean close; deleted as soon as a process starts writing
recovery/  latest.json (the last recovery report) + before-<stamp>/ raw file snapshots
```

A data page's CRC32 covers header bytes `[0:60]` **and** all 4032 row bytes
`[64:4096]`. The database header's CRC32 sits at byte 4092 and covers
`[0:4092]`. A frame's CRC32 covers its record header plus payload. Both
formats are little-endian.

**LSNs** are 64-bit wall-clock nanoseconds, forced strictly increasing. They
stay comparable across process restarts, which is what lets recovery compare a
page's stored LSN against log records after the process that wrote them is
gone.

### WAL record types

| Type | Payload | Purpose |
| --- | --- | --- |
| `BEGIN` | — | First record of a transaction. Forced immediately, so a page change can never reach disk without its transaction record. |
| `UPDATE` | before row + after row (64 B) | One row change. `prev_lsn` chains backwards for undo. |
| `FPI` | full 4096-byte page image | Logged on a transaction's *first* touch of a page. Heals a page torn by a crash mid-write. |
| `CLR` | after-image + `undo_next` LSN | Compensation during undo. Its presence lets a later recovery resume instead of repeating the undo. |
| `COMMIT` | — | Durability point. On stable storage before `commit()` returns. |
| `ABORT` | — | Written **last**, after every CLR is durable. Means "this transaction is fully undone". |

Transaction classification is purely from those records:
`COMMIT` present → COMMITTED; `ABORT` present → ROLLED_BACK; otherwise → LOSER.

---

## The protocol

**Write.** `BEGIN` is appended and forced. For each change: an `FPI` if this
is the transaction's first touch of the page, then an `UPDATE` holding the
before and after row. The change is applied to the cached page and the page's
LSN is set to the `UPDATE`'s LSN. Before that page is written to disk, the WAL
is flushed through its LSN (`Wal.ensure_flushed`).

**Commit.** Append `COMMIT`, fsync the WAL, report success. Pages stay dirty;
they may be written later or at clean close.

**Rollback.** Walk back along `prev_lsn`, restoring before-images and appending
a `CLR` per step. Flush the CLRs, then append `ABORT`, then flush again. `ABORT`
never becomes visible before its CLRs are durable — otherwise a recovery that
saw `ABORT` would skip an undo that never finished.

**Crash.** Nothing special happens: the process dies. Dirty pages may be lost
or half-written; the log may end mid-frame.

**Restart** (`minidb/recovery.py`):

1. **Analysis** — scan the WAL, group records by transaction, classify each as
   COMMITTED / ROLLED_BACK / LOSER. Truncate a torn log tail. The report keeps
   the *pre-truncation* boundary, so `torn_tail_bytes` and the tail hex show
   what the crashed process left behind.
2. **Redo** — replay `FPI`, `UPDATE`, `CLR` in LSN order, skipping any page
   whose stored LSN is already ≥ the record's. This is idempotent, so a second
   recovery (or a restart after a crash mid-redo) is safe. An `FPI` replaces a
   page whose CRC32 failed.
3. **Undo** — for each loser, walk its `prev_lsn` chain backwards from its last
   record, restoring before-images and appending `CLR`s. If an earlier attempt
   left `CLR`s behind, follow their `undo_next` instead of redoing them. Append
   `ABORT` once done.
4. Write back only the pages that actually changed, resealed with fresh CRCs.

A clean open still runs redo (cheap and idempotent) but finds no losers and
writes nothing back — that keeps the fault injector's byte budget focused on
the workload, not on recovery itself.

---

## CLI

```bash
python3 -m minidb init    DB                       # create data.db + wal.log
python3 -m minidb insert  DB --count 126 --value 100 --tag base
python3 -m minidb run     DB script.json          # batch ops (insert/update/delete/commit/rollback/flush)
python3 -m minidb crash   DB --count 10 --wal-bytes 4480 --capacity 2
python3 -m minidb recover DB                      # runs recovery, prints the JSON report
python3 -m minidb pages   DB --max-rows 2         # page viewer: header, rows, CRC table
python3 -m minidb wal     DB                      # frames + LOG BOUNDARY (valid end, tail)
python3 -m minidb inspect DB                      # everything, read-only
python3 -m minidb dump    DB                      # live rows as JSON
python3 -m minidb check   DB                      # exit 1 if any CRC is bad or the WAL is torn
```

Fault plans are JSON, either inline or a file path:

```json
{"wal_bytes": 4480, "label": "cut mid-frame"}
{"data_bytes": 300, "label": "torn page"}
{"wal_write_index": 3}
```

`wal_bytes` / `data_bytes` are cumulative budgets across the process's writes;
the write that crosses the budget is truncated and the process exits with
status 90. `*_write_index` targets the Nth write of that kind. `insert`,
`run`, and `crash` accept `--fault`. `run` and `crash` also take
`--in-process`, which raises `Crash` instead of exiting (used by tests).

`run` script:

```json
{"operations": [
  {"op": "insert", "key": 1, "value": 10, "tag": "a"},
  {"op": "update", "key": 1, "value": 11},
  {"op": "delete", "key": 1},
  {"op": "commit"}
]}
```

---

## Walkthrough: crash → recover → compare

This is the scenario `demo.py` prints. Start with a committed transaction that
fills page 1 and puts 10 rows on page 2, then crash an *uncommitted* transaction
mid-page write:

```bash
python3 -m minidb init /tmp/db
python3 -m minidb insert /tmp/db --count 126 --key-start 1  --value 100 --tag base
python3 -m minidb insert /tmp/db --count 10  --key-start 127 --value 1270 --tag base
python3 -m minidb crash  /tmp/db --count 10 --key-start 1000 --value 7 --tag loser \
        --data-bytes 300 --capacity 2
```

`crash` exits 90. Page 2 is now torn — its new row area was partly written but
its CRC never was:

```
PAGE CHECKSUMS
   page              page_lsn   rows      stored    computed  ok
     0*                     -      -  0x63b60fc9  0x63b60fc9  YES   (db header)
      1   1795643402531767768    126  0x94028a20  0x94028a20  YES
      2   1795643403105108210    ???  0x9c7ecaba  0xd9e82e9b  NO

$ python3 -m minidb check /tmp/db ; echo $?
{"pages_ok": 1, "pages_total": 2, "wal_records": 154, "wal_torn": false,
 "problems": ["page 2: CRC mismatch"]}
1
```

Opening the store recovers it (`python3 -m minidb recover /tmp/db`):

```
ANALYSIS
  log records            154
  committed              [1, 2]
  losers (undone)        [3]

REDO
  records applied        22
  pages healed by FPI    1      ← page 2 was torn and restored from its full page image

UNDO
  losers                 [3]
  CLRs written           10     ← the uncommitted rows are rolled back
```

After recovery every page checks out, the loser's rows are gone, and the
committed rows are untouched. `inspect` also shows the before/after diff against
the raw snapshot taken just before recovery:

```
RECORD DIFF  (before snapshot -> after recovery)
  + key 127    added by recovery   -> 1270, 'base'      ← redo applied committed writes
  ...
  torn pages before recovery: page2

PAGE CHECKSUMS                       AFTER RECOVERY
  2   1795643403105108210  ???  ... NO      →    2   1795643411321753911  10  ... YES
```

The "added by recovery" rows are the committed transaction's rows that existed
only in the log at crash time; redo wrote them to a page the snapshot still saw
as torn.

### A torn log instead

Cutting the WAL mid-frame leaves a different shape of damage. `wal` reports the
boundary; `check` flags it; recovery truncates the tail and still finds the
loser:

```
LOG BOUNDARY
  valid end              9080 B
  file size              9176 B
  tail bytes             96 B
  torn tail              YES
  ! tail at 9080: frame claims 96 B but only 88 B are present
```

`python3 -m minidb recover /tmp/db` reports `torn_tail_bytes: 96`, truncates it,
classifies the transaction as a LOSER (its `COMMIT` never made it), and undoes
its rows.

---

## Fault injection

The injector (`minidb/faults.py`) knows nothing about records or pages. It only
counts bytes written to each file. When a write would cross its budget it:

1. writes the allowed prefix,
2. fsyncs whatever prefix landed,
3. terminates the process — `os._exit(90)` in real mode, `Crash` (a
   `BaseException`, so no `except Exception` can swallow it) in test mode.

Every write in the engine goes through `write_with_fault`: WAL appends and page
flushes, including the page writes *during recovery*. That is why recovery
itself can be crashed and resumed.

Because both on-disk formats carry CRC32 checksums, a torn frame cannot be
replayed, a torn page cannot be trusted as data, and both are detected rather
than silently accepted.

---

## Tests

54 tests, ~3 minutes. `python3 -m unittest discover -s tests -p 'test_*.py'`.

| File | What it covers |
| --- | --- |
| `test_format.py` (16) | Row/page/WAL encoding. Every single-byte flip in a page's covered region breaks its CRC. Frame and header corruption is detected. A torn frame truncates the scan. |
| `test_engine.py` (16) | Insert/update/delete/rollback, reopen, multi-page transactions, rollback across pages, duplicate keys, `DatabaseFull`, steal policy, uncommitted rows undone on reopen, committed rows survive without a page flush, log record ordering (FPI before first UPDATE per page). |
| `test_crash_sweep.py` (8) | **The core guarantee.** Cuts *every byte* of a small WAL (`TestWalByteSweep`), and every 13th byte of a 4 KiB page write (`TestPageByteSweep`), in-process; then repeats representative cuts with a genuine `os._exit` child (`TestRealProcessDeath`). After every cut: baseline intact, the crashing transaction all-or-nothing, every page CRC-valid. |
| `test_recovery.py` (14) | Recovery is idempotent on a second open; multiple losers undone; crash *during* undo and *during* redo, each finished by a later run; rollback logs CLRs then a single trailing ABORT; the viewer's output for pages, torn pages, WAL boundary, report sections, and before/after snapshots. |

The byte sweeps run in-process with `durable=False` (fsync skipped, kernel page
cache kept) — enough to model process death, and what keeps thousands of cut
points fast. `TestRealProcessDeath` and normal operation use real fsyncs.

`demo.py --exhaustive` runs the same idea as a standalone check: every WAL byte
plus a page sweep, asserting atomicity and CRC validity at every cut.

---

## Design notes

- **Split pages, not a single slot array.** Any page can be addressed in place
  (`offset == page_no * PAGE_SIZE`), so a torn page damages only itself; an FPI
  or a later UPDATE repairs it without touching neighbours.
- **FPI on first touch.** Cheapest way to make a torn page recoverable: the log
  holds a complete, checksum-valid image of the page as it was before the
  transaction's first change to it.
- **Undo is backwards through `prev_lsn`.** Each `UPDATE` knows its predecessor,
  so undo does not need to rescan the log or care about interleaving.
- **CLRs make undo restartable.** A crash mid-undo leaves `CLR` records whose
  `undo_next` says where to continue; a later recovery follows them instead of
  repeating the work. `ABORT` is only appended once the whole chain is done.
- **The viewer reads raw files.** It deliberately bypasses `Store`, so it can
  show the torn page, the bad checksum, and the uncommitted log tail that a
  recovered store would have already fixed.
- **`CLEAN` marker is an optimization, not a correctness gate.** Recovery runs
  on every open regardless; the marker only lets a clean open skip the
  snapshot and the page write-back.

Small on purpose: linear scans, no index, one transaction per process. It is a
teaching store whose value is being small enough to inspect completely.
