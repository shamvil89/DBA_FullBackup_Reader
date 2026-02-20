# Backup Format Test – Findings

This document records the procedure and results of reverse-engineering SQL Server backup file layout by generating controlled backups and comparing them.

---

## 1. Purpose

- **Determinism:** Same database backed up twice with same options – are the two .bak files identical?
- **Layout change:** After a small data change, which regions of the .bak differ (header vs data)?
- **Block structure:** In compressed backups, is there a repeating block header (e.g. 20 bytes) before each compressed chunk?

Findings inform whether we can rely on fixed offsets, skip bytes, or use block-level metadata to avoid a full scan.

---

## 2. Procedure

### 2.1 Prerequisites

- SQL Server running (e.g. `DESKTOP-CIS3NI4`), PowerShell SqlServer module (`Invoke-Sqlcmd`).
- Credentials in `backup_format_test/.env` (copy from `.env.example`; set `SQL_PASSWORD`).
- Python 3 (for analysis scripts).

### 2.2 Generate backups

From the project root:

```powershell
$env:SQL_PASSWORD = "your_password"   # or use .env
.\backup_format_test\run_sql.ps1 01_create_db.sql
.\backup_format_test\run_sql.ps1 02_backup_uncompressed.sql
.\backup_format_test\run_sql.ps1 03_backup_compressed.sql
.\backup_format_test\run_sql.ps1 04_backup_twice.sql
.\backup_format_test\run_sql.ps1 05_backup_after_change.sql
```

Backups are written to `backup_format_test/outputs/`:

- `backup_unc.bak` – uncompressed
- `backup_comp.bak` – compressed
- `twice1.bak`, `twice2.bak` – same DB, two runs (determinism)
- `after_change.bak` – after one row update

### 2.3 Run analysis

From the project root:

```powershell
# Determinism: compare two backups of the same DB
python backup_format_test\analysis\compare_baks.py backup_format_test\outputs\twice1.bak backup_format_test\outputs\twice2.bak

# Layout change: compare before vs after update
python backup_format_test\analysis\compare_baks.py backup_format_test\outputs\backup_unc.bak backup_format_test\outputs\after_change.bak

# Optional: data region only (get offset with bakread --print-data-offset)
python backup_format_test\analysis\compare_baks.py ... --data-start <N>

# Block structure in compressed backup
python backup_format_test\analysis\scan_blocks.py backup_format_test\outputs\backup_comp.bak --header-size 20
```

### 2.4 Optional: get data region offset

```powershell
.\build\Release\bakread.exe --bak backup_format_test\outputs\backup_unc.bak --print-data-offset
```

Use the printed `data_start_offset=` value as `--data-start N` in the analysis scripts to restrict comparison or scanning to the data region.

---

## 3. Results

*Run the procedure in section 2 when SQL Server is available. If backups were not generated (e.g. SQL Server not reachable or sqlcmd timeout in the environment where tests were first run), execute the steps locally and paste the command outputs below.*

### 3.1 Determinism (twice1.bak vs twice2.bak)

**Command:** `python backup_format_test\analysis\compare_baks.py backup_format_test\outputs\twice1.bak backup_format_test\outputs\twice2.bak`

**Output:**

```
First difference at byte offset (from start of file): 52
Common prefix length (from start): 52
File1 context [36..68]: 00000000000000000000000002008705767bf45405000000010000000800030000
File2 context [36..68]: 00000000000000000000000002008705139f89ac05000000010000000800030000
```

**Interpretation:** Files are not identical. The first difference is at offset 52 (early header). The differing 8-byte values (e.g. `767bf45405` vs `139f89ac05`) look like timestamps or LSNs. So backup layout is not fully deterministic – at least one header field (likely backup time or LSN) changes between runs.

---

### 3.2 Layout change (backup_unc.bak vs after_change.bak)

**Command:** `python backup_format_test\analysis\compare_baks.py backup_format_test\outputs\backup_unc.bak backup_format_test\outputs\after_change.bak`

**Output:**

```
First difference at byte offset (from start of file): 52
Common prefix length (from start): 52
File1 context [36..68]: 00000000000000000000000002008705227d995405000000010000000800030000
File2 context [36..68]: 000000000000000000000000020087056cee4c9a05000000010000000800030000
File lengths: 3198976 vs 3330048
```

**Interpretation:** First difference again at offset 52 (header). File lengths differ (3,198,976 vs 3,330,048), so the data region size also changed after the update. The common prefix is only 52 bytes – header metadata and then content differ.

---

### 3.3 Compressed backup block scan (backup_comp.bak)

**Command:** `python backup_format_test\analysis\scan_blocks.py backup_format_test\outputs\backup_comp.bak --header-size 20`

**Output:**

```
Data region: 528384 bytes (from file offset 0)
Sampling first 20 bytes at several offsets:
  +       0 (       0 file): 4d5353514c42414b0200000003000000ffffffff
  +   65536 (   65536 file): 106925f4dda71bb20bacaa164487765a091a32a3
  +  131072 (  131072 file): 2e2b4173f326eac8e32a31ee888dbeccbcd3710e
  +  196608 (  196608 file): c10f7c8131b0e0e33ecd0132600f987f7befe66d
  +  262144 (  262144 file): 1c420cf38d817ba60b4102832291ac7b0ce77c45
  +  327680 (  327680 file): 2900a310e513aa53ccb5531c1465813744ee73c0
  +  393216 (  393216 file): 90c0a4524a80bc193a85d923785da38f99a7210e
  +  458752 (  458752 file): 000246019222b60b53805fd6024100041dfb583c
  +  524288 (  524288 file): 53464d420000008000100e010000000000000000
```

**Interpretation:** Offset 0: `4d5353514c42414b` = "MSSQLBAK" (backup magic). Samples at 64K intervals show no fixed repeating 20-byte header – bytes vary (compressed payload). At offset 524288, `53464d42` = "SFMB" (MTF block type), suggesting block boundaries in the stream. No simple fixed-stride block header; layout is variable.

---

### 3.4 Data region offset (from bakread)

**Command:** `.\build\Release\bakread.exe --bak backup_format_test\outputs\backup_unc.bak --print-data-offset`

**Output:** Build used did not include `--print-data-offset` (rebuild from source after adding the option to get this value).

---

## 4. Run environment note

- **Date run:** 2026-02-20
- **SQL Server:** DESKTOP-CIS3NI4 (or as in .env)
- **Backups generated:** Yes

---

## 5. Conclusions

- **Determinism:** Backups of the same DB taken twice are not byte-identical. First difference at offset 52 (header); likely a timestamp or LSN. So we cannot assume identical layout across runs for the first ~52 bytes; the rest was not compared in this run (files may be same length – determinism test did not report length difference).
- **Data region:** After a small update, first difference is still at offset 52; file lengths differ (backup_unc 3.2 MB vs after_change 3.3 MB). So both header and data region size/content change. No stable “data start” offset that we can rely on without parsing the header.
- **Compressed blocks:** No fixed 20-byte repeating header at regular intervals. First 8 bytes at file start are "MSSQLBAK"; later samples show varying compressed data; at 512 KB we see "SFMB" (MTF block). Block layout is variable; would require parsing MTF/block structure to skip or index.
- **Implications for bakread:** We must parse the backup header to find the data region; we cannot assume fixed offsets. For compressed backups, we cannot skip by fixed block stride; must follow MTF/block structure or scan. Full-stream scan (current approach) remains appropriate unless we add full MTF block parsing and optional selective decompression.

---

## 6. Large database (50+ MB) and allocation identification

### Purpose

Test how we can **identify** the set of pages (allocation unit) for a table and use that when working with a .bak file – e.g. to filter which pages we decode instead of relying only on the backup’s internal catalog.

### Procedure

1. **Create large DB and table (50+ MB):**  
   `.\backup_format_test\run_sql.ps1 06_create_large_db.sql`  
   Creates `BackupFormatTestLarge` and `dbo.LargeTable` with ~7500 rows (~7 KB each) so the table is over 50 MB.

2. **Get allocation info from the live database:**  
   `.\backup_format_test\run_sql.ps1 07_get_allocation_info.sql`  
   Runs `sys.dm_db_database_page_allocations(DB_ID(), OBJECT_ID('dbo.LargeTable'), ...)` and returns **(file_id, page_id)** for every data page of the table. Capture this result (e.g. to CSV) as the **reference list** of pages for `dbo.LargeTable`.

3. **Backup the large database:**  
   `.\backup_format_test\run_sql.ps1 08_backup_large.sql`  
   Produces `outputs/backup_large.bak` (50+ MB).

### How we can use the allocation list

- **Identification:** The list from step 2 is the set of (file_id, page_id) that belong to the table **in the live DB at the time of the query**. If the backup was taken from that same DB at roughly the same time, the same pages (and obj_id in page headers) should appear in the .bak.
- **When reading the .bak:** We still have to **find** where each (file_id, page_id) lives in the backup (no built-in offset index). So we either:  
  - **Scan once** and build a (file_id, page_id) → file_offset map; then for each page in the allocation list, **seek(offset)** and read 8 KB; or  
  - **Scan once** and only **cache or process** pages whose (file_id, page_id) is in the allocation list (use the list as a filter instead of resolving the backup’s catalog).
- **Older .bak:** If we use the allocation list from the live DB for an **older** backup, layout may have changed (more/fewer pages). The list is then a best-effort candidate set; we still match by (file_id, page_id) and optionally by page header obj_id when reading the backup.

### Results

- **Table size reported by sp_spaceused after 06:**  
  - `rows: 7500`, `reserved: 60296 KB`, `data: 60000 KB`, `index_size: 288 KB`, `unused: 8 KB`  
  - Approximately **58.6 MB** of table data.

- **Number of data pages returned by 07:** **7500** data pages (file_id=1, page_id starting at 352, etc.).  
  - Allocation info saved to `outputs/allocation_large.csv` (7501 lines including header).

- **backup_large.bak size:** **64.1 MB** (uncompressed).

- **bakread extract from backup_large.bak for dbo.LargeTable:**  
  - **SUCCESS** – bakread parsed the 64 MB backup in ~1.2 seconds.  
  - Cached 7867 pages (61 MB); found `dbo.LargeTable` (object_id=901578250, 3 columns, heap).  
  - Identified 7500 candidate data pages, extracted rows successfully (tested with `--max-rows 10`).

### Observations

1. The 7500 data pages returned by `sys.dm_db_database_page_allocations` match the 7500 candidate pages bakread discovered by scanning the backup and filtering by object_id.
2. The allocation list provides an **external reference** that could be used as a filter: instead of resolving the backup's catalog, pass the known (file_id, page_id) set and skip pages not in the list.
3. For **older backups** of the same table: pages may have been added/removed, but (file_id, page_id) matching still works for pages that exist in both. The allocation list acts as a "hint" rather than absolute truth.
4. The backup is ~64 MB vs ~60 MB data – overhead is ~4 MB (header, catalog pages, other system pages).

### Next steps (optional)

- [DONE] Implemented `--allocation-hint` CLI option in bakread.

---

## 7. Post-backup data scenario: What can be recovered?

### Purpose

Test what happens when data is added to the database **after** a backup was taken, and we use allocation hints from the **current** database against an **older** backup.

### Test Setup

| State | Rows | Pages | Backup |
|-------|------|-------|--------|
| Original (backup_large.bak) | 7,500 | 7,500 | backup_large.bak (64 MB) |
| After 500 inserts | 8,000 | 8,038 | backup_large_v2.bak (73 MB) |

### Test Results

#### Scenario A: NEW hints (8038 pages) on OLD backup (7500 rows)

```
bakread --bak backup_large.bak --allocation-hint allocation_after_insert.csv ...
```

**Result:**
```
Loaded 8038 allocation hints
Allocation hint filtered to 7500 pages (from 8038 hints)
SUCCESS: 7500 rows exported
```

**Interpretation:** The 538 new pages (for the 500 new rows) don't exist in the old backup, so they're simply not matched. All 7500 original rows are recovered successfully.

#### Scenario B: OLD hints (7500 pages) on NEW backup (8000 rows)

```
bakread --bak backup_large_v2.bak --allocation-hint allocation_large.csv ...
```

**Result:**
```
Loaded 7500 allocation hints
Allocation hint filtered to 7500 pages (from 7500 hints)
SUCCESS: 7500 rows exported
```

**Interpretation:** Only the 7500 pages from the original allocation hint are processed. The 500 new rows (on 538 new pages) are **not extracted** because their page IDs aren't in the hint set.

### Summary: How much data can be recovered?

| Scenario | Hints From | Backup From | Recoverable |
|----------|------------|-------------|-------------|
| A | Current DB (more data) | Older backup | **100%** of backup contents |
| B | Older DB (less data) | Newer backup | **Only rows on hinted pages** |

### Key Insights

1. **Using newer hints on older backup**: Works perfectly. Pages that don't exist in the backup are silently skipped. You recover 100% of what's in the backup.

2. **Using older hints on newer backup**: Recovers only the subset of data on the hinted pages. New rows added after the hint was captured are **missed**.

3. **Page reuse**: SQL Server may reuse page IDs after DELETEs. If rows were deleted and pages reused, the same page_id could contain different data. The hint would still match, but the content would be the new data.

4. **Best practice**: 
   - For **older backups**: Use hints from current DB (safe - extra hints are ignored)
   - For **current/recent backups**: Use hints captured at the same time as the backup
   - For **full extraction**: Don't use hints at all (let bakread resolve from backup catalog)

### When to use allocation hints

| Use Case | Recommended |
|----------|-------------|
| Extract specific table from multi-table DB | Yes - significant speedup |
| Extract from backup matching current schema | Yes |
| Extract from older backup of same table | Yes (use current hints) |
| Extract ALL data from any backup | No - skip hints, use catalog |
| Recover data after schema changes | No - schema mismatch risk |
