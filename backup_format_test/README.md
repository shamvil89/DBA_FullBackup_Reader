# Backup Format Test

Reverse-engineering SQL Server backup file layout by generating controlled backups and comparing them to find metadata and block structure patterns.

## Prerequisites

- Windows with SQL Server (e.g. `DESKTOP-CIS3NI4`)
- PowerShell with **SqlServer** module (`Invoke-Sqlcmd`). Install with: `Install-Module -Name SqlServer -Scope CurrentUser`
- Python 3 (for analysis scripts)

## Security

**Do not commit real credentials.** Server name and username may appear in examples. Store the password only in:

- Environment variables (`SQL_SERVER`, `SQL_USER`, `SQL_PASSWORD`), or
- A local `backup_format_test/.env` file (copy from `.env.example`; `.env` is gitignored)

Never put the real `sa` (or any) password in any committed file.

## Setup

1. Copy `.env.example` to `.env` in this directory:
   ```powershell
   cd backup_format_test
   copy .env.example .env
   ```
2. Edit `.env` and set `SQL_PASSWORD` to your real password (other values may already match).

## Running SQL Scripts

From the project root or from `backup_format_test/`:

```powershell
.\backup_format_test\run_sql.ps1 01_create_db.sql
.\backup_format_test\run_sql.ps1 02_backup_uncompressed.sql
.\backup_format_test\run_sql.ps1 03_backup_compressed.sql
.\backup_format_test\run_sql.ps1 04_backup_twice.sql
.\backup_format_test\run_sql.ps1 05_backup_after_change.sql
```

Backups are written to `backup_format_test/outputs/` by default (created if missing).

### Large database (50+ MB) and allocation identification

To test allocation-unit / page identification with a larger backup:

```powershell
.\backup_format_test\run_sql.ps1 06_create_large_db.sql   # Creates BackupFormatTestLarge, ~50+ MB table
.\backup_format_test\run_sql.ps1 07_get_allocation_info.sql   # Output (file_id, page_id) for dbo.LargeTable
.\backup_format_test\run_sql.ps1 08_backup_large.sql   # Writes backup_format_test\outputs\backup_large.bak
```

- **06** creates database `BackupFormatTestLarge` and table `dbo.LargeTable` with enough rows (~7500 x 7 KB) to exceed 50 MB.
- **07** queries `sys.dm_db_database_page_allocations` and returns the data pages (file_id, page_id) for that table. Run it after the DB exists (live or after restore). Capture the result set to use as a "reference" list of pages for the table.
- **08** backs up the large database to `outputs/backup_large.bak`.

To capture allocation info to a file (PowerShell):

```powershell
Invoke-Sqlcmd -ServerInstance $env:SQL_SERVER -Username $env:SQL_USER -Password $env:SQL_PASSWORD -InputFile backup_format_test\sql\07_get_allocation_info.sql -TrustServerCertificate | Export-Csv -Path backup_format_test\outputs\allocation_large.csv -NoTypeInformation
```

See FINDINGS.md section 6 for how this allocation list can be used when reading the .bak (e.g. as a filter instead of parsing the backup catalog).

## Analysis (after backups exist)

From the project root:

```powershell
python backup_format_test\analysis\compare_baks.py backup_format_test\outputs\twice1.bak backup_format_test\outputs\twice2.bak
python backup_format_test\analysis\compare_baks.py backup_format_test\outputs\backup_unc.bak backup_format_test\outputs\after_change.bak
python backup_format_test\analysis\scan_blocks.py backup_format_test\outputs\backup_comp.bak --header-size 20
```

Optional: use `--data-start N` to restrict comparison or scan to the data region. To get N, run:
`bakread --bak path\to\file.bak --print-data-offset`
and use the printed `data_start_offset` value.
