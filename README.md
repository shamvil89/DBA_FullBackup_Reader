# bakread -- SQL Server .bak Backup Table Extractor

A production-grade CLI tool written in C++17 that reads SQL Server full backup (.bak) files and extracts data from a specified table into CSV, Parquet, or JSON Lines format.

## Features

- **Direct extraction** from .bak files without restoring to SQL Server
- **Restore mode** for TDE-encrypted or complex backups
- **Striped backup support** - multiple .bak files from a single backup
- **Large backup support** - indexed mode with parallel scanning for 50GB+ backups
- **SQL Server authentication** - Windows Auth or SQL Server login
- **GUI application** - Modern Azure Data Studio-inspired interface
- **Multiple output formats** - CSV, Parquet, JSON Lines

## Architecture

The tool operates in two modes:

### Mode A: Direct Parse (Best-Effort)

Attempts to read the .bak file directly without restoring to SQL Server:

1. **BackupStream** -- Streaming reader for the MTF-based backup format
2. **BackupHeaderParser** -- Extracts backup metadata, detects TDE/encryption
3. **Decompressor** -- Handles SQL Server compressed backup blocks (LZXPRESS variant + deflate fallback)
4. **CatalogReader** -- Scans system catalog pages (`sysschobjs`, `syscolpars`) to resolve table schema
5. **RowDecoder** -- Parses the FixedVar row format from 8KB data pages
6. **PageParser** -- Interprets page headers, slot arrays, IAM chains, and allocation maps

**Limitations**: Does not support TDE-encrypted databases, backup-level encryption, LOB overflow data, or all SQL Server version variations. Fails gracefully and falls back to Mode B.

### Mode B: Restore & Extract (Reliable Fallback)

The guaranteed working path:

1. Connects to a SQL Server instance via ODBC
2. Runs `RESTORE HEADERONLY` / `RESTORE FILELISTONLY` for metadata
3. Provisions TDE certificates if needed (import from .cer/.pvk or .pfx files)
4. Restores the database to a temporary name with file relocation
5. Reads table schema from `sys.columns` / `sys.types` / `sys.indexes`
6. Streams rows via ODBC cursor
7. Drops the temporary database and cleans up certificates

### Mode Selection

```
--mode auto      Try direct parse first, fall back to restore (default)
--mode direct    Only attempt direct .bak parsing
--mode restore   Skip parsing, restore to SQL Server
```

## Indexed Mode (Large Backups)

For backups larger than 1GB, indexed mode provides memory-efficient extraction:

### How It Works

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         PHASE 1: PARALLEL SCAN                               │
├─────────────────────────────────────────────────────────────────────────────┤
│  Thread 1          Thread 2          Thread 3          Thread 4              │
│  ┌─────────┐       ┌─────────┐       ┌─────────┐       ┌─────────┐          │
│  │ Stripe1 │       │ Stripe2 │       │ Stripe3 │       │ Stripe4 │          │
│  │  .bak   │       │  .bak   │       │  .bak   │       │  .bak   │          │
│  └────┬────┘       └────┬────┘       └────┬────┘       └────┬────┘          │
│       ▼                 ▼                 ▼                 ▼                │
│  64KB chunks       64KB chunks       64KB chunks       64KB chunks          │
│  (8 pages)         (8 pages)         (8 pages)         (8 pages)            │
└─────────────────────────────────────────────────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│                      PAGE INDEX + LRU CACHE                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│  • In-memory index: (file_id, page_id) → (stripe, offset, type)             │
│  • Persistent index files (.idx) for subsequent runs                        │
│  • LRU cache for frequently accessed pages (configurable size)              │
│  • On-demand page retrieval via index lookup + seek                         │
└─────────────────────────────────────────────────────────────────────────────┘
```

### Memory Comparison

| Backup Size | Traditional (in-memory) | Indexed Mode (512MB cache) |
|-------------|-------------------------|----------------------------|
| 10GB        | ~10.5GB RAM             | ~640MB RAM                 |
| 50GB        | ~52GB RAM               | ~640MB RAM                 |
| 100GB       | ~104GB RAM              | ~640MB RAM                 |

### Indexed Mode CLI Options

```
--indexed               Use indexed page store (recommended for >1GB backups)
--cache-size MB         LRU cache size in MB (default: 256)
--index-dir PATH        Directory for index files (default: next to backup)
--force-rescan          Ignore existing index files and rescan
```

### Example: 50GB Striped Backup

```bash
bakread --bak StackOverflow_1of4.bak --bak StackOverflow_2of4.bak \
        --bak StackOverflow_3of4.bak --bak StackOverflow_4of4.bak \
        --table dbo.Users --out users.csv --format csv \
        --indexed --cache-size 512
```

## Building

### Prerequisites

- CMake 3.20+
- C++17 compiler (MSVC 2019+, GCC 9+, Clang 10+)
- ODBC Driver 17 or 18 for SQL Server (Mode B)
- Optional: Apache Arrow with Parquet (for Parquet output)
- Optional: zlib (for deflate decompression)

### Using vcpkg (Recommended on Windows)

```bash
git clone https://github.com/microsoft/vcpkg.git
cd vcpkg && bootstrap-vcpkg.bat
vcpkg install arrow:x64-windows zlib:x64-windows

cd /path/to/SQLBAKReader
cmake -B build -S . -DCMAKE_TOOLCHAIN_FILE=/path/to/vcpkg/scripts/buildsystems/vcpkg.cmake
cmake --build build --config Release
```

### Manual Build

```bash
mkdir build && cd build
cmake .. -DCMAKE_BUILD_TYPE=Release
cmake --build .
```

### Build Options

| Option | Default | Description |
|--------|---------|-------------|
| `BAKREAD_ENABLE_PARQUET` | ON | Build with Apache Arrow / Parquet support |
| `BAKREAD_ENABLE_TESTS` | OFF | Build test suite |

## Usage

### Basic Examples

```bash
# Extract to CSV
bakread --bak backup.bak --table dbo.Orders --out orders.csv --format csv

# Extract to Parquet (large-scale analytics)
bakread --bak backup.bak --table dbo.Users --out users.parquet --format parquet

# Extract with row limit and column filter
bakread --bak backup.bak --table dbo.Sales --out sales.csv --format csv \
        --max-rows 100000 --columns "OrderId,Amount,OrderDate"

# Restore mode with explicit SQL Server instance
bakread --bak backup.bak --table dbo.Sales --out sales.jsonl --format jsonl \
        --mode restore --target-server ".\SQLEXPRESS"
```

### Striped Backups (Multiple Files)

```bash
# SQL Server often creates striped backups for large databases
bakread --bak stripe1.bak --bak stripe2.bak --bak stripe3.bak \
        --table dbo.Orders --out orders.csv --format csv
```

### SQL Server Authentication

```bash
# Windows Authentication (default)
bakread --bak backup.bak --table dbo.Users --out users.csv --format csv \
        --mode restore --target-server "SERVER\INSTANCE"

# SQL Server Authentication
bakread --bak backup.bak --table dbo.Users --out users.csv --format csv \
        --mode restore --target-server "SERVER\INSTANCE" \
        --sql-user sa --sql-password "YourPassword"

# Using environment variable for password (recommended)
export BAKREAD_SQL_PASSWORD="YourPassword"
bakread --bak backup.bak --table dbo.Users --out users.csv --format csv \
        --mode restore --target-server "SERVER\INSTANCE" --sql-user sa
```

### TDE-Encrypted Databases

TDE (Transparent Data Encryption) requires restore mode with certificate provisioning.

```bash
# Using separate certificate (.cer) and private key (.pvk) files
bakread --bak tde_backup.bak --table dbo.Sensitive --out data.csv --format csv \
        --mode restore --target-server ".\SQLEXPRESS" \
        --tde-cert-pfx /path/to/cert.cer \
        --tde-cert-key /path/to/cert_key.pvk \
        --tde-cert-password "KeyPassword" \
        --cleanup-keys

# Using combined PFX file
bakread --bak backup.bak --table dbo.Sensitive --out data.csv --format csv \
        --mode restore --target-server ".\SQLEXPRESS" \
        --tde-cert-pfx /path/to/cert.pfx \
        --tde-cert-password "$CERT_PASS" \
        --cleanup-keys
```

### List Tables in a Backup

```bash
# Direct mode (fast, but may not work with TDE/compressed backups)
bakread --bak backup.bak --list-tables

# Restore mode (more reliable)
bakread --bak backup.bak --list-tables --target-server ".\SQLEXPRESS"

# With indexed mode for large backups
bakread --bak large_backup.bak --list-tables --indexed
```

### Allocation Hints (Performance Optimization)

If you know which pages contain your target table, you can provide allocation hints:

```bash
# First, query allocation info from a live database
# SELECT file_id, page_id FROM sys.dm_db_database_page_allocations(...)

# Then use the hints file to filter pages during extraction
bakread --bak backup.bak --table dbo.Orders --out orders.csv --format csv \
        --allocation-hint allocation_info.csv
```

## GUI Application

Two GUI applications are provided:

### Modern GUI (`gui_modern.py`)

Azure Data Studio-inspired interface with:
- Dark theme
- SQL Server connection dialog (Windows/SQL Auth)
- Backup file list management
- TDE certificate configuration
- Indexed mode options for large backups
- Live output streaming with color-coded messages

```bash
python gui_modern.py
```

### Classic GUI (`gui.py`)

Simpler Tkinter-based interface with all essential options.

```bash
python gui.py
```

## All CLI Flags

### Required

| Flag | Description |
|------|-------------|
| `--bak PATH` | Path to a .bak backup file (repeat for striped backups) |
| `--table schema.table` | Schema-qualified table name (e.g., dbo.Orders) |
| `--out PATH` | Output file path |
| `--format csv\|parquet\|jsonl` | Output format |

### Mode Selection

| Flag | Description |
|------|-------------|
| `--mode auto\|direct\|restore` | Execution mode (default: auto) |

### Filtering

| Flag | Description |
|------|-------------|
| `--backupset N` | Select backup set by position |
| `--columns "c1,c2"` | Column filter (comma-separated) |
| `--where "condition"` | SQL WHERE clause (restore mode only) |
| `--max-rows N` | Maximum rows to export |
| `--delimiter ","` | CSV delimiter character |
| `--allocation-hint FILE` | CSV with (file_id,page_id) for page filtering |

### Large Backup Mode

| Flag | Description |
|------|-------------|
| `--indexed` | Use indexed page store (recommended for >1GB) |
| `--cache-size MB` | LRU cache size in MB (default: 256) |
| `--index-dir PATH` | Directory for index files |
| `--force-rescan` | Ignore existing index files |

### SQL Server Connection

| Flag | Description |
|------|-------------|
| `--target-server SERVER` | Target SQL Server for restore mode |
| `--sql-user USER` | SQL Server login (default: Windows Auth) |
| `--sql-password PASS` | SQL Server password (or set `BAKREAD_SQL_PASSWORD`) |

### TDE / Encryption

| Flag | Description |
|------|-------------|
| `--tde-cert-pfx PATH` | Certificate file (.cer or .pfx) |
| `--tde-cert-key PATH` | Private key file (.pvk) |
| `--tde-cert-password VALUE` | Key password (or set `BAKREAD_TDE_PASSWORD`) |
| `--backup-cert-pfx PATH` | Certificate for backup-level encryption |
| `--source-server SERVER` | Source SQL Server for cert export |
| `--master-key-password VALUE` | Database master key password |
| `--allow-key-export-to-disk` | Allow temp key export to disk |
| `--cleanup-keys` | Remove imported certs after extraction |

### Logging

| Flag | Description |
|------|-------------|
| `--verbose, -v` | Enable debug logging |
| `--log FILE` | Write log to file |

### Special Modes

| Flag | Description |
|------|-------------|
| `--list-tables` | List all tables in the backup and exit |
| `--print-data-offset` | Print data region offset and exit |

## Supported Data Types

| Type | Mode A | Mode B | Notes |
|------|--------|--------|-------|
| INT, BIGINT, SMALLINT, TINYINT | Full | Full | |
| BIT | Full | Full | |
| FLOAT, REAL | Full | Full | |
| DECIMAL, NUMERIC | Full | Full | Via double conversion in Mode A |
| MONEY, SMALLMONEY | Full | Full | |
| CHAR, VARCHAR | Full | Full | |
| NCHAR, NVARCHAR | Full | Full | UTF-16LE to UTF-8 conversion |
| DATETIME, DATETIME2 | Full | Full | |
| SMALLDATETIME, DATE | Full | Full | |
| UNIQUEIDENTIFIER | Full | Full | Mixed-endian GUID handling |
| BINARY, VARBINARY | Full | Full | Hex output in CSV/JSON |
| TIME, DATETIMEOFFSET | Partial | Full | Hex in Mode A, string in Mode B |
| TEXT, NTEXT, IMAGE | Pointer only | Full | LOB data requires Mode B |
| XML | No | Full | |
| VARCHAR(MAX), NVARCHAR(MAX) | Pointer only | Full | Row-overflow requires Mode B |

## SQL Server Version Support

- SQL Server 2012 (11.x) through 2022 (16.x)
- Both Standard and Enterprise editions
- Compressed and uncompressed backups
- Striped backups (multiple .bak files)
- TDE-encrypted databases (Mode B only)

## Module Architecture

```
src/
  main.cpp               Entry point and CLI dispatch
  cli.cpp                Argument parsing and validation
  logging.cpp            Timestamped logging to console and file
  backup_stream.cpp      Streaming .bak file reader (4MB buffered IO)
  backup_header.cpp      MTF/SQL Server backup header parser
  decompressor.cpp       LZXPRESS + deflate decompression
  row_decoder.cpp        FixedVar row format parser (all SQL types)
  catalog_reader.cpp     System catalog page scanner
  direct_extractor.cpp   Mode A orchestrator
  restore_adapter.cpp    ODBC-based restore and query (Mode B)
  tde_handler.cpp        TDE certificate detection/provisioning
  csv_writer.cpp         CSV output (UTF-8, RFC 4180 escaping)
  parquet_writer.cpp     Apache Arrow Parquet output (Snappy compression)
  json_writer.cpp        JSON Lines output + writer factory
  pipeline.cpp           Multi-threaded producer-consumer pipeline
  page_index.cpp         Page index for indexed mode
  lru_cache.cpp          LRU cache for page storage
  indexed_page_store.cpp Parallel scanner and indexed page access

include/bakread/
  cli.h                  Options struct and CLI parsing
  types.h                SQL types, row values, progress callbacks
  page.h                 Page header structures (8KB SQL Server pages)
  backup_stream.h        Streaming file reader interface
  backup_header.h        MTF header structures
  decompressor.h         Decompression interface
  catalog_reader.h       System catalog structures
  row_decoder.h          Row decoding interface
  direct_extractor.h     Direct mode interface
  restore_adapter.h      Restore mode interface with ODBC
  page_index.h           Page index for O(1) lookups
  lru_cache.h            Thread-safe LRU cache
  indexed_page_store.h   Parallel scanning and indexed access
```

## Performance Characteristics

- **Streaming IO**: 4MB read buffer, never loads full backup into memory (traditional mode)
- **Indexed mode**: Parallel scanning with configurable LRU cache
- **Memory efficient**: Traditional mode ~500MB, indexed mode configurable (default 256MB cache)
- **Batched Parquet writes**: 64K rows per batch for columnar efficiency
- **Periodic flush**: CSV/JSONL flush every 50K rows for crash safety
- **Progress reporting**: Percentage and row count updates

## Security

- **No plaintext secrets**: Passwords accepted via CLI, environment variables, or secure prompt
- **No key derivation from .bak**: The tool does NOT claim it can recover TDE keys from a backup file
- **Least privilege**: Only requires permissions to restore and query
- **Automatic cleanup**: Temporary databases and certificates are dropped after extraction
- **Secure password handling**: GUI passes SQL passwords via environment variables

## Known Limitations

1. **Mode A reliability**: Direct parsing is best-effort. Complex backups, unusual page layouts, or version-specific structures may not parse correctly.
2. **TDE in Mode A**: Not supported. Encrypted databases require Mode B.
3. **LOB data in Mode A**: MAX types and row-overflow data only return pointer stubs.
4. **Differential/Log backups**: Only full backups are supported.
5. **Compressed striped backups**: Direct mode parsing of large compressed striped backups may be slow during header parsing.

## Testing

### Test Matrix

- SQL Server 2012, 2016, 2019, 2022
- TDE enabled and disabled
- Backup compression on and off
- Heap tables and clustered index tables
- Unicode data (NVARCHAR with CJK characters)
- Large tables (10M+ rows)
- Striped backups (2, 4, 8 stripes)
- All supported data types

### Validation

- Compare exported row counts with `SELECT COUNT(*)` on restored DB
- Checksum comparison of exported data vs. SQL query results
- Round-trip validation for numeric precision

### Test Scripts

See `backup_format_test/` directory for test SQL scripts and automation:
- `run_sql.ps1` - PowerShell script to execute test scenarios
- `sql/` - SQL scripts for creating test databases and backups

## License

Proprietary. All rights reserved.
