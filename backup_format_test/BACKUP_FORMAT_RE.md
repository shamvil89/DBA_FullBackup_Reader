# SQL Server Backup File Format - Reverse Engineering Analysis

This document presents a comprehensive reverse engineering analysis of SQL Server backup (.bak) file format based on binary analysis, differential comparison, and code examination.

---

## 1. Executive Summary

SQL Server backup files use two distinct container formats depending on compression:

| Format | Magic | When Used | Compression |
|--------|-------|-----------|-------------|
| **MTF (Microsoft Tape Format)** | `TAPE` (0x45504154) | Uncompressed backups | None |
| **VDI/MSSQLBAK** | `MSSQLBAK` | Compressed backups | LZXPRESS (LZ77 variant) |

Both formats ultimately contain the same payload: SQL Server database pages (8KB each) plus metadata.

---

## 2. File Format Detection

### 2.1 Magic Number Identification

```
Offset 0x0000:
  Uncompressed: 54 41 50 45 ("TAPE")
  Compressed:   4D 53 53 51 4C 42 41 4B ("MSSQLBAK")
```

### 2.2 Detection Algorithm

```c
bool is_mtf_format(const uint8_t* data) {
    return memcmp(data, "TAPE", 4) == 0;
}

bool is_mssqlbak_format(const uint8_t* data) {
    return memcmp(data, "MSSQLBAK", 8) == 0;
}
```

---

## 3. Uncompressed Backup Format (MTF)

### 3.1 Overall Structure

```
+------------------+ 0x0000
|   MTF TAPE       |  512-4096 bytes
|   Header         |
+------------------+ 0x0400-0x1000
|   SFMB Block     |  Soft filemark block
+------------------+ 0x1000
|   SSET Block     |  Start of backup set
|   (Dataset meta) |
+------------------+ 0x2000
|   VOLB Block     |  Volume info
+------------------+ 0x2400
|   MSCI Block     |  SQL Server Media Component Info
|   (DB name, etc) |
+------------------+ 0x2800
|   MSDA Block     |  SQL Server Data Area (header)
+------------------+ 0x3800
|   APAD padding   |
+------------------+ 0x4000
|   DATABASE PAGES |  8KB aligned SQL Server pages
|   (8KB each)     |
|   ...            |
+------------------+ varies
|   SFMB / ESET    |  End markers
+------------------+ EOF
```

### 3.2 MTF Block Signatures

All MTF blocks are aligned to 512-byte boundaries:

| Signature | Hex Value | Purpose |
|-----------|-----------|---------|
| `TAPE` | 0x45504154 | Tape/media header |
| `SSET` | 0x54455353 | Start of backup set |
| `VOLB` | 0x424C4F56 | Volume block |
| `DIRB` | 0x42524944 | Directory block |
| `FILE` | 0x454C4946 | File block |
| `ESET` | 0x54455345 | End of backup set |
| `SFMB` | 0x424D4653 | Soft filemark block |
| `MSCI` | 0x4943534D | SQL Server media component info |
| `MSDA` | 0x4144534D | SQL Server data area start |

### 3.3 MTF Block Header Structure (46 bytes)

```c
#pragma pack(push, 1)
struct MtfBlockHeader {
    uint32_t block_type;           // 0x00: TAPE, SSET, etc.
    uint32_t block_attributes;     // 0x04: Flags
    uint16_t os_id;                // 0x08: OS identifier
    uint8_t  os_version_major;     // 0x0A
    uint8_t  os_version_minor;     // 0x0B
    uint64_t displayable_size;     // 0x0C: Size for display
    uint64_t format_logical_addr;  // 0x14: Logical block address
    uint16_t reserved_for_mbc;     // 0x1C
    uint8_t  reserved1[6];         // 0x1E
    uint32_t control_block_id;     // 0x24
    uint32_t string_storage_off;   // 0x28
    uint16_t string_storage_size;  // 0x2C
    // OS-specific data follows
};
#pragma pack(pop)
```

### 3.4 TAPE Header Details

Located at file offset 0x0000:

```
Offset  Field                 Size    Example Value
------  -----                 ----    -------------
0x00    Signature "TAPE"      4       54 41 50 45
0x04    Block size            4       00 00 03 00 (196608)
0x08    Format version        2       8C 00 (140)
0x0A    Format logic block    2       0E 01 (270)
0x30    Media family ID       4       (varies)
0x34    Timestamp/LSN         8       (varies per backup)
0x5E    "Microsoft SQL Server" (UTF-16LE string)
```

### 3.5 MSCI Block (SQL Server Media Component Info)

Located at offset 0x2800 (varies):

Contains:
- Database name (UTF-16LE)
- Server name
- Backup timestamp
- Compatibility level
- Encryption/TDE flags

```
Offset  Field                    Notes
------  -----                    -----
0x00    "MSCI" signature         4D 53 43 49
0x38    "MQCI" sub-block         Media Query Component Info
0x50    "SCIN" marker            Component version info
0x9D8+  Database name (UTF-16)   "BackupFormatTestDB"
```

### 3.6 Page Data Region

Database pages start at offset 0x4000 (16384) in analyzed samples:

- Pages are 8KB (8192 bytes) each
- Aligned to 8KB boundaries within the data region
- No per-page container header (raw pages)

---

## 4. Compressed Backup Format (MSSQLBAK/VDI)

### 4.1 Overall Structure

```
+------------------+ 0x0000
|   MSSQLBAK       |  8-byte magic
|   Header         |
+------------------+ 0x0008
|   VDI Metadata   |  Version, flags
+------------------+ 0x1000
|   SFMB Block     |  Soft filemark
+------------------+ 0x2000
|   Compressed     |  LZXPRESS compressed
|   Page Stream    |  database pages
|   ...            |
+------------------+ varies
|   SFMB / ESET    |  End markers
+------------------+ EOF
```

### 4.2 MSSQLBAK Header

```
Offset  Field           Size    Value
------  -----           ----    -----
0x00    Magic           8       "MSSQLBAK"
0x08    Version         2       0x0002
0x0A    Reserved        2       0x0000
0x0C    Flags           4       0x00000003
0x10-1F Padding/FF      varies  FF FF FF FF...
```

### 4.3 Compression Block Structure

Compressed blocks use magic `0xDAC0`:

```c
#pragma pack(push, 1)
struct CompressedBlockHeader {
    uint16_t magic;             // 0xDAC0
    uint16_t header_size;       // Size of this header
    uint32_t compressed_size;   // Compressed data size
    uint32_t uncompressed_size; // Original size
};
#pragma pack(pop)
```

### 4.4 Compression Algorithm: LZXPRESS (LZ77 Variant)

SQL Server uses Microsoft LZXPRESS plain format:

```
Structure:
- 32-bit flags dword controls 32 items
- Flag bit = 0: Next byte is a literal
- Flag bit = 1: Next 2 bytes encode (offset, length) match

Match encoding (16-bit):
  Bits 15-3: Offset - 1 (13 bits, max offset 8191)
  Bits 2-0:  Length - 3 (3 bits, values 0-6 mean 3-9)
  
  If length bits = 7 (0b111):
    Extended length follows as additional byte(s)
```

### 4.5 Entropy Analysis

Analyzed `backup_comp.bak` (516 KB):

| Region | Entropy | Interpretation |
|--------|---------|----------------|
| 0x0000-0x1000 | 0.6 | Header (low entropy) |
| 0x1000-0x2000 | 0.04 | SFMB padding (mostly zeros) |
| 0x2000-0x3000 | 7.6 | Compressed data (high) |
| 0x3000+ | 7.8-7.9 | Compressed data (high) |

---

## 5. SQL Server Page Format

### 5.1 Page Header (96 bytes)

```c
#pragma pack(push, 1)
struct PageHeader {
    uint8_t  header_version;     // 0x00: Always 0x01
    uint8_t  type;               // 0x01: Page type
    uint8_t  type_flag_bits;     // 0x02
    uint8_t  level;              // 0x03: B-tree level
    uint16_t flag_bits;          // 0x04
    uint16_t index_id;           // 0x06
    uint32_t prev_page;          // 0x08
    uint16_t prev_file;          // 0x0C
    uint16_t pminlen;            // 0x0E: Min record length
    uint32_t next_page;          // 0x10
    uint16_t next_file;          // 0x14
    uint16_t slot_count;         // 0x16: Number of records
    uint32_t obj_id;             // 0x18: Allocation unit obj_id
    uint16_t free_count;         // 0x1C
    uint16_t free_data;          // 0x1E: Offset to free space
    uint32_t this_page;          // 0x20: This page's page_id
    uint16_t this_file;          // 0x24: This page's file_id
    uint16_t reserved_count;     // 0x26
    uint32_t lsn_file;           // 0x28: LSN file number
    uint32_t lsn_offset;         // 0x2C: LSN offset
    uint16_t lsn_slot;           // 0x30: LSN slot
    uint16_t xact_reserved;      // 0x32
    uint32_t xdes_id1;           // 0x34
    uint32_t xdes_id2;           // 0x38
    uint16_t ghost_rec_count;    // 0x3C
    uint16_t torn_bits;          // 0x3E: Or checksum
    uint8_t  reserved[32];       // 0x40-0x5F: Padding
};
#pragma pack(pop)
```

### 5.2 Page Types

| Type | Value | Description |
|------|-------|-------------|
| Data | 1 | User/system data pages |
| Index | 2 | Index pages |
| TextMix | 3 | LOB mixed extent |
| TextTree | 4 | LOB tree root |
| Sort | 7 | Sort temporary |
| GAM | 8 | Global Allocation Map |
| SGAM | 9 | Shared GAM |
| IAM | 10 | Index Allocation Map |
| PFS | 11 | Page Free Space |
| Boot | 13 | Database boot page |
| FileHeader | 15 | File header page |
| DiffMap | 16 | Differential map |
| MLMap | 17 | Minimally logged map |

### 5.3 Page Layout

```
+------------------+ Offset 0
|   Page Header    |  96 bytes (0x60)
+------------------+ Offset 96
|   Data Area      |  Variable (rows stored here)
|   (grows down)   |
+------------------+
|   Free Space     |
+------------------+
|   Slot Array     |  2 bytes per slot (grows up from end)
+------------------+ Offset 8192
```

### 5.4 Record (Row) Structure

```c
struct RowHeader {
    uint8_t  status_bits_a;   // Bit flags (null bitmap, var cols, etc.)
    uint8_t  status_bits_b;   // Additional flags
    uint16_t fixed_data_end;  // Offset where fixed columns end
    // Fixed-length columns follow
    // Then: null bitmap (2-byte count + ceil(count/8) bytes)
    // Then: variable-length column offsets + data
};
```

Status bits:
- 0x10: Has null bitmap
- 0x20: Has variable-length columns
- 0x40: Has version tag
- 0x04: Forwarded stub
- 0x07 mask: Record type (0=primary, 1=forwarded, etc.)

---

## 6. Differential Analysis Results

### 6.1 Determinism Test (twice1.bak vs twice2.bak)

Same database backed up twice without changes:

```
Files are NOT byte-identical
390 difference regions found
First difference at offset 0x34 (52 bytes into file)
```

**Varying fields:**
- Offset 0x34-0x37: Timestamp/LSN (different per backup)
- Offset 0xA2-0xB1: GUID (media set identifier)
- Offset 0x2064+: Backup set identifiers

**Conclusion:** Backup files contain per-backup timestamps and identifiers that change even when database content is identical.

### 6.2 Structure After Data Change

Comparing backup before and after a row update:

```
First difference: Still at offset 0x34 (header)
File size change: 3,198,976 -> 3,330,048 (+131 KB)
```

**Conclusion:** Both header metadata and data region change. Cannot rely on fixed offsets; must parse headers.

---

## 7. System Catalog Structure

### 7.1 Well-Known Object IDs (Page Header obj_id)

| obj_id | System Table | Purpose |
|--------|--------------|---------|
| 5 | sys.sysrowsets | Rowset metadata |
| 7 | sys.sysallocunits | Allocation units |
| 27 | sys.sysowners | Schema owners |
| 34 | sys.sysschobjs | All objects |
| 41 | sys.syscolpars | Columns |
| 54 | sys.sysidxstats | Index statistics |

### 7.2 Object Resolution Chain

To find data pages for a user table:

```
1. sys.sysschobjs (obj_id=34)
   -> Find table by name, get object_id

2. sys.sysrowsets (obj_id=5)  
   -> Map object_id to rowsetid (hobt_id)

3. sys.sysallocunits (obj_id=7)
   -> Map container_id (=hobt_id) to allocation_unit_id
   -> Page header obj_id = (allocation_unit_id >> 16) & 0xFFFF

4. Scan data pages where page.obj_id matches calculated value
```

---

## 8. Signature Matching Summary

### 8.1 File-Level Signatures

| Offset | Signature | Format |
|--------|-----------|--------|
| 0x0000 | `TAPE` | Uncompressed MTF |
| 0x0000 | `MSSQLBAK` | Compressed VDI |
| 0x1000 | `SFMB` | Soft filemark |
| 0x2000 | `SSET` | Backup set start |
| varies | `MSCI` | SQL Server media info |
| varies | `MSDA` | Data area start |
| varies | `ESET` | Backup set end |

### 8.2 Compression Signatures

| Signature | Hex | Meaning |
|-----------|-----|---------|
| 0xDAC0 | C0 DA | Compressed block header |
| zlib | 78 9C | Standard zlib (rare) |

### 8.3 Page-Level Signatures

| Field | Valid Range | Check |
|-------|-------------|-------|
| header_version | 0x01 | Must equal 1 |
| type | 1-17 | Excluding 5,6,12,14 |
| this_file | 1-32 | Reasonable file ID |
| slot_count | 0-999 | Records per page |
| free_count | 0-8192 | Free bytes |

---

## 9. Implementation Guidance

### 9.1 Reading Uncompressed Backups

```python
def read_uncompressed_backup(path):
    with open(path, 'rb') as f:
        # Verify TAPE signature
        sig = f.read(4)
        assert sig == b'TAPE'
        
        # Find MSDA block (scan 512-byte aligned)
        f.seek(0)
        while True:
            pos = f.tell()
            block = f.read(4)
            if block == b'MSDA':
                data_start = pos
                break
            f.seek(pos + 512)
        
        # Pages start at next 8KB boundary after MSDA
        page_start = (data_start + 8191) & ~8191
        if page_start < 0x4000:
            page_start = 0x4000
        
        # Read pages
        f.seek(page_start)
        while True:
            page = f.read(8192)
            if len(page) < 8192:
                break
            yield parse_page(page)
```

### 9.2 Reading Compressed Backups

```python
def read_compressed_backup(path):
    with open(path, 'rb') as f:
        # Verify MSSQLBAK signature
        sig = f.read(8)
        assert sig == b'MSSQLBAK'
        
        # Find compressed data region (after SFMB)
        f.seek(0x2000)  # Typical compressed data start
        
        # Decompress stream
        decompressed = decompress_stream(f.read())
        
        # Parse pages from decompressed data
        offset = 0
        while offset + 8192 <= len(decompressed):
            yield parse_page(decompressed[offset:offset+8192])
            offset += 8192
```

---

## 10. Tools and References

### 10.1 Analysis Tools Used

- PowerShell for binary inspection
- Custom hex dump and entropy analysis
- Differential comparison scripts

### 10.2 Related Documentation

- Microsoft Tape Format (MTF) Specification
- SQL Server Internals books (Delaney, et al.)
- VDI (Virtual Device Interface) documentation

### 10.3 Files Analyzed

| File | Size | Type | Purpose |
|------|------|------|---------|
| backup_unc.bak | 3.05 MB | Uncompressed | Primary analysis |
| backup_comp.bak | 516 KB | Compressed | Compression analysis |
| twice1.bak | 3.05 MB | Uncompressed | Determinism test |
| twice2.bak | 3.05 MB | Uncompressed | Determinism test |

---

## 11. Appendix: Hex Dump Samples

### A.1 TAPE Header (First 128 bytes)

```
0000: 54 41 50 45 00 00 03 00 8C 00 0E 01 00 00 00 00  TAPE............
0010: 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  ................
0020: 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  ................
0030: 02 00 87 05 22 7D 99 54 05 00 00 00 01 00 00 00  ...."}.T........
0040: 08 00 03 00 00 00 00 00 00 00 00 00 00 00 00 00  ................
0050: 2C 00 5E 00 00 04 00 12 1F A8 AA 22 8D 01 4D 00  ,.^........"..M.
0060: 69 00 63 00 72 00 6F 00 73 00 6F 00 66 00 74 00  i.c.r.o.s.o.f.t.
0070: 20 00 53 00 51 00 4C 00 20 00 53 00 65 00 72 00   .S.Q.L. .S.e.r.
```

### A.2 FileHeader Page (First 128 bytes)

```
4000: 01 0F 00 00 00 02 00 00 00 00 00 00 00 00 00 00  ................
4010: 00 00 00 00 00 00 01 00 63 00 00 00 06 1B F8 04  ........c.......
4020: 00 00 00 00 01 00 00 00 00 00 00 00 00 00 00 00  ................
4030: 01 00 00 00 00 00 00 00 00 00 00 00 1B ED 4C 3D  ..............L=
4040: 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  ................
4050: 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00 00  ................
4060: 30 00 08 00 00 00 00 00 3C 00 00 00 00 00 00 00  0.......<.......
4070: 00 00 3B 00 9A 00 9A 00 9C 00 9E 00 A2 00 A6 00  ..;.............
```

### A.3 MSSQLBAK Header

```
0000: 4D 53 53 51 4C 42 41 4B 02 00 00 00 03 00 00 00  MSSQLBAK........
0010: FF FF FF FF FF FF FF FF 00 00 00 00 00 00 00 00  ................
```

---

## 12. TDE (Transparent Data Encryption) Analysis

### 12.1 TDE Backup Characteristics

TDE-encrypted databases produce uncompressed MTF backups where:
- File header and MTF blocks are NOT encrypted
- SQL Server page headers (first 96 bytes) are NOT encrypted
- Page DATA (bytes 96-8191) IS encrypted with AES-256

### 12.2 Entropy Profile

```
Region          Entropy    Status
------          -------    ------
0x0000-0x4000   0.2-3.7    Headers (unencrypted)
0x4000-0x6000   3.7        System pages (partially encrypted)
0x6000+         7.9+       Data pages (fully encrypted body)
```

### 12.3 Page Header Visibility

TDE does NOT encrypt page headers:

```
TDE Page at 0x6000 (PFS page):
006000: 01 0B 00 00 00 0A 00 00  <- header_version=1, type=11 (PFS)
006010: 00 00 00 00 00 00 01 00  <- readable metadata
006020: 63 00 00 00 02 00 FC 1F  <- obj_id=99, free info
...
006060: [ENCRYPTED DATA STARTS]  <- page body is ciphertext
```

### 12.4 Detection Method

To detect TDE in direct mode:
1. Parse page headers (readable)
2. Check entropy of page body (bytes 96-8191)
3. If entropy > 7.5 AND pages appear valid, likely TDE

### 12.5 TDE Key Hierarchy

```
Master Key (master database)
    |
    v
Certificate (database-level)
    |
    v
Database Encryption Key (DEK)
    |
    v
AES-256 encryption of each page
```

To decrypt: Need certificate + private key + DEK

---

## 13. File Statistics Summary

### 13.1 Analyzed Files

| File | Size | Format | Pages |
|------|------|--------|-------|
| backup_unc.bak | 3.05 MB | MTF | 313 |
| backup_comp.bak | 516 KB | VDI | ~313 compressed |
| backup_large.bak | 64 MB | MTF | ~7,867 |
| tde_backup.bak | 3.17 MB | MTF | ~327 |

### 13.2 Page Type Distribution (typical database)

| Type | Count | Percentage |
|------|-------|------------|
| Data | 134 | 42.8% |
| Index | 118 | 37.7% |
| IAM | 51 | 16.3% |
| TextMix | 3 | 1.0% |
| System (GAM/SGAM/PFS/etc) | 7 | 2.2% |

### 13.3 Compression Ratio

```
Uncompressed: 3,198,976 bytes (3.05 MB)
Compressed:     528,384 bytes (516 KB)
Ratio: 6.05:1 (83.5% reduction)
```

---

## 14. Key Findings Summary

1. **Two Formats**: Uncompressed uses MTF (TAPE header), compressed uses VDI (MSSQLBAK header)

2. **Non-Deterministic**: Same database produces different backups each time (timestamps, LSNs vary)

3. **Page Alignment**: Database pages start at 8KB boundaries, typically at file offset 0x4000+

4. **Compression**: Uses LZXPRESS (LZ77 variant) with 0xDAC0 block magic

5. **TDE**: Encrypts page body (bytes 96+) but NOT page headers (bytes 0-95)

6. **System Catalog**: Use obj_id mapping chain: object_id -> rowsetid -> allocation_unit_id -> page obj_id

7. **Variable Header Size**: Must scan for MTF blocks; cannot assume fixed offsets

---

*Document generated: 2026-02-21*
*Analysis performed on SQL Server 2022 backups*
*Tools: PowerShell binary analysis, entropy calculation, differential comparison*
