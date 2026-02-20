# TDE (Transparent Data Encryption) Testing Results

## Test Setup

1. Created a TDE-encrypted database `TDETestDB` with:
   - Master key in master database
   - Certificate `TDE_Test_Cert` for encryption
   - Database encryption key with AES_256 algorithm
   - Test table `dbo.SecretData` with 5 rows of sample data

2. Created backup:
   - `tde_backup.bak` - uncompressed backup of the TDE database (3.3 MB)
   
3. Exported certificate files:
   - `tde_cert.cer` - certificate file
   - `tde_cert_key.pvk` - private key file (encrypted with password)

## Test Results

### Direct Mode

**Result: FAILED (expected)**

```
[INFO ] Phase 3: Resolving table 'dbo.SecretData' from system catalog...
[INFO ] Found 0 system objects via page scan
[INFO ] Direct mode failed: Failed to resolve table 'dbo.SecretData' from system catalog
```

**Why it fails:**
- TDE encrypts data pages at rest using the database encryption key
- Pages in the backup are encrypted - the raw bytes cannot be parsed
- Without the decryption key, bakread cannot read system catalog pages
- bakread's direct mode currently has no TDE decryption capability

**Technical notes:**
- TDE uses symmetric key encryption (AES) on 8KB pages
- The backup header does NOT indicate TDE status (bakread shows `TDE=no`)
- Pages appear valid at 8KB boundaries but content is ciphertext

### Restore Mode

**Result: SUCCESS**

```
[INFO ] Database restored successfully: bakread_tmp_4754_8164
[INFO ] Restore extraction complete: 5 rows
[INFO ] SUCCESS: 5 rows exported
```

**Requirements:**
- The TDE certificate must exist on the target SQL Server BEFORE restore
- If restoring to a different server, the certificate must be imported first
- bakread's `--tde-cert-pfx` option can import certificates (when using PFX format)

**Extracted data:**
```csv
id,secret_value,amount,created
1,Confidential record 1,1000.50,2026-02-20 21:34:40
2,Sensitive data item 2,2500.75,2026-02-20 21:34:40
3,Protected information 3,750.00,2026-02-20 21:34:40
4,Encrypted content 4,3200.25,2026-02-20 21:34:40
5,Secret value 5,150.99,2026-02-20 21:34:40
```

## Bug Fix Applied

During testing, discovered an ODBC issue where RESTORE DATABASE would return successfully but leave the database in RESTORING state. The fix:

**Problem:** `SQLMoreResults()` returns `SQL_SUCCESS_WITH_INFO` for informational messages during RESTORE, but the code only checked for `SQL_SUCCESS`.

**Fix:** Updated `OdbcConnection::execute()` to handle both `SQL_SUCCESS` and `SQL_SUCCESS_WITH_INFO` when consuming result sets after RESTORE commands.

```cpp
// Before (broken):
while (SQLMoreResults(stmt_) == SQL_SUCCESS) { ... }

// After (fixed):
while ((mr = SQLMoreResults(stmt_)) == SQL_SUCCESS || mr == SQL_SUCCESS_WITH_INFO) { ... }
```

## Recommendations

### For TDE Database Extraction:

1. **Same Server:** Use restore mode - the certificate is already present
2. **Different Server:**
   - Export certificate from source: `BACKUP CERTIFICATE ... TO FILE ... WITH PRIVATE KEY`
   - Convert to PFX if needed (using `pvk2pfx.exe` or OpenSSL)
   - Use bakread with `--tde-cert-pfx` and `--tde-cert-password`

### For Direct Mode TDE Support (Future Work):

Adding TDE decryption to direct mode would require:
1. Parsing the TDE metadata from backup header (certificate thumbprint)
2. Loading the certificate/private key
3. Implementing AES decryption for each data page
4. Significant complexity - may not be worth it vs restore mode

## Files Created

- `backup_format_test/outputs/tde_backup.bak` - TDE database backup
- `backup_format_test/outputs/tde_cert.cer` - Certificate export
- `backup_format_test/outputs/tde_cert_key.pvk` - Private key export
- `backup_format_test/outputs/tde_extract.csv` - Extracted data via restore mode
