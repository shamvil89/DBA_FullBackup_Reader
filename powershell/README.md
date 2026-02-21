# SqlBakReader PowerShell Module

PowerShell module for reading SQL Server .bak backup files directly without requiring SQL Server.

## Requirements

- PowerShell 5.1 or later (Windows PowerShell or PowerShell Core)
- Windows x64
- The `bakread.dll` native library (built from the SQLBAKReader project)

## Installation

### Option 1: Build from Source

1. Build the SQLBAKReader project:
   ```powershell
   cd F:\SQLBAKReader
   cmake -B build -S . -DCMAKE_BUILD_TYPE=Release
   cmake --build build --config Release
   ```

2. The build process automatically copies `bakread.dll` to the `powershell/bin` folder.

3. Import the module:
   ```powershell
   Import-Module F:\SQLBAKReader\powershell\SqlBakReader.psd1
   ```

### Option 2: Manual Installation

1. Copy the `powershell` folder to a location in your PowerShell module path:
   ```powershell
   Copy-Item -Recurse F:\SQLBAKReader\powershell "$env:USERPROFILE\Documents\PowerShell\Modules\SqlBakReader"
   ```

2. Copy `bakread.dll` and any dependencies to the module's `bin` folder.

3. Import the module:
   ```powershell
   Import-Module SqlBakReader
   ```

## Usage

### Get Backup Information

```powershell
# Get metadata about a backup file
Get-BakInfo -Path "C:\Backups\MyDatabase.bak"

# Output:
# Path               : C:\Backups\MyDatabase.bak
# DatabaseName       : MyDatabase
# ServerName         : SQLSERVER01
# BackupType         : Full
# CompatibilityLevel : 150
# IsCompressed       : True
# IsEncrypted        : False
# IsTde              : False
# BackupSize         : 1073741824
# CompressedSize     : 268435456
# CompressionRatio   : 75
# BackupStartDate    : 2024-01-15 10:30:00
# BackupFinishDate   : 2024-01-15 10:35:00
```

### List Tables in Backup

```powershell
# List all user tables
Get-BakTable -Path "C:\Backups\MyDatabase.bak"

# Filter by schema
Get-BakTable -Path "C:\Backups\MyDatabase.bak" | Where-Object { $_.SchemaName -eq 'dbo' }

# Output:
# SchemaName TableName   FullName       ObjectId RowCount PageCount
# ---------- ---------   --------       -------- -------- ---------
# dbo        Users       dbo.Users      12345    1000     50
# dbo        Orders      dbo.Orders     12346    50000    2500
# dbo        Products    dbo.Products   12347    500      25
```

### Read Table Data

```powershell
# Read all rows from a table
Read-BakTable -Path "C:\Backups\MyDatabase.bak" -Table "dbo.Users"

# Read with row limit
Read-BakTable -Path "C:\Backups\MyDatabase.bak" -Table "dbo.Orders" -MaxRows 100

# Read specific columns
Read-BakTable -Path "C:\Backups\MyDatabase.bak" -Table "dbo.Products" -Column "Id", "Name", "Price"

# Export to CSV
Read-BakTable -Path "C:\Backups\MyDatabase.bak" -Table "dbo.Users" | Export-Csv -Path "users.csv" -NoTypeInformation

# Use indexed mode for large backups (reduces memory usage)
Read-BakTable -Path "C:\Backups\LargeDatabase.bak" -Table "dbo.BigTable" -IndexedMode -CacheSizeMB 512
```

### Striped Backups

```powershell
# Work with striped backup files
$stripePaths = @(
    "C:\Backups\MyDatabase_stripe1.bak",
    "C:\Backups\MyDatabase_stripe2.bak",
    "C:\Backups\MyDatabase_stripe3.bak"
)

Get-BakInfo -Path $stripePaths
Get-BakTable -Path $stripePaths
Read-BakTable -Path $stripePaths -Table "dbo.Users"
```

### Pipeline Usage

```powershell
# Process multiple backup files
Get-ChildItem "C:\Backups\*.bak" | ForEach-Object {
    $info = Get-BakInfo -Path $_.FullName
    [PSCustomObject]@{
        File = $_.Name
        Database = $info.DatabaseName
        BackupType = $info.BackupType
        Size = "{0:N2} GB" -f ($info.BackupSize / 1GB)
    }
}

# Compare table schemas across backups
$schema1 = Get-BakTable -Path "C:\Backups\Backup1.bak"
$schema2 = Get-BakTable -Path "C:\Backups\Backup2.bak"
Compare-Object $schema1 $schema2 -Property FullName
```

### Advanced: Manual Handle Management

For performance-critical scenarios where you need to perform multiple operations:

```powershell
$reader = Open-BakReader -Path "C:\Backups\MyDatabase.bak"
try {
    # Perform multiple operations with the same reader
    # (Currently limited - most operations create their own readers)
}
finally {
    Close-BakReader $reader
}
```

## Limitations

- **TDE-encrypted backups**: Direct extraction is not supported for TDE-encrypted databases. The backup must be restored to SQL Server first.
- **Backup-level encryption**: Encrypted backup files are not supported in direct mode.
- **LOB data**: Large object types (TEXT, NTEXT, IMAGE, XML) with data stored off-row may not be fully extracted.
- **Complex page layouts**: Some advanced table features may not be fully supported.

## Troubleshooting

### "bakread.dll not found"

Ensure the DLL is in one of these locations:
- `<module>/bin/bakread.dll`
- `<module>/bakread.dll`
- `<repo>/build/Release/bakread.dll`

### "Failed to open backup file"

- Verify the .bak file exists and is accessible
- Ensure the file is a valid SQL Server backup (not a compressed archive)
- Check if the backup is encrypted or TDE-protected

### Memory errors with large backups

Use the `-IndexedMode` parameter with `Read-BakTable`:
```powershell
Read-BakTable -Path "large.bak" -Table "dbo.BigTable" -IndexedMode -CacheSizeMB 512
```

## Functions

| Function | Description |
|----------|-------------|
| `Get-BakInfo` | Get backup file metadata |
| `Get-BakTable` | List all user tables in backup |
| `Read-BakTable` | Extract table data from backup |
| `Open-BakReader` | Open a reader handle for advanced use |
| `Close-BakReader` | Close a reader handle |

## License

See the main SQLBAKReader project for license information.
