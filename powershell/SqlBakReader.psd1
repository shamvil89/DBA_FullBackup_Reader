@{
    RootModule = 'SqlBakReader.psm1'
    ModuleVersion = '1.0.0'
    GUID = 'a8f2c4e6-1b3d-4e5f-8a9c-0d1e2f3a4b5c'
    Author = 'SQLBAKReader Team'
    CompanyName = 'SQLBAKReader'
    Copyright = '(c) 2026. All rights reserved.'
    Description = 'PowerShell module for reading SQL Server .bak backup files directly without requiring SQL Server. Supports listing tables, extracting data, and querying backup metadata.'
    PowerShellVersion = '5.1'
    
    FunctionsToExport = @(
        'Get-BakInfo',
        'Get-BakTable',
        'Read-BakTable',
        'Open-BakReader',
        'Close-BakReader'
    )
    
    CmdletsToExport = @()
    VariablesToExport = @()
    AliasesToExport = @()
    
    PrivateData = @{
        PSData = @{
            Tags = @('SQL', 'SQLServer', 'Backup', 'BAK', 'Database', 'Recovery')
            LicenseUri = ''
            ProjectUri = ''
            ReleaseNotes = @'
Version 1.0.0
- Initial release
- Get-BakInfo: Read backup file metadata
- Get-BakTable: List all user tables in backup
- Read-BakTable: Extract table data from backup
- Open-BakReader/Close-BakReader: Manual reader lifecycle management
'@
        }
    }
}
