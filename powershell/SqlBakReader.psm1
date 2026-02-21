#Requires -Version 5.1

# SqlBakReader PowerShell Module
# Provides direct access to SQL Server .bak backup files without requiring SQL Server

$script:ModulePath = $PSScriptRoot
$script:DllPath = $null
$script:DllLoaded = $false
$script:DllHandle = [IntPtr]::Zero

#region P/Invoke Definitions

$script:BakReadApi = @"
using System;
using System.Runtime.InteropServices;

namespace SqlBakReader
{
    public static class NativeLibrary
    {
        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern IntPtr LoadLibrary(string lpFileName);
        
        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern IntPtr GetProcAddress(IntPtr hModule, string lpProcName);
        
        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern bool FreeLibrary(IntPtr hModule);
        
        [DllImport("kernel32.dll", SetLastError = true)]
        public static extern uint GetLastError();
        
        [DllImport("kernel32.dll", CharSet = CharSet.Auto, SetLastError = true)]
        public static extern bool SetDllDirectory(string lpPathName);
    }

    public enum BakReadResult : int
    {
        OK = 0,
        ErrorFileNotFound = 1,
        ErrorInvalidFormat = 2,
        ErrorTdeDetected = 3,
        ErrorEncryptionDetected = 4,
        ErrorTableNotFound = 5,
        ErrorInternal = 6,
        ErrorInvalidHandle = 7,
        ErrorNoMoreRows = 8
    }

    public enum BakSqlType : byte
    {
        Unknown = 0,
        TinyInt = 48,
        SmallInt = 52,
        Int = 56,
        BigInt = 127,
        Bit = 104,
        Float = 62,
        Real = 59,
        Decimal = 106,
        Numeric = 108,
        Money = 60,
        SmallMoney = 122,
        Date = 40,
        Time = 41,
        DateTime = 61,
        DateTime2 = 42,
        SmallDateTime = 58,
        DateTimeOffset = 43,
        Char = 175,
        VarChar = 167,
        NChar = 239,
        NVarChar = 231,
        Text = 35,
        NText = 99,
        Binary = 173,
        VarBinary = 165,
        Image = 34,
        UniqueId = 36,
        Xml = 241,
        Timestamp = 189,
        SqlVariant = 98
    }

    public enum BakBackupType : byte
    {
        Unknown = 0,
        Full = 1,
        Differential = 2,
        Log = 3
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct BakBackupInfo
    {
        public IntPtr DatabaseName;
        public IntPtr ServerName;
        public BakBackupType BackupType;
        public int CompatibilityLevel;
        public int IsCompressed;
        public int IsEncrypted;
        public int IsTde;
        public ulong BackupSize;
        public ulong CompressedSize;
        public IntPtr BackupStartDate;
        public IntPtr BackupFinishDate;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct BakTableInfoData
    {
        public IntPtr SchemaName;
        public IntPtr TableName;
        public IntPtr FullName;
        public int ObjectId;
        public long RowCount;
        public long PageCount;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct BakColumnInfo
    {
        public IntPtr Name;
        public BakSqlType Type;
        public short MaxLength;
        public byte Precision;
        public byte Scale;
        public int IsNullable;
        public int IsIdentity;
        public int IsComputed;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct BakModuleInfo
    {
        public int ObjectId;
        public IntPtr SchemaName;
        public IntPtr Name;
        public IntPtr Type;
        public IntPtr TypeDesc;
        public IntPtr Definition;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct BakPrincipalInfo
    {
        public int PrincipalId;
        public IntPtr Name;
        public IntPtr Type;
        public IntPtr TypeDesc;
        public int OwningPrincipalId;
        public IntPtr DefaultSchema;
        public int IsFixedRole;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct BakRoleMemberInfo
    {
        public int RolePrincipalId;
        public int MemberPrincipalId;
        public IntPtr RoleName;
        public IntPtr MemberName;
    }

    [StructLayout(LayoutKind.Sequential)]
    public struct BakPermissionInfo
    {
        public int ClassType;
        public IntPtr ClassDesc;
        public int MajorId;
        public int MinorId;
        public IntPtr PermissionName;
        public IntPtr State;
        public IntPtr GranteeName;
        public IntPtr GrantorName;
        public IntPtr ObjectName;
        public IntPtr SchemaName;
    }

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate void BakProgressCallback(
        ulong bytesProcessed,
        ulong bytesTotal,
        ulong rowsExported,
        double pct,
        IntPtr userData);

    [UnmanagedFunctionPointer(CallingConvention.Cdecl)]
    public delegate int BakRowCallback(
        IntPtr values,
        int columnCount,
        IntPtr userData);

    public static class BakReadApi
    {
        private const string DllName = "bakread.dll";

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern BakReadResult bakread_open(
            [MarshalAs(UnmanagedType.LPArray, ArraySubType = UnmanagedType.LPStr)] string[] bakPaths,
            int pathCount,
            out IntPtr outHandle);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern void bakread_close(IntPtr handle);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr bakread_get_error(IntPtr handle);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern BakReadResult bakread_get_info(IntPtr handle, out BakBackupInfo outInfo);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern BakReadResult bakread_list_tables(
            IntPtr handle,
            out IntPtr outTables,
            out int outCount);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern void bakread_free_table_list(IntPtr tables, int count);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern BakReadResult bakread_set_table(
            IntPtr handle,
            [MarshalAs(UnmanagedType.LPStr)] string schema,
            [MarshalAs(UnmanagedType.LPStr)] string table);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern BakReadResult bakread_set_columns(
            IntPtr handle,
            [MarshalAs(UnmanagedType.LPArray, ArraySubType = UnmanagedType.LPStr)] string[] columns,
            int columnCount);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern BakReadResult bakread_set_max_rows(IntPtr handle, long maxRows);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern BakReadResult bakread_set_indexed_mode(IntPtr handle, int enabled, UIntPtr cacheMb);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern BakReadResult bakread_get_schema(
            IntPtr handle,
            out IntPtr outColumns,
            out int outCount);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern void bakread_free_schema(IntPtr columns, int count);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern BakReadResult bakread_extract(
            IntPtr handle,
            BakRowCallback callback,
            IntPtr userData,
            out ulong outRowCount);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern IntPtr bakread_version();

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern BakReadResult bakread_list_modules(
            IntPtr handle,
            out IntPtr outModules,
            out int outCount);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern void bakread_free_module_list(IntPtr modules, int count);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern BakReadResult bakread_list_principals(
            IntPtr handle,
            out IntPtr outPrincipals,
            out int outCount);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern void bakread_free_principal_list(IntPtr principals, int count);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern BakReadResult bakread_list_role_members(
            IntPtr handle,
            out IntPtr outMembers,
            out int outCount);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern void bakread_free_role_member_list(IntPtr members, int count);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern BakReadResult bakread_list_permissions(
            IntPtr handle,
            out IntPtr outPermissions,
            out int outCount);

        [DllImport(DllName, CallingConvention = CallingConvention.Cdecl)]
        public static extern void bakread_free_permission_list(IntPtr permissions, int count);

        public static string GetString(IntPtr ptr)
        {
            if (ptr == IntPtr.Zero) return null;
            return Marshal.PtrToStringAnsi(ptr);
        }

        public static string GetError(IntPtr handle)
        {
            var ptr = bakread_get_error(handle);
            return GetString(ptr);
        }
    }
}
"@

#endregion

#region Module Initialization

function Initialize-BakReadApi {
    if ($script:DllLoaded) { return $true }
    
    # Find the DLL
    $possiblePaths = @(
        (Join-Path $script:ModulePath "bin\bakread.dll"),
        (Join-Path $script:ModulePath "bakread.dll"),
        (Join-Path (Split-Path $script:ModulePath -Parent) "build\Release\bakread.dll"),
        (Join-Path (Split-Path $script:ModulePath -Parent) "build\Debug\bakread.dll")
    )
    
    foreach ($path in $possiblePaths) {
        if (Test-Path $path) {
            $script:DllPath = (Resolve-Path $path).Path
            break
        }
    }
    
    if (-not $script:DllPath) {
        throw "bakread.dll not found. Please build the project first or copy bakread.dll to the module's bin folder."
    }
    
    # Add DLL directory to search path using SetDllDirectory
    $dllDir = Split-Path $script:DllPath -Parent
    
    # Load the type first so NativeLibrary is available
    try {
        Add-Type -TypeDefinition $script:BakReadApi -ErrorAction Stop
    }
    catch {
        if ($_.Exception.Message -notmatch "already exists") {
            throw
        }
    }
    
    # Set DLL directory for dependent DLLs
    [SqlBakReader.NativeLibrary]::SetDllDirectory($dllDir) | Out-Null
    
    # Pre-load the DLL explicitly
    $script:DllHandle = [SqlBakReader.NativeLibrary]::LoadLibrary($script:DllPath)
    if ($script:DllHandle -eq [IntPtr]::Zero) {
        $errorCode = [SqlBakReader.NativeLibrary]::GetLastError()
        throw "Failed to load bakread.dll from '$($script:DllPath)'. Error code: $errorCode"
    }
    
    $script:DllLoaded = $true
    return $true
}

#endregion

#region Helper Functions

function ConvertTo-BakTableInfo {
    param([SqlBakReader.BakTableInfoData]$TableData)
    
    [PSCustomObject]@{
        PSTypeName   = 'SqlBakReader.TableInfo'
        SchemaName   = [SqlBakReader.BakReadApi]::GetString($TableData.SchemaName)
        TableName    = [SqlBakReader.BakReadApi]::GetString($TableData.TableName)
        FullName     = [SqlBakReader.BakReadApi]::GetString($TableData.FullName)
        ObjectId     = $TableData.ObjectId
        RowCount     = if ($TableData.RowCount -ge 0) { $TableData.RowCount } else { $null }
        PageCount    = if ($TableData.PageCount -ge 0) { $TableData.PageCount } else { $null }
    }
}

function ConvertTo-BakColumnInfo {
    param([SqlBakReader.BakColumnInfo]$ColumnData)
    
    [PSCustomObject]@{
        PSTypeName   = 'SqlBakReader.ColumnInfo'
        Name         = [SqlBakReader.BakReadApi]::GetString($ColumnData.Name)
        Type         = $ColumnData.Type.ToString()
        MaxLength    = $ColumnData.MaxLength
        Precision    = $ColumnData.Precision
        Scale        = $ColumnData.Scale
        IsNullable   = $ColumnData.IsNullable -ne 0
        IsIdentity   = $ColumnData.IsIdentity -ne 0
        IsComputed   = $ColumnData.IsComputed -ne 0
    }
}

function ConvertTo-BackupTypeString {
    param([SqlBakReader.BakBackupType]$BackupType)
    
    switch ($BackupType) {
        'Full' { 'Full' }
        'Differential' { 'Differential' }
        'Log' { 'Transaction Log' }
        default { 'Unknown' }
    }
}

function Get-SqlTypeName {
    param([SqlBakReader.BakSqlType]$SqlType)
    
    switch ($SqlType) {
        'TinyInt' { 'tinyint' }
        'SmallInt' { 'smallint' }
        'Int' { 'int' }
        'BigInt' { 'bigint' }
        'Bit' { 'bit' }
        'Float' { 'float' }
        'Real' { 'real' }
        'Decimal' { 'decimal' }
        'Numeric' { 'numeric' }
        'Money' { 'money' }
        'SmallMoney' { 'smallmoney' }
        'Date' { 'date' }
        'Time' { 'time' }
        'DateTime' { 'datetime' }
        'DateTime2' { 'datetime2' }
        'SmallDateTime' { 'smalldatetime' }
        'DateTimeOffset' { 'datetimeoffset' }
        'Char' { 'char' }
        'VarChar' { 'varchar' }
        'NChar' { 'nchar' }
        'NVarChar' { 'nvarchar' }
        'Text' { 'text' }
        'NText' { 'ntext' }
        'Binary' { 'binary' }
        'VarBinary' { 'varbinary' }
        'Image' { 'image' }
        'UniqueId' { 'uniqueidentifier' }
        'Xml' { 'xml' }
        'Timestamp' { 'timestamp' }
        'SqlVariant' { 'sql_variant' }
        default { 'unknown' }
    }
}

#endregion

#region Public Functions

<#
.SYNOPSIS
    Gets metadata information about a SQL Server backup file.

.DESCRIPTION
    Reads the backup header from a .bak file and returns metadata including
    database name, server name, backup type, compression status, and more.

.PARAMETER Path
    Path to one or more .bak files. For striped backups, provide all stripe files.

.EXAMPLE
    Get-BakInfo -Path "C:\Backups\MyDatabase.bak"
    
    Returns metadata for the specified backup file.

.EXAMPLE
    Get-BakInfo -Path "C:\Backups\stripe1.bak", "C:\Backups\stripe2.bak"
    
    Returns metadata for a striped backup.

.OUTPUTS
    PSCustomObject with backup metadata properties.
#>
function Get-BakInfo {
    [CmdletBinding()]
    [OutputType([PSCustomObject])]
    param(
        [Parameter(Mandatory = $true, Position = 0, ValueFromPipeline = $true)]
        [ValidateScript({ Test-Path $_ })]
        [string[]]$Path
    )
    
    begin {
        Initialize-BakReadApi | Out-Null
    }
    
    process {
        $resolvedPaths = $Path | ForEach-Object { (Resolve-Path $_).Path }
        
        $handle = [IntPtr]::Zero
        try {
            $result = [SqlBakReader.BakReadApi]::bakread_open($resolvedPaths, $resolvedPaths.Count, [ref]$handle)
            
            if ($result -ne [SqlBakReader.BakReadResult]::OK) {
                $errorMessage = [SqlBakReader.BakReadApi]::GetError($handle)
                throw "Failed to open backup file: $errorMessage (Result: $result)"
            }
            
            $info = New-Object SqlBakReader.BakBackupInfo
            $result = [SqlBakReader.BakReadApi]::bakread_get_info($handle, [ref]$info)
            
            if ($result -ne [SqlBakReader.BakReadResult]::OK) {
                $errorMessage = [SqlBakReader.BakReadApi]::GetError($handle)
                throw "Failed to read backup info: $errorMessage (Result: $result)"
            }
            
            [PSCustomObject]@{
                PSTypeName         = 'SqlBakReader.BackupInfo'
                Path               = $resolvedPaths -join '; '
                DatabaseName       = [SqlBakReader.BakReadApi]::GetString($info.DatabaseName)
                ServerName         = [SqlBakReader.BakReadApi]::GetString($info.ServerName)
                BackupType         = ConvertTo-BackupTypeString $info.BackupType
                CompatibilityLevel = $info.CompatibilityLevel
                IsCompressed       = $info.IsCompressed -ne 0
                IsEncrypted        = $info.IsEncrypted -ne 0
                IsTde              = $info.IsTde -ne 0
                BackupSize         = $info.BackupSize
                CompressedSize     = $info.CompressedSize
                CompressionRatio   = if ($info.BackupSize -gt 0 -and $info.CompressedSize -gt 0) {
                    [math]::Round(($info.BackupSize - $info.CompressedSize) / $info.BackupSize * 100, 2)
                } else { $null }
                BackupStartDate    = [SqlBakReader.BakReadApi]::GetString($info.BackupStartDate)
                BackupFinishDate   = [SqlBakReader.BakReadApi]::GetString($info.BackupFinishDate)
            }
        }
        finally {
            if ($handle -ne [IntPtr]::Zero) {
                [SqlBakReader.BakReadApi]::bakread_close($handle)
            }
        }
    }
}

<#
.SYNOPSIS
    Lists all user tables in a SQL Server backup file.

.DESCRIPTION
    Reads the system catalog from a .bak file and returns a list of all
    user tables with their schema, name, and estimated row/page counts.

.PARAMETER Path
    Path to one or more .bak files. For striped backups, provide all stripe files.

.EXAMPLE
    Get-BakTable -Path "C:\Backups\MyDatabase.bak"
    
    Lists all tables in the backup file.

.EXAMPLE
    Get-BakTable -Path "C:\Backups\MyDatabase.bak" | Where-Object { $_.SchemaName -eq 'dbo' }
    
    Lists only tables in the dbo schema.

.OUTPUTS
    Collection of PSCustomObject with table information.
#>
function Get-BakTable {
    [CmdletBinding()]
    [OutputType([PSCustomObject[]])]
    param(
        [Parameter(Mandatory = $true, Position = 0, ValueFromPipeline = $true)]
        [ValidateScript({ Test-Path $_ })]
        [string[]]$Path
    )
    
    begin {
        Initialize-BakReadApi | Out-Null
    }
    
    process {
        $resolvedPaths = $Path | ForEach-Object { (Resolve-Path $_).Path }
        
        $handle = [IntPtr]::Zero
        try {
            $result = [SqlBakReader.BakReadApi]::bakread_open($resolvedPaths, $resolvedPaths.Count, [ref]$handle)
            
            if ($result -ne [SqlBakReader.BakReadResult]::OK) {
                $errorMessage = [SqlBakReader.BakReadApi]::GetError($handle)
                throw "Failed to open backup file: $errorMessage (Result: $result)"
            }
            
            $tablesPtr = [IntPtr]::Zero
            $tableCount = 0
            $result = [SqlBakReader.BakReadApi]::bakread_list_tables($handle, [ref]$tablesPtr, [ref]$tableCount)
            
            if ($result -ne [SqlBakReader.BakReadResult]::OK) {
                $errorMessage = [SqlBakReader.BakReadApi]::GetError($handle)
                throw "Failed to list tables: $errorMessage (Result: $result)"
            }
            
            $structSize = [System.Runtime.InteropServices.Marshal]::SizeOf([type][SqlBakReader.BakTableInfoData])
            
            for ($i = 0; $i -lt $tableCount; $i++) {
                $tablePtr = [IntPtr]::Add($tablesPtr, $i * $structSize)
                $tableData = [System.Runtime.InteropServices.Marshal]::PtrToStructure($tablePtr, [type][SqlBakReader.BakTableInfoData])
                ConvertTo-BakTableInfo $tableData
            }
            
            [SqlBakReader.BakReadApi]::bakread_free_table_list($tablesPtr, $tableCount)
        }
        finally {
            if ($handle -ne [IntPtr]::Zero) {
                [SqlBakReader.BakReadApi]::bakread_close($handle)
            }
        }
    }
}

<#
.SYNOPSIS
    Reads table data from a SQL Server backup file.

.DESCRIPTION
    Extracts data from a specific table in a .bak file directly, without
    requiring SQL Server. Returns rows as PSObjects with typed properties.

.PARAMETER Path
    Path to one or more .bak files. For striped backups, provide all stripe files.

.PARAMETER Table
    The table to read in schema.table format (e.g., "dbo.Users").

.PARAMETER Column
    Optional list of columns to return. If not specified, all columns are returned.

.PARAMETER MaxRows
    Maximum number of rows to return. Default is unlimited.

.PARAMETER IndexedMode
    Use indexed mode for large backups. This reduces memory usage by not
    caching all pages in memory.

.PARAMETER CacheSizeMB
    Cache size in MB for indexed mode. Default is 256 MB.

.EXAMPLE
    Read-BakTable -Path "C:\Backups\MyDatabase.bak" -Table "dbo.Users"
    
    Reads all rows from the Users table.

.EXAMPLE
    Read-BakTable -Path "C:\Backups\MyDatabase.bak" -Table "dbo.Orders" -MaxRows 100
    
    Reads the first 100 rows from the Orders table.

.EXAMPLE
    Read-BakTable -Path "C:\Backups\MyDatabase.bak" -Table "dbo.Products" -Column "Id", "Name", "Price"
    
    Reads only the specified columns from the Products table.

.OUTPUTS
    Collection of PSCustomObject with table row data.
#>
function Read-BakTable {
    [CmdletBinding()]
    [OutputType([PSCustomObject[]])]
    param(
        [Parameter(Mandatory = $true, Position = 0)]
        [ValidateScript({ Test-Path $_ })]
        [string[]]$Path,
        
        [Parameter(Mandatory = $true, Position = 1)]
        [ValidatePattern('^[\w\[\]]+\.[\w\[\]]+$')]
        [string]$Table,
        
        [Parameter()]
        [string[]]$Column,
        
        [Parameter()]
        [int64]$MaxRows = -1,
        
        [Parameter()]
        [switch]$IndexedMode,
        
        [Parameter()]
        [int]$CacheSizeMB = 256
    )
    
    begin {
        Initialize-BakReadApi | Out-Null
    }
    
    process {
        $resolvedPaths = $Path | ForEach-Object { (Resolve-Path $_).Path }
        
        # Parse schema.table
        $parts = $Table -split '\.'
        if ($parts.Count -ne 2) {
            throw "Table must be in schema.table format (e.g., 'dbo.Users')"
        }
        $schema = $parts[0].Trim('[', ']')
        $tableName = $parts[1].Trim('[', ']')
        
        $handle = [IntPtr]::Zero
        $columnNames = @()
        $rows = [System.Collections.Generic.List[PSObject]]::new()
        
        try {
            $result = [SqlBakReader.BakReadApi]::bakread_open($resolvedPaths, $resolvedPaths.Count, [ref]$handle)
            
            if ($result -ne [SqlBakReader.BakReadResult]::OK) {
                $errorMessage = [SqlBakReader.BakReadApi]::GetError($handle)
                throw "Failed to open backup file: $errorMessage (Result: $result)"
            }
            
            # Set indexed mode if requested
            if ($IndexedMode) {
                $result = [SqlBakReader.BakReadApi]::bakread_set_indexed_mode($handle, 1, [UIntPtr]::new($CacheSizeMB))
                if ($result -ne [SqlBakReader.BakReadResult]::OK) {
                    Write-Warning "Failed to enable indexed mode, continuing with standard mode"
                }
            }
            
            # Set target table
            $result = [SqlBakReader.BakReadApi]::bakread_set_table($handle, $schema, $tableName)
            if ($result -ne [SqlBakReader.BakReadResult]::OK) {
                $errorMessage = [SqlBakReader.BakReadApi]::GetError($handle)
                throw "Failed to set table: $errorMessage (Result: $result)"
            }
            
            # Set columns if specified
            if ($Column -and $Column.Count -gt 0) {
                $result = [SqlBakReader.BakReadApi]::bakread_set_columns($handle, $Column, $Column.Count)
                if ($result -ne [SqlBakReader.BakReadResult]::OK) {
                    $errorMessage = [SqlBakReader.BakReadApi]::GetError($handle)
                    throw "Failed to set columns: $errorMessage (Result: $result)"
                }
            }
            
            # Set max rows
            if ($MaxRows -gt 0) {
                $result = [SqlBakReader.BakReadApi]::bakread_set_max_rows($handle, $MaxRows)
                if ($result -ne [SqlBakReader.BakReadResult]::OK) {
                    Write-Warning "Failed to set max rows limit"
                }
            }
            
            # Extract rows using callback - collect raw data first with temp column names
            $rawRows = [System.Collections.Generic.List[object[]]]::new()
            
            $rowCallback = [SqlBakReader.BakRowCallback]{
                param($valuesPtr, $colCount, $userData)
                
                if ($colCount -le 0) { return 0 }
                
                $ptrSize = [IntPtr]::Size
                $rowData = @()
                
                for ($i = 0; $i -lt $colCount; $i++) {
                    $valuePtr = [System.Runtime.InteropServices.Marshal]::ReadIntPtr($valuesPtr, $i * $ptrSize)
                    $value = if ($valuePtr -ne [IntPtr]::Zero) {
                        [System.Runtime.InteropServices.Marshal]::PtrToStringAnsi($valuePtr)
                    } else { $null }
                    $rowData += $value
                }
                
                $rawRows.Add($rowData)
                return 0  # Continue extraction
            }
            
            $rowCount = [uint64]0
            $result = [SqlBakReader.BakReadApi]::bakread_extract($handle, $rowCallback, [IntPtr]::Zero, [ref]$rowCount)
            
            if ($result -ne [SqlBakReader.BakReadResult]::OK) {
                $errorMsg = [SqlBakReader.BakReadApi]::GetError($handle)
                
                switch ($result) {
                    'ErrorTdeDetected' {
                        throw "Backup is TDE encrypted. Direct extraction is not supported for TDE backups."
                    }
                    'ErrorEncryptionDetected' {
                        throw "Backup is encrypted. Direct extraction is not supported for encrypted backups."
                    }
                    'ErrorTableNotFound' {
                        throw "Table '$Table' not found in backup. Use Get-BakTable to list available tables."
                    }
                    default {
                        throw "Failed to extract data: $errorMsg (Result: $result)"
                    }
                }
            }
            
            # Now get schema - it should be populated after extraction
            $columnsPtr = [IntPtr]::Zero
            $columnCount = 0
            $result = [SqlBakReader.BakReadApi]::bakread_get_schema($handle, [ref]$columnsPtr, [ref]$columnCount)
            
            if ($result -eq [SqlBakReader.BakReadResult]::OK -and $columnCount -gt 0) {
                $structSize = [System.Runtime.InteropServices.Marshal]::SizeOf([type][SqlBakReader.BakColumnInfo])
                for ($i = 0; $i -lt $columnCount; $i++) {
                    $colPtr = [IntPtr]::Add($columnsPtr, $i * $structSize)
                    $colData = [System.Runtime.InteropServices.Marshal]::PtrToStructure($colPtr, [type][SqlBakReader.BakColumnInfo])
                    $columnNames += [SqlBakReader.BakReadApi]::GetString($colData.Name)
                }
                [SqlBakReader.BakReadApi]::bakread_free_schema($columnsPtr, $columnCount)
            }
            
            # Convert raw rows to PSObjects with proper column names
            foreach ($rawRow in $rawRows) {
                $row = [ordered]@{}
                for ($i = 0; $i -lt $rawRow.Count; $i++) {
                    $colName = if ($i -lt $columnNames.Count) { $columnNames[$i] } else { "Column$i" }
                    $row[$colName] = $rawRow[$i]
                }
                $rows.Add([PSCustomObject]$row)
            }
            
            Write-Verbose "Extracted $rowCount rows from $Table"
            $rows
        }
        finally {
            if ($handle -ne [IntPtr]::Zero) {
                [SqlBakReader.BakReadApi]::bakread_close($handle)
            }
        }
    }
}

<#
.SYNOPSIS
    Opens a BAK reader handle for advanced operations.

.DESCRIPTION
    Opens a reader handle that can be used for multiple operations without
    re-parsing the backup file. Use Close-BakReader when done.

.PARAMETER Path
    Path to one or more .bak files.

.EXAMPLE
    $reader = Open-BakReader -Path "C:\Backups\MyDatabase.bak"
    try {
        # Use $reader for operations
    }
    finally {
        Close-BakReader $reader
    }

.OUTPUTS
    IntPtr handle to be used with other functions.
#>
function Open-BakReader {
    [CmdletBinding()]
    [OutputType([IntPtr])]
    param(
        [Parameter(Mandatory = $true, Position = 0)]
        [ValidateScript({ Test-Path $_ })]
        [string[]]$Path
    )
    
    Initialize-BakReadApi | Out-Null
    
    $resolvedPaths = $Path | ForEach-Object { (Resolve-Path $_).Path }
    
    $handle = [IntPtr]::Zero
    $result = [SqlBakReader.BakReadApi]::bakread_open($resolvedPaths, $resolvedPaths.Count, [ref]$handle)
    
    if ($result -ne [SqlBakReader.BakReadResult]::OK) {
        $errorMessage = [SqlBakReader.BakReadApi]::GetError($handle)
        throw "Failed to open backup file: $errorMessage (Result: $result)"
    }
    
    return $handle
}

<#
.SYNOPSIS
    Closes a BAK reader handle.

.DESCRIPTION
    Closes and releases resources for a reader handle opened with Open-BakReader.

.PARAMETER Handle
    The reader handle to close.

.EXAMPLE
    Close-BakReader $reader
#>
function Close-BakReader {
    [CmdletBinding()]
    param(
        [Parameter(Mandatory = $true, Position = 0)]
        [IntPtr]$Handle
    )
    
    if ($Handle -ne [IntPtr]::Zero) {
        [SqlBakReader.BakReadApi]::bakread_close($Handle)
    }
}

<#
.SYNOPSIS
    Lists stored procedures, functions, and views from a SQL Server backup file.

.DESCRIPTION
    Extracts module definitions (stored procedures, functions, views) directly from
    a .bak file without requiring SQL Server.

.PARAMETER Path
    Path to one or more .bak files. For striped backups, provide all stripe files.

.PARAMETER Type
    Filter by module type: Procedure, Function, View, or All (default).

.PARAMETER IncludeDefinition
    Include the T-SQL definition text in the output (can be large).

.EXAMPLE
    Get-BakModule -Path "C:\Backups\MyDatabase.bak"
    
    Lists all stored procedures, functions, and views.

.EXAMPLE
    Get-BakModule -Path "C:\Backups\MyDatabase.bak" -Type Procedure -IncludeDefinition
    
    Gets all stored procedures with their T-SQL definitions.

.OUTPUTS
    Collection of PSCustomObject with module information.
#>
function Get-BakModule {
    [CmdletBinding()]
    [OutputType([PSCustomObject[]])]
    param(
        [Parameter(Mandatory = $true, Position = 0, ValueFromPipeline = $true)]
        [ValidateScript({ Test-Path $_ })]
        [string[]]$Path,
        
        [Parameter()]
        [ValidateSet('All', 'Procedure', 'Function', 'View')]
        [string]$Type = 'All',
        
        [Parameter()]
        [switch]$IncludeDefinition
    )
    
    begin {
        Initialize-BakReadApi | Out-Null
    }
    
    process {
        $resolvedPaths = $Path | ForEach-Object { (Resolve-Path $_).Path }
        
        $handle = [IntPtr]::Zero
        try {
            $result = [SqlBakReader.BakReadApi]::bakread_open($resolvedPaths, $resolvedPaths.Count, [ref]$handle)
            
            if ($result -ne [SqlBakReader.BakReadResult]::OK) {
                $errorMessage = [SqlBakReader.BakReadApi]::GetError($handle)
                throw "Failed to open backup file: $errorMessage (Result: $result)"
            }
            
            $modulesPtr = [IntPtr]::Zero
            $moduleCount = 0
            $result = [SqlBakReader.BakReadApi]::bakread_list_modules($handle, [ref]$modulesPtr, [ref]$moduleCount)
            
            if ($result -ne [SqlBakReader.BakReadResult]::OK) {
                $errorMessage = [SqlBakReader.BakReadApi]::GetError($handle)
                throw "Failed to list modules: $errorMessage (Result: $result)"
            }
            
            $structSize = [System.Runtime.InteropServices.Marshal]::SizeOf([type][SqlBakReader.BakModuleInfo])
            
            for ($i = 0; $i -lt $moduleCount; $i++) {
                $modPtr = [IntPtr]::Add($modulesPtr, $i * $structSize)
                $modData = [System.Runtime.InteropServices.Marshal]::PtrToStructure($modPtr, [type][SqlBakReader.BakModuleInfo])
                
                $typeDesc = [SqlBakReader.BakReadApi]::GetString($modData.TypeDesc)
                
                # Filter by type
                $include = $false
                switch ($Type) {
                    'All' { $include = $true }
                    'Procedure' { $include = $typeDesc -like '*PROCEDURE*' }
                    'Function' { $include = $typeDesc -like '*FUNCTION*' }
                    'View' { $include = $typeDesc -eq 'VIEW' }
                }
                
                if (-not $include) { continue }
                
                $obj = [PSCustomObject]@{
                    PSTypeName   = 'SqlBakReader.ModuleInfo'
                    ObjectId     = $modData.ObjectId
                    SchemaName   = [SqlBakReader.BakReadApi]::GetString($modData.SchemaName)
                    Name         = [SqlBakReader.BakReadApi]::GetString($modData.Name)
                    Type         = [SqlBakReader.BakReadApi]::GetString($modData.Type).Trim()
                    TypeDesc     = $typeDesc
                }
                
                if ($IncludeDefinition) {
                    $obj | Add-Member -NotePropertyName 'Definition' -NotePropertyValue ([SqlBakReader.BakReadApi]::GetString($modData.Definition))
                }
                
                $obj
            }
            
            [SqlBakReader.BakReadApi]::bakread_free_module_list($modulesPtr, $moduleCount)
        }
        finally {
            if ($handle -ne [IntPtr]::Zero) {
                [SqlBakReader.BakReadApi]::bakread_close($handle)
            }
        }
    }
}

<#
.SYNOPSIS
    Lists database principals (users, roles) from a SQL Server backup file.

.DESCRIPTION
    Extracts database principal information directly from a .bak file without
    requiring SQL Server.

.PARAMETER Path
    Path to one or more .bak files. For striped backups, provide all stripe files.

.PARAMETER Type
    Filter by principal type: All, User, Role, or ApplicationRole.

.EXAMPLE
    Get-BakPrincipal -Path "C:\Backups\MyDatabase.bak"
    
    Lists all database principals.

.EXAMPLE
    Get-BakPrincipal -Path "C:\Backups\MyDatabase.bak" -Type Role
    
    Lists only database roles.

.OUTPUTS
    Collection of PSCustomObject with principal information.
#>
function Get-BakPrincipal {
    [CmdletBinding()]
    [OutputType([PSCustomObject[]])]
    param(
        [Parameter(Mandatory = $true, Position = 0, ValueFromPipeline = $true)]
        [ValidateScript({ Test-Path $_ })]
        [string[]]$Path,
        
        [Parameter()]
        [ValidateSet('All', 'User', 'Role', 'ApplicationRole')]
        [string]$Type = 'All'
    )
    
    begin {
        Initialize-BakReadApi | Out-Null
    }
    
    process {
        $resolvedPaths = $Path | ForEach-Object { (Resolve-Path $_).Path }
        
        $handle = [IntPtr]::Zero
        try {
            $result = [SqlBakReader.BakReadApi]::bakread_open($resolvedPaths, $resolvedPaths.Count, [ref]$handle)
            
            if ($result -ne [SqlBakReader.BakReadResult]::OK) {
                $errorMessage = [SqlBakReader.BakReadApi]::GetError($handle)
                throw "Failed to open backup file: $errorMessage (Result: $result)"
            }
            
            $principalsPtr = [IntPtr]::Zero
            $principalCount = 0
            $result = [SqlBakReader.BakReadApi]::bakread_list_principals($handle, [ref]$principalsPtr, [ref]$principalCount)
            
            if ($result -ne [SqlBakReader.BakReadResult]::OK) {
                $errorMessage = [SqlBakReader.BakReadApi]::GetError($handle)
                throw "Failed to list principals: $errorMessage (Result: $result)"
            }
            
            $structSize = [System.Runtime.InteropServices.Marshal]::SizeOf([type][SqlBakReader.BakPrincipalInfo])
            
            for ($i = 0; $i -lt $principalCount; $i++) {
                $prinPtr = [IntPtr]::Add($principalsPtr, $i * $structSize)
                $prinData = [System.Runtime.InteropServices.Marshal]::PtrToStructure($prinPtr, [type][SqlBakReader.BakPrincipalInfo])
                
                $typeDesc = [SqlBakReader.BakReadApi]::GetString($prinData.TypeDesc)
                
                # Filter by type
                $include = $false
                switch ($Type) {
                    'All' { $include = $true }
                    'User' { $include = $typeDesc -like '*USER*' }
                    'Role' { $include = $typeDesc -like '*ROLE*' -and $typeDesc -notlike '*APPLICATION*' }
                    'ApplicationRole' { $include = $typeDesc -like '*APPLICATION*' }
                }
                
                if (-not $include) { continue }
                
                [PSCustomObject]@{
                    PSTypeName          = 'SqlBakReader.PrincipalInfo'
                    PrincipalId         = $prinData.PrincipalId
                    Name                = [SqlBakReader.BakReadApi]::GetString($prinData.Name)
                    Type                = [SqlBakReader.BakReadApi]::GetString($prinData.Type)
                    TypeDesc            = $typeDesc
                    OwningPrincipalId   = $prinData.OwningPrincipalId
                    DefaultSchema       = [SqlBakReader.BakReadApi]::GetString($prinData.DefaultSchema)
                    IsFixedRole         = $prinData.IsFixedRole -ne 0
                }
            }
            
            [SqlBakReader.BakReadApi]::bakread_free_principal_list($principalsPtr, $principalCount)
        }
        finally {
            if ($handle -ne [IntPtr]::Zero) {
                [SqlBakReader.BakReadApi]::bakread_close($handle)
            }
        }
    }
}

<#
.SYNOPSIS
    Lists database role memberships from a SQL Server backup file.

.DESCRIPTION
    Extracts role membership information directly from a .bak file without
    requiring SQL Server.

.PARAMETER Path
    Path to one or more .bak files. For striped backups, provide all stripe files.

.EXAMPLE
    Get-BakRoleMember -Path "C:\Backups\MyDatabase.bak"
    
    Lists all role memberships.

.OUTPUTS
    Collection of PSCustomObject with role membership information.
#>
function Get-BakRoleMember {
    [CmdletBinding()]
    [OutputType([PSCustomObject[]])]
    param(
        [Parameter(Mandatory = $true, Position = 0, ValueFromPipeline = $true)]
        [ValidateScript({ Test-Path $_ })]
        [string[]]$Path
    )
    
    begin {
        Initialize-BakReadApi | Out-Null
    }
    
    process {
        $resolvedPaths = $Path | ForEach-Object { (Resolve-Path $_).Path }
        
        $handle = [IntPtr]::Zero
        try {
            $result = [SqlBakReader.BakReadApi]::bakread_open($resolvedPaths, $resolvedPaths.Count, [ref]$handle)
            
            if ($result -ne [SqlBakReader.BakReadResult]::OK) {
                $errorMessage = [SqlBakReader.BakReadApi]::GetError($handle)
                throw "Failed to open backup file: $errorMessage (Result: $result)"
            }
            
            $membersPtr = [IntPtr]::Zero
            $memberCount = 0
            $result = [SqlBakReader.BakReadApi]::bakread_list_role_members($handle, [ref]$membersPtr, [ref]$memberCount)
            
            if ($result -ne [SqlBakReader.BakReadResult]::OK) {
                $errorMessage = [SqlBakReader.BakReadApi]::GetError($handle)
                throw "Failed to list role members: $errorMessage (Result: $result)"
            }
            
            $structSize = [System.Runtime.InteropServices.Marshal]::SizeOf([type][SqlBakReader.BakRoleMemberInfo])
            
            for ($i = 0; $i -lt $memberCount; $i++) {
                $memPtr = [IntPtr]::Add($membersPtr, $i * $structSize)
                $memData = [System.Runtime.InteropServices.Marshal]::PtrToStructure($memPtr, [type][SqlBakReader.BakRoleMemberInfo])
                
                [PSCustomObject]@{
                    PSTypeName          = 'SqlBakReader.RoleMemberInfo'
                    RolePrincipalId     = $memData.RolePrincipalId
                    MemberPrincipalId   = $memData.MemberPrincipalId
                    RoleName            = [SqlBakReader.BakReadApi]::GetString($memData.RoleName)
                    MemberName          = [SqlBakReader.BakReadApi]::GetString($memData.MemberName)
                }
            }
            
            [SqlBakReader.BakReadApi]::bakread_free_role_member_list($membersPtr, $memberCount)
        }
        finally {
            if ($handle -ne [IntPtr]::Zero) {
                [SqlBakReader.BakReadApi]::bakread_close($handle)
            }
        }
    }
}

<#
.SYNOPSIS
    Lists database permissions from a SQL Server backup file.

.DESCRIPTION
    Extracts database permission information directly from a .bak file without
    requiring SQL Server.

.PARAMETER Path
    Path to one or more .bak files. For striped backups, provide all stripe files.

.PARAMETER GenerateScript
    Output T-SQL GRANT/DENY statements instead of raw permission data.

.EXAMPLE
    Get-BakPermission -Path "C:\Backups\MyDatabase.bak"
    
    Lists all database permissions.

.EXAMPLE
    Get-BakPermission -Path "C:\Backups\MyDatabase.bak" -GenerateScript
    
    Generates GRANT/DENY T-SQL statements for all permissions.

.OUTPUTS
    Collection of PSCustomObject with permission information, or T-SQL strings if -GenerateScript is specified.
#>
function Get-BakPermission {
    [CmdletBinding()]
    [OutputType([PSCustomObject[]])]
    param(
        [Parameter(Mandatory = $true, Position = 0, ValueFromPipeline = $true)]
        [ValidateScript({ Test-Path $_ })]
        [string[]]$Path,
        
        [Parameter()]
        [switch]$GenerateScript
    )
    
    begin {
        Initialize-BakReadApi | Out-Null
    }
    
    process {
        $resolvedPaths = $Path | ForEach-Object { (Resolve-Path $_).Path }
        
        $handle = [IntPtr]::Zero
        try {
            $result = [SqlBakReader.BakReadApi]::bakread_open($resolvedPaths, $resolvedPaths.Count, [ref]$handle)
            
            if ($result -ne [SqlBakReader.BakReadResult]::OK) {
                $errorMessage = [SqlBakReader.BakReadApi]::GetError($handle)
                throw "Failed to open backup file: $errorMessage (Result: $result)"
            }
            
            $permsPtr = [IntPtr]::Zero
            $permCount = 0
            $result = [SqlBakReader.BakReadApi]::bakread_list_permissions($handle, [ref]$permsPtr, [ref]$permCount)
            
            if ($result -ne [SqlBakReader.BakReadResult]::OK) {
                $errorMessage = [SqlBakReader.BakReadApi]::GetError($handle)
                throw "Failed to list permissions: $errorMessage (Result: $result)"
            }
            
            $structSize = [System.Runtime.InteropServices.Marshal]::SizeOf([type][SqlBakReader.BakPermissionInfo])
            
            for ($i = 0; $i -lt $permCount; $i++) {
                $permPtr = [IntPtr]::Add($permsPtr, $i * $structSize)
                $permData = [System.Runtime.InteropServices.Marshal]::PtrToStructure($permPtr, [type][SqlBakReader.BakPermissionInfo])
                
                $permName = [SqlBakReader.BakReadApi]::GetString($permData.PermissionName)
                $state = [SqlBakReader.BakReadApi]::GetString($permData.State)
                $grantee = [SqlBakReader.BakReadApi]::GetString($permData.GranteeName)
                $grantor = [SqlBakReader.BakReadApi]::GetString($permData.GrantorName)
                $objName = [SqlBakReader.BakReadApi]::GetString($permData.ObjectName)
                $schemaName = [SqlBakReader.BakReadApi]::GetString($permData.SchemaName)
                $classDesc = [SqlBakReader.BakReadApi]::GetString($permData.ClassDesc)
                
                if ($GenerateScript) {
                    # Generate T-SQL script
                    $stateKeyword = switch ($state) {
                        'GRANT_WITH_GRANT_OPTION' { 'GRANT' }
                        'GRANT' { 'GRANT' }
                        'DENY' { 'DENY' }
                        'REVOKE' { 'REVOKE' }
                        default { 'GRANT' }
                    }
                    
                    $withGrant = if ($state -eq 'GRANT_WITH_GRANT_OPTION') { ' WITH GRANT OPTION' } else { '' }
                    
                    $target = switch ($classDesc) {
                        'DATABASE' { '' }
                        'OBJECT_OR_COLUMN' {
                            if ($objName) {
                                if ($schemaName) { " ON [$schemaName].[$objName]" }
                                else { " ON [$objName]" }
                            } else { '' }
                        }
                        'SCHEMA' { " ON SCHEMA::[$schemaName]" }
                        default { '' }
                    }
                    
                    "$stateKeyword $permName$target TO [$grantee]$withGrant;"
                }
                else {
                    [PSCustomObject]@{
                        PSTypeName      = 'SqlBakReader.PermissionInfo'
                        ClassType       = $permData.ClassType
                        ClassDesc       = $classDesc
                        MajorId         = $permData.MajorId
                        MinorId         = $permData.MinorId
                        PermissionName  = $permName
                        State           = $state
                        GranteeName     = $grantee
                        GrantorName     = $grantor
                        ObjectName      = $objName
                        SchemaName      = $schemaName
                    }
                }
            }
            
            [SqlBakReader.BakReadApi]::bakread_free_permission_list($permsPtr, $permCount)
        }
        finally {
            if ($handle -ne [IntPtr]::Zero) {
                [SqlBakReader.BakReadApi]::bakread_close($handle)
            }
        }
    }
}

<#
.SYNOPSIS
    Generates a complete database security script from a SQL Server backup file.

.DESCRIPTION
    Extracts all security information (principals, role memberships, permissions)
    from a .bak file and generates a T-SQL script to recreate the security setup.

.PARAMETER Path
    Path to one or more .bak files. For striped backups, provide all stripe files.

.PARAMETER OutputPath
    Optional path to save the script to a file.

.EXAMPLE
    Get-BakSecurityScript -Path "C:\Backups\MyDatabase.bak"
    
    Outputs the security script to the console.

.EXAMPLE
    Get-BakSecurityScript -Path "C:\Backups\MyDatabase.bak" -OutputPath "C:\Scripts\security.sql"
    
    Saves the security script to a file.

.OUTPUTS
    T-SQL script as a string.
#>
function Get-BakSecurityScript {
    [CmdletBinding()]
    [OutputType([string])]
    param(
        [Parameter(Mandatory = $true, Position = 0)]
        [ValidateScript({ Test-Path $_ })]
        [string[]]$Path,
        
        [Parameter()]
        [string]$OutputPath
    )
    
    $script = [System.Text.StringBuilder]::new()
    
    [void]$script.AppendLine("-- Database Security Script")
    [void]$script.AppendLine("-- Generated from backup file: $($Path -join ', ')")
    [void]$script.AppendLine("-- Generated on: $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')")
    [void]$script.AppendLine("")
    
    # Principals
    [void]$script.AppendLine("-- =============================================")
    [void]$script.AppendLine("-- Database Principals (Users and Roles)")
    [void]$script.AppendLine("-- =============================================")
    [void]$script.AppendLine("")
    
    $principals = Get-BakPrincipal -Path $Path
    foreach ($p in $principals) {
        if ($p.PrincipalId -le 4) { continue }  # Skip system principals
        
        switch ($p.TypeDesc) {
            'SQL_USER' {
                [void]$script.AppendLine("IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'$($p.Name)')")
                [void]$script.AppendLine("    CREATE USER [$($p.Name)] WITHOUT LOGIN;")
            }
            'DATABASE_ROLE' {
                if (-not $p.IsFixedRole) {
                    [void]$script.AppendLine("IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'$($p.Name)')")
                    [void]$script.AppendLine("    CREATE ROLE [$($p.Name)];")
                }
            }
            'APPLICATION_ROLE' {
                [void]$script.AppendLine("-- Application Role: $($p.Name) (password required)")
            }
        }
    }
    [void]$script.AppendLine("")
    
    # Role memberships
    [void]$script.AppendLine("-- =============================================")
    [void]$script.AppendLine("-- Role Memberships")
    [void]$script.AppendLine("-- =============================================")
    [void]$script.AppendLine("")
    
    $roleMembers = Get-BakRoleMember -Path $Path
    foreach ($rm in $roleMembers) {
        if ($rm.RoleName -and $rm.MemberName) {
            [void]$script.AppendLine("ALTER ROLE [$($rm.RoleName)] ADD MEMBER [$($rm.MemberName)];")
        }
    }
    [void]$script.AppendLine("")
    
    # Permissions
    [void]$script.AppendLine("-- =============================================")
    [void]$script.AppendLine("-- Database Permissions")
    [void]$script.AppendLine("-- =============================================")
    [void]$script.AppendLine("")
    
    $permissions = Get-BakPermission -Path $Path -GenerateScript
    foreach ($perm in $permissions) {
        [void]$script.AppendLine($perm)
    }
    
    $result = $script.ToString()
    
    if ($OutputPath) {
        $result | Out-File -FilePath $OutputPath -Encoding UTF8
        Write-Host "Security script saved to: $OutputPath"
    }
    else {
        $result
    }
}

#endregion

# Export public functions
Export-ModuleMember -Function @(
    'Get-BakInfo',
    'Get-BakTable',
    'Read-BakTable',
    'Open-BakReader',
    'Close-BakReader',
    'Get-BakModule',
    'Get-BakPrincipal',
    'Get-BakRoleMember',
    'Get-BakPermission',
    'Get-BakSecurityScript'
)
