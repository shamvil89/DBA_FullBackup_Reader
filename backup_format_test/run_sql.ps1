# Run a SQL script via Invoke-Sqlcmd (SqlServer module). Reads credentials from env or .env.
# Usage: .\run_sql.ps1 <script>   e.g. .\run_sql.ps1 01_create_db.sql
# Requires: Install-Module -Name SqlServer -Scope CurrentUser
# From project root: .\backup_format_test\run_sql.ps1 01_create_db.sql

param(
    [Parameter(Mandatory = $true, Position = 0)]
    [string]$ScriptName
)

$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
$SqlDir   = Join-Path $ScriptDir "sql"
$OutDir   = Join-Path $ScriptDir "outputs"

# Load .env if present (do not commit .env)
$EnvFile = Join-Path $ScriptDir ".env"
if (Test-Path $EnvFile) {
    Get-Content $EnvFile | ForEach-Object {
        if ($_ -match '^\s*([A-Za-z_][A-Za-z0-9_]*)\s*=\s*(.*)\s*$' -and $_.Trim() -notmatch '^\s*#') {
            $key = $matches[1].Trim()
            $val = $matches[2].Trim()
            Set-Item -Path "Env:$key" -Value $val
        }
    }
}

$Server = $env:SQL_SERVER
$User   = $env:SQL_USER
$Pass   = $env:SQL_PASSWORD
if (-not $Server) { $Server = "DESKTOP-CIS3NI4" }
if (-not $User)   { $User   = "sa" }
if (-not $Pass) {
    Write-Error "SQL password not set. Set SQL_PASSWORD in env or in backup_format_test\.env (copy from .env.example)."
}

$ScriptPath = Join-Path $SqlDir $ScriptName
if (-not (Test-Path $ScriptPath)) {
    Write-Error "Script not found: $ScriptPath"
}

if (-not (Get-Command Invoke-Sqlcmd -ErrorAction SilentlyContinue)) {
    Write-Error "Invoke-Sqlcmd not found. Install the SqlServer module: Install-Module -Name SqlServer -Scope CurrentUser"
}

# Ensure output directory for backups (use full path so SQL Server can write)
if (-not (Test-Path $OutDir)) { New-Item -ItemType Directory -Path $OutDir -Force | Out-Null }
$OutDir = [System.IO.Path]::GetFullPath($OutDir)
$BackupPath   = Join-Path $OutDir "backup_unc.bak"
$BackupComp   = Join-Path $OutDir "backup_comp.bak"
$Twice1       = Join-Path $OutDir "twice1.bak"
$Twice2       = Join-Path $OutDir "twice2.bak"
$AfterChange  = Join-Path $OutDir "after_change.bak"
$LargeBackup  = Join-Path $OutDir "backup_large.bak"

$params = @{
    ServerInstance         = $Server
    Username               = $User
    Password               = $Pass
    InputFile              = $ScriptPath
    TrustServerCertificate = $true
}

$LargeBackup2 = Join-Path $OutDir "backup_large_v2.bak"
$TDEBackup    = Join-Path $OutDir "tde_backup.bak"
$TDECertPath  = Join-Path $OutDir "tde_cert.cer"
$TDEKeyPath   = Join-Path $OutDir "tde_cert_key.pvk"

switch ($ScriptName) {
    "02_backup_uncompressed.sql" { $params["Variable"] = @("BackupPath=$BackupPath") }
    "03_backup_compressed.sql"   { $params["Variable"] = @("BackupPath=$BackupComp") }
    "04_backup_twice.sql"       { $params["Variable"] = @("BackupPath1=$Twice1", "BackupPath2=$Twice2") }
    "05_backup_after_change.sql" { $params["Variable"] = @("BackupPath=$AfterChange") }
    "08_backup_large.sql"       { $params["Variable"] = @("BackupPath=$LargeBackup") }
    "11_backup_large_v2.sql"    { $params["Variable"] = @("BackupPath=$LargeBackup2") }
    "13_backup_tde_cert.sql"    { $params["Variable"] = @("CertPath=$TDECertPath", "KeyPath=$TDEKeyPath", "KeyPassword=TDE_Export_P@ss!") }
    "14_backup_tde_database.sql" { $params["Variable"] = @("BackupPath=$TDEBackup") }
}

Invoke-Sqlcmd @params
