<#
.SYNOPSIS
    Builds and validates the FastWrappers-TSQL packages (.dacpac, .bak and replayable .sql).

.DESCRIPTION
    Expects the project to be built. Run it on a dedicated SQL Server instance (LocalDB in CI):
    the script stops if the database already exists, and it enables CLR on the instance.
    Any failed check stops the script with a non-zero exit code, so that a release is never
    published with a missing or broken package.

.EXAMPLE
    powershell -File .github\scripts\Build-Packages.ps1 -ServerInstance "(localdb)\FWTest" -SqlPackage SqlPackage.exe -OutputDir C:\temp\pkg -MaxDatabaseVersion 0
#>
param(
    [Parameter(Mandatory = $true)][string]$ServerInstance,
    [Parameter(Mandatory = $true)][string]$SqlPackage,
    [string]$OutputDir = 'release',
    [string]$BuildDir = 'bin\Output',
    # Git tag being released (e.g. v1.2.0 or 1.2.0). Must match the version of the code when provided.
    [string]$Tag = '',
    # Highest database version allowed in the .bak: 904 = SQL Server 2019. 0 disables the check.
    [int]$MaxDatabaseVersion = 904
)

$ErrorActionPreference = 'Stop'
Set-StrictMode -Version Latest

$DatabaseName = 'FastWrappers-TSQL'
$RestoreTestDatabase = 'FastWrappers-TSQL_RestoreTest'
$RepoRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path

function Write-Step([string]$Message) { Write-Host "`n=== $Message" }

function Assert-True([bool]$Condition, [string]$Message) {
    if (-not $Condition) { throw "CHECK FAILED: $Message" }
    Write-Host "[OK] $Message"
}

# Runs a script made of batches separated by GO, on a single connection. SQL errors stop the script.
function Invoke-Sql([string]$Sql, [string]$Database = 'master') {
    $connection = New-Object System.Data.SqlClient.SqlConnection(
        "Server=$ServerInstance;Database=$Database;Integrated Security=True;TrustServerCertificate=True;Connect Timeout=60;Pooling=False")
    $connection.Open()
    try {
        foreach ($batch in [regex]::Split($Sql, '(?im)^\s*GO\s*$')) {
            if ([string]::IsNullOrWhiteSpace($batch)) { continue }
            $command = $connection.CreateCommand()
            $command.CommandText = $batch
            $command.CommandTimeout = 600
            [void]$command.ExecuteNonQuery()
        }
    }
    finally { $connection.Close() }
}

function Get-SqlScalar([string]$Sql, [string]$Database = 'master') {
    $connection = New-Object System.Data.SqlClient.SqlConnection(
        "Server=$ServerInstance;Database=$Database;Integrated Security=True;TrustServerCertificate=True;Connect Timeout=60;Pooling=False")
    $connection.Open()
    try {
        $command = $connection.CreateCommand()
        $command.CommandText = $Sql
        $command.CommandTimeout = 600
        return $command.ExecuteScalar()
    }
    finally { $connection.Close() }
}

# Drops the databases created by this script, so that it can run again on the same instance
# and that deleting a LocalDB instance does not leave orphan database files behind.
$script:CreatedDatabases = $false
function Remove-CreatedDatabases {
    if ($script:CreatedDatabases) {
        Invoke-Sql "DROP DATABASE IF EXISTS [$RestoreTestDatabase]; DROP DATABASE IF EXISTS [$DatabaseName];"
        $script:CreatedDatabases = $false
    }
}
trap {
    try { Remove-CreatedDatabases } catch { Write-Warning "Cleanup failed: $_" }
    break
}

function Test-Installation([string]$Label, [string]$Database = $DatabaseName) {
    $assemblyName = Get-SqlScalar "SELECT clr_name FROM sys.assemblies WHERE name = 'FastWrappers_TSQL'" $Database
    Assert-True ($assemblyName -like "*version=$AssemblyVersion,*") "${Label}: assembly version is $AssemblyVersion"
    foreach ($procedure in 'xp_RunFastTransfer_secure', 'xp_RunFastBCP_secure') {
        $exists = Get-SqlScalar "SELECT COUNT(*) FROM sys.objects WHERE object_id = OBJECT_ID('dbo.$procedure') AND type = 'PC'" $Database
        Assert-True ($exists -eq 1) "${Label}: $procedure exists"
    }
    $encrypted = Get-SqlScalar "SELECT dbo.EncryptString(N'test')" $Database
    Assert-True (-not [string]::IsNullOrEmpty("$encrypted")) "${Label}: the CLR function dbo.EncryptString runs"
}

# ---------------------------------------------------------------------
Write-Step 'Check that the versions are aligned'
# ---------------------------------------------------------------------
$dllPath = Join-Path $BuildDir 'FastWrappers_TSQL.dll'
$dacpacPath = Join-Path $BuildDir 'FastWrappers_TSQL.dacpac'
Assert-True (Test-Path $dllPath) "$dllPath exists"
Assert-True (Test-Path $dacpacPath) "$dacpacPath exists"

$AssemblyVersion = (Get-Item $dllPath).VersionInfo.FileVersion
Assert-True ($AssemblyVersion -match '^\d+\.\d+\.\d+\.\d+$') "assembly version '$AssemblyVersion' has 4 parts"
$Version = ($AssemblyVersion -split '\.')[0..2] -join '.'
Write-Host "Version: $Version"

$escaped = [regex]::Escape($Version)
$expectations = @(
    @{ File = 'FastWrappers_TSQL.sqlproj';     Pattern = "<DacVersion>$([regex]::Escape($AssemblyVersion))</DacVersion>" },
    @{ File = 'FastWrappers-TSQL.sql';         Pattern = "FastWrappers-TSQL $escaped - Complete" },
    @{ File = 'FastWrappers-TSQL.sql';         Pattern = "Assembly v$escaped'" },
    @{ File = 'xp_RunFastTransfer_secure.sql'; Pattern = "FastWrappers-TSQL $escaped," },
    @{ File = 'xp_RunFastBCP_secure.sql';      Pattern = "FastWrappers-TSQL $escaped," },
    @{ File = 'samples\samples.sql';           Pattern = "FastWrappers-TSQL $escaped " },
    @{ File = 'README.md';                     Pattern = "(?m)^\| $escaped \|" },
    @{ File = 'README.md';                     Pattern = "Assembly v$escaped'" }
)
foreach ($expectation in $expectations) {
    $content = [IO.File]::ReadAllText((Join-Path $RepoRoot $expectation.File))
    Assert-True ($content -match $expectation.Pattern) "$($expectation.File) matches the version"
}
if ($Tag) {
    Assert-True (($Tag -replace '^v', '') -eq $Version) "tag '$Tag' matches the version $Version"
}

# ---------------------------------------------------------------------
Write-Step 'Prepare the output directory'
# ---------------------------------------------------------------------
Write-Host "SQL Server version: $(Get-SqlScalar "SELECT SERVERPROPERTY('ProductVersion')")"
$existing = Get-SqlScalar "SELECT COUNT(*) FROM sys.databases WHERE name IN (N'$DatabaseName', N'$RestoreTestDatabase')"
Assert-True ($existing -eq 0) "no $DatabaseName database on $ServerInstance (the script needs a dedicated instance)"
$script:CreatedDatabases = $true

New-Item -ItemType Directory -Force -Path $OutputDir | Out-Null
$OutputDir = (Resolve-Path $OutputDir).Path
$outDacpac = Join-Path $OutputDir 'FastWrappers-TSQL.dacpac'
$outBak = Join-Path $OutputDir 'FastWrappers-TSQL.bak'
$outSql = Join-Path $OutputDir 'FastWrappers-TSQL.sql'
Remove-Item $outDacpac, $outBak, $outSql -ErrorAction SilentlyContinue
Copy-Item $dacpacPath $outDacpac

# ---------------------------------------------------------------------
Write-Step 'Deploy the .dacpac'
# ---------------------------------------------------------------------
& $SqlPackage /Action:Publish "/SourceFile:$outDacpac" "/TargetServerName:$ServerInstance" "/TargetDatabaseName:$DatabaseName" /TargetTrustServerCertificate:True /p:IncludeCompositeObjects=True
if ($LASTEXITCODE -ne 0) { throw "SqlPackage failed with exit code $LASTEXITCODE" }

$dllBytes = [IO.File]::ReadAllBytes((Resolve-Path $dllPath).Path)
$sha512 = [Security.Cryptography.SHA512]::Create()
$dllHash = ([BitConverter]::ToString($sha512.ComputeHash($dllBytes))) -replace '-', ''
$deployedHash = Get-SqlScalar "SELECT CONVERT(varchar(130), HASHBYTES('SHA2_512', af.content), 2) FROM sys.assembly_files af JOIN sys.assemblies a ON a.assembly_id = af.assembly_id WHERE a.name = 'FastWrappers_TSQL' AND af.file_id = 1" $DatabaseName
Assert-True ($deployedHash -eq $dllHash) 'the deployed assembly is the built assembly'

# ---------------------------------------------------------------------
Write-Step 'Create the .bak'
# ---------------------------------------------------------------------
$databaseVersion = [int](Get-SqlScalar "SELECT DATABASEPROPERTYEX(N'$DatabaseName', 'Version')")
Write-Host "Database version: $databaseVersion"
if ($MaxDatabaseVersion -gt 0) {
    Assert-True ($databaseVersion -le $MaxDatabaseVersion) "database version $databaseVersion <= $MaxDatabaseVersion"
}
Invoke-Sql "BACKUP DATABASE [$DatabaseName] TO DISK = N'$outBak' WITH INIT"
Assert-True (Test-Path $outBak) 'the .bak exists'

# ---------------------------------------------------------------------
Write-Step 'Create the replayable .sql'
# ---------------------------------------------------------------------
$assemblyHex = '0x' + ([BitConverter]::ToString($dllBytes) -replace '-', '')
$template = [IO.File]::ReadAllText((Join-Path $RepoRoot 'FastWrappers-TSQL.sql'))
Assert-True ($template.Contains('__ASSEMBLY_FROM_0X__')) 'the template holds the assembly placeholder'
$installSql = $template.Replace('__ASSEMBLY_FROM_0X__', $assemblyHex)
[IO.File]::WriteAllText($outSql, $installSql, (New-Object Text.UTF8Encoding($true)))

# ---------------------------------------------------------------------
Write-Step 'Check that the .sql upgrades a .bak/.dacpac installation (with its generated procedures)'
# ---------------------------------------------------------------------
Invoke-Sql @"
CREATE USER [ci_member] WITHOUT LOGIN;
ALTER ROLE [FastBCP_Executor] ADD MEMBER [ci_member];
"@ $DatabaseName
Invoke-Sql $installSql
Test-Installation 'upgrade'
Assert-True ((Get-SqlScalar "SELECT COUNT(*) FROM sys.objects WHERE name IN ('RunFastTransfer_Secure', 'RunFastBCP_Secure')" $DatabaseName) -eq 0) 'the generated procedures are removed'

# ---------------------------------------------------------------------
Write-Step 'Check that the .sql can be replayed'
# ---------------------------------------------------------------------
Invoke-Sql $installSql
Test-Installation 'replay'
Assert-True ((Get-SqlScalar "SELECT COUNT(*) FROM sys.database_role_members WHERE member_principal_id = USER_ID('ci_member') AND role_principal_id = USER_ID('FastBCP_Executor')" $DatabaseName) -eq 1) 'replay: the members of the roles are kept'
foreach ($grant in @(
        @('xp_RunFastTransfer_secure', 'FastTransfer_Executor'),
        @('xp_RunFastBCP_secure', 'FastBCP_Executor'),
        @('EncryptString', 'FastTransfer_Executor'),
        @('EncryptString', 'FastBCP_Executor'))) {
    $granted = Get-SqlScalar "SELECT COUNT(*) FROM sys.database_permissions WHERE major_id = OBJECT_ID('dbo.$($grant[0])') AND grantee_principal_id = USER_ID('$($grant[1])') AND permission_name = 'EXECUTE' AND state = 'G'" $DatabaseName
    Assert-True ($granted -eq 1) "replay: EXECUTE on $($grant[0]) is granted to $($grant[1])"
}
Assert-True ((Get-SqlScalar "SELECT COUNT(*) FROM sys.trusted_assemblies WHERE description = N'FastWrappers_TSQL Assembly v$Version'") -eq 1) 'replay: one trusted hash for this version'

# ---------------------------------------------------------------------
Write-Step 'Check that the .sql installs on a server without the database'
# ---------------------------------------------------------------------
Invoke-Sql "DROP DATABASE [$DatabaseName]"
Invoke-Sql $installSql
Test-Installation 'new installation'

# ---------------------------------------------------------------------
Write-Step 'Check that the .bak can be restored, with the logical file names of the README'
# ---------------------------------------------------------------------
$dataPath = Join-Path $OutputDir 'restore_test.mdf'
$logPath = Join-Path $OutputDir 'restore_test.ldf'
Invoke-Sql "RESTORE DATABASE [$RestoreTestDatabase] FROM DISK = N'$outBak' WITH MOVE N'$DatabaseName' TO N'$dataPath', MOVE N'${DatabaseName}_log' TO N'$logPath'"
Test-Installation 'restored .bak' $RestoreTestDatabase

Write-Step 'Packages'
Remove-CreatedDatabases
Get-ChildItem $OutputDir -File | Format-Table Name, Length -AutoSize | Out-String | Write-Host
foreach ($file in $outDacpac, $outBak, $outSql) { Assert-True (Test-Path $file) "$(Split-Path $file -Leaf) is ready" }
