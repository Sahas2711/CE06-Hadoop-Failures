#Requires -Version 5.1
<#
  CE-06 Hadoop Practical - Environment Audit
  Read-only audit: detects installed dependencies, versions, env vars, PATH,
  ports, and Hadoop service state. Does NOT modify the system.
  Output: console table + evidence\reports\environment_audit_report.txt
#>

[CmdletBinding()]
param(
    [string]$ReportDir = (Join-Path (Split-Path $PSScriptRoot -Parent) 'evidence\reports')
)

$ErrorActionPreference = 'SilentlyContinue'
$ProgressPreference    = 'SilentlyContinue'

# ---------- helpers ----------
function Get-CmdVersion {
    param([string]$Exe, [string[]]$ArgList = @('--version'))
    try {
        # native tools (java -version, ssh -V) write to stderr; route both streams
        $out = cmd /c "`"$Exe`" $($ArgList -join ' ') 2>&1" | Out-String
        if ($out) { return ($out.Trim() -split "`r?`n" | Select-Object -First 1).Trim() }
        return '(no output)'
    } catch { return $null }
}

function Get-ExePath {
    param([string]$Name)
    $c = Get-Command $Name -ErrorAction SilentlyContinue
    if ($c -and $c.Source) { return $c.Source }
    if ($c -and $c.Path)   { return $c.Path }
    return $null
}

$rows = New-Object System.Collections.Generic.List[object]
function Add-Row {
    param($Component, $Version, $Path, $PathStatus, $ConfigStatus, $Action)
    $rows.Add([pscustomobject]@{
        Component           = $Component
        'Installed Version' = $Version
        'Detected Path'     = $Path
        'PATH Status'       = $PathStatus
        'Configuration'     = $ConfigStatus
        'Required Action'   = $Action
    })
}

$findings = New-Object System.Collections.Generic.List[string]

# ---------- 1. OS / hardware ----------
$os = Get-CimInstance Win32_OperatingSystem
$cpu = Get-CimInstance Win32_Processor
$osCaption = "$($os.Caption) ($($os.Version)) $($os.OSArchitecture)"
$ramTotalGB = [math]::Round($os.TotalVisibleMemorySize / 1MB, 2)
$ramFreeGB  = [math]::Round($os.FreePhysicalMemory / 1MB, 2)
$cpuName = $cpu.Name.Trim()
$cores = $cpu.NumberOfCores
$logical = $cpu.NumberOfLogicalProcessors

Add-Row 'Operating System' $osCaption 'n/a' 'n/a' 'INSTALLED' 'None'
Add-Row 'CPU' "$cpuName ($cores cores / $logical threads)" 'n/a' 'n/a' 'INSTALLED' 'None'
Add-Row 'RAM' "$ramTotalGB GB total / $ramFreeGB GB free" 'n/a' 'n/a' `
    $(if ($ramTotalGB -ge 7) { 'INSTALLED' } else { 'NEEDS VERIFICATION' }) `
    $(if ($ramFreeGB -lt 1.0) { 'Low free RAM - close apps before Hadoop runs' } else { 'None' })

$drives = Get-PSDrive -PSProvider FileSystem | Where-Object { $_.Free -ne $null } |
    ForEach-Object { "$($_.Name): free $([math]::Round($_.Free/1GB,1)) GB / used $([math]::Round($_.Used/1GB,1)) GB" }
Add-Row 'Disk Space' ($drives -join '; ') 'n/a' 'n/a' 'INSTALLED' 'None'

# ---------- 2. Java ----------
$javaPath = Get-ExePath 'java'
$javacPath = Get-ExePath 'javac'
$javaHome = $env:JAVA_HOME

if ($javaPath) {
    $javaVerRaw = Get-CmdVersion $javaPath @('-version')
    $javaVer = if ($javaVerRaw) { $javaVerRaw } else { 'version unknown' }
} else {
    $javaVer = 'NOT FOUND'
}

$javaHomeValid = $false
$javaHomeVer = 'n/a'
if ($javaHome) {
    $javaHomeValid = Test-Path (Join-Path $javaHome 'bin\java.exe')
    if ($javaHomeValid) { $javaHomeVer = Get-CmdVersion (Join-Path $javaHome 'bin\java.exe') @('-version') }
}

$javaPathStatus = if ($javaPath) { 'IN PATH' } else { 'NOT IN PATH' }
$javaConf = if ($javaHome) {
    if ($javaHomeValid) { "JAVA_HOME set: $javaHome" } else { "JAVA_HOME INVALID: $javaHome" }
} else { 'JAVA_HOME NOT SET' }

Add-Row 'Java Runtime (java)' $javaVer $(if ($javaPath) { $javaPath } else { '-' }) $javaPathStatus $javaConf `
    $(if (-not $javaPath) { 'INSTALL Java' }
      elseif (-not $javaHome) { 'SET JAVA_HOME' }
      elseif (-not $javaHomeValid) { 'FIX JAVA_HOME' } else { 'None' })

if ($javacPath) {
    $javacVer = Get-CmdVersion $javacPath @('-version')
    Add-Row 'Java Compiler (javac)' $javacVer $javacPath $javaPathStatus $javaConf $(if ($javacVer) { 'None' } else { 'VERIFY' })
} else {
    Add-Row 'Java Compiler (javac)' 'NOT FOUND' '-' 'NOT IN PATH' 'MISSING' 'INSTALL JDK (full JDK, not just JRE)'
}

# ---------- 3. Hadoop ----------
$hadoopHome = $env:HADOOP_HOME
$hadoopConfDir = $env:HADOOP_CONF_DIR
$hadoopCmd = Get-ExePath 'hadoop'
$hadoopBinLocal = $null
if ($hadoopHome -and (Test-Path $hadoopHome)) {
    foreach ($cand in @((Join-Path $hadoopHome 'bin\hadoop.cmd'), (Join-Path $hadoopHome 'bin\hadoop'))) {
        if (Test-Path $cand) { $hadoopBinLocal = $cand; break }
    }
}

$hadoopVer = 'NOT FOUND'
if ($hadoopCmd) { $hadoopVer = Get-CmdVersion $hadoopCmd @('version') }
elseif ($hadoopBinLocal) { $hadoopVer = Get-CmdVersion $hadoopBinLocal @('version') }
if (-not $hadoopVer) { $hadoopVer = 'NOT FOUND' }

$hadoopPathStatus = if ($hadoopCmd) { 'IN PATH' } elseif ($hadoopBinLocal) { 'NOT IN PATH (dir exists)' } else { 'NOT FOUND' }
$hadoopConfStatus = 'n/a'
if ($hadoopHome) {
    if (-not (Test-Path $hadoopHome)) { $hadoopConfStatus = "HADOOP_HOME INVALID: $hadoopHome" }
    elseif (-not $hadoopConfDir)      { $hadoopConfStatus = 'HADOOP_CONF_DIR NOT SET (defaults to $HADOOP_HOME/etc/hadoop)' }
    elseif (-not (Test-Path $hadoopConfDir)) { $hadoopConfStatus = "HADOOP_CONF_DIR INVALID: $hadoopConfDir" }
    else { $hadoopConfStatus = "HADOOP_CONF_DIR: $hadoopConfDir" }
} else { $hadoopConfStatus = 'HADOOP_HOME NOT SET' }

Add-Row 'Hadoop' $hadoopVer $(if ($hadoopBinLocal) { $hadoopBinLocal } else { '-' }) $hadoopPathStatus $hadoopConfStatus `
    $(if (-not $hadoopBinLocal) { 'INSTALL Hadoop' }
      elseif (-not $hadoopCmd) { 'ADD Hadoop bin to PATH' }
      elseif ($hadoopConfStatus -like 'HADOOP_HOME NOT SET') { 'SET HADOOP_HOME' } else { 'None' })

# winutils (needed for native Windows Hadoop HDFS operations)
$winutils = $null
if ($hadoopBinLocal) {
    $binDir = Split-Path $hadoopBinLocal -Parent
    $winutils = Get-ChildItem $binDir -Filter 'winutils.exe' -ErrorAction SilentlyContinue | Select-Object -First 1
}
Add-Row 'winutils.exe (Windows Hadoop native)' `
    $(if ($winutils) { 'PRESENT' } else { 'ABSENT' }) `
    $(if ($winutils) { $winutils.FullName } else { "$hadoopHome\bin" }) 'n/a' `
    $(if ($winutils) { 'INSTALLED' } else { 'MISSING (required for HDFS on Windows native mode)' }) `
    $(if ($winutils) { 'None' } else { 'REQUIRED if running Hadoop directly on Windows' })

# ---------- 4. Hadoop config content check ----------
$confDirEffective = if ($hadoopConfDir -and (Test-Path $hadoopConfDir)) { $hadoopConfDir }
                     elseif ($hadoopHome) { Join-Path $hadoopHome 'etc\hadoop' } else { $null }

$configFiles = @('core-site.xml','hdfs-site.xml','mapred-site.xml','yarn-site.xml')
$configState = @()
$configuredProps = @()
if ($confDirEffective -and (Test-Path $confDirEffective)) {
    foreach ($f in $configFiles) {
        $fp = Join-Path $confDirEffective $f
        if (-not (Test-Path $fp)) { $configState += "$f=ABSENT"; continue }
        [xml]$xml = Get-Content $fp -Raw -ErrorAction SilentlyContinue
        if (-not $xml) { $configState += "$f=UNREADABLE"; continue }
        $props = @($xml.configuration.property | Where-Object { $_ -and $_.name })
        $named = @($props | ForEach-Object { $_.name })
        $configuredProps += $named
        if ($props.Count -eq 0) { $configState += "$f=EMPTY(default)" }
        else { $configState += "$f=CONFIGURED($($props.Count) props)" }
    }
} else { $configState += 'NO CONF DIR FOUND' }

$relevantKeys = @('mapreduce.map.maxattempts','mapreduce.map.speculative','mapreduce.framework.name',
                  'yarn.nodemanager.resource.memory-mb','mapreduce.jobhistory.address','fs.defaultFS')
$relevantState = @()
foreach ($k in $relevantKeys) {
    $present = $configuredProps -contains $k
    $relevantState += "$k=$(if ($present){'SET'}else{'default/absent'})"
}

Add-Row 'Hadoop Config Files' ($configState -join '; ') $(if ($confDirEffective) { $confDirEffective } else { '-' }) 'n/a' `
    $(if (($configState -match 'EMPTY').Count -eq 4) { 'DEFAULT/EMPTY - cluster never configured' }
      elseif ($configState -match 'CONFIGURED') { 'PARTIALLY CONFIGURED' } else { 'NEEDS VERIFICATION' }) `
    'Write pseudo-distributed config (Phase 2)'

# ---------- 5. Python / Maven / other tools ----------
$pyPath = Get-ExePath 'python'
$pyVer = if ($pyPath) { Get-CmdVersion $pyPath @('--version') } else { 'NOT FOUND' }
if (-not $pyVer) { $pyVer = 'NOT FOUND' }
Add-Row 'Python' $pyVer $(if ($pyPath) { $pyPath } else { '-' }) $(if ($pyPath) { 'IN PATH' } else { 'NOT IN PATH' }) `
    'INSTALLED' $(if ($pyPath) { 'None' } else { 'INSTALL Python (needed for dataset prep)' })

$mvnPath = Get-ExePath 'mvn'
$mvnVer = if ($mvnPath) { (Get-CmdVersion $mvnPath @('-version')) } else { 'NOT FOUND' }
if (-not $mvnVer) { $mvnVer = 'NOT FOUND' }
Add-Row 'Maven' $mvnVer $(if ($mvnPath) { $mvnPath } else { '-' }) $(if ($mvnPath) { 'IN PATH' } else { 'NOT IN PATH' }) `
    'INSTALLED' $(if ($mvnPath) { 'None (optional - javac also available)' } else { 'OPTIONAL: install or use javac + hadoop classpath' })

$sshPath = Get-ExePath 'ssh'
$sshVer = if ($sshPath) { (Get-CmdVersion $sshPath @('-V')) } else { 'NOT FOUND' }
if (-not $sshVer) { $sshVer = 'NOT FOUND' }
$sshdSvc = Get-Service sshd -ErrorAction SilentlyContinue
Add-Row 'SSH Client' $sshVer $(if ($sshPath) { $sshPath } else { '-' }) $(if ($sshPath) { 'IN PATH' } else { 'NOT IN PATH' }) `
    $(if ($sshdSvc) { "sshd service: $($sshdSvc.Status)" } else { 'sshd service ABSENT' }) `
    $(if (-not $sshPath) { 'INSTALL OpenSSH client' } else { 'None' })

$dockerPath = Get-ExePath 'docker'
Add-Row 'Docker' $(if ($dockerPath) { Get-CmdVersion $dockerPath @('--version') } else { 'NOT FOUND' }) `
    $(if ($dockerPath) { $dockerPath } else { '-' }) $(if ($dockerPath) { 'IN PATH' } else { 'NOT IN PATH' }) `
    'INSTALLED' 'None (optional alternative)'

# ---------- 6. WSL2 ----------
$wslStatus = (& wsl.exe --status 2>&1 | Out-String) -replace "`0",''
$wslListRaw = (& wsl.exe --list --verbose 2>&1 | Out-String) -replace "`0",''
$wslLines = ($wslListRaw -split "`r?`n") | Where-Object { $_.Trim() -ne '' }
$wslDefault = if ($wslStatus -match 'Default Distribution:\s*(\S+)') { $Matches[1] } else { '?' }
$wslVer = if ($wslStatus -match 'Default Version:\s*(\d+)') { "WSL2" } else { 'WSL (version unknown)' }
$distroLine = ($wslLines | Where-Object { $_ -match 'Ubuntu|Canonical' } | Select-Object -First 1)
$distroState = if ($distroLine) { ($distroLine -replace '\s+',' ').Trim() } else { 'No Ubuntu distro found' }

$wslStatusText = if ($wslStatus -match 'WSL 2 is enabled|Default Version') { $wslVer } else { 'WSL NOT AVAILABLE / NOT ENABLED' }
Add-Row 'WSL' "$wslStatusText (default distro: $wslDefault)" 'wsl.exe' `
    $(if (Get-Command wsl.exe -ErrorAction SilentlyContinue) { 'IN PATH' } else { 'NOT IN PATH' }) `
    $distroState `
    $(if ($distroLine) { 'None (distro present)' } elseif ($wslStatus -match 'Version') { 'INSTALL Ubuntu distro' } else { 'ENABLE WSL2 (dism.exe /online /enable-feature /featurename:VirtualMachinePlatform)' })

# ---------- 7. Hadoop daemons / services / ports ----------
$javaProcs = Get-CimInstance Win32_Processor -ErrorAction SilentlyContinue | Out-Null
$procs = Get-Process -ErrorAction SilentlyContinue |
    Where-Object { $_.ProcessName -match 'namenode|datanode|resourcemanager|nodemanager|jobhistory|secondarynamenode|hadoop|yarn' } |
    Select-Object ProcessName, Id
$daemonState = if ($procs) { (($procs | ForEach-Object { "$($_.ProcessName)(PID $($_.Id))" }) -join ', ') } else { 'No Hadoop daemons running' }
Add-Row 'Hadoop Daemons' $daemonState 'n/a' 'n/a' $(if ($procs) { 'RUNNING' } else { 'NOT RUNNING' }) 'Start services (Phase 2) if needed'

$portList = @{
    '9870'  = 'HDFS NameNode UI'
    '9864'  = 'HDFS DataNode UI'
    '8088'  = 'YARN ResourceManager UI'
    '8042'  = 'YARN NodeManager UI'
    '19888' = 'MapReduce JobHistory UI'
    '9000'  = 'HDFS RPC (fs.defaultFS)'
    '8032'  = 'YARN RM scheduler'
    '22'    = 'SSH'
    '3306'  = 'MySQL (conflict check)'
    '5432'  = 'PostgreSQL (conflict check)'
}
$portRows = @()
foreach ($p in $portList.Keys) {
    $listen = Get-NetTCPConnection -LocalPort $p -State Listen -ErrorAction SilentlyContinue
    $owner = ''
    if ($listen) {
        $procName = (Get-Process -Id $listen[0].OwningProcess -ErrorAction SilentlyContinue).ProcessName
        $owner = " owned by $procName"
    }
    $label = $portList[$p]
    $portRows += "port $p ($label): $(if ($listen){"LISTENING$owner"}else{'free'})"
}
Add-Row 'Ports' ($portRows -join '; ') 'n/a' 'n/a' `
    $(if (Get-NetTCPConnection -LocalPort 9870,8088,19888 -State Listen -ErrorAction SilentlyContinue) { 'HADOOP PORTS ACTIVE' } else { 'HADOOP PORTS FREE' }) 'None'

# ---------- 8. HDFS reachability (read-only) ----------
$hdfsReach = 'NOT TESTED (no cluster running)'
if (Get-NetTCPConnection -LocalPort 9000,9870 -State Listen -ErrorAction SilentlyContinue) {
    try {
        $r = Invoke-WebRequest -Uri 'http://localhost:9870/' -UseBasicParsing -TimeoutSec 5
        $hdfsReach = "NameNode UI HTTP $($r.StatusCode)"
    } catch { $hdfsReach = "NameNode port open but UI error: $($_.Exception.Message)" }
}
Add-Row 'HDFS' $hdfsReach 'n/a' 'n/a' $(if ($hdfsReach -match '200') { 'HEALTHY' } else { 'NOT RUNNING' }) 'Start HDFS in Phase 2/6'

# ---------- 9. Path inspection for Hadoop/Java entries ----------
$machinePath = [Environment]::GetEnvironmentVariable('Path','Machine')
$userPath    = [Environment]::GetEnvironmentVariable('Path','User')
$pathProblems = @()
if ($hadoopBinLocal -and (($machinePath + ';' + $userPath) -notlike "*$hadoopBinLocal*")) {
    $pathProblems += "Hadoop bin ($hadoopBinLocal) NOT in user/machine PATH"
}
if ($javaPath -and (($machinePath + ';' + $userPath) -notlike "*$(Split-Path $javaPath)*")) {
    $pathProblems += "java dir NOT in user/machine PATH"
}
$jdkDirs = Get-ChildItem 'C:\Program Files\Eclipse Adoptium','C:\Program Files\Java','C:\Program Files\Zulu' -Directory -ErrorAction SilentlyContinue
$javaHomes = ($jdkDirs | ForEach-Object { $_.FullName }) -join '; '
Add-Row 'JDKs on Disk' $(if ($jdkDirs) { ($jdkDirs.Name -join '; ') } else { 'none found in standard dirs' }) $javaHomes 'n/a' 'INSTALLED' 'None'
Add-Row 'PATH Verification' $(if ($pathProblems) { $pathProblems -join '; ' } else { 'All key entries present in PATH' }) 'n/a' `
    $(if ($pathProblems) { 'PROBLEM' } else { 'OK' }) 'n/a' $(if ($pathProblems) { 'Fix PATH entries' } else { 'None' })

# ---------- findings / missing deps ----------
if (-not $javaPath)             { $findings += 'MISSING: Java runtime not found on PATH.' }
if (-not $javacPath)            { $findings += 'MISSING: JDK (javac) not found on PATH.' }
if (-not $javaHome)             { $findings += 'MISCONFIGURED: JAVA_HOME is not set.' }
if ($javaHome -and -not $javaHomeValid) { $findings += "MISCONFIGURED: JAVA_HOME points to invalid path: $javaHome" }
if (-not $hadoopBinLocal)       { $findings += 'MISSING: Hadoop is not installed (HADOOP_HOME not found or invalid).' }
if ($hadoopBinLocal -and -not $hadoopCmd) { $findings += "MISCONFIGURED: Hadoop bin dir exists ($hadoopBinLocal) but is NOT on PATH." }
if ($configState -match 'EMPTY' -and $hadoopBinLocal) { $findings += 'MISCONFIGURED: Hadoop config files are all defaults (never configured for pseudo-distributed mode).' }
if (-not $winutils -and $hadoopBinLocal) { $findings += 'MISSING: winutils.exe absent - HDFS native operations on Windows will fail (needed for Windows-native mode).' }
if (-not $pyPath)               { $findings += 'MISSING: Python not found (needed for dataset preparation script).' }
if (-not $distroLine -and -not $hadoopBinLocal) { $findings += 'MISSING: No usable Hadoop environment (no Windows Hadoop config, verify WSL Ubuntu.' }
if (-not $sshdSvc -and -not $hadoopBinLocal) { $findings += 'NEEDS VERIFICATION: sshd not running; required for Hadoop daemons on some setups.' }
if ($procs)                     { $findings += 'NOTE: Hadoop daemons currently RUNNING - ports may be in use.' }
if ($ramFreeGB -lt 1.0)         { $findings += "WARNING: only $ramFreeGB GB RAM free - close background apps before running the cluster." }
if (-not $findings)             { $findings += 'No missing dependencies detected.' }

# ---------- report ----------
$stamp = Get-Date -Format 'yyyy-MM-dd HH:mm:ss'
if (-not (Test-Path $ReportDir)) { New-Item -ItemType Directory -Path $ReportDir -Force | Out-Null }
$reportFile = Join-Path $ReportDir 'environment_audit_report.txt'

$table = $rows | Format-Table -AutoSize -Wrap | Out-String -Width 220

$report = @"
================================================================================
 CE-06 HADOOP PRACTICAL - ENVIRONMENT AUDIT REPORT
 Student : NAGAR SAHAS PRAKASH (Roll 202301040048)
 Subject : CE-06 - Failures and Speculative Execution
 Generated: $stamp
 Host    : $env:COMPUTERNAME / $env:USERNAME
 Mode    : READ-ONLY (no system changes performed)
================================================================================

COMPONENT SUMMARY TABLE
--------------------------------------------------------------------------------
$table
MISSING DEPENDENCIES / CONFIGURATION PROBLEMS
--------------------------------------------------------------------------------
$(($findings | ForEach-Object { "- $_" }) -join "`n")

RAW WSL STATUS
--------------------------------------------------------------------------------
$($wslStatus -replace "`0",'')

RAW WSL DISTROS
--------------------------------------------------------------------------------
$($wslListRaw -replace "`0",'')

================================================================================
"@

Set-Content -Path $reportFile -Value $report -Encoding UTF8

Write-Host $report
Write-Host "Report saved to: $reportFile" -ForegroundColor Cyan
