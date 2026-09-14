[CmdletBinding()]
param(
    [Parameter(Mandatory)]
    [ValidateSet('windows-nightly-worker', 'windows-review-worker', 'windows-pipeline-worker', 'windows-recovery-worker')]
    [string]$Command,
    [ValidatePattern('^\d{4}-\d{2}-\d{2}$')]
    [string]$BatchDate
)

$ErrorActionPreference = 'Stop'
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$workerPython = Join-Path $projectRoot '.venv\Scripts\python.exe'
$workerLogDir = Join-Path $projectRoot 'data\windows-runtime\logs'
New-Item -ItemType Directory -Path $workerLogDir -Force | Out-Null
$workerLog = Join-Path $workerLogDir ("{0}-{1}-{2}.log" -f $Command, (Get-Date -Format 'yyyyMMdd-HHmmss'), $PID)
Set-Location -LiteralPath $projectRoot
Start-Transcript -LiteralPath $workerLog -Force | Out-Null
$workerExitCode = 1
try {
    Write-Output "$(Get-Date -Format o) Starting $Command batch=$BatchDate"
    $workerArgs = @('-u', '-m', 'simplejobsearch.cli', $Command)
    if ($BatchDate) { $workerArgs += @('--batch-date', $BatchDate) }
    & $workerPython @workerArgs
    $workerExitCode = $LASTEXITCODE
    Write-Output "$(Get-Date -Format o) Worker exit code: $workerExitCode"
} catch {
    Write-Output "$(Get-Date -Format o) Worker exception: $($_.Exception.Message)"
} finally {
    Stop-Transcript | Out-Null
}
exit $workerExitCode
