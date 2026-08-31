[CmdletBinding()]
param(
    [ValidateRange(0, 60)]
    [int]$DelaySeconds = 0
)

$ErrorActionPreference = 'Stop'
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$runtimeDir = Join-Path $projectRoot 'data\windows-runtime'
$logDir = Join-Path $runtimeDir 'logs'
$logFile = Join-Path $logDir 'portal-stop.log'
New-Item -ItemType Directory -Path $logDir -Force | Out-Null

function Write-PortalLog {
    param([Parameter(Mandatory)][string]$Message)
    $entry = "$(Get-Date -Format o) $Message"
    Add-Content -LiteralPath $logFile -Value $entry -Encoding utf8
}

trap {
    Write-PortalLog "FAILED: $($_.Exception.Message)"
    throw
}

$targets = @(
    @{
        Name = 'ngrok'
        PidFile = (Join-Path $runtimeDir 'ngrok.pid')
        Marker = (Join-Path $projectRoot 'ngrok-policy.yml')
    },
    @{
        Name = 'NiceGUI'
        PidFile = (Join-Path $runtimeDir 'nicegui.pid')
        Marker = 'jobsimplesearch.cli web'
    }
)

if ($DelaySeconds -gt 0) {
    Write-PortalLog "Shutdown requested; waiting $DelaySeconds second(s)."
    Start-Sleep -Seconds $DelaySeconds
} else {
    Write-PortalLog 'Shutdown requested without delay.'
}

foreach ($target in $targets) {
    if (-not (Test-Path -LiteralPath $target.PidFile)) { continue }
    $savedPid = 0
    $pidText = (Get-Content -LiteralPath $target.PidFile -Raw).Trim()
    if (-not [int]::TryParse($pidText, [ref]$savedPid)) {
        Remove-Item -LiteralPath $target.PidFile -Force
        continue
    }
    $processInfo = Get-CimInstance Win32_Process -Filter "ProcessId = $savedPid" -ErrorAction SilentlyContinue
    if ($processInfo -and $processInfo.CommandLine -like "*$($target.Marker)*") {
        Stop-Process -Id $savedPid -Force
        $message = "Stopped $($target.Name) process $savedPid"
        Write-Output $message
        Write-PortalLog $message
    } elseif ($processInfo) {
        $message = "Refused to stop PID $savedPid because it is not the managed $($target.Name) process."
        Write-Warning $message
        Write-PortalLog $message
    }
    Remove-Item -LiteralPath $target.PidFile -Force
}

Write-PortalLog 'Shutdown task completed.'
