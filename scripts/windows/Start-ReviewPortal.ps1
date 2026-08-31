[CmdletBinding()]
param()

$ErrorActionPreference = 'Stop'
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$runtimeDir = Join-Path $projectRoot 'data\windows-runtime'
$logDir = Join-Path $runtimeDir 'logs'
$webPidFile = Join-Path $runtimeDir 'nicegui.pid'
$ngrokPidFile = Join-Path $runtimeDir 'ngrok.pid'
$envFile = Join-Path $projectRoot '.env'
$policyFile = Join-Path $projectRoot 'ngrok-policy.yml'
$python = Join-Path $projectRoot '.venv\Scripts\python.exe'

New-Item -ItemType Directory -Path $runtimeDir -Force | Out-Null
New-Item -ItemType Directory -Path $logDir -Force | Out-Null

function Get-EnvSetting {
    param([Parameter(Mandatory)][string]$Name)
    if (-not (Test-Path -LiteralPath $envFile)) { return $null }
    $line = Get-Content -LiteralPath $envFile | Where-Object {
        $_ -match ('^\s*' + [regex]::Escape($Name) + '\s*=')
    } | Select-Object -Last 1
    if (-not $line) { return $null }
    $value = ($line -split '=', 2)[1].Trim()
    return $value.Trim('"').Trim("'")
}

function Get-ManagedProcess {
    param(
        [Parameter(Mandatory)][string]$PidFile,
        [Parameter(Mandatory)][string]$CommandMarker
    )
    if (-not (Test-Path -LiteralPath $PidFile)) { return $null }
    $savedPid = 0
    if (-not [int]::TryParse((Get-Content -LiteralPath $PidFile -Raw).Trim(), [ref]$savedPid)) {
        return $null
    }
    $processInfo = Get-CimInstance Win32_Process -Filter "ProcessId = $savedPid" -ErrorAction SilentlyContinue
    if (-not $processInfo) { return $null }
    if ($processInfo.CommandLine -notlike "*$CommandMarker*") { return $null }
    return $processInfo
}

function Wait-LocalWeb {
    param([int]$Port)
    $deadline = (Get-Date).AddSeconds(60)
    do {
        try {
            $response = Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:$Port/review" -TimeoutSec 3
            if ($response.StatusCode -eq 200) { return }
        } catch {
            Start-Sleep -Milliseconds 500
        }
    } while ((Get-Date) -lt $deadline)
    throw "NiceGUI did not become ready on port $Port within 60 seconds."
}

function Wait-NgrokTunnel {
    param([string]$PublicBaseUrl)
    $deadline = (Get-Date).AddSeconds(45)
    do {
        try {
            $tunnels = Invoke-RestMethod -Uri 'http://127.0.0.1:4040/api/tunnels' -TimeoutSec 3
            if ($tunnels.tunnels.public_url -contains $PublicBaseUrl) { return }
        } catch {
            Start-Sleep -Milliseconds 500
        }
    } while ((Get-Date) -lt $deadline)
    throw "ngrok did not expose $PublicBaseUrl within 45 seconds."
}

if (-not (Test-Path -LiteralPath $python)) {
    throw "Virtual-environment Python not found: $python"
}
if (-not (Test-Path -LiteralPath $policyFile)) {
    throw "ngrok traffic policy not found: $policyFile"
}

$webPortText = Get-EnvSetting -Name 'WEB_PORT'
$webPort = if ($webPortText) { [int]$webPortText } else { 5000 }
$publicBaseUrlText = Get-EnvSetting -Name 'PUBLIC_BASE_URL'
$publicBaseUrl = if ($publicBaseUrlText) { $publicBaseUrlText.TrimEnd('/') } else { '' }
if (-not $publicBaseUrl) {
    throw 'PUBLIC_BASE_URL must be configured before starting the review portal.'
}

$webProcess = Get-ManagedProcess -PidFile $webPidFile -CommandMarker 'jobsimplesearch.cli web'
if (-not $webProcess) {
    try {
        $existing = Invoke-WebRequest -UseBasicParsing -Uri "http://127.0.0.1:$webPort/review" -TimeoutSec 2
        if ($existing.StatusCode -eq 200) {
            throw "Port $webPort already has an unmanaged web process. Stop it before using Windows automation."
        }
    } catch {
        if ($_.Exception.Message -like '*unmanaged web process*') { throw }
    }
    $web = Start-Process -FilePath $python `
        -ArgumentList @('-m', 'jobsimplesearch.cli', 'web') `
        -WorkingDirectory $projectRoot `
        -WindowStyle Hidden `
        -RedirectStandardOutput (Join-Path $logDir 'nicegui.out.log') `
        -RedirectStandardError (Join-Path $logDir 'nicegui.err.log') `
        -PassThru
    Set-Content -LiteralPath $webPidFile -Value $web.Id -Encoding ascii
}
Wait-LocalWeb -Port $webPort

$ngrokProcess = Get-ManagedProcess -PidFile $ngrokPidFile -CommandMarker $policyFile
if (-not $ngrokProcess) {
    try {
        $existingTunnels = Invoke-RestMethod -Uri 'http://127.0.0.1:4040/api/tunnels' -TimeoutSec 2
        if ($existingTunnels.tunnels) {
            throw 'An unmanaged ngrok agent is already running. Stop it before using Windows automation.'
        }
    } catch {
        if ($_.Exception.Message -like '*unmanaged ngrok agent*') { throw }
    }
    $ngrokCommand = Get-Command ngrok -ErrorAction Stop
    $ngrok = Start-Process -FilePath $ngrokCommand.Source `
        -ArgumentList @(
            'http',
            "$webPort",
            '--url',
            $publicBaseUrl,
            '--traffic-policy-file',
            $policyFile
        ) `
        -WorkingDirectory $projectRoot `
        -WindowStyle Hidden `
        -RedirectStandardOutput (Join-Path $logDir 'ngrok.out.log') `
        -RedirectStandardError (Join-Path $logDir 'ngrok.err.log') `
        -PassThru
    Set-Content -LiteralPath $ngrokPidFile -Value $ngrok.Id -Encoding ascii
}
Wait-NgrokTunnel -PublicBaseUrl $publicBaseUrl

Write-Output "Review portal ready at $publicBaseUrl"
