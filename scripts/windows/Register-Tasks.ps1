[CmdletBinding()]
param(
    [string]$NightlyTaskName = 'JobSimpleSearch-Nightly',
    [string]$ReminderTaskName = 'JobSimpleSearch-ReviewReminder',
    [string]$PipelineTaskName = 'JobSimpleSearch-Continue',
    [string]$PortalStopTaskName = 'JobSimpleSearch-ClosePortal',
    [ValidateRange(0, 60)]
    [int]$PortalShutdownDelaySeconds = 3,
    [string]$NightlyAt = '22:30'
)

$ErrorActionPreference = 'Stop'
$projectRoot = (Resolve-Path (Join-Path $PSScriptRoot '..\..')).Path
$currentUser = [System.Security.Principal.WindowsIdentity]::GetCurrent().Name
$principal = New-ScheduledTaskPrincipal `
    -UserId $currentUser `
    -LogonType Interactive `
    -RunLevel Limited
$settings = New-ScheduledTaskSettingsSet `
    -StartWhenAvailable `
    -WakeToRun `
    -MultipleInstances IgnoreNew `
    -ExecutionTimeLimit (New-TimeSpan -Hours 8)

function New-ScriptAction {
    param(
        [Parameter(Mandatory)][string]$ScriptName,
        [string]$AdditionalArguments = ''
    )
    $scriptPath = Join-Path $PSScriptRoot $ScriptName
    $arguments = "-NoProfile -ExecutionPolicy Bypass -File `"$scriptPath`""
    if ($AdditionalArguments) {
        $arguments += " $AdditionalArguments"
    }
    return New-ScheduledTaskAction `
        -Execute 'powershell.exe' `
        -Argument $arguments `
        -WorkingDirectory $projectRoot
}

$nightlyTask = New-ScheduledTask `
    -Action (New-ScriptAction -ScriptName 'Run-Nightly.ps1') `
    -Trigger (New-ScheduledTaskTrigger -Daily -At $NightlyAt) `
    -Principal $principal `
    -Settings $settings `
    -Description 'Search jobs, start the review portal, then send the search email.'
Register-ScheduledTask -TaskName $NightlyTaskName -InputObject $nightlyTask -Force | Out-Null

$reminderTask = New-ScheduledTask `
    -Action (New-ScriptAction -ScriptName 'Run-ReviewReminder.ps1') `
    -Trigger (New-ScheduledTaskTrigger -Daily -At '08:00') `
    -Principal $principal `
    -Settings $settings `
    -Description 'Ensure the review portal is online and send the review reminder.'
Register-ScheduledTask -TaskName $ReminderTaskName -InputObject $reminderTask -Force | Out-Null

$pipelineTask = New-ScheduledTask `
    -Action (New-ScriptAction -ScriptName 'Run-Pipeline.ps1') `
    -Principal $principal `
    -Settings $settings `
    -Description 'On-demand independent details, metadata, AI, and Post-AI worker.'
Register-ScheduledTask -TaskName $PipelineTaskName -InputObject $pipelineTask -Force | Out-Null

$portalStopTask = New-ScheduledTask `
    -Action (New-ScriptAction `
        -ScriptName 'Stop-ReviewPortal.ps1' `
        -AdditionalArguments "-DelaySeconds $PortalShutdownDelaySeconds") `
    -Principal $principal `
    -Settings $settings `
    -Description 'On-demand PID-validated shutdown of managed NiceGUI and ngrok.'
Register-ScheduledTask -TaskName $PortalStopTaskName -InputObject $portalStopTask -Force | Out-Null

Write-Output "Registered $NightlyTaskName at $NightlyAt"
Write-Output "Registered $ReminderTaskName at 08:00"
Write-Output "Registered on-demand task $PipelineTaskName"
Write-Output "Registered on-demand task $PortalStopTaskName"
Write-Output 'Tasks use the current interactive Windows account and require it to be logged on.'
