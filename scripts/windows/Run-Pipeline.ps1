[CmdletBinding()]
param(
    [ValidatePattern('^\d{4}-\d{2}-\d{2}$')]
    [string]$BatchDate
)

$pipelineArgs = @{Command='windows-pipeline-worker'}
if ($BatchDate) { $pipelineArgs.BatchDate = $BatchDate }
& (Join-Path $PSScriptRoot 'Invoke-Worker.ps1') @pipelineArgs
exit $LASTEXITCODE
