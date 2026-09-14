[CmdletBinding()]
param()

& (Join-Path $PSScriptRoot 'Invoke-Worker.ps1') -Command 'windows-recovery-worker'
exit $LASTEXITCODE
