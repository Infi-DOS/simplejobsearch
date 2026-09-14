[CmdletBinding()]
param()

& (Join-Path $PSScriptRoot 'Invoke-Worker.ps1') -Command 'windows-nightly-worker'
exit $LASTEXITCODE
