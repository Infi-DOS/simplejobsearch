[CmdletBinding()]
param()

& (Join-Path $PSScriptRoot 'Invoke-Worker.ps1') -Command 'windows-afternoon-quoted-search-worker'
exit $LASTEXITCODE
