param(
  [Parameter(Mandatory=$true)][string]$Runtime,
  [ValidateSet('server','broker','doctor')][string]$Component='server'
)
$ErrorActionPreference='Stop'
$Project=Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
Set-Location $Project
$Python=Join-Path $Project '.venv\Scripts\python.exe'
if (!(Test-Path $Python)) { throw 'Create .venv first. See docs/WINDOWS_SETUP.md' }
$RuntimePath=(Resolve-Path -LiteralPath $Runtime).Path
$env:POLIV_DATA_ROOT=$RuntimePath
$env:POLIV_CONFIG_LOCAL=Join-Path $RuntimePath 'config.local.toml'
if (!(Test-Path $env:POLIV_CONFIG_LOCAL)) { throw 'config.local.toml missing in runtime' }
if ($Component -eq 'server') {
  & $Python -m uvicorn server.main:app --host 127.0.0.1 --port 8080 --workers 1
} elseif ($Component -eq 'doctor') {
  & $Python -m server.tools.preflight --runtime $RuntimePath
} else {
  $Setup=Get-Content -LiteralPath (Join-Path $RuntimePath 'setup.json') -Raw | ConvertFrom-Json
  $Broker=Join-Path $Setup.mosquitto_dir 'mosquitto.exe'
  & $Broker -c (Join-Path $RuntimePath 'broker\mosquitto.conf') -v
}
if ($LASTEXITCODE -ne 0) { throw "Component $Component exited with code $LASTEXITCODE" }
