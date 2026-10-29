param(
  [string]$Runtime,
  [string]$ProjectRoot,
  [ValidateSet('server','broker','doctor')][string]$Component='server'
)
$ErrorActionPreference='Stop'
if ([string]::IsNullOrWhiteSpace($ProjectRoot)) {
  $ProjectRoot=Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
}
$Project=(Resolve-Path -LiteralPath $ProjectRoot).Path
Set-Location -LiteralPath $Project
if ([string]::IsNullOrWhiteSpace($Runtime)) {
  $Runtime=Join-Path (Split-Path $Project -Parent) 'poliv-runtime'
}
Write-Host "Project: $Project"
Write-Host "Runtime: $Runtime"
if (!(Test-Path -LiteralPath (Join-Path $Project 'requirements.lock'))) {
  throw 'Project root is invalid: requirements.lock missing.'
}
$Python=Join-Path $Project '.venv\Scripts\python.exe'
if (!(Test-Path -LiteralPath $Python)) { throw 'Create .venv first. See docs/WINDOWS_SETUP.md' }
$RuntimePath=(Resolve-Path -LiteralPath $Runtime).Path
$env:POLIV_DATA_ROOT=$RuntimePath
$env:POLIV_CONFIG_LOCAL=Join-Path $RuntimePath 'config.local.toml'
if (!(Test-Path -LiteralPath $env:POLIV_CONFIG_LOCAL)) { throw 'config.local.toml missing in runtime' }
if ($Component -eq 'server') {
  & $Python -m uvicorn server.main:app --host 127.0.0.1 --port 8080 --workers 1
} elseif ($Component -eq 'doctor') {
  & $Python -m server.tools.preflight --runtime $RuntimePath
} else {
  $Setup=Get-Content -LiteralPath (Join-Path $RuntimePath 'setup.json') -Raw -Encoding UTF8 | ConvertFrom-Json
  $Broker=Join-Path $Setup.mosquitto_dir 'mosquitto.exe'
  & $Broker -c (Join-Path $RuntimePath 'broker\mosquitto.conf') -v
}
if ($LASTEXITCODE -ne 0) { throw "Component $Component exited with code $LASTEXITCODE" }
