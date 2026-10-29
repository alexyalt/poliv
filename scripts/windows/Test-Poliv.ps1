param(
  [string]$Runtime,
  [string]$ProjectRoot,
  [ValidateSet('software','controller')][string]$Stage='software',
  [string]$Resume
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
$env:PYTHONUTF8='1'
$Python=Join-Path $Project '.venv\Scripts\python.exe'
if (!(Test-Path -LiteralPath $Python)) {
  $Launcher=Get-Command py -ErrorAction SilentlyContinue
  $HasPython=$false
  if ($Launcher) {
    & py -3.13 -c 'import sys; print(sys.version)' *> $null
    $HasPython=($LASTEXITCODE -eq 0)
  }
  if (!$HasPython) {
    Write-Host 'Python 3.13 x64 not found. Install it with:' -ForegroundColor Yellow
    Write-Host 'winget install --id Python.Python.3.13 --exact --scope user' -ForegroundColor Cyan
    Write-Host 'Then open a NEW PowerShell window and check: py -3.13 --version'
    throw 'Python 3.13 is required; .venv has not been created.'
  }
  & py -3.13 -m venv (Join-Path $Project '.venv')
  if ($LASTEXITCODE -ne 0) { throw 'Failed to create .venv.' }
}
# Activation is scoped to this process; explicit Python also works under restrictive policies.
$env:VIRTUAL_ENV=Join-Path $Project '.venv'
$env:PATH=(Join-Path $env:VIRTUAL_ENV 'Scripts')+';'+$env:PATH
& $Python -m pip install -q -r requirements.lock
if ($LASTEXITCODE -ne 0) { throw 'Dependency installation failed.' }
$Arguments=@('-m','server.tools.test_wizard','--runtime',$Runtime,'--stage',$Stage)
if ($Resume) { $Arguments+=@('--resume',$Resume) }
& $Python @Arguments
exit $LASTEXITCODE
