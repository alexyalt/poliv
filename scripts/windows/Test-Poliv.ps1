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
  & py -3.13 -m venv (Join-Path $Project '.venv')
  if ($LASTEXITCODE -ne 0) { throw 'Install Python 3.13 x64 with py launcher.' }
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
