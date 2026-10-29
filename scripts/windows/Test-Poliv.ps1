param(
  [Parameter(Mandatory=$true)][string]$Runtime,
  [ValidateSet('software','controller')][string]$Stage='software',
  [string]$Resume
)
$ErrorActionPreference='Stop'
$Project=Split-Path (Split-Path $PSScriptRoot -Parent) -Parent
Set-Location $Project
$env:PYTHONUTF8='1'
$Python=Join-Path $Project '.venv\Scripts\python.exe'
if (!(Test-Path $Python)) {
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
