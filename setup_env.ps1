param(
    [switch]$WithGui,
    [string]$PythonExe = ""
)

$ErrorActionPreference = "Stop"
Set-Location $PSScriptRoot

if ([string]::IsNullOrWhiteSpace($PythonExe)) {
    if (Get-Command py -ErrorAction SilentlyContinue) {
        & py -3 -m venv .venv
    }
    elseif (Get-Command python -ErrorAction SilentlyContinue) {
        & python -m venv .venv
    }
    else {
        throw "Nao encontrei 'py' nem 'python' no PATH."
    }
}
else {
    & $PythonExe -m venv .venv
}

$venvPython = Join-Path $PSScriptRoot ".venv\Scripts\python.exe"
if (-not (Test-Path $venvPython)) {
    throw "Nao encontrei o Python da venv em $venvPython"
}

& $venvPython -m pip install --upgrade pip

$requirementsFile = if ($WithGui) {
    Join-Path $PSScriptRoot "requirements_full.txt"
}
else {
    Join-Path $PSScriptRoot "requirements_pipeline.txt"
}

& $venvPython -m pip install -r $requirementsFile

Write-Host ""
Write-Host "Ambiente criado em .venv"
Write-Host "Python: $venvPython"
Write-Host "Requirements: $(Split-Path $requirementsFile -Leaf)"
Write-Host ""
Write-Host "Exemplo de uso do pipeline:"
Write-Host "  & `"$venvPython`" .\\nanum_pipeline_29.py"
Write-Host ""
Write-Host "Exemplo de uso da GUI de configuracao:"
Write-Host "  & `"$venvPython`" .\\pipeline29_config_gui.py"
Write-Host ""
Write-Host "Exemplo de uso do viewer rapido:"
Write-Host "  & `"$venvPython`" .\\standalone_kibox_cycle_viewer_fast.py"
