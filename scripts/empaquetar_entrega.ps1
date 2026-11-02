<#
.SYNOPSIS
    Empaqueta el proyecto para la entrega SIN entornos virtuales, credenciales ni caches.

.DESCRIPTION
    Copia el proyecto a una carpeta temporal excluyendo:
      - venv/, .venv/, env/            (entornos virtuales)
      - .env                           (credenciales; se entrega solo .env.example)
      - __pycache__/, .pytest_cache/, .ipynb_checkpoints/, .cache.sqlite, .coverage, htmlcov/
      - dist/ y cualquier .zip previo
    Luego genera dist/entrega_medallon_<fecha>.zip y verifica que no contenga nada prohibido.

.EXAMPLE
    # Desde la carpeta medallon/
    powershell -ExecutionPolicy Bypass -File scripts/empaquetar_entrega.ps1
#>
$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

$raiz = Split-Path -Parent $PSScriptRoot          # carpeta medallon/
$nombre = Split-Path -Leaf $raiz
$fecha = Get-Date -Format "yyyyMMdd_HHmm"
$dist = Join-Path $raiz "dist"
$zip = Join-Path $dist "entrega_medallon_$fecha.zip"
$staging = Join-Path ([System.IO.Path]::GetTempPath()) "entrega_$fecha"
$destino = Join-Path $staging $nombre

$carpetasExcluidas = @("venv", ".venv", "env", "ENV", "__pycache__", ".pytest_cache",
                       ".ipynb_checkpoints", "htmlcov", "dist", ".git")
$archivosExcluidos = @(".env", ".cache.sqlite", ".coverage", "*.zip", "*.pyc")

Write-Host "Empaquetando $raiz ..."
New-Item -ItemType Directory -Force -Path $destino | Out-Null
New-Item -ItemType Directory -Force -Path $dist | Out-Null

# robocopy: /E recursivo, /XD carpetas excluidas, /XF archivos excluidos
robocopy $raiz $destino /E /XD $carpetasExcluidas /XF $archivosExcluidos /NFL /NDL /NJH /NJS /NP | Out-Null
if ($LASTEXITCODE -ge 8) { throw "robocopy fallo con codigo $LASTEXITCODE" }

# Verificacion: nada prohibido dentro del paquete
$prohibidos = Get-ChildItem -Path $destino -Recurse -Force | Where-Object {
    ($_.PSIsContainer -and $carpetasExcluidas -contains $_.Name) -or
    (-not $_.PSIsContainer -and ($_.Name -eq ".env" -or $_.Name -eq ".cache.sqlite"))
}
if ($prohibidos) {
    $prohibidos | ForEach-Object { Write-Warning "Prohibido: $($_.FullName)" }
    throw "El paquete contiene archivos/carpetas prohibidos. Abortando."
}

Add-Type -AssemblyName System.IO.Compression.FileSystem
if (Test-Path $zip) { Remove-Item $zip -Force }
[System.IO.Compression.ZipFile]::CreateFromDirectory($destino, $zip)
Remove-Item -Recurse -Force $staging

$tam = [math]::Round((Get-Item $zip).Length / 1MB, 2)
Write-Host "[OK] Paquete generado: $zip ($tam MB)"
Write-Host "     Excluidos: entornos virtuales, .env, caches y __pycache__."

