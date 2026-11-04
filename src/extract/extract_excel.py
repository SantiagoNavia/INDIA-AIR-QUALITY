import hashlib
import json
import logging
import os
import re
import shutil
from datetime import datetime, timezone

import pandas as pd

log = logging.getLogger(__name__)

# Columnas que el resto del pipeline necesita del Excel original
COLUMNAS_REQUERIDAS = [
    "Location_id", "Location_name", "latitude", "longitude",
    "Sensor_id", "Parameter", "Unit", "datetime_first", "datetime_last",
]


def calcular_sha256(ruta: str) -> str:
    """Calcula el hash criptografico SHA-256 de un archivo para control de version."""
    h = hashlib.sha256()
    with open(ruta, "rb") as f:
        for bloque in iter(lambda: f.read(65536), b""):
            h.update(bloque)
    return h.hexdigest()


def limpiar_snapshots_antiguos(runs_dir: str, max_snapshots: int = 48) -> None:
    """Conserva hasta las ultimas `max_snapshots` carpetas de corridas en Bronze."""
    if not os.path.isdir(runs_dir) or max_snapshots <= 0:
        return
    subcarpetas = sorted(
        [
            os.path.join(runs_dir, d)
            for d in os.listdir(runs_dir)
            if os.path.isdir(os.path.join(runs_dir, d))
        ]
    )
    excedentes = len(subcarpetas) - max_snapshots
    for vieja in subcarpetas[: max(0, excedentes)]:
        shutil.rmtree(vieja, ignore_errors=True)


def extraer_excel_india(fuente: dict) -> pd.DataFrame:
    """
    Capa BRONZE: Lee el archivo Excel de calidad del aire en India sin modificar los datos.
    Tecnología: XLSX · pandas (openpyxl).

    Lanza FileNotFoundError si el archivo no existe y ValueError si faltan columnas
    requeridas, para que el pipeline falle con un mensaje claro y no mas adelante.
    """
    ruta = fuente["path"]
    hora_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")
    log.info("[BRONZE - EXCEL] Hora UTC: %s | Procedencia: %s", hora_utc, ruta)

    if os.getenv("ETL_SIMULAR_FALLO", "").lower() == "excel":
        raise FileNotFoundError("[FALLO INDUCIDO P4] Simulación de archivo Excel ausente o inaccesible.")

    if not os.path.exists(ruta):
        raise FileNotFoundError(f"No se encontro el Excel de origen: {ruta}")

    sha256 = calcular_sha256(ruta)
    df_raw = pd.read_excel(ruta, engine="openpyxl")

    faltantes = [c for c in COLUMNAS_REQUERIDAS if c not in df_raw.columns]
    if faltantes:
        raise ValueError(f"El Excel no tiene las columnas requeridas: {faltantes}")

    log.info(
        "[BRONZE - EXCEL] Registros crudos: %s filas | Columnas: %s | Estaciones: %s | SHA-256 (version): %s...",
        f"{len(df_raw):,}", df_raw.shape[1], df_raw["Location_id"].nunique(), sha256[:16],
    )
    return df_raw


def respaldar_excel_por_corrida(
    fuente: dict,
    runs_dir: str = "data/bronze/runs",
    run_tag: str | None = None,
    max_snapshots: int = 48,
) -> dict:
    """Lee el Excel estatico, controla su version por SHA-256 y guarda una copia inalterada
    organizada por identificador de ejecucion / fecha-hora en `data/bronze/runs/<run_tag>/`.
    """
    ruta_origen = fuente["path"]
    df_raw = extraer_excel_india(fuente)
    sha256 = calcular_sha256(ruta_origen)
    mtime_utc = datetime.fromtimestamp(os.path.getmtime(ruta_origen), tz=timezone.utc).isoformat(timespec="seconds")
    extraido_utc = datetime.now(timezone.utc).isoformat(timespec="seconds")

    if not run_tag:
        run_tag = datetime.now(timezone.utc).strftime("run_%Y%m%dT%H%M%SZ")
    else:
        run_tag = re.sub(r"[^A-Za-z0-9_.-]", "_", str(run_tag))

    carpeta_corrida = os.path.join(runs_dir, run_tag)
    os.makedirs(carpeta_corrida, exist_ok=True)
    ruta_copia = os.path.join(carpeta_corrida, os.path.basename(ruta_origen))
    shutil.copy2(ruta_origen, ruta_copia)

    meta = {
        "run_tag": run_tag,
        "extraido_utc": extraido_utc,
        "procedencia": ruta_origen,
        "copia_bronze_path": ruta_copia,
        "sha256_version": sha256,
        "archivo_mtime_utc": mtime_utc,
        "tamano_bytes": os.path.getsize(ruta_origen),
        "filas_crudas": len(df_raw),
        "columnas": int(df_raw.shape[1]),
        "estaciones_crudas": int(df_raw["Location_id"].nunique()),
    }
    with open(os.path.join(carpeta_corrida, "metadata_excel.json"), "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    limpiar_snapshots_antiguos(runs_dir, max_snapshots=max_snapshots)
    log.info("[BRONZE - EXCEL] Copia inalterada por corrida guardada en: %s", ruta_copia)
    return meta
