import logging
import os

import pandas as pd

log = logging.getLogger(__name__)

# Columnas que el resto del pipeline necesita del Excel original
COLUMNAS_REQUERIDAS = [
    "Location_id", "Location_name", "latitude", "longitude",
    "Sensor_id", "Parameter", "Unit", "datetime_first", "datetime_last",
]


def extraer_excel_india(fuente: dict) -> pd.DataFrame:
    """
    Capa BRONZE: Lee el archivo Excel de calidad del aire en India sin modificar los datos.
    Tecnología: XLSX · pandas (openpyxl).

    Lanza FileNotFoundError si el archivo no existe y ValueError si faltan columnas
    requeridas, para que el pipeline falle con un mensaje claro y no mas adelante.
    """
    ruta = fuente["path"]
    log.info("[BRONZE - EXCEL] Extrayendo dataset de India desde: %s", ruta)
    if not os.path.exists(ruta):
        raise FileNotFoundError(f"No se encontro el Excel de origen: {ruta}")

    df_raw = pd.read_excel(ruta, engine="openpyxl")

    faltantes = [c for c in COLUMNAS_REQUERIDAS if c not in df_raw.columns]
    if faltantes:
        raise ValueError(f"El Excel no tiene las columnas requeridas: {faltantes}")

    log.info("Registros crudos cargados: %s filas, %s columnas, %s estaciones.",
             f"{len(df_raw):,}", df_raw.shape[1], df_raw["Location_id"].nunique())
    return df_raw
