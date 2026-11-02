"""Escritura de archivos de las capas Silver y Gold.

Las transformaciones son funciones puras (reciben y devuelven DataFrames);
toda la escritura a disco se concentra aqui para poder probarlas de forma aislada.
"""
import logging
import os

import pandas as pd

log = logging.getLogger(__name__)


def leer_silver_sensores(ruta: str) -> pd.DataFrame:
    """Lee el Silver de sensores (Excel) restaurando los tipos de fecha."""
    df = pd.read_csv(ruta)
    for col in ["datetime_first", "datetime_last"]:
        df[col] = pd.to_datetime(df[col], errors="coerce", utc=True)
    return df


def leer_silver_horario(ruta: str) -> pd.DataFrame:
    """Lee el Silver horario (API) restaurando los tipos de fecha."""
    return pd.read_csv(ruta, parse_dates=["date"])


def guardar_csv(df: pd.DataFrame, ruta: str) -> str:
    """Guarda un DataFrame como CSV creando la carpeta si no existe."""
    os.makedirs(os.path.dirname(ruta) or ".", exist_ok=True)
    df.to_csv(ruta, index=False)
    log.info("Archivo guardado: %s (%s filas)", ruta, f"{len(df):,}")
    return ruta


def upsert_csv(df_nuevo: pd.DataFrame, ruta: str, claves: list[str]) -> tuple[pd.DataFrame, int]:
    """Carga incremental (upsert) sobre un CSV existente.

    - Las filas existentes cuya clave aparece en df_nuevo se reemplazan.
    - Las filas existentes con otra clave se conservan (historial).
    - Si el archivo no existe o no se puede leer, se usa solo df_nuevo.

    Devuelve (df_acumulado, n_filas_previas_conservadas). NO escribe el archivo:
    eso lo hace guardar_csv para que el llamador pueda recalcular columnas antes.
    """
    if not os.path.exists(ruta):
        return df_nuevo.copy(), 0

    try:
        df_existente = pd.read_csv(ruta)
    except Exception as e:  # archivo corrupto o vacio
        log.warning("No se pudo leer el historico %s (%s); se genera desde cero.", ruta, e)
        return df_nuevo.copy(), 0

    faltantes = [c for c in claves if c not in df_existente.columns]
    if faltantes:
        log.warning("El historico %s no tiene las columnas clave %s; se reemplaza.", ruta, faltantes)
        return df_nuevo.copy(), 0

    # Igualar tipos de fecha con los del lote nuevo para poder ordenar/comparar
    for col in df_nuevo.columns:
        if col in df_existente.columns and pd.api.types.is_datetime64_any_dtype(df_nuevo[col]):
            df_existente[col] = pd.to_datetime(df_existente[col], errors="coerce")

    def _clave(df: pd.DataFrame) -> pd.Series:
        return df[claves].astype(str).agg("|".join, axis=1)

    claves_nuevas = set(_clave(df_nuevo))
    df_previo = df_existente[~_clave(df_existente).isin(claves_nuevas)]
    df_acumulado = pd.concat([df_previo, df_nuevo], ignore_index=True)
    log.info("[INCREMENTAL] %s: %s filas previas conservadas + %s nuevas/actualizadas = %s",
             ruta, f"{len(df_previo):,}", f"{len(df_nuevo):,}", f"{len(df_acumulado):,}")
    return df_acumulado, len(df_previo)
