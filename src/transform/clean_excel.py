import logging

import pandas as pd

log = logging.getLogger(__name__)

# Mapeo estandarizado de contaminantes de calidad del aire
MAPA_CONTAMINANTES_AIRE = {
    "pm25": "pm2_5",
    "pm2.5": "pm2_5",
    "pm10": "pm10",
    "no2": "nitrogen_dioxide",
    "so2": "sulfur_dioxide",
    "co": "carbon_monoxide",
    "o3": "ozone",
    "no": "nitrogen_monoxide",
    "nox": "nitrogen_oxides",
    "pm1": "pm1",
}


def _normalizar_unidad(u: str) -> str:
    """Corrige encodings dañados del Excel original a unidades estándar legibles."""
    u_str = str(u).strip()
    if "g/m" in u_str:
        return "ug/m3"
    elif "particles" in u_str or "cm" in u_str:
        return "particles/cm3"
    return u_str


def limpiar_aire_india(df: pd.DataFrame) -> pd.DataFrame:
    """
    Capa SILVER (Excel): limpieza del catalogo de sensores.
    Funcion pura: recibe el DataFrame Bronze y devuelve el Silver (no escribe archivos).
    """
    log.info("[SILVER - INDIA] Iniciando transformacion y limpieza de calidad del aire en India")
    total_crudo = len(df)
    df_clean = df.copy()

    # Normalizar nombres de columnas a minúsculas sin espacios
    df_clean.columns = [c.strip().lower() for c in df_clean.columns]

    # Filtrar únicamente métricas de calidad del aire (eliminar clima)
    df_clean["parameter"] = df_clean["parameter"].astype(str).str.strip().str.lower()
    df_clean = df_clean[df_clean["parameter"].isin(MAPA_CONTAMINANTES_AIRE.keys())].copy()
    df_clean["parameter_std"] = df_clean["parameter"].map(MAPA_CONTAMINANTES_AIRE)
    filas_aire = len(df_clean)
    log.info("Filtrado de variables: %s filas crudas -> %s registros de calidad del aire.",
             f"{total_crudo:,}", f"{filas_aire:,}")
    log.info("(Se eliminaron %s filas de variables climaticas: temperatura, humedad, viento, etc.)",
             f"{total_crudo - filas_aire:,}")

    # Eliminar duplicados por sensor_id
    duplicados_antes = len(df_clean)
    df_clean = df_clean.drop_duplicates(subset=["sensor_id"]).reset_index(drop=True)
    duplicados_removidos = duplicados_antes - len(df_clean)
    log.info("Duplicados eliminados por sensor_id: %s", duplicados_removidos)

    # Normalizar unidades (corregir encodings a ug/m3 y particles/cm3)
    df_clean["unit"] = df_clean["unit"].apply(_normalizar_unidad)

    # Normalizar identificadores y coordenadas.
    # Un id o coordenada invalida NO se rellena con 0 (crearia una estacion ficticia
    # en el JOIN con la API): la fila se descarta y se reporta en el log.
    df_clean["location_id"] = pd.to_numeric(df_clean["location_id"], errors="coerce")
    df_clean["sensor_id"] = pd.to_numeric(df_clean["sensor_id"], errors="coerce")
    df_clean["latitude"] = pd.to_numeric(df_clean["latitude"], errors="coerce").round(4)
    df_clean["longitude"] = pd.to_numeric(df_clean["longitude"], errors="coerce").round(4)
    invalidas = df_clean[["location_id", "sensor_id", "latitude", "longitude"]].isna().any(axis=1)
    if invalidas.any():
        log.warning("Se descartan %s filas con id o coordenadas invalidas.", int(invalidas.sum()))
    df_clean = df_clean[~invalidas].reset_index(drop=True)
    df_clean["location_id"] = df_clean["location_id"].astype("int64")
    df_clean["sensor_id"] = df_clean["sensor_id"].astype("int64")

    # TRATAMIENTO DE NULOS EN FECHAS (datetime_first y datetime_last)
    # Convertir a datetime UTC de forma segura
    for col in ["datetime_first", "datetime_last"]:
        if col in df_clean.columns:
            df_clean[col] = pd.to_datetime(df_clean[col], errors="coerce", utc=True)

    # Indicador booleano de calidad: identifica si el sensor tiene historial temporal registrado
    df_clean["tiene_historial_fechas"] = df_clean["datetime_first"].notna() & df_clean["datetime_last"].notna()

    # Cálculo de días de monitoreo: para nulos se imputa 0.0 días con flag explícito
    duracion = (df_clean["datetime_last"] - df_clean["datetime_first"]).dt.total_seconds() / 86400
    df_clean["dias_monitoreo"] = duracion.round(1).fillna(0.0)

    # Limpieza de textos y metadatos: imputación de nombres vacíos
    df_clean["location_name"] = df_clean["location_name"].fillna("Desconocido").astype(str).str.strip()
    df_clean["country_name"] = "India"
    df_clean["country_code"] = "IN"
    df_clean["tipo_metrica"] = "calidad_del_aire"

    # Validación de calidad
    sin_historial = int((~df_clean["tiene_historial_fechas"]).sum())
    log.info("Tratamiento de nulos: %s sensores sin fecha tratados (dias_monitoreo=0.0, "
             "flag tiene_historial_fechas=False).", sin_historial)
    log.info("Resumen Silver India: %s sensores de aire | %s estaciones | contaminantes: %s",
             f"{len(df_clean):,}", f"{df_clean['location_id'].nunique():,}",
             sorted(df_clean["parameter_std"].unique()))

    return df_clean
