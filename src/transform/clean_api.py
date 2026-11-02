import logging

import pandas as pd

log = logging.getLogger(__name__)

# Categorias del indice europeo de calidad del aire (EAQI)
EAQI_CATEGORIAS = {
    (0, 20):   "Muy bueno",
    (20, 40):  "Bueno",
    (40, 60):  "Moderado",
    (60, 80):  "Pobre",
    (80, 100): "Muy pobre",
}

# Nombre de la API -> nombre estandar (el mismo que usa el Silver del Excel)
RENOMBRAR_CONTAMINANTES = {"sulphur_dioxide": "sulfur_dioxide"}

COLS_CONTAMINANTES = [
    "pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide",
    "sulfur_dioxide", "ozone", "european_aqi"
]

COLUMNAS_SILVER = [
    "location_id", "location_name", "latitude", "longitude",
    "lat_modelo", "lon_modelo", "date", "fecha", "hora"
] + COLS_CONTAMINANTES + ["categoria_aqi", "country_name", "country_code", "tipo_metrica"]


def _categoria_eaqi(valor: float) -> str:
    """Clasifica el indice europeo de calidad del aire en categoria cualitativa."""
    if pd.isna(valor):
        return "Sin dato"
    for (lo, hi), cat in EAQI_CATEGORIAS.items():
        if lo <= valor < hi:
            return cat
    return "Extremadamente pobre"


def limpiar_aire_india_api(df_crudo: pd.DataFrame, df_estaciones: pd.DataFrame) -> pd.DataFrame:
    """
    Capa SILVER (API): limpieza de la serie horaria y asignacion a cada estacion.
    Funcion pura: recibe el DataFrame Bronze + estaciones y devuelve el Silver (no escribe archivos).
    """
    log.info("[SILVER - INDIA API] Limpieza de valores de calidad del aire")
    df = df_crudo.rename(columns=RENOMBRAR_CONTAMINANTES).copy()

    # 1. Duplicados y fechas ausentes
    filas_inicio = len(df)
    df = df.dropna(subset=["date"]).drop_duplicates(subset=["lat_consulta", "lon_consulta", "date"])
    log.info("Filas crudas: %s -> sin duplicados ni fechas vacias: %s", f"{filas_inicio:,}", f"{len(df):,}")

    # 2. Tipado numerico y valores imposibles
    for col in COLS_CONTAMINANTES:
        df[col] = pd.to_numeric(df[col], errors="coerce").astype("float64")
    negativos = int((df[COLS_CONTAMINANTES] < 0).sum().sum())
    df[COLS_CONTAMINANTES] = df[COLS_CONTAMINANTES].where(df[COLS_CONTAMINANTES] >= 0)

    # 3. Asignar a cada estacion los valores de su punto de consulta (JOIN Excel <-> API)
    est = df_estaciones[["location_id", "location_name", "latitude", "longitude", "lat_consulta", "lon_consulta"]]
    df = est.merge(df, on=["lat_consulta", "lon_consulta"], how="inner")
    df = df.sort_values(["location_id", "date"]).reset_index(drop=True)
    sin_datos = df_estaciones["location_id"].nunique() - df["location_id"].nunique()
    log.info("Estaciones con datos de la API: %s (sin datos: %s)", df["location_id"].nunique(), sin_datos)
    if sin_datos:
        log.warning("%s estaciones del Excel no recibieron datos de la API.", sin_datos)

    # Control de integridad del JOIN: una sola fila por estacion y hora
    duplicadas = int(df.duplicated(subset=["location_id", "date"]).sum())
    if duplicadas:
        raise ValueError(f"El JOIN estacion-API genero {duplicadas} filas duplicadas (location_id, date).")

    # 4. Nulos: interpolacion temporal dentro de cada estacion
    nulos_antes = int(df[COLS_CONTAMINANTES].isna().sum().sum())
    df[COLS_CONTAMINANTES] = (
        df.groupby("location_id")[COLS_CONTAMINANTES]
        .transform(lambda s: s.interpolate(limit_direction="both"))
        .round(2)
    )
    nulos_despues = int(df[COLS_CONTAMINANTES].isna().sum().sum())
    log.info("Valores negativos anulados: %s", negativos)
    log.info("Nulos: %s antes de interpolar -> %s despues", nulos_antes, nulos_despues)

    # 5. Columnas derivadas y metadatos de India
    df["fecha"] = df["date"].dt.date.astype(str)
    df["hora"] = df["date"].dt.hour
    df["categoria_aqi"] = df["european_aqi"].apply(_categoria_eaqi)
    df["country_name"] = "India"
    df["country_code"] = "IN"
    df["tipo_metrica"] = "calidad_del_aire"

    df = df[COLUMNAS_SILVER]
    if len(df):
        log.info("Resultado: %s filas (%s estaciones x %s horas)", f"{len(df):,}",
                 df["location_id"].nunique(), df.groupby("location_id").size().max())
    return df
