import logging

import pandas as pd

from src.quality.checks import distancia_haversine_km
from src.transform.naqi_india import calcular_naqi_dataframe

log = logging.getLogger(__name__)

ZONA_HORARIA = "Asia/Kolkata"

MAPA_NAQI_DIA = {
    "pm2_5": "pm2_5_promedio_dia",
    "pm10": "pm10_promedio_dia",
    "nitrogen_dioxide": "no2_promedio_dia",
    "sulfur_dioxide": "so2_promedio_dia",
    "carbon_monoxide": "co_promedio_dia",
    "ozone": "o3_promedio_dia",
}

MAPA_NAQI_ACTUAL = {
    "pm2_5": "pm2_5_actual",
    "pm10": "pm10_actual",
    "nitrogen_dioxide": "no2_actual",
    "sulfur_dioxide": "so2_actual",
    "carbon_monoxide": "co_actual",
    "ozone": "o3_actual",
}


def _clasificar_cobertura(n: int) -> str:
    if n >= 5:
        return "Red Completa (5+ contaminantes)"
    elif n >= 3:
        return "Monitoreo Medio (3-4 contaminantes)"
    return "Monitoreo Basico (1-2 contaminantes)"


def indicadores_estacion(df_sensores: pd.DataFrame, decimales_coord: int = 1) -> pd.DataFrame:
    """Del Excel (Silver sensores): informacion de la red de monitoreo, una fila por estacion."""
    df_est = (
        df_sensores.groupby(["location_id", "location_name", "latitude", "longitude"])
        .agg(
            total_contaminantes=("parameter_std", "nunique"),
            contaminantes_monitoreados=("parameter_std", lambda s: ", ".join(sorted(s.unique()))),
            mide_pm2_5=("parameter_std", lambda s: "pm2_5" in s.values),
            mide_pm10=("parameter_std", lambda s: "pm10" in s.values),
            mide_no2=("parameter_std", lambda s: "nitrogen_dioxide" in s.values),
            mide_co=("parameter_std", lambda s: "carbon_monoxide" in s.values),
            mide_o3=("parameter_std", lambda s: "ozone" in s.values),
            dias_operacion_max=("dias_monitoreo", "max"),
            fecha_inicio_red=("datetime_first", "min"),
            fecha_ultima_medicion=("datetime_last", "max"),
        )
        .reset_index()
    )
    if df_est["location_id"].duplicated().any():
        raise ValueError("Hay location_id con nombre o coordenadas distintas en el Silver de sensores.")

    df_est["nivel_cobertura"] = df_est["total_contaminantes"].apply(_clasificar_cobertura)
    # Fechas como texto AAAA-MM-DD; las estaciones sin historial quedan como "Sin registro".
    for col in ["fecha_inicio_red", "fecha_ultima_medicion"]:
        df_est[col] = pd.to_datetime(df_est[col], utc=True).dt.strftime("%Y-%m-%d").fillna("Sin registro")
    df_est["dias_operacion_max"] = df_est["dias_operacion_max"].fillna(0.0).round(1)

    # Coordenadas de consulta y conteo de estaciones que comparten celda del modelo (~0.1°)
    df_est["lat_consulta"] = df_est["latitude"].round(decimales_coord)
    df_est["lon_consulta"] = df_est["longitude"].round(decimales_coord)
    df_est["id_punto_consulta"] = (
        df_est["lat_consulta"].map(lambda x: f"{x:.{decimales_coord}f}")
        + "_"
        + df_est["lon_consulta"].map(lambda x: f"{x:.{decimales_coord}f}")
    )
    df_est["n_estaciones_mismo_punto"] = (
        df_est.groupby("id_punto_consulta")["location_id"].transform("size").astype("int64")
    )
    df_est["valor_compartido"] = df_est["n_estaciones_mismo_punto"] > 1
    return df_est


def lectura_actual(df_aire: pd.DataFrame, ahora: pd.Timestamp) -> pd.DataFrame:
    """De la API: ultima hora disponible hasta 'ahora' (hora de India), una fila por estacion."""
    hasta_ahora = df_aire[df_aire["date"] <= ahora].sort_values("date")
    actual = hasta_ahora.groupby("location_id").tail(1).rename(columns={
        "date": "hora_lectura_actual",
        "pm2_5": "pm2_5_actual", "pm10": "pm10_actual",
        "nitrogen_dioxide": "no2_actual", "sulfur_dioxide": "so2_actual",
        "carbon_monoxide": "co_actual", "ozone": "o3_actual",
        "european_aqi": "aqi_actual", "categoria_aqi": "categoria_aqi_actual",
    })[["location_id", "hora_lectura_actual", "pm2_5_actual", "pm10_actual", "no2_actual",
        "so2_actual", "co_actual", "o3_actual", "aqi_actual", "categoria_aqi_actual"]].copy()

    naqi_act = calcular_naqi_dataframe(actual, MAPA_NAQI_ACTUAL)
    actual["naqi_india_actual"] = naqi_act["naqi"]
    actual["categoria_naqi_actual"] = naqi_act["categoria"]
    return actual


def resumen_diario(df_aire: pd.DataFrame) -> pd.DataFrame:
    """De la API: promedios y maximos por estacion y fecha + indice NAQI de India (CPCB)."""
    agg_spec = dict(
        pm2_5_promedio_dia=("pm2_5", "mean"),
        pm2_5_max_dia=("pm2_5", "max"),
        pm10_promedio_dia=("pm10", "mean"),
        pm10_max_dia=("pm10", "max"),
        no2_promedio_dia=("nitrogen_dioxide", "mean"),
        so2_promedio_dia=("sulfur_dioxide", "mean"),
        co_promedio_dia=("carbon_monoxide", "mean"),
        o3_promedio_dia=("ozone", "mean"),
        aqi_promedio_dia=("european_aqi", "mean"),
        aqi_max_dia=("european_aqi", "max"),
        categoria_aqi_predominante=("categoria_aqi", lambda s: s.mode()[0] if not s.empty else "Sin dato"),
    )
    if {"lat_modelo", "lon_modelo"}.issubset(df_aire.columns):
        agg_spec["lat_modelo"] = ("lat_modelo", "first")
        agg_spec["lon_modelo"] = ("lon_modelo", "first")

    dia = (
        df_aire.groupby(["location_id", "fecha"])
        .agg(**agg_spec)
        .round(2)
        .reset_index()
        .rename(columns={"fecha": "fecha_lectura"})
    )

    naqi_dia = calcular_naqi_dataframe(dia, MAPA_NAQI_DIA)
    dia["naqi_india_promedio_dia"] = naqi_dia["naqi"]
    dia["categoria_naqi_india"] = naqi_dia["categoria"]
    dia["contaminante_dominante_naqi"] = naqi_dia["dominante"]
    return dia


def generar_gold_india(
    df_sensores: pd.DataFrame,
    df_aire: pd.DataFrame,
    ahora: pd.Timestamp | None = None,
    decimales_coord: int = 1,
    dias_vigencia_estacion: int = 365,
) -> pd.DataFrame:
    """
    Capa GOLD: una fila por estacion y fecha (Excel + API).
    Funcion pura: no lee config ni escribe archivos. 'ahora' (hora local de India, sin tz)
    se puede fijar para obtener resultados reproducibles; por defecto es la hora actual.
    """
    log.info("[GOLD - INDIA] Indicadores por estacion (Excel + API: EAQI europeo + NAQI India)")
    if ahora is None:
        ahora = pd.Timestamp.now(tz=ZONA_HORARIA).floor("h").tz_localize(None)

    df_est = indicadores_estacion(df_sensores, decimales_coord=decimales_coord)
    dia = resumen_diario(df_aire)
    actual = lectura_actual(df_aire, ahora)

    # Union por estacion (validate detecta duplicados inesperados en las claves)
    df_gold = (
        df_est.merge(dia, on="location_id", how="left", validate="one_to_many")
        .merge(actual, on="location_id", how="left", validate="many_to_one")
    )

    # Si alguna estacion no tiene lectura de API en esta ejecucion, asignar la fecha de 'ahora'
    df_gold["fecha_lectura"] = df_gold["fecha_lectura"].fillna(ahora.strftime("%Y-%m-%d"))

    # Distancia geodesica (km) entre la estacion fisica y el nodo de rejilla del modelo CAMS
    if {"lat_modelo", "lon_modelo"}.issubset(df_gold.columns):
        df_gold["distancia_modelo_km"] = distancia_haversine_km(
            df_gold["latitude"], df_gold["longitude"], df_gold["lat_modelo"], df_gold["lon_modelo"]
        )
    else:
        df_gold["lat_modelo"] = df_gold["lat_consulta"]
        df_gold["lon_modelo"] = df_gold["lon_consulta"]
        df_gold["distancia_modelo_km"] = distancia_haversine_km(
            df_gold["latitude"], df_gold["longitude"], df_gold["lat_modelo"], df_gold["lon_modelo"]
        )

    # Vigencia del sensor fisico en el catalogo respecto a la fecha de lectura modelada
    ult_med = pd.to_datetime(df_gold["fecha_ultima_medicion"], format="%Y-%m-%d", errors="coerce")
    f_lect = pd.to_datetime(df_gold["fecha_lectura"], format="%Y-%m-%d", errors="coerce")
    dias_inactivo = (f_lect - ult_med).dt.days
    df_gold["estacion_activa_reciente"] = (dias_inactivo.notna() & (dias_inactivo <= dias_vigencia_estacion))

    df_gold["pais"] = "India"
    df_gold["codigo_pais"] = "IN"
    df_gold["fuente_ubicacion"] = "Excel AIR_QUALITY_INDIA_DATASET"
    df_gold["fuente_valores"] = "API Open-Meteo / CAMS (modelo, aproximado)"

    con_api = int(df_gold["aqi_promedio_dia"].notna().sum())
    compartidas = int(df_gold.drop_duplicates("location_id")["valor_compartido"].sum())
    log.info(
        "Filas estacion-fecha: %s | con API: %s | sin API: %s | estaciones en celda compartida: %s",
        len(df_gold), con_api, len(df_gold) - con_api, compartidas,
    )
    return df_gold


def calcular_ranking(df_gold: pd.DataFrame) -> pd.DataFrame:
    """Calcula rankings diarios:
    - ranking_peor_aire: por estacion segun EAQI europeo (1 = peor aire; empata en celdas compartidas).
    - ranking_peor_aire_naqi: por estacion segun NAQI de India (CPCB).
    - ranking_peor_aire_celda: por punto de consulta unico (sin empates artificiales por compartir celda).
    """
    df = df_gold.copy()
    df["ranking_peor_aire"] = (
        df.groupby("fecha_lectura")["aqi_promedio_dia"]
        .rank(ascending=False, method="min")
        .astype("Int64")
    )

    if "naqi_india_promedio_dia" in df.columns:
        df["ranking_peor_aire_naqi"] = (
            df.groupby("fecha_lectura")["naqi_india_promedio_dia"]
            .rank(ascending=False, method="min")
            .astype("Int64")
        )

    if "id_punto_consulta" in df.columns:
        celdas = (
            df[["fecha_lectura", "id_punto_consulta", "aqi_promedio_dia"]]
            .drop_duplicates(["fecha_lectura", "id_punto_consulta"])
            .copy()
        )
        celdas["ranking_peor_aire_celda"] = (
            celdas.groupby("fecha_lectura")["aqi_promedio_dia"]
            .rank(ascending=False, method="dense")
            .astype("Int64")
        )
        df = df.drop(columns=["ranking_peor_aire_celda"], errors="ignore").merge(
            celdas[["fecha_lectura", "id_punto_consulta", "ranking_peor_aire_celda"]],
            on=["fecha_lectura", "id_punto_consulta"],
            how="left",
        )

    return df.sort_values(
        ["fecha_lectura", "ranking_peor_aire", "location_id"],
        ascending=[False, True, True]
    ).reset_index(drop=True)


def resumen_gold(df_gold_nuevo: pd.DataFrame) -> None:
    """Escribe en el log un resumen del lote de Gold generado (EAQI + NAQI India)."""
    log.info("Categoria predominante EAQI (Europeo) del lote actual:")
    for cat, n in df_gold_nuevo["categoria_aqi_predominante"].value_counts().items():
        log.info("  - %-22s: %4d filas estacion-fecha", cat, n)
    if "categoria_naqi_india" in df_gold_nuevo.columns:
        log.info("Categoria NAQI (India CPCB) del lote actual:")
        for cat, n in df_gold_nuevo["categoria_naqi_india"].value_counts().items():
            log.info("  - %-22s: %4d filas estacion-fecha", cat, n)
    log.info("Top 5 estaciones con peor aire (lote actual):")
    top = df_gold_nuevo.dropna(subset=["aqi_promedio_dia"]).sort_values("aqi_promedio_dia", ascending=False).head(5)
    for _, r in top.iterrows():
        naqi_txt = f" | NAQI {r['naqi_india_promedio_dia']:5.1f}" if "naqi_india_promedio_dia" in r else ""
        log.info(
            "  %-42s %s EAQI %6.1f%s | PM2.5 %6.1f",
            str(r["location_name"])[:42], r["fecha_lectura"], r["aqi_promedio_dia"], naqi_txt, r["pm2_5_promedio_dia"],
        )
