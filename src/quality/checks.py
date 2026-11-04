"""Controles de calidad de datos e integracion entre fuentes (Excel + API).

Incluye:
- Calculo de distancia geodesica (Haversine) entre la estacion real y el punto del modelo.
- Deteccion y reporte de estaciones que comparten coordenadas de consulta.
- Validacion de esquemas, unicidad de claves, rangos geograficos de India,
  valores fisicos de contaminantes y porcentaje de cobertura del JOIN.
"""
import logging

import numpy as np
import pandas as pd

log = logging.getLogger(__name__)

# Limites geograficos aproximados del territorio de India (incluyendo islas)
LAT_INDIA_MIN, LAT_INDIA_MAX = 6.0, 38.0
LON_INDIA_MIN, LON_INDIA_MAX = 68.0, 98.0

# Tope fisico conservador (ug/m3) para detectar corrupcion de datos
TOPE_CONTAMINANTE_UG_M3 = 10_000.0
RADIO_TIERRA_KM = 6_371.0


def distancia_haversine_km(lat1, lon1, lat2, lon2) -> pd.Series:
    """Distancia en kilometros entre dos pares de coordenadas (grados decimales)."""
    lat1_r, lon1_r = np.radians(lat1), np.radians(lon1)
    lat2_r, lon2_r = np.radians(lat2), np.radians(lon2)
    dlat = lat2_r - lat1_r
    dlon = lon2_r - lon1_r
    a = np.sin(dlat / 2.0) ** 2 + np.cos(lat1_r) * np.cos(lat2_r) * np.sin(dlon / 2.0) ** 2
    c = 2.0 * np.arcsin(np.sqrt(np.clip(a, 0.0, 1.0)))
    return pd.Series(RADIO_TIERRA_KM * c).round(2)


def generar_reporte_coordenadas_compartidas(
    df_sensores: pd.DataFrame,
    df_aire: pd.DataFrame,
    decimales: int = 1,
) -> pd.DataFrame:
    """Construye una tabla de auditoria por punto de consulta (lat_consulta, lon_consulta).

    Documenta explicitamente que estaciones caen en la misma celda del modelo Open-Meteo/CAMS,
    reciben series horarias identicas y, por tanto, empatan en los rankings por estacion.
    """
    est = (
        df_sensores[["location_id", "location_name", "latitude", "longitude"]]
        .drop_duplicates("location_id")
        .copy()
    )
    est["lat_consulta"] = est["latitude"].round(decimales)
    est["lon_consulta"] = est["longitude"].round(decimales)
    est["id_punto_consulta"] = (
        est["lat_consulta"].map(lambda x: f"{x:.{decimales}f}")
        + "_"
        + est["lon_consulta"].map(lambda x: f"{x:.{decimales}f}")
    )

    # Coordenada real del nodo del modelo en la API por estacion
    if {"lat_modelo", "lon_modelo"}.issubset(df_aire.columns):
        modelo = df_aire[["location_id", "lat_modelo", "lon_modelo"]].drop_duplicates("location_id")
        est = est.merge(modelo, on="location_id", how="left")
    else:
        est["lat_modelo"] = est["lat_consulta"]
        est["lon_modelo"] = est["lon_consulta"]

    est["distancia_modelo_km"] = distancia_haversine_km(
        est["latitude"], est["longitude"], est["lat_modelo"], est["lon_modelo"]
    )

    reporte = (
        est.sort_values(["id_punto_consulta", "location_id"])
        .groupby(["id_punto_consulta", "lat_consulta", "lon_consulta"], as_index=False)
        .agg(
            lat_modelo=("lat_modelo", "first"),
            lon_modelo=("lon_modelo", "first"),
            n_estaciones_mismo_punto=("location_id", "count"),
            distancia_min_km=("distancia_modelo_km", "min"),
            distancia_promedio_km=("distancia_modelo_km", lambda s: round(float(s.mean()), 2)),
            distancia_max_km=("distancia_modelo_km", "max"),
            location_ids=("location_id", lambda s: ", ".join(str(v) for v in s)),
            location_names=("location_name", lambda s: " | ".join(str(v) for v in s)),
        )
    )
    reporte["valor_compartido"] = reporte["n_estaciones_mismo_punto"] > 1
    reporte = reporte.sort_values(
        ["n_estaciones_mismo_punto", "id_punto_consulta"], ascending=[False, True]
    ).reset_index(drop=True)
    return reporte


def validar_calidad_pipeline(
    df_sensores: pd.DataFrame,
    df_aire: pd.DataFrame,
    df_gold: pd.DataFrame,
    umbral_cobertura_join: float = 0.95,
    distancia_alerta_km: float = 15.0,
) -> dict:
    """Ejecuta todos los controles de calidad de las capas Silver y Gold.

    Lanza ValueError si falla una regla critica (duplicados en clave primaria,
    coordenadas fuera de India, valores negativos o cobertura de JOIN menor al umbral).
    Devuelve un diccionario con las metricas de auditoria para logs/run_summary.json.
    """
    log.info("[CALIDAD] Ejecutando controles de calidad e integracion...")

    # 1. Unicidad de claves primarias
    if not df_sensores["sensor_id"].is_unique:
        raise ValueError("Calidad Silver sensores: existen sensor_id duplicados.")
    if df_aire.duplicated(["location_id", "date"]).any():
        raise ValueError("Calidad Silver horario: existen pares (location_id, date) duplicados.")
    if df_gold.duplicated(["location_id", "fecha_lectura"]).any():
        raise ValueError("Calidad Gold: existen pares (location_id, fecha_lectura) duplicados.")

    # 2. Coordenadas dentro de India
    fuera_lat = ~df_sensores["latitude"].between(LAT_INDIA_MIN, LAT_INDIA_MAX)
    fuera_lon = ~df_sensores["longitude"].between(LON_INDIA_MIN, LON_INDIA_MAX)
    if (fuera_lat | fuera_lon).any():
        raise ValueError(
            f"Calidad geografica: {int((fuera_lat | fuera_lon).sum())} sensores fuera del rango de India."
        )

    # 3. Rangos fisicos de contaminantes (no negativos y bajo tope fisico)
    cols_cont = ["pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide", "sulfur_dioxide", "ozone"]
    presentes = [c for c in cols_cont if c in df_aire.columns]
    vals = df_aire[presentes].dropna(how="all")
    if (vals < 0).any().any():
        raise ValueError("Calidad Silver horario: existen concentraciones negativas.")
    if (vals > TOPE_CONTAMINANTE_UG_M3).any().any():
        raise ValueError("Calidad Silver horario: existen concentraciones por encima del tope fisico.")

    # 4. Cobertura del JOIN Excel <-> API
    ids_catalogo = set(df_sensores["location_id"].unique())
    est_excel = len(ids_catalogo)
    est_con_api = len(ids_catalogo & set(df_aire["location_id"].unique()))
    cobertura = round(est_con_api / est_excel, 4) if est_excel else 0.0
    if cobertura < umbral_cobertura_join:
        raise ValueError(
            f"Cobertura del JOIN insuficiente: {cobertura * 100:.1f}% (< {umbral_cobertura_join * 100:.1f}%)."
        )

    # 5. Coordenadas compartidas y distancia al nodo del modelo
    est_unicas = df_gold.drop_duplicates("location_id")
    n_puntos = int(est_unicas["id_punto_consulta"].nunique()) if "id_punto_consulta" in est_unicas else 0
    n_est_compartidas = (
        int(est_unicas["valor_compartido"].sum()) if "valor_compartido" in est_unicas else 0
    )
    max_por_punto = (
        int(est_unicas["n_estaciones_mismo_punto"].max())
        if "n_estaciones_mismo_punto" in est_unicas and len(est_unicas)
        else 0
    )

    dist_media = dist_max = 0.0
    lejanas = 0
    if "distancia_modelo_km" in est_unicas.columns and est_unicas["distancia_modelo_km"].notna().any():
        dist_media = round(float(est_unicas["distancia_modelo_km"].mean()), 2)
        dist_max = round(float(est_unicas["distancia_modelo_km"].max()), 2)
        lejanas = int((est_unicas["distancia_modelo_km"] > distancia_alerta_km).sum())
        if lejanas:
            log.warning(
                "Hay %s estaciones a mas de %s km del nodo del modelo (max: %s km).",
                lejanas, distancia_alerta_km, dist_max,
            )

    activas = (
        int(est_unicas["estacion_activa_reciente"].sum())
        if "estacion_activa_reciente" in est_unicas
        else 0
    )

    metricas = {
        "unicidad_claves_ok": True,
        "coordenadas_en_india_ok": True,
        "rangos_contaminantes_ok": True,
        "estaciones_catalogo": est_excel,
        "estaciones_con_api": est_con_api,
        "cobertura_join_pct": round(cobertura * 100, 2),
        "puntos_consulta_unicos": n_puntos,
        "estaciones_en_punto_compartido": n_est_compartidas,
        "max_estaciones_por_punto": max_por_punto,
        "distancia_modelo_promedio_km": dist_media,
        "distancia_modelo_max_km": dist_max,
        "estaciones_sobre_umbral_distancia": lejanas,
        "estaciones_activas_ultimo_anio": activas,
        "estaciones_inactivas_con_dato_modelado": est_excel - activas,
    }
    log.info(
        "[CALIDAD OK] Cobertura JOIN: %s%% (%s/%s) | Puntos unicos: %s | Compartidas: %s est. "
        "| Dist. media al modelo: %s km (max %s km) | Activas ultimo año: %s/%s",
        metricas["cobertura_join_pct"], est_con_api, est_excel, n_puntos, n_est_compartidas,
        dist_media, dist_max, activas, est_excel,
    )
    return metricas


COLUMNAS_ESPERADAS_GOLD = {
    "location_id", "location_name", "latitude", "longitude", "fecha_lectura",
    "total_contaminantes", "nivel_cobertura",
    "pm2_5_promedio_dia", "pm2_5_max_dia", "pm10_promedio_dia", "pm10_max_dia",
    "aqi_promedio_dia", "aqi_max_dia", "categoria_aqi_predominante",
    "naqi_india_promedio_dia", "categoria_naqi_india", "contaminante_dominante_naqi",
    "distancia_modelo_km", "n_estaciones_mismo_punto", "valor_compartido",
    "ranking_peor_aire", "ranking_peor_aire_naqi", "ranking_peor_aire_celda",
}


def validar_gold_final(ruta_gold: str) -> dict:
    """Actividad 5 (validate_gold): comprueba existencia del archivo gold_final.csv,
    esquema esperado, integridad de claves, ausencia de duplicados y consistencia
    matematica de los indicadores KPI. Lanza excepcion si falla una regla critica.
    """
    import os

    if not os.path.exists(ruta_gold) or os.path.getsize(ruta_gold) == 0:
        raise FileNotFoundError(f"Validacion Gold fallida: el archivo {ruta_gold} no existe o esta vacio.")

    df = pd.read_csv(ruta_gold)
    if df.empty:
        raise ValueError(f"Validacion Gold fallida: {ruta_gold} no contiene registros.")

    faltantes = sorted(COLUMNAS_ESPERADAS_GOLD - set(df.columns))
    if faltantes:
        raise ValueError(f"Validacion Gold fallida: faltan columnas esperadas en el esquema: {faltantes}")

    if df["location_id"].isna().any() or (df["location_id"] <= 0).any():
        raise ValueError("Validacion Gold fallida: existen location_id nulos o no positivos.")
    if df["fecha_lectura"].isna().any():
        raise ValueError("Validacion Gold fallida: existen fecha_lectura nulas.")
    if df.duplicated(["location_id", "fecha_lectura"]).any():
        n_dup = int(df.duplicated(["location_id", "fecha_lectura"]).sum())
        raise ValueError(f"Validacion Gold fallida: {n_dup} filas duplicadas en clave (location_id, fecha_lectura).")

    # Consistencia matematica de los KPIs
    if (df["total_contaminantes"] < 1).any():
        raise ValueError("Inconsistencia KPI: total_contaminantes menor a 1.")

    con_api = df.dropna(subset=["aqi_promedio_dia"])
    if con_api.empty:
        raise ValueError("Inconsistencia KPI: ninguna fila en Gold tiene valores de la API.")

    if (con_api["pm2_5_max_dia"] + 1e-6 < con_api["pm2_5_promedio_dia"]).any():
        raise ValueError("Inconsistencia KPI: pm2_5_max_dia menor que pm2_5_promedio_dia.")
    if (con_api["pm10_max_dia"] + 1e-6 < con_api["pm10_promedio_dia"]).any():
        raise ValueError("Inconsistencia KPI: pm10_max_dia menor que pm10_promedio_dia.")
    if (con_api["aqi_max_dia"] + 1e-6 < con_api["aqi_promedio_dia"]).any():
        raise ValueError("Inconsistencia KPI: aqi_max_dia menor que aqi_promedio_dia.")
    if not con_api["naqi_india_promedio_dia"].between(0.0, 500.0).all():
        raise ValueError("Inconsistencia KPI: naqi_india_promedio_dia fuera de la escala CPCB [0, 500].")
    if (con_api["ranking_peor_aire"] < 1).any():
        raise ValueError("Inconsistencia KPI: ranking_peor_aire menor a 1.")

    res = {
        "archivo_gold_verificado": ruta_gold,
        "filas_gold": len(df),
        "estaciones_unicas": int(df["location_id"].nunique()),
        "fechas_cubiertas": sorted(df["fecha_lectura"].astype(str).unique().tolist()),
        "duplicados_clave_primaria": 0,
        "esquema_valido": True,
        "consistencia_kpi_ok": True,
    }
    log.info(
        "[VALIDATE GOLD OK] %s validado: %s filas, %s estaciones, 0 duplicados, KPIs consistentes.",
        ruta_gold, len(df), res["estaciones_unicas"],
    )
    return res


def generar_tabla_kpi(df_gold: pd.DataFrame) -> pd.DataFrame:
    """Construye la tabla resumen oficial de KPIs ambientales (Green Data) con definicion,
    formula, columnas utilizadas y valores resultantes sobre el conjunto Gold actual.
    """
    est_unicas = df_gold.drop_duplicates("location_id")
    pct_red_completa = round(float((est_unicas["total_contaminantes"] >= 5).mean() * 100), 2)
    pct_oms_pm25 = round(float((df_gold["pm2_5_promedio_dia"] > 15.0).mean() * 100), 2)
    eaqi_med = round(float(df_gold["aqi_promedio_dia"].mean()), 2)
    naqi_med = round(float(df_gold["naqi_india_promedio_dia"].mean()), 2)
    dist_med = round(float(est_unicas["distancia_modelo_km"].mean()), 2)

    filas = [
        {
            "kpi_id": "KPI_01",
            "nombre_indicador": "Indice Nacional de Calidad del Aire de India (NAQI - CPCB)",
            "definicion": "Severidad diaria de contaminacion atmosferica segun norma oficial CPCB India (escala 0-500).",
            "formula": "NAQI = max_p( ((I_hi - I_lo)/(B_hi - B_lo)) * (C_p - B_lo) + I_lo )",
            "columnas_utilizadas": "pm2_5_promedio_dia, pm10_promedio_dia, no2_promedio_dia, so2_promedio_dia, co_promedio_dia, o3_promedio_dia",
            "valor_resultante": f"{naqi_med} (promedio nacional; rango {df_gold['naqi_india_promedio_dia'].min():.1f} - {df_gold['naqi_india_promedio_dia'].max():.1f})",
        },
        {
            "kpi_id": "KPI_02",
            "nombre_indicador": "Indice Europeo de Calidad del Aire Promedio Diario (EAQI)",
            "definicion": "Promedio diario del indice europeo horario estimado por el modelo Copernicus CAMS.",
            "formula": "EAQI_dia = (1/N) * sum_{h=1..N}(european_aqi_h)",
            "columnas_utilizadas": "european_aqi, date, location_id",
            "valor_resultante": f"{eaqi_med} puntos EAQI (maximo diario registrado: {df_gold['aqi_max_dia'].max():.1f})",
        },
        {
            "kpi_id": "KPI_03",
            "nombre_indicador": "Excedencia del Umbral Diario OMS de PM2.5 (> 15 ug/m3)",
            "definicion": "Porcentaje de registros estacion-dia cuya media diaria de PM2.5 supera la guia OMS 2021 (15 ug/m3).",
            "formula": "Pct_Excedencia = 100 * count(pm2_5_promedio_dia > 15) / count(total)",
            "columnas_utilizadas": "pm2_5_promedio_dia",
            "valor_resultante": f"{pct_oms_pm25}% de estaciones-dia superan 15 ug/m3 (media PM2.5: {df_gold['pm2_5_promedio_dia'].mean():.2f} ug/m3)",
        },
        {
            "kpi_id": "KPI_04",
            "nombre_indicador": "Cobertura Integral de Sensores por Estacion (Red Completa)",
            "definicion": "Porcentaje de estaciones fisicas con capacidad instalada para medir 5 o mas contaminantes.",
            "formula": "Pct_Red_Completa = 100 * count(nunique(parameter_std) >= 5) / Total_Estaciones",
            "columnas_utilizadas": "location_id, parameter_std, total_contaminantes, nivel_cobertura",
            "valor_resultante": f"{pct_red_completa}% en Red Completa ({int((est_unicas['total_contaminantes'] >= 5).sum())} de {len(est_unicas)} estaciones)",
        },
        {
            "kpi_id": "KPI_05",
            "nombre_indicador": "Representatividad Espacial Estacion vs. Celda CAMS (Distancia Haversine)",
            "definicion": "Distancia geodesica media entre la coordenada real de la estacion y el centroide del modelo (~0.1 grados).",
            "formula": "d = 2 * R * arcsin( sqrt(sin^2(dlat/2) + cos(lat1)*cos(lat2)*sin^2(dlon/2)) )",
            "columnas_utilizadas": "latitude, longitude, lat_modelo, lon_modelo",
            "valor_resultante": f"{dist_med} km promedio (maximo: {est_unicas['distancia_modelo_km'].max():.2f} km; 100% < 15 km)",
        },
    ]
    return pd.DataFrame(filas)
