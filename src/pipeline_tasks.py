"""Funciones modulares para cada etapa del DAG `etl_green_data_hourly` (Apache Airflow).

Separa por completo la logica ETL de la definicion del DAG para cumplir con:
- Cero duplicacion de logica de transformacion dentro de `dags/etl_green_data_hourly.py`.
- Paso exclusivo de diccionarios ligeros de metadatos/rutas por XCom (nunca DataFrames).
- Facilidad de pruebas unitarias e integrales sin requerir un servidor Airflow activo.
"""
from __future__ import annotations

import json
import logging
import os
import re
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from src.extract import extract_api, extract_excel
from src.load.load_datawarehouse import cargar_gold_a_postgres
from src.quality.checks import (
    generar_reporte_coordenadas_compartidas,
    generar_tabla_kpi,
    validar_calidad_pipeline,
    validar_gold_final,
)
from src.transform import clean_api, clean_excel, gold_india
from src.utils import io
from src.utils.config import cargar_config

log = logging.getLogger(__name__)

_DOCKER_MEDALLON = Path("/opt/airflow/medallon")
PROYECTO_DIR = (
    _DOCKER_MEDALLON
    if _DOCKER_MEDALLON.exists()
    else Path(__file__).resolve().parent.parent
)


def normalizar_run_tag(run_tag: str | None = None) -> str:
    """Genera o limpia un identificador de corrida seguro para carpetas Windows/Linux."""
    if not run_tag:
        return datetime.now(timezone.utc).strftime("run_%Y%m%dT%H%M%SZ")
    return re.sub(r"[^A-Za-z0-9_.-]", "_", str(run_tag))


def _preparar_contexto(
    config_path: str = "config/config.yaml",
    proyecto_dir: str | Path | None = None,
) -> tuple[Path, dict]:
    raiz = Path(proyecto_dir) if proyecto_dir is not None else PROYECTO_DIR
    os.chdir(raiz)
    return raiz, cargar_config(config_path)


# =====================================================================
# 1. Actividad 1: Extraccion desde API REST hacia Bronze
# =====================================================================
def run_extract_api_bronze(
    run_tag: str | None = None,
    config_path: str = "config/config.yaml",
    proyecto_dir: str | Path | None = None,
) -> dict:
    """Extrae contaminantes horarios desde la API REST Open-Meteo (CAMS) y persiste el JSON
    crudo tanto en `data/bronze/bronze_aire_india_api.json` como en `data/bronze/runs/<run_tag>/`.
    """
    _, config = _preparar_contexto(config_path, proyecto_dir)
    rutas = config["path"]
    fuente_excel = config["sources"]["aire_india_excel"]
    fuente_api = config["sources"]["aire_india_api"]
    tag = normalizar_run_tag(run_tag)

    ruta_excel = fuente_excel["path"]
    if not os.path.exists(ruta_excel):
        raise FileNotFoundError(f"No se encontro el catalogo base de coordenadas: {ruta_excel}")

    df_catalogo_raw = pd.read_excel(ruta_excel, engine="openpyxl")
    df_estaciones = extract_api.obtener_estaciones(
        df_catalogo_raw,
        decimales=fuente_api.get("decimales_coordenadas", 1),
        filtrar_contaminantes=True,
    )

    runs_dir = rutas.get("bronze_runs_dir", "data/bronze/runs")
    ruta_bronze_json = rutas["bronze_api_india_json"]
    ruta_snapshot_json = os.path.join(runs_dir, tag, "bronze_aire_india_api.json")

    df_bronze_aire = extract_api.extraer_api_india(
        fuente_api,
        df_estaciones,
        ruta_bronze_json,
        ruta_snapshot_json=ruta_snapshot_json,
        run_tag=tag,
    )

    max_snaps = config.get("quality", {}).get("max_snapshots_bronze", 48)
    extract_excel.limpiar_snapshots_antiguos(runs_dir, max_snapshots=max_snaps)

    puntos_unicos = int(df_estaciones[["lat_consulta", "lon_consulta"]].drop_duplicates().shape[0])
    meta = {
        "run_tag": tag,
        "extraido_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "procedencia": fuente_api["url"],
        "api_bronze_path": ruta_bronze_json,
        "snapshot_bronze_path": ruta_snapshot_json,
        "estaciones_objetivo": len(df_estaciones),
        "puntos_consulta_unicos": puntos_unicos,
        "filas_horarias_extraidas": len(df_bronze_aire),
    }
    log.info(
        "[EXTRACT_API_BRONZE OK] Corrida=%s | Fuente=%s | Puntos=%s | Registros extraidos=%s",
        tag, fuente_api["url"], puntos_unicos, len(df_bronze_aire),
    )
    return meta


# =====================================================================
# 2. Actividad 2: Extraccion desde fuente Excel hacia Bronze
# =====================================================================
def run_extract_excel_bronze(
    run_tag: str | None = None,
    config_path: str = "config/config.yaml",
    proyecto_dir: str | Path | None = None,
) -> dict:
    """Lee el Excel estatico de estaciones/sensores, registra SHA-256 de control de version
    y preserva copia original inalterada en `data/bronze/runs/<run_tag>/`.
    """
    _, config = _preparar_contexto(config_path, proyecto_dir)
    rutas = config["path"]
    tag = normalizar_run_tag(run_tag)
    runs_dir = rutas.get("bronze_runs_dir", "data/bronze/runs")
    max_snaps = config.get("quality", {}).get("max_snapshots_bronze", 48)

    meta = extract_excel.respaldar_excel_por_corrida(
        config["sources"]["aire_india_excel"],
        runs_dir=runs_dir,
        run_tag=tag,
        max_snapshots=max_snaps,
    )
    log.info(
        "[EXTRACT_EXCEL_BRONZE OK] Corrida=%s | Fuente=%s | Filas leidas=%s | SHA-256=%s...",
        meta["run_tag"], meta["procedencia"], meta["filas_crudas"], meta["sha256_version"][:16],
    )
    return meta


# =====================================================================
# 3. Actividad 3: Transformacion y limpieza en capa Silver
# =====================================================================
def run_transform_silver(
    api_meta: dict | None = None,
    excel_meta: dict | None = None,
    config_path: str = "config/config.yaml",
    proyecto_dir: str | Path | None = None,
) -> dict:
    """Limpia, tipa, estandariza e integra los archivos Bronze de Excel y API en la capa Silver.
    Solo debe ejecutarse cuando AMBAS extracciones Bronze hayan finalizado con exito.
    """
    _, config = _preparar_contexto(config_path, proyecto_dir)
    rutas = config["path"]
    decimales = config.get("sources", {}).get("aire_india_api", {}).get("decimales_coordenadas", 1)

    ruta_excel_bronze = (
        (excel_meta or {}).get("copia_bronze_path")
        or config["sources"]["aire_india_excel"]["path"]
    )
    if not os.path.exists(ruta_excel_bronze):
        ruta_excel_bronze = config["sources"]["aire_india_excel"]["path"]

    ruta_api_bronze = (api_meta or {}).get("snapshot_bronze_path")
    if not ruta_api_bronze or not os.path.exists(ruta_api_bronze):
        ruta_api_bronze = (api_meta or {}).get("api_bronze_path") or rutas["bronze_api_india_json"]

    # 3.1 Limpieza Silver del catalogo Excel
    df_raw_excel = extract_excel.extraer_excel_india({"path": ruta_excel_bronze})
    df_silver_sensores = clean_excel.limpiar_aire_india(df_raw_excel)
    ruta_sensores = rutas["silver_sensores_india_dir"]
    io.guardar_csv(df_silver_sensores, ruta_sensores)

    descartes_excel = len(df_raw_excel) - len(df_silver_sensores)
    log.info(
        "[TRANSFORM_SILVER - EXCEL] Entrada=%s | Salida=%s | Descartes=%s",
        len(df_raw_excel), len(df_silver_sensores), descartes_excel,
    )

    # 3.2 Limpieza Silver de la serie horaria API + JOIN espacial con estaciones
    df_bronze_aire = extract_api.leer_bronze_api_json(ruta_api_bronze)
    df_estaciones = extract_api.obtener_estaciones(df_silver_sensores, decimales=decimales)

    entrada_api = len(df_bronze_aire)
    sin_dup_api = len(
        df_bronze_aire.dropna(subset=["date"]).drop_duplicates(subset=["lat_consulta", "lon_consulta", "date"])
    )
    descartes_api = entrada_api - sin_dup_api

    df_silver_aire = clean_api.limpiar_aire_india_api(df_bronze_aire, df_estaciones)
    ruta_horario = rutas["silver_aire_india_horario_dir"]
    df_acum, previas = io.upsert_csv(df_silver_aire, ruta_horario, ["location_id", "date"])
    df_acum = df_acum.sort_values(["date", "location_id"]).reset_index(drop=True)
    io.guardar_csv(df_acum, ruta_horario)

    log.info(
        "[TRANSFORM_SILVER - API+JOIN] Entrada celdas-hora=%s | Descartes=%s | Salida estacion-hora (lote)=%s | Acumulado=%s",
        entrada_api, descartes_api, len(df_silver_aire), len(df_acum),
    )

    return {
        "silver_sensores_path": ruta_sensores,
        "silver_horario_path": ruta_horario,
        "excel_filas_entrada": len(df_raw_excel),
        "excel_filas_salida": len(df_silver_sensores),
        "excel_filas_descartadas": descartes_excel,
        "estaciones_limpias": int(df_silver_sensores["location_id"].nunique()),
        "api_filas_entrada": entrada_api,
        "api_filas_descartadas": descartes_api,
        "horario_filas_lote": len(df_silver_aire),
        "horario_filas_previas": previas,
        "horario_filas_totales": len(df_acum),
    }


# =====================================================================
# 4. Actividad 4: Construccion de capa Gold e indicadores KPI
# =====================================================================
def run_build_gold(
    silver_meta: dict | None = None,
    config_path: str = "config/config.yaml",
    proyecto_dir: str | Path | None = None,
    actualizar_db: bool | None = None,
) -> dict:
    """Construye el dataset analitico consolidado `data/gold/gold_final.csv` (y su alias
    `gold_aire_india.csv`), calcula 5 KPIs ambientales (incluyendo EAQI europeo y NAQI CPCB
    de India), guarda la tabla resumen de KPIs y sincroniza PostgreSQL si existe `.env`.
    """
    raiz, config = _preparar_contexto(config_path, proyecto_dir)
    rutas = config["path"]
    cfg_calidad = config.get("quality", {})
    decimales = config.get("sources", {}).get("aire_india_api", {}).get("decimales_coordenadas", 1)
    dias_vigencia = cfg_calidad.get("dias_vigencia_estacion", 365)

    ruta_sensores = (silver_meta or {}).get("silver_sensores_path") or rutas["silver_sensores_india_dir"]
    ruta_horario = (silver_meta or {}).get("silver_horario_path") or rutas["silver_aire_india_horario_dir"]

    df_sensores = io.leer_silver_sensores(ruta_sensores)
    df_aire = io.leer_silver_horario(ruta_horario)

    df_gold_nuevo = gold_india.generar_gold_india(
        df_sensores,
        df_aire,
        decimales_coord=decimales,
        dias_vigencia_estacion=dias_vigencia,
    )
    gold_india.resumen_gold(df_gold_nuevo)

    dir_gold_base = os.path.dirname(rutas.get("gold_aire_india_dir", "data/gold/gold_aire_india.csv")) or "data/gold"
    ruta_gold_final = rutas.get("gold_final_dir") or os.path.join(dir_gold_base, "gold_final.csv")
    ruta_gold_compat = rutas.get("gold_aire_india_dir") or ruta_gold_final
    ruta_base_upsert = ruta_gold_final if os.path.exists(ruta_gold_final) else ruta_gold_compat

    df_gold, previas = io.upsert_csv(df_gold_nuevo, ruta_base_upsert, ["location_id", "fecha_lectura"])
    df_gold = gold_india.calcular_ranking(df_gold)

    io.guardar_csv(df_gold, ruta_gold_final)
    if os.path.normpath(ruta_gold_compat) != os.path.normpath(ruta_gold_final):
        io.guardar_csv(df_gold, ruta_gold_compat)

    # Reporte de auditoria de celdas compartidas y tabla oficial de KPIs
    ruta_reporte = rutas.get("reporte_coordenadas_compartidas_dir") or os.path.join(
        dir_gold_base, "reporte_coordenadas_compartidas.csv"
    )
    io.guardar_csv(
        generar_reporte_coordenadas_compartidas(df_sensores, df_aire, decimales=decimales),
        ruta_reporte,
    )

    ruta_kpi = rutas.get("tabla_kpi_dir") or os.path.join(dir_gold_base, "tabla_kpi_resumen.csv")
    df_kpi = generar_tabla_kpi(df_gold)
    io.guardar_csv(df_kpi, ruta_kpi)

    # Sincronizacion idempotente con PostgreSQL Data Warehouse cuando existe .env
    if actualizar_db is None:
        debe_cargar_db = (raiz / ".env").exists() or bool(os.getenv("DATABASE_URL"))
    else:
        debe_cargar_db = actualizar_db

    registros_db = None
    if debe_cargar_db:
        registros_db = int(cargar_gold_a_postgres(df_gold))

    resumen_gold = {
        "gold_final_path": ruta_gold_final,
        "gold_compat_path": ruta_gold_compat,
        "tabla_kpi_path": ruta_kpi,
        "filas_lote": len(df_gold_nuevo),
        "filas_previas_conservadas": previas,
        "filas_totales": len(df_gold),
        "estaciones": int(df_gold["location_id"].nunique()),
        "eaqi_promedio_nacional": round(float(df_gold["aqi_promedio_dia"].mean()), 2),
        "naqi_india_promedio_nacional": round(float(df_gold["naqi_india_promedio_dia"].mean()), 2),
        "pm2_5_promedio_nacional": round(float(df_gold["pm2_5_promedio_dia"].mean()), 2),
        "pct_excedencia_oms_pm25": round(float((df_gold["pm2_5_promedio_dia"] > 15.0).mean() * 100), 2),
        "postgres_registros": registros_db,
    }
    log.info(
        "[BUILD_GOLD OK] Archivo=%s | Filas=%s | EAQI medio=%s | NAQI India medio=%s | Excedencia OMS PM2.5=%s%%",
        ruta_gold_final,
        resumen_gold["filas_totales"],
        resumen_gold["eaqi_promedio_nacional"],
        resumen_gold["naqi_india_promedio_nacional"],
        resumen_gold["pct_excedencia_oms_pm25"],
    )
    return resumen_gold


# =====================================================================
# 5. Actividad 5: Validacion automatica de la salida Gold
# =====================================================================
def run_validate_gold(
    gold_meta: dict | None = None,
    config_path: str = "config/config.yaml",
    proyecto_dir: str | Path | None = None,
) -> dict:
    """Ejecuta controles automaticos sobre `gold_final.csv` y la integracion Silver-Gold.
    Si cualquier verificacion critica falla, lanza una excepcion para marcar la tarea en rojo.
    """
    _, config = _preparar_contexto(config_path, proyecto_dir)
    rutas = config["path"]
    cfg_calidad = config.get("quality", {})

    dir_gold_base = os.path.dirname(rutas.get("gold_aire_india_dir", "data/gold/gold_aire_india.csv")) or "data/gold"
    ruta_gold_final = (
        (gold_meta or {}).get("gold_final_path")
        or rutas.get("gold_final_dir")
        or os.path.join(dir_gold_base, "gold_final.csv")
    )

    verificacion_gold = validar_gold_final(ruta_gold_final)

    df_sensores = io.leer_silver_sensores(rutas["silver_sensores_india_dir"])
    df_aire = io.leer_silver_horario(rutas["silver_aire_india_horario_dir"])
    df_gold = pd.read_csv(ruta_gold_final)

    metricas_integracion = validar_calidad_pipeline(
        df_sensores,
        df_aire,
        df_gold,
        umbral_cobertura_join=cfg_calidad.get("umbral_cobertura_join", 0.95),
        distancia_alerta_km=cfg_calidad.get("distancia_alerta_km", 15.0),
    )

    resultado = {
        "estado": "OK",
        "validado_utc": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "gold_final": verificacion_gold,
        "calidad_integracion": metricas_integracion,
        "resumen_kpi": gold_meta or {},
    }

    ruta_resumen = config.get("logging", {}).get("resumen_ejecucion", "logs/run_summary.json")
    os.makedirs(os.path.dirname(ruta_resumen) or ".", exist_ok=True)
    with open(ruta_resumen, "w", encoding="utf-8") as f:
        json.dump(
            {
                "orquestador": "Apache Airflow (etl_green_data_hourly)",
                **resultado,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )
    log.info("[VALIDATE_GOLD OK] Todas las verificaciones de calidad y KPIs superadas sin errores.")
    return resultado
