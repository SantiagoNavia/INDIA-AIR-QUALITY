"""DAG de Apache Airflow: Pipeline ETL Calidad del Aire en India (Arquitectura Medallion).

Orquesta las 7 etapas del pipeline:
    1. t1_extraer_excel_bronze          -> Verifica y lee el Excel crudo de estaciones.
    2. t2_limpiar_excel_silver          -> Limpia sensores y guarda silver_aire_india_sensores.csv.
    3. t3_extraer_api_bronze            -> Consulta Open-Meteo por lotes y guarda bronze_aire_india_api.json.
    4. t4_limpiar_api_silver            -> Limpia serie horaria, une con estaciones y actualiza silver_aire_india_horario.csv.
    5. t5_construir_capa_gold           -> Calcula indicadores EAQI, NAQI India y rankings en gold_aire_india.csv.
    6. t6_validar_calidad_integracion   -> Ejecuta controles de calidad (checks.py) y guarda run_summary.json.
    7. t7_cargar_postgres_datawarehouse -> Hace UPSERT idempotente en PostgreSQL (DB_GOLD_INDIA) y actualiza vistas.

Diseño para Airflow:
    - Las tareas NO pasan DataFrames pesados por XCom; cada capa lee y escribe sus artefactos
      en data/bronze, data/silver y data/gold, pasando únicamente diccionarios ligeros de métricas.
"""
from __future__ import annotations

import json
import logging
import os
import sys
from datetime import datetime, timedelta
from pathlib import Path

# Asegurar que la raiz del proyecto (medallon/) este en sys.path dentro del worker de Airflow.
# En Docker Compose el proyecto se monta en /opt/airflow/medallon; localmente vive en ../
_DOCKER_MEDALLON = Path("/opt/airflow/medallon")
PROYECTO_DIR = (
    _DOCKER_MEDALLON
    if _DOCKER_MEDALLON.exists()
    else Path(__file__).resolve().parent.parent
)
if str(PROYECTO_DIR) not in sys.path:
    sys.path.insert(0, str(PROYECTO_DIR))

from src.extract import extract_api, extract_excel
from src.load.load_datawarehouse import cargar_gold_a_postgres
from src.quality.checks import (
    generar_reporte_coordenadas_compartidas,
    validar_calidad_pipeline,
)
from src.transform import clean_api, clean_excel, gold_india
from src.utils import io
from src.utils.config import cargar_config

log = logging.getLogger(__name__)


def _contexto_proyecto() -> dict:
    """Posiciona el directorio de trabajo en la raiz del proyecto y devuelve config."""
    os.chdir(PROYECTO_DIR)
    return cargar_config("config/config.yaml")


# =====================================================================
# Funciones de cada tarea (pueden ejecutarse desde Airflow o en tests)
# =====================================================================
def tarea_extraer_excel_bronze(**_context) -> dict:
    config = _contexto_proyecto()
    df_raw = extract_excel.extraer_excel_india(config["sources"]["aire_india_excel"])
    return {
        "excel_filas_crudas": len(df_raw),
        "excel_estaciones_crudas": int(df_raw["Location_id"].nunique()),
    }


def tarea_limpiar_excel_silver(**_context) -> dict:
    config = _contexto_proyecto()
    df_raw = extract_excel.extraer_excel_india(config["sources"]["aire_india_excel"])
    df_silver = clean_excel.limpiar_aire_india(df_raw)
    ruta_salida = config["path"]["silver_sensores_india_dir"]
    io.guardar_csv(df_silver, ruta_salida)
    return {
        "silver_sensores": len(df_silver),
        "silver_estaciones": int(df_silver["location_id"].nunique()),
        "ruta": ruta_salida,
    }


def tarea_extraer_api_bronze(**_context) -> dict:
    config = _contexto_proyecto()
    rutas = config["path"]
    fuente_api = config["sources"]["aire_india_api"]

    df_sensores = io.leer_silver_sensores(rutas["silver_sensores_india_dir"])
    df_estaciones = extract_api.obtener_estaciones(
        df_sensores, fuente_api.get("decimales_coordenadas", 1)
    )
    ruta_json = rutas["bronze_api_india_json"]
    df_bronze_aire = extract_api.extraer_api_india(fuente_api, df_estaciones, ruta_json)
    return {
        "api_puntos_consulta": int(df_estaciones[["lat_consulta", "lon_consulta"]].drop_duplicates().shape[0]),
        "api_estaciones_compartidas": int((df_estaciones["n_estaciones_mismo_punto"] > 1).sum()),
        "api_filas_horarias": len(df_bronze_aire),
        "ruta_json": ruta_json,
    }


def tarea_limpiar_api_silver(**_context) -> dict:
    config = _contexto_proyecto()
    rutas = config["path"]
    fuente_api = config["sources"]["aire_india_api"]

    df_sensores = io.leer_silver_sensores(rutas["silver_sensores_india_dir"])
    df_estaciones = extract_api.obtener_estaciones(
        df_sensores, fuente_api.get("decimales_coordenadas", 1)
    )
    df_bronze_aire = extract_api.leer_bronze_api_json(rutas["bronze_api_india_json"])
    df_silver_aire = clean_api.limpiar_aire_india_api(df_bronze_aire, df_estaciones)

    ruta_horario = rutas["silver_aire_india_horario_dir"]
    df_acum, previas = io.upsert_csv(df_silver_aire, ruta_horario, ["location_id", "date"])
    df_acum = df_acum.sort_values(["date", "location_id"]).reset_index(drop=True)
    io.guardar_csv(df_acum, ruta_horario)
    return {
        "horario_filas_lote": len(df_silver_aire),
        "horario_filas_previas": previas,
        "horario_filas_totales": len(df_acum),
    }


def tarea_construir_capa_gold(**_context) -> dict:
    config = _contexto_proyecto()
    rutas = config["path"]
    cfg_calidad = config.get("quality", {})
    decimales = config.get("sources", {}).get("aire_india_api", {}).get("decimales_coordenadas", 1)
    dias_vigencia = cfg_calidad.get("dias_vigencia_estacion", 365)

    df_sensores = io.leer_silver_sensores(rutas["silver_sensores_india_dir"])
    df_aire = io.leer_silver_horario(rutas["silver_aire_india_horario_dir"])

    df_gold_nuevo = gold_india.generar_gold_india(
        df_sensores, df_aire, decimales_coord=decimales, dias_vigencia_estacion=dias_vigencia
    )
    gold_india.resumen_gold(df_gold_nuevo)

    ruta_gold = rutas["gold_aire_india_dir"]
    df_gold, previas = io.upsert_csv(df_gold_nuevo, ruta_gold, ["location_id", "fecha_lectura"])
    df_gold = gold_india.calcular_ranking(df_gold)
    io.guardar_csv(df_gold, ruta_gold)

    ruta_reporte = rutas.get(
        "reporte_coordenadas_compartidas_dir", "data/gold/reporte_coordenadas_compartidas.csv"
    )
    df_reporte = generar_reporte_coordenadas_compartidas(df_sensores, df_aire, decimales=decimales)
    io.guardar_csv(df_reporte, ruta_reporte)

    return {
        "gold_filas_lote": len(df_gold_nuevo),
        "gold_filas_totales": len(df_gold),
        "eaqi_promedio_nacional": round(float(df_gold_nuevo["aqi_promedio_dia"].mean()), 2),
        "naqi_india_promedio_nacional": round(float(df_gold_nuevo["naqi_india_promedio_dia"].mean()), 2),
    }


def tarea_validar_calidad_integracion(**_context) -> dict:
    import pandas as pd

    config = _contexto_proyecto()
    rutas = config["path"]
    cfg_calidad = config.get("quality", {})

    df_sensores = io.leer_silver_sensores(rutas["silver_sensores_india_dir"])
    df_aire = io.leer_silver_horario(rutas["silver_aire_india_horario_dir"])
    df_gold = pd.read_csv(rutas["gold_aire_india_dir"])

    metricas = validar_calidad_pipeline(
        df_sensores,
        df_aire,
        df_gold,
        umbral_cobertura_join=cfg_calidad.get("umbral_cobertura_join", 0.95),
        distancia_alerta_km=cfg_calidad.get("distancia_alerta_km", 15.0),
    )

    ruta_resumen = config.get("logging", {}).get("resumen_ejecucion", "logs/run_summary.json")
    os.makedirs(os.path.dirname(ruta_resumen) or ".", exist_ok=True)
    with open(ruta_resumen, "w", encoding="utf-8") as f:
        json.dump(
            {
                "orquestador": "Apache Airflow",
                "timestamp": datetime.now().isoformat(timespec="seconds"),
                "estado": "OK",
                "calidad": metricas,
            },
            f,
            ensure_ascii=False,
            indent=2,
        )
    return metricas


def tarea_cargar_postgres_datawarehouse(**_context) -> dict:
    import pandas as pd

    config = _contexto_proyecto()
    df_gold = pd.read_csv(config["path"]["gold_aire_india_dir"])
    total = cargar_gold_a_postgres(df_gold)
    return {"postgres_registros_totales": int(total), "tabla": "gold_aire_india"}


# =====================================================================
# Definicion del DAG de Apache Airflow
# =====================================================================
try:
    from airflow import DAG
    try:
        from airflow.providers.standard.operators.python import PythonOperator
    except ImportError:
        from airflow.operators.python import PythonOperator

    ARGUMENTOS_POR_DEFECTO = {
        "owner": "santiago_navia",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    }

    with DAG(
        dag_id="etl_calidad_aire_india_medallion",
        description="ETL Medallion de Calidad del Aire en India (Excel + API Open-Meteo -> Silver -> Gold -> PostgreSQL)",
        default_args=ARGUMENTOS_POR_DEFECTO,
        start_date=datetime(2026, 10, 1),
        schedule="@daily",
        catchup=False,
        max_active_runs=1,
        tags=["medallion", "calidad_aire", "india", "postgresql"],
    ) as dag:
        t1 = PythonOperator(
            task_id="t1_extraer_excel_bronze",
            python_callable=tarea_extraer_excel_bronze,
        )
        t2 = PythonOperator(
            task_id="t2_limpiar_excel_silver",
            python_callable=tarea_limpiar_excel_silver,
        )
        t3 = PythonOperator(
            task_id="t3_extraer_api_bronze",
            python_callable=tarea_extraer_api_bronze,
        )
        t4 = PythonOperator(
            task_id="t4_limpiar_api_silver",
            python_callable=tarea_limpiar_api_silver,
        )
        t5 = PythonOperator(
            task_id="t5_construir_capa_gold",
            python_callable=tarea_construir_capa_gold,
        )
        t6 = PythonOperator(
            task_id="t6_validar_calidad_integracion",
            python_callable=tarea_validar_calidad_integracion,
        )
        t7 = PythonOperator(
            task_id="t7_cargar_postgres_datawarehouse",
            python_callable=tarea_cargar_postgres_datawarehouse,
        )

        # Grafo de dependencias: Bronze -> Silver -> Gold -> Calidad -> PostgreSQL
        t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7
except ImportError:
    # Permite importar y probar las funciones de las tareas en entornos donde
    # apache-airflow no esta instalado (solo corre dentro del contenedor Docker).
    dag = None

