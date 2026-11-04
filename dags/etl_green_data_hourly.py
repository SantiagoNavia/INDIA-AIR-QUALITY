"""DAG de Apache Airflow: `etl_green_data_hourly` (Arquitectura Medallion - Green Data India).

Planificacion y zonas horarias:
    - Expresion cron: `0 * * * *` (disparo automatico cada hora en el minuto 00).
    - Zona horaria del planificador (Airflow Scheduler): `UTC` (estandar operativo en contenedores
      para evitar ambiguedades por cambios de horario y garantizar marcas `run_id` monotonicas).
    - Zona horaria funcional de los datos ambientales: `Asia/Kolkata` (IST, UTC+05:30), solicitada
      explicitamente a la API REST Open-Meteo para alinear los promedios diarios con el dia local en India.

Grafo de las 5 tareas y paralelismo:
    [extract_api_bronze, extract_excel_bronze] >> transform_silver >> build_gold >> validate_gold

    1. `extract_api_bronze`   -> Consulta API REST Open-Meteo por lotes y guarda JSON crudo en Bronze.
    2. `extract_excel_bronze` -> Lee Excel estatico, controla version SHA-256 y respalda copia por corrida.
    3. `transform_silver`     -> Se ejecuta SOLO si ambas extracciones terminan en exito (`all_success`);
                                 limpia, estandariza e integra ambas fuentes en la capa Silver.
    4. `build_gold`           -> Genera `data/gold/gold_final.csv`, calcula 5 KPIs ambientales (EAQI + NAQI)
                                 y sincroniza PostgreSQL (`DB_GOLD_INDIA`) de forma idempotente si existe `.env`.
    5. `validate_gold`        -> Verifica existencia, esquema, unicidad de claves y consistencia matematica
                                 de KPIs; lanza excepcion si falla cualquier regla critica.

Diseño modular:
    - La logica ETL reside en `src/pipeline_tasks.py` y los modulos de `src/`.
    - Entre tareas unicamente viajan diccionarios ligeros con rutas y conteos por XCom (nunca DataFrames).
"""
from __future__ import annotations

import sys
from datetime import datetime, timedelta
from pathlib import Path

_DOCKER_MEDALLON = Path("/opt/airflow/medallon")
PROYECTO_DIR = (
    _DOCKER_MEDALLON
    if _DOCKER_MEDALLON.exists()
    else Path(__file__).resolve().parent.parent
)
if str(PROYECTO_DIR) not in sys.path:
    sys.path.insert(0, str(PROYECTO_DIR))

from src import pipeline_tasks  # noqa: E402

try:
    from airflow.sdk import dag, get_current_context, task

    AIRFLOW_DISPONIBLE = True
except ImportError:
    try:
        from airflow.decorators import dag, task
        from airflow.operators.python import get_current_context

        AIRFLOW_DISPONIBLE = True
    except ImportError:
        AIRFLOW_DISPONIBLE = False

        def get_current_context() -> dict:
            raise RuntimeError("Fuera de contexto de ejecucion de Apache Airflow.")


def _obtener_run_id_actual() -> str | None:
    """Obtiene el `run_id` de la corrida activa en Airflow (o `None` en pruebas locales)."""
    try:
        ctx = get_current_context()
        return ctx.get("run_id") or getattr(ctx.get("dag_run"), "run_id", None)
    except Exception:
        return None



# Wrappers ligeros invocables tanto por Airflow como por pytest

def tarea_extract_api_bronze(run_tag: str | None = None) -> dict:
    return pipeline_tasks.run_extract_api_bronze(
        run_tag=run_tag or _obtener_run_id_actual(),
        proyecto_dir=PROYECTO_DIR,
    )


def tarea_extract_excel_bronze(run_tag: str | None = None) -> dict:
    return pipeline_tasks.run_extract_excel_bronze(
        run_tag=run_tag or _obtener_run_id_actual(),
        proyecto_dir=PROYECTO_DIR,
    )


def tarea_transform_silver(api_meta: dict | None = None, excel_meta: dict | None = None) -> dict:
    return pipeline_tasks.run_transform_silver(
        api_meta=api_meta,
        excel_meta=excel_meta,
        proyecto_dir=PROYECTO_DIR,
    )


def tarea_build_gold(silver_meta: dict | None = None) -> dict:
    return pipeline_tasks.run_build_gold(
        silver_meta=silver_meta,
        proyecto_dir=PROYECTO_DIR,
    )


def tarea_validate_gold(gold_meta: dict | None = None) -> dict:
    return pipeline_tasks.run_validate_gold(
        gold_meta=gold_meta,
        proyecto_dir=PROYECTO_DIR,
    )



# Definicion declarativa del DAG (TaskFlow API `@dag` y `@task`)

DEFAULT_ARGS = {
    "owner": "santiago_navia",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

DAG_CONFIG = {
    "dag_id": "etl_green_data_hourly",
    "description": (
        "Pipeline ETL Medallion horario de Calidad del Aire en India "
        "(API REST Open-Meteo + Excel -> Silver -> Gold -> Validacion)"
    ),
    "default_args": DEFAULT_ARGS,
    "start_date": datetime(2026, 10, 1),
    "schedule": "0 * * * *",
    "catchup": False,
    "max_active_runs": 1,
    "tags": ["ETL", "medallion", "green-data"],
}

if AIRFLOW_DISPONIBLE:

    @dag(**DAG_CONFIG)
    def etl_green_data_hourly():
        """Orquesta las 5 tareas Medallion con extraccion paralela en Bronze."""

        @task(task_id="extract_api_bronze")
        def extract_api_bronze() -> dict:
            return tarea_extract_api_bronze()

        @task(task_id="extract_excel_bronze")
        def extract_excel_bronze() -> dict:
            return tarea_extract_excel_bronze()

        @task(task_id="transform_silver", trigger_rule="all_success")
        def transform_silver(api_meta: dict, excel_meta: dict) -> dict:
            return tarea_transform_silver(api_meta=api_meta, excel_meta=excel_meta)

        @task(task_id="build_gold", trigger_rule="all_success")
        def build_gold(silver_meta: dict) -> dict:
            return tarea_build_gold(silver_meta=silver_meta)

        @task(task_id="validate_gold", trigger_rule="all_success")
        def validate_gold(gold_meta: dict) -> dict:
            return tarea_validate_gold(gold_meta=gold_meta)

        # Extracciones Bronze independientes en paralelo -> Silver -> Gold -> Validacion
        api_meta = extract_api_bronze()
        excel_meta = extract_excel_bronze()
        silver_meta = transform_silver(api_meta, excel_meta)
        gold_meta = build_gold(silver_meta)
        validate_gold(gold_meta)

    dag_etl = etl_green_data_hourly()
else:
    dag_etl = None

