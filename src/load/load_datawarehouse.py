"""Carga de la capa Gold al Data Warehouse (PostgreSQL).

Lee los datos analiticos consolidados de la capa Gold (gold_aire_india.csv o DataFrame)
y los inserta de manera idempotente (UPSERT) en PostgreSQL bajo el esquema publico.
Tambien crea vistas analiticas (modelo dimensional Hechos-Dimensiones) para facilitar
la visualizacion y analisis directo en pgAdmin.
"""
import logging
import os
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

log = logging.getLogger(__name__)

# Buscar archivo .env tanto en la raiz del proyecto como en la carpeta medallon/
RUTA_BASE = Path(__file__).resolve().parent.parent.parent
for ruta_env in [RUTA_BASE / ".env", Path(".env")]:
    if ruta_env.exists():
        load_dotenv(ruta_env)
        break


def obtener_motor_postgres() -> Engine:
    """Construye el motor SQLAlchemy utilizando variables de entorno."""
    url = os.getenv("DATABASE_URL")
    if not url:
        user = os.getenv("DB_USER", "postgres")
        pwd = os.getenv("DB_PASSWORD", "")
        host = os.getenv("DB_HOST", "localhost")
        port = os.getenv("DB_PORT", "5432")
        db = os.getenv("DB_NAME", "DB_GOLD_INDIA")
        url = f"postgresql+psycopg2://{user}:{pwd}@{host}:{port}/{db}"

    return create_engine(url, echo=False)


def crear_vistas_analiticas(engine: Engine) -> None:
    """Crea vistas analiticas para pgAdmin simulando un modelo dimensional (Hechos y Dimensiones)."""
    sql_vistas = """
    -- 1. Dimension Estaciones (Catálogo físico de infraestructura)
    CREATE OR REPLACE VIEW vw_dim_estaciones AS
    SELECT DISTINCT
        location_id,
        location_name,
        latitude,
        longitude,
        nivel_cobertura,
        total_contaminantes,
        contaminantes_monitoreados,
        mide_pm2_5,
        mide_pm10,
        mide_no2,
        mide_co,
        mide_o3,
        dias_operacion_max,
        fecha_inicio_red,
        fecha_ultima_medicion,
        estacion_activa_reciente,
        distancia_modelo_km,
        id_punto_consulta,
        n_estaciones_mismo_punto,
        valor_compartido
    FROM gold_aire_india;

    -- 2. Hechos Calidad del Aire Diaria (Metricas y Rankings)
    CREATE OR REPLACE VIEW vw_fact_calidad_aire_diaria AS
    SELECT
        location_id,
        fecha_lectura,
        pm2_5_promedio_dia,
        pm10_promedio_dia,
        no2_promedio_dia,
        so2_promedio_dia,
        co_promedio_dia,
        o3_promedio_dia,
        aqi_promedio_dia AS eaqi_europeo,
        categoria_aqi_predominante AS categoria_eaqi,
        naqi_india_promedio_dia AS naqi_india,
        categoria_naqi_india,
        contaminante_dominante_naqi,
        ranking_peor_aire AS ranking_eaqi,
        ranking_peor_aire_naqi,
        ranking_peor_aire_celda
    FROM gold_aire_india;

    -- 3. Top Estaciones con Peor Calidad del Aire
    CREATE OR REPLACE VIEW vw_top_peor_aire AS
    SELECT
        fecha_lectura,
        ranking_peor_aire_naqi,
        location_name,
        naqi_india_promedio_dia,
        categoria_naqi_india,
        contaminante_dominante_naqi,
        pm2_5_promedio_dia,
        pm10_promedio_dia
    FROM gold_aire_india
    WHERE naqi_india_promedio_dia IS NOT NULL
    ORDER BY fecha_lectura DESC, ranking_peor_aire_naqi ASC;
    """
    with engine.begin() as conn:
        conn.execute(text(sql_vistas))
    log.info("[DATAWAREHOUSE] Vistas analiticas creadas en PostgreSQL (vw_dim_estaciones, vw_fact_calidad_aire_diaria, vw_top_peor_aire).")


def cargar_gold_a_postgres(df: pd.DataFrame | None = None) -> int:
    """Inserta/actualiza los registros de la capa Gold en la tabla 'gold_aire_india' de PostgreSQL.

    Usa una tabla intermedia (staging) y un comando ON CONFLICT (location_id, fecha_lectura)
    para garantizar idempotencia total (si se ejecuta varias veces, no duplica).
    """
    if df is None:
        ruta_csv = RUTA_BASE / "data/gold/gold_aire_india.csv"
        if not ruta_csv.exists():
            ruta_csv = Path("data/gold/gold_aire_india.csv")
        log.info("[DATAWAREHOUSE] Leyendo datos Gold desde: %s", ruta_csv)
        df = pd.read_csv(ruta_csv)

    log.info("[DATAWAREHOUSE] Iniciando carga de %s registros a PostgreSQL...", len(df))
    engine = obtener_motor_postgres()

    tabla_destino = "gold_aire_india"
    tabla_stg = "stg_gold_aire_india"

    # Asegurar tipos adecuados para PostgreSQL
    df_cargar = df.copy()

    # 1. Subir lote a tabla staging temporal
    df_cargar.to_sql(tabla_stg, engine, if_exists="replace", index=False)

    # 2. Crear tabla destino con Clave Primaria compuesta si no existe
    columnas_sql = []
    with engine.connect() as conn:
        stg_cols = conn.execute(text(f"SELECT column_name, data_type FROM information_schema.columns WHERE table_name = '{tabla_stg}';")).fetchall()
        for col_name, dtype in stg_cols:
            tipo = "DOUBLE PRECISION" if "double" in dtype or "numeric" in dtype else dtype
            if col_name == "location_id":
                tipo = "BIGINT"
            elif col_name in ("fecha_lectura", "pais", "codigo_pais"):
                tipo = "VARCHAR(50)"
            columnas_sql.append(f'"{col_name}" {tipo}')

    definicion_cols = ",\n        ".join(columnas_sql)
    sql_crear_tabla = f"""
    CREATE TABLE IF NOT EXISTS {tabla_destino} (
        {definicion_cols},
        PRIMARY KEY (location_id, fecha_lectura)
    );
    """

    # 3. Upsert idempotente desde staging a destino
    cols_nombres = [f'"{c}"' for c in df_cargar.columns]
    lista_cols = ", ".join(cols_nombres)
    actualizaciones = [f'{c} = EXCLUDED.{c}' for c in cols_nombres if c not in ('"location_id"', '"fecha_lectura"')]
    str_actualizaciones = ",\n        ".join(actualizaciones)

    sql_upsert = f"""
    INSERT INTO {tabla_destino} ({lista_cols})
    SELECT {lista_cols} FROM {tabla_stg}
    ON CONFLICT (location_id, fecha_lectura)
    DO UPDATE SET
        {str_actualizaciones};
    DROP TABLE IF EXISTS {tabla_stg};
    """

    with engine.begin() as conn:
        conn.execute(text(sql_crear_tabla))
        conn.execute(text(sql_upsert))

    # 4. Crear vistas analiticas
    crear_vistas_analiticas(engine)

    with engine.connect() as conn:
        total_en_db = conn.execute(text(f"SELECT COUNT(*) FROM {tabla_destino};")).scalar()

    log.info("[DATAWAREHOUSE OK] Carga completada exitosamente. Total registros en PostgreSQL (%s): %s", tabla_destino, total_en_db)
    return total_en_db


# Alias para compatibilidad
def cargar_datos(df: pd.DataFrame | None = None) -> None:
    cargar_gold_a_postgres(df)


if __name__ == "__main__":
    import sys
    from src.utils.logger import configurar_logger

    configurar_logger(nivel="INFO")
    print("=" * 70)
    print("  CARGA DE CAPA GOLD A POSTGRESQL (DATA WAREHOUSE)")
    print("=" * 70)
    total = cargar_gold_a_postgres()
    print(f"\n[OK] Registros activos en PostgreSQL: {total:,}")
