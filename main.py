
#  ORQUESTADOR ETL
#  Proyecto: Calidad del Aire en India
# Excel estaciones y sensores (donde y que se mide)
# API valores de calidad del aire en esas coordenadas
# Realizado por Santiago Navia Soto
#
# Uso:
#   python main.py              -> pipeline completo (Bronze -> Silver -> Gold -> Calidad -> PostgreSQL si existe .env)
#   python main.py --solo-gold  -> regenera Gold, controles de calidad y actualiza PostgreSQL desde los archivos Silver
#   python main.py --sin-db     -> omite la carga a PostgreSQL aunque exista .env
import argparse
import json
import logging
import os
import time
from datetime import datetime

# 01 - Extraccion (Bronze)
import src.extract.extract_excel as extractor_excel
import src.extract.extract_api   as extractor_api

# 02 - Transformacion (Silver)
import src.transform.clean_excel as transform_sensores
import src.transform.clean_api   as transform_aire

# 03 - Capa Gold y Calidad
import src.transform.gold_india as gold_india
from src.quality.checks import (
    generar_reporte_coordenadas_compartidas,
    generar_tabla_kpi,
    validar_calidad_pipeline,
    validar_gold_final,
)

# 04 - Carga a Data Warehouse (PostgreSQL)
from src.load.load_datawarehouse import cargar_gold_a_postgres

# Utilidades: configuracion, logging y escritura de archivos
from src.utils.config import cargar_config
from src.utils.logger import configurar_logger
from src.utils import io

BASE_DIR = os.path.dirname(os.path.abspath(__file__))


def _etapa(log: logging.Logger, titulo: str) -> None:
    log.info("-" * 70)
    log.info(">>> %s", titulo)
    log.info("-" * 70)


def construir_gold(config: dict, df_sensores, df_aire, resumen: dict, log: logging.Logger):
    """Genera el lote Gold, aplica la carga incremental, recalcula rankings, valida calidad y guarda."""
    rutas = config["path"]
    cfg_calidad = config.get("quality", {})
    decimales = config.get("sources", {}).get("aire_india_api", {}).get("decimales_coordenadas", 1)
    dias_vigencia = cfg_calidad.get("dias_vigencia_estacion", 365)

    ruta_gold = rutas["gold_aire_india_dir"]
    dir_gold_base = os.path.dirname(ruta_gold) or "data/gold"
    ruta_gold_final = rutas.get("gold_final_dir") or os.path.join(dir_gold_base, "gold_final.csv")

    df_gold_nuevo = gold_india.generar_gold_india(
        df_sensores, df_aire, decimales_coord=decimales, dias_vigencia_estacion=dias_vigencia
    )
    gold_india.resumen_gold(df_gold_nuevo)
    df_gold, previas = io.upsert_csv(df_gold_nuevo, ruta_gold, ["location_id", "fecha_lectura"])
    df_gold = gold_india.calcular_ranking(df_gold)
    io.guardar_csv(df_gold, ruta_gold)
    if os.path.normpath(ruta_gold_final) != os.path.normpath(ruta_gold):
        io.guardar_csv(df_gold, ruta_gold_final)

    # Reporte de coordenadas compartidas, tabla oficial de KPIs y controles de calidad
    ruta_reporte = rutas.get("reporte_coordenadas_compartidas_dir") or os.path.join(
        dir_gold_base, "reporte_coordenadas_compartidas.csv"
    )
    df_reporte_coords = generar_reporte_coordenadas_compartidas(df_sensores, df_aire, decimales=decimales)
    io.guardar_csv(df_reporte_coords, ruta_reporte)

    ruta_kpi = rutas.get("tabla_kpi_dir") or os.path.join(dir_gold_base, "tabla_kpi_resumen.csv")
    io.guardar_csv(generar_tabla_kpi(df_gold), ruta_kpi)

    validar_gold_final(ruta_gold_final)

    metricas_calidad = validar_calidad_pipeline(
        df_sensores,
        df_aire,
        df_gold_nuevo,
        umbral_cobertura_join=cfg_calidad.get("umbral_cobertura_join", 0.95),
        distancia_alerta_km=cfg_calidad.get("distancia_alerta_km", 15.0),
    )

    resumen["gold"] = {
        "filas_lote": len(df_gold_nuevo),
        "filas_historicas_conservadas": previas,
        "filas_totales": len(df_gold),
        "estaciones": int(df_gold_nuevo["location_id"].nunique()),
        "fechas": sorted(df_gold_nuevo["fecha_lectura"].astype(str).unique().tolist()),
        "filas_sin_api": int(df_gold_nuevo["aqi_promedio_dia"].isna().sum()),
        "eaqi_promedio_nacional": round(float(df_gold_nuevo["aqi_promedio_dia"].mean()), 2),
        "naqi_india_promedio_nacional": round(float(df_gold_nuevo["naqi_india_promedio_dia"].mean()), 2),
    }
    resumen["calidad"] = metricas_calidad
    return df_gold


def pipeline_completo(config: dict, resumen: dict, log: logging.Logger) -> None:
    rutas = config["path"]

    # 01 - EXTRACCION / BRONZE
    # Leer el Excel y consumir la API. Datos originales sin modificar.
    _etapa(log, "ETAPA 01 - EXTRACCION / BRONZE (Datos crudos)")
    t0 = time.perf_counter()

    # 1.1 Excel: catalogo de estaciones y sensores de India
    df_bronze_sensores = extractor_excel.extraer_excel_india(config["sources"]["aire_india_excel"])

    # 2.1 Excel -> Silver. Se limpia antes de consultar la API para que solo se consulten
    # estaciones que miden contaminantes (las que solo miden clima no aportan al Gold).
    _etapa(log, "ETAPA 02a - TRANSFORMACION / SILVER Excel (Limpieza y estandarizacion)")
    df_silver_sensores = transform_sensores.limpiar_aire_india(df_bronze_sensores)
    io.guardar_csv(df_silver_sensores, rutas["silver_sensores_india_dir"])

    # 1.2 API: se consulta con las coordenadas de las estaciones del Excel
    _etapa(log, "ETAPA 01b - EXTRACCION / BRONZE API")
    fuente_api = config["sources"]["aire_india_api"]
    df_estaciones = extractor_api.obtener_estaciones(
        df_silver_sensores, fuente_api.get("decimales_coordenadas", 1)
    )
    ruta_bronze_json = rutas["bronze_api_india_json"]
    df_bronze_aire = extractor_api.extraer_api_india(fuente_api, df_estaciones, ruta_bronze_json)
    resumen["bronze"] = {
        "excel_filas": len(df_bronze_sensores),
        "excel_estaciones": int(df_bronze_sensores["Location_id"].nunique()),
        "api_puntos_consulta": int(df_estaciones[["lat_consulta", "lon_consulta"]].drop_duplicates().shape[0]),
        "api_estaciones_en_punto_compartido": int((df_estaciones["n_estaciones_mismo_punto"] > 1).sum()),
        "api_filas_horarias": len(df_bronze_aire),
        "api_json_bytes": os.path.getsize(ruta_bronze_json),
    }

    # 2.2 API -> Silver: valores por estacion y hora (carga incremental)
    _etapa(log, "ETAPA 02b - TRANSFORMACION / SILVER API (Limpieza y estandarizacion)")
    df_silver_aire = transform_aire.limpiar_aire_india_api(df_bronze_aire, df_estaciones)
    ruta_horario = rutas["silver_aire_india_horario_dir"]
    df_horario_acum, previas = io.upsert_csv(df_silver_aire, ruta_horario, ["location_id", "date"])
    df_horario_acum = df_horario_acum.sort_values(["date", "location_id"]).reset_index(drop=True)
    io.guardar_csv(df_horario_acum, ruta_horario)
    resumen["silver"] = {
        "sensores": len(df_silver_sensores),
        "estaciones": int(df_silver_sensores["location_id"].nunique()),
        "horario_filas_lote": len(df_silver_aire),
        "horario_filas_totales": len(df_horario_acum),
        "horario_estaciones_con_api": int(df_silver_aire["location_id"].nunique()),
    }

    # 03 - CAPA GOLD: una fila por estacion y fecha (Excel + API)
    _etapa(log, "ETAPA 03 - CAPA GOLD E INTEGRACION (Indicadores EAQI + NAQI India + Calidad)")
    construir_gold(config, df_silver_sensores, df_silver_aire, resumen, log)
    resumen["duracion_segundos"] = round(time.perf_counter() - t0, 2)


def pipeline_solo_gold(config: dict, resumen: dict, log: logging.Logger) -> None:
    """Regenera Gold a partir de los Silver ya guardados (no consulta la API)."""
    rutas = config["path"]
    t0 = time.perf_counter()
    _etapa(log, "MODO --solo-gold: Gold desde archivos Silver")
    df_sensores = io.leer_silver_sensores(rutas["silver_sensores_india_dir"])
    df_aire = io.leer_silver_horario(rutas["silver_aire_india_horario_dir"])
    log.info("Silver leidos: %s sensores, %s filas horarias", f"{len(df_sensores):,}", f"{len(df_aire):,}")
    construir_gold(config, df_sensores, df_aire, resumen, log)
    resumen["duracion_segundos"] = round(time.perf_counter() - t0, 2)


def main(argv=None):
    parser = argparse.ArgumentParser(description="ETL Calidad del Aire India - Arquitectura Medallion")
    parser.add_argument("--solo-gold", action="store_true",
                        help="Regenera la capa Gold desde los archivos Silver, sin consultar la API.")
    parser.add_argument("--cargar-db", action="store_true",
                        help="Fuerza la carga a PostgreSQL (ya es automatica cuando existe .env).")
    parser.add_argument("--sin-db", action="store_true",
                        help="Omite la carga a PostgreSQL aunque exista el archivo .env.")
    parser.add_argument("--config", default="config/config.yaml", help="Ruta del archivo de configuracion.")
    args = parser.parse_args(argv)

    os.chdir(BASE_DIR)  # las rutas del config son relativas a la carpeta del proyecto
    config = cargar_config(args.config)
    cfg_log = config.get("logging", {})
    log = configurar_logger(cfg_log.get("archivo", "logs/log.txt"), cfg_log.get("nivel", "INFO"))

    modo = "solo_gold" if args.solo_gold else "completo"
    resumen = {"inicio": datetime.now().isoformat(timespec="seconds"), "modo": modo}

    log.info("=" * 70)
    log.info("ETL DE CALIDAD DEL AIRE EN INDIA - ARQUITECTURA MEDALLION (modo: %s)", modo)
    log.info("Excel (estaciones) + API REST Open-Meteo (valores MODELADOS de calidad del aire)")
    log.info("=" * 70)

    try:
        if args.solo_gold:
            pipeline_solo_gold(config, resumen, log)
        else:
            pipeline_completo(config, resumen, log)

        tiene_env = os.path.exists(os.path.join(BASE_DIR, ".env")) or bool(os.getenv("DATABASE_URL"))
        if (tiene_env or args.cargar_db) and not args.sin_db:
            _etapa(log, "ETAPA 04 - CARGA AUTOMATICA A DATA WAREHOUSE (PostgreSQL)")
            import pandas as pd
            ruta_gold = config["path"]["gold_aire_india_dir"]
            total_db = cargar_gold_a_postgres(pd.read_csv(ruta_gold))
            resumen["datawarehouse"] = {
                "registros_cargados": total_db,
                "base_datos": "PostgreSQL",
                "tabla": "gold_aire_india",
                "vistas": ["vw_dim_estaciones", "vw_fact_calidad_aire_diaria", "vw_top_peor_aire"]
            }
        else:
            log.info("[DATAWAREHOUSE] Sin archivo .env activo (o --sin-db); se omite carga a PostgreSQL.")

        resumen["estado"] = "OK"
    except Exception as e:
        resumen["estado"] = "ERROR"
        resumen["error"] = f"{type(e).__name__}: {e}"
        log.exception("El pipeline fallo: %s", e)
        raise
    finally:
        resumen["fin"] = datetime.now().isoformat(timespec="seconds")
        ruta_resumen = cfg_log.get("resumen_ejecucion", "logs/run_summary.json")
        os.makedirs(os.path.dirname(ruta_resumen) or ".", exist_ok=True)
        with open(ruta_resumen, "w", encoding="utf-8") as f:
            json.dump(resumen, f, ensure_ascii=False, indent=2, default=str)
        log.info("Resumen de ejecucion guardado en %s", ruta_resumen)

    # 04 - VALIDACION Y ENTREGA
    _etapa(log, "ETAPA 04 - VALIDACION Y ENTREGA (Artefactos generados)")
    for clave in [
        "bronze_excel_india",
        "bronze_api_india_json",
        "silver_sensores_india_dir",
        "silver_aire_india_horario_dir",
        "gold_final_dir",
        "gold_aire_india_dir",
        "tabla_kpi_dir",
        "reporte_coordenadas_compartidas_dir",
    ]:
        ruta = config["path"].get(clave)
        if ruta:
            log.info("%s %-38s -> %s", "[OK]   " if os.path.exists(ruta) else "[FALTA]", clave, ruta)
    log.info("=" * 70)
    log.info("Pipeline completado con exito en %ss.", resumen.get("duracion_segundos"))
    log.info("=" * 70)


if __name__ == "__main__":
    main()
