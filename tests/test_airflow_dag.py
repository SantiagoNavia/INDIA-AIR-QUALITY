"""Pruebas automatizadas del DAG `etl_green_data_hourly` y sus 5 tareas modulares."""
from datetime import datetime, timedelta

import pandas as pd
import pytest
import yaml

import dags.etl_green_data_hourly as dag_mod
import src.extract.extract_api as extract_api
from conftest import RespuestaFalsa, respuesta_api
from src import pipeline_tasks
from src.quality.checks import validar_gold_final


@pytest.fixture
def entorno_dag(tmp_path, bronze_excel, monkeypatch, sin_esperas):
    """Configura un entorno aislado con Excel, config.yaml y API simulada para las 5 tareas."""
    (tmp_path / "data/bronze").mkdir(parents=True)
    (tmp_path / "config").mkdir(parents=True)
    ruta_excel = tmp_path / "data/bronze/india.xlsx"
    bronze_excel.to_excel(ruta_excel, index=False)

    config = {
        "logging": {
            "archivo": str(tmp_path / "logs/log.txt"),
            "resumen_ejecucion": str(tmp_path / "logs/run_summary.json"),
        },
        "path": {
            "bronze_runs_dir": str(tmp_path / "data/bronze/runs"),
            "bronze_excel_india": str(ruta_excel),
            "bronze_api_india_json": str(tmp_path / "data/bronze/api.json"),
            "silver_sensores_india_dir": str(tmp_path / "data/silver/sensores.csv"),
            "silver_aire_india_horario_dir": str(tmp_path / "data/silver/horario.csv"),
            "gold_final_dir": str(tmp_path / "data/gold/gold_final.csv"),
            "gold_aire_india_dir": str(tmp_path / "data/gold/gold_aire_india.csv"),
            "reporte_coordenadas_compartidas_dir": str(tmp_path / "data/gold/reporte.csv"),
            "tabla_kpi_dir": str(tmp_path / "data/gold/tabla_kpi_resumen.csv"),
        },
        "quality": {
            "umbral_cobertura_join": 0.95,
            "distancia_alerta_km": 15.0,
            "dias_vigencia_estacion": 365,
            "max_snapshots_bronze": 3,
        },
        "sources": {
            "aire_india_excel": {"path": str(ruta_excel)},
            "aire_india_api": {
                "url": "https://api.falsa",
                "batch_size": 50,
                "pausa_segundos": 0,
                "decimales_coordenadas": 1,
                "params": {"timezone": "Asia/Kolkata"},
            },
        },
    }
    (tmp_path / "config/config.yaml").write_text(yaml.safe_dump(config), encoding="utf-8")

    def get(url, params, timeout):
        datos = [respuesta_api(la, lo, horas=6) for la, lo in zip(params["latitude"], params["longitude"])]
        return RespuestaFalsa(200, datos[0] if len(datos) == 1 else datos)

    monkeypatch.setattr(extract_api.requests, "get", get)
    monkeypatch.setattr(dag_mod, "PROYECTO_DIR", tmp_path)
    monkeypatch.setattr(pipeline_tasks, "PROYECTO_DIR", tmp_path)
    monkeypatch.setattr(pipeline_tasks, "cargar_gold_a_postgres", lambda df=None: len(df))
    return tmp_path, config


def test_configuracion_declarativa_del_dag_horario():
    """Verifica todos los parametros exigidos en las Secciones 7 y 8 de la guia del laboratorio."""
    cfg = dag_mod.DAG_CONFIG
    assert cfg["dag_id"] == "etl_green_data_hourly"
    assert cfg["schedule"] == "0 * * * *"
    assert cfg["catchup"] is False
    assert cfg["max_active_runs"] == 1
    assert isinstance(cfg["start_date"], datetime) and cfg["start_date"] < datetime(2026, 11, 1)
    assert cfg["default_args"]["retries"] == 2
    assert cfg["default_args"]["retry_delay"] == timedelta(minutes=5)
    assert {"ETL", "medallion", "green-data"}.issubset(set(cfg["tags"]))


def test_flujo_completo_de_las_5_tareas_y_paralelismo_bronze(entorno_dag, bronze_excel):
    """Ejecuta las 2 extracciones Bronze de forma independiente (sin que API dependa de Silver),
    seguidas de transform_silver -> build_gold -> validate_gold."""
    tmp_path, config = entorno_dag
    run_id = "scheduled__2026-11-04T03:00:00+00:00"

    # Extraccion paralela: API corre ANTES que Excel para demostrar independencia total
    api_meta = dag_mod.tarea_extract_api_bronze(run_tag=run_id)
    excel_meta = dag_mod.tarea_extract_excel_bronze(run_tag=run_id)

    assert api_meta["estaciones_objetivo"] == 3  # excluye estacion solo-clima (id 40)
    assert api_meta["puntos_consulta_unicos"] == 2
    assert excel_meta["filas_crudas"] == len(bronze_excel)
    assert len(excel_meta["sha256_version"]) == 64

    # Verifica que ambas copias inalteradas conviven en la misma carpeta de corrida sanitizada
    carpeta_run = tmp_path / "data/bronze/runs" / api_meta["run_tag"]
    assert ":" not in api_meta["run_tag"]
    assert (carpeta_run / "india.xlsx").exists()
    assert (carpeta_run / "metadata_excel.json").exists()
    assert (carpeta_run / "bronze_aire_india_api.json").exists()

    # Silver
    silver_meta = dag_mod.tarea_transform_silver(api_meta=api_meta, excel_meta=excel_meta)
    assert silver_meta["estaciones_limpias"] == 3
    assert silver_meta["excel_filas_descartadas"] == len(bronze_excel) - silver_meta["excel_filas_salida"]
    assert silver_meta["horario_filas_totales"] == 3 * 6

    # Gold + KPIs
    gold_meta = dag_mod.tarea_build_gold(silver_meta=silver_meta)
    assert gold_meta["filas_totales"] == 3
    assert (tmp_path / "data/gold/gold_final.csv").exists()
    df_kpi = pd.read_csv(config["path"]["tabla_kpi_dir"])
    assert len(df_kpi) == 5

    # Validacion automatica Gold
    val_meta = dag_mod.tarea_validate_gold(gold_meta=gold_meta)
    assert val_meta["estado"] == "OK"
    assert val_meta["gold_final"]["duplicados_clave_primaria"] == 0
    assert val_meta["calidad_integracion"]["cobertura_join_pct"] == 100.0


def test_p4_fallo_inducido_y_recuperacion_en_reintento(entorno_dag):
    """Prueba P4: crea el flag de un solo uso, comprueba que el intento 1 falla con log claro
    y que el reintento automatico (intento 2) se recupera sin intervencion humana."""
    tmp_path, _ = entorno_dag
    flag = tmp_path / "data/bronze/SIMULAR_FALLO_UNA_VEZ.flag"
    flag.write_text("simular_503", encoding="utf-8")

    with pytest.raises(RuntimeError, match="FALLO INDUCIDO P4"):
        dag_mod.tarea_extract_api_bronze(run_tag="run_p4")

    assert not flag.exists()  # flag consumido en el primer intento
    api_meta_reintento = dag_mod.tarea_extract_api_bronze(run_tag="run_p4")
    assert api_meta_reintento["filas_horarias_extraidas"] == 2 * 6


def test_p5_idempotencia_en_reejecucion_y_rotacion_snapshots(entorno_dag):
    """Prueba P5: dos corridas consecutivas no duplican filas en Silver ni en Gold y mantienen
    los mismos indicadores; ademas se respeta el tope de snapshots en Bronze."""
    tmp_path, config = entorno_dag

    for idx in range(4):
        tag = f"run_0{idx + 1}"
        api_m = dag_mod.tarea_extract_api_bronze(run_tag=tag)
        exc_m = dag_mod.tarea_extract_excel_bronze(run_tag=tag)
        sil_m = dag_mod.tarea_transform_silver(api_m, exc_m)
        gol_m = dag_mod.tarea_build_gold(sil_m)
        dag_mod.tarea_validate_gold(gol_m)
        if idx == 0:
            gold_corrida_1 = pd.read_csv(config["path"]["gold_final_dir"])

    gold_corrida_n = pd.read_csv(config["path"]["gold_final_dir"])
    assert len(gold_corrida_1) == len(gold_corrida_n) == 3
    assert not gold_corrida_n.duplicated(["location_id", "fecha_lectura"]).any()
    pd.testing.assert_frame_equal(gold_corrida_1, gold_corrida_n)

    # max_snapshots_bronze=3 en config de prueba -> solo quedan las 3 corridas mas recientes
    runs = sorted(p.name for p in (tmp_path / "data/bronze/runs").iterdir() if p.is_dir())
    assert runs == ["run_02", "run_03", "run_04"]


def test_validate_gold_detecta_duplicados_y_archivos_invalidos(entorno_dag):
    """Garantiza que validate_gold falle de forma controlada ante corrupcion o duplicados."""
    tmp_path, config = entorno_dag
    api_m = dag_mod.tarea_extract_api_bronze(run_tag="r1")
    exc_m = dag_mod.tarea_extract_excel_bronze(run_tag="r1")
    sil_m = dag_mod.tarea_transform_silver(api_m, exc_m)
    dag_mod.tarea_build_gold(sil_m)

    ruta_gold = config["path"]["gold_final_dir"]
    df = pd.read_csv(ruta_gold)
    df_dup = pd.concat([df, df.iloc[[0]]], ignore_index=True)
    df_dup.to_csv(ruta_gold, index=False)

    with pytest.raises(ValueError, match="filas duplicadas"):
        validar_gold_final(ruta_gold)

    with pytest.raises(FileNotFoundError):
        validar_gold_final(str(tmp_path / "data/gold/inexistente.csv"))


def test_coordenadas_paralelas_coinciden_con_silver_real():
    """Garantiza que `extract_api_bronze` (leyendo Excel crudo en paralelo) consulte exactamente
    las mismas 460 celdas y 763 estaciones que `transform_silver` sobre los datos reales de India."""
    from pathlib import Path
    from src.utils import io

    raiz = Path(__file__).resolve().parent.parent
    ruta_raw = raiz / "data/bronze/AIR_QUALITY_INDIA_DATASET.xlsx"
    ruta_sil = raiz / "data/silver/silver_aire_india_sensores.csv"
    if not (ruta_raw.exists() and ruta_sil.exists()):
        pytest.skip("Datos reales no disponibles")

    est_paralelo = extract_api.obtener_estaciones(pd.read_excel(ruta_raw), decimales=1, filtrar_contaminantes=True)
    est_silver = extract_api.obtener_estaciones(io.leer_silver_sensores(str(ruta_sil)), decimales=1)

    assert len(est_paralelo) == len(est_silver) == 763
    c_par = set(zip(est_paralelo["lat_consulta"], est_paralelo["lon_consulta"]))
    c_sil = set(zip(est_silver["lat_consulta"], est_silver["lon_consulta"]))
    assert c_par == c_sil and len(c_par) == 460

