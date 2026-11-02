"""Pruebas de las tareas del DAG de Apache Airflow (dags/dag_calidad_aire_india.py)."""
import yaml

import dags.dag_calidad_aire_india as dag_mod
import src.extract.extract_api as extract_api
from conftest import RespuestaFalsa, respuesta_api


def test_flujo_completo_de_las_7_tareas_del_dag(tmp_path, bronze_excel, monkeypatch, sin_esperas):
    """Ejecuta en orden las 7 funciones de tareas del DAG y verifica que cada una
    lea/escriba en su capa y devuelva diccionarios ligeros para XCom."""
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
            "bronze_excel_india": str(ruta_excel),
            "bronze_api_india_json": str(tmp_path / "data/bronze/api.json"),
            "silver_sensores_india_dir": str(tmp_path / "data/silver/sensores.csv"),
            "silver_aire_india_horario_dir": str(tmp_path / "data/silver/horario.csv"),
            "gold_aire_india_dir": str(tmp_path / "data/gold/gold.csv"),
            "reporte_coordenadas_compartidas_dir": str(tmp_path / "data/gold/reporte.csv"),
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
    monkeypatch.setattr(dag_mod, "cargar_gold_a_postgres", lambda df=None: len(df))

    r1 = dag_mod.tarea_extraer_excel_bronze()
    assert r1["excel_filas_crudas"] == len(bronze_excel)

    r2 = dag_mod.tarea_limpiar_excel_silver()
    assert r2["silver_estaciones"] == 3

    r3 = dag_mod.tarea_extraer_api_bronze()
    assert r3["api_puntos_consulta"] == 2

    r4 = dag_mod.tarea_limpiar_api_silver()
    assert r4["horario_filas_totales"] == 3 * 6

    r5 = dag_mod.tarea_construir_capa_gold()
    assert r5["gold_filas_totales"] == 3

    r6 = dag_mod.tarea_validar_calidad_integracion()
    assert r6["cobertura_join_pct"] == 100.0

    r7 = dag_mod.tarea_cargar_postgres_datawarehouse()
    assert r7 == {"postgres_registros_totales": 3, "tabla": "gold_aire_india"}

