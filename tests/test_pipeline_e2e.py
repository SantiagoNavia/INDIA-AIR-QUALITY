"""Prueba de punta a punta: ejecuta main.py con datos de prueba y la API simulada.

Verifica que el pipeline completo genere Bronze, Silver, Gold, el log y el resumen
de ejecucion, y que el modo --solo-gold regenere Gold desde Silver.
"""
import json
import logging

import pandas as pd
import pytest
import yaml

import main
import src.extract.extract_api as extract_api
from conftest import RespuestaFalsa, respuesta_api


@pytest.fixture
def proyecto(tmp_path, bronze_excel, monkeypatch, sin_esperas):
    """Crea un proyecto temporal (config + Excel) y simula la API."""
    (tmp_path / "data" / "bronze").mkdir(parents=True)
    ruta_excel = tmp_path / "data" / "bronze" / "india.xlsx"
    bronze_excel.to_excel(ruta_excel, index=False)

    config = {
        "logging": {"archivo": str(tmp_path / "logs" / "log.txt"),
                    "resumen_ejecucion": str(tmp_path / "logs" / "run_summary.json")},
        "path": {
            "bronze_excel_india": str(ruta_excel),
            "bronze_api_india_json": str(tmp_path / "data/bronze/api.json"),
            "silver_sensores_india_dir": str(tmp_path / "data/silver/sensores.csv"),
            "silver_aire_india_horario_dir": str(tmp_path / "data/silver/horario.csv"),
            "gold_aire_india_dir": str(tmp_path / "data/gold/gold.csv"),
        },
        "sources": {
            "aire_india_excel": {"path": str(ruta_excel)},
            "aire_india_api": {"url": "https://api.falsa", "batch_size": 50, "pausa_segundos": 0,
                               "decimales_coordenadas": 1, "params": {"timezone": "Asia/Kolkata"}},
        },
    }
    ruta_config = tmp_path / "config.yaml"
    ruta_config.write_text(yaml.safe_dump(config), encoding="utf-8")

    def get(url, params, timeout):
        datos = [respuesta_api(la, lo, horas=24) for la, lo in zip(params["latitude"], params["longitude"])]
        return RespuestaFalsa(200, datos[0] if len(datos) == 1 else datos)
    monkeypatch.setattr(extract_api.requests, "get", get)
    monkeypatch.setattr(main, "cargar_gold_a_postgres", lambda df=None: len(df) if df is not None else 3)

    yield config, str(ruta_config)
    # Cierra los handlers del log para liberar el archivo (Windows)
    for h in list(logging.getLogger().handlers):
        logging.getLogger().removeHandler(h)
        h.close()


def test_pipeline_completo(proyecto):
    config, ruta_config = proyecto
    main.main(["--config", ruta_config])
    p = config["path"]

    bronze = json.load(open(p["bronze_api_india_json"], encoding="utf-8"))
    assert len(bronze["lotes"]) == 1

    sensores = pd.read_csv(p["silver_sensores_india_dir"])
    horario = pd.read_csv(p["silver_aire_india_horario_dir"])
    gold = pd.read_csv(p["gold_aire_india_dir"])
    assert sensores["location_id"].nunique() == 3
    assert len(horario) == 3 * 24
    assert len(gold) == 3 and gold["ranking_peor_aire"].min() == 1

    log = open(config["logging"]["archivo"], encoding="utf-8").read()
    assert "ETAPA 01" in log and "CAPA GOLD" in log and "Pipeline completado" in log

    resumen = json.load(open(config["logging"]["resumen_ejecucion"], encoding="utf-8"))
    assert resumen["estado"] == "OK"
    assert resumen["bronze"]["api_puntos_consulta"] == 2
    assert resumen["bronze"]["api_estaciones_en_punto_compartido"] == 2
    assert resumen["gold"]["filas_sin_api"] == 0


def test_segunda_ejecucion_no_duplica(proyecto):
    config, ruta_config = proyecto
    main.main(["--config", ruta_config])
    main.main(["--config", ruta_config])
    p = config["path"]
    assert len(pd.read_csv(p["silver_aire_india_horario_dir"])) == 3 * 24
    assert len(pd.read_csv(p["gold_aire_india_dir"])) == 3


def test_modo_solo_gold(proyecto):
    config, ruta_config = proyecto
    main.main(["--config", ruta_config])
    gold_1 = pd.read_csv(config["path"]["gold_aire_india_dir"])

    main.main(["--config", ruta_config, "--solo-gold"])
    gold_2 = pd.read_csv(config["path"]["gold_aire_india_dir"])
    cols = [c for c in gold_1.columns if not c.endswith("_actual") and c != "hora_lectura_actual"]
    pd.testing.assert_frame_equal(gold_1[cols], gold_2[cols])
    resumen = json.load(open(config["logging"]["resumen_ejecucion"], encoding="utf-8"))
    assert resumen["modo"] == "solo_gold"


def test_error_queda_registrado(proyecto, monkeypatch):
    config, ruta_config = proyecto
    monkeypatch.setattr(extract_api.requests, "get", lambda *a, **k: RespuestaFalsa(400, text="malo"))
    with pytest.raises(RuntimeError):
        main.main(["--config", ruta_config])
    resumen = json.load(open(config["logging"]["resumen_ejecucion"], encoding="utf-8"))
    assert resumen["estado"] == "ERROR" and "HTTP 400" in resumen["error"]

