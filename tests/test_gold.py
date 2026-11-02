"""Pruebas de los indicadores de la capa Gold."""
import pandas as pd
import pytest

from src.extract.extract_api import obtener_estaciones
from src.transform import gold_india
from src.transform.clean_api import limpiar_aire_india_api
from src.transform.clean_excel import limpiar_aire_india
from test_clean_api import crudo_desde_respuestas

AHORA = pd.Timestamp("2026-10-14 02:00")


@pytest.fixture
def silver(bronze_excel):
    sensores = limpiar_aire_india(bronze_excel)
    est = obtener_estaciones(sensores)
    aire = limpiar_aire_india_api(crudo_desde_respuestas(est, horas=4), est)
    return sensores, aire


@pytest.fixture
def gold(silver):
    return gold_india.generar_gold_india(*silver, ahora=AHORA)


def test_una_fila_por_estacion_y_fecha(gold):
    assert sorted(gold["location_id"]) == [1, 2, 3]
    assert not gold.duplicated(["location_id", "fecha_lectura"]).any()


def test_indicadores_de_red_desde_el_excel(gold):
    g = gold.set_index("location_id")
    assert g.loc[1, "total_contaminantes"] == 3
    assert g.loc[1, "contaminantes_monitoreados"] == "nitrogen_dioxide, pm10, pm2_5"
    assert bool(g.loc[1, "mide_pm2_5"]) and bool(g.loc[1, "mide_no2"]) and not bool(g.loc[1, "mide_co"])
    assert bool(g.loc[2, "mide_co"])
    assert g.loc[1, "nivel_cobertura"] == "Monitoreo Medio (3-4 contaminantes)"
    assert g.loc[2, "nivel_cobertura"] == "Monitoreo Basico (1-2 contaminantes)"
    assert g.loc[1, "dias_operacion_max"] == 20.0
    assert g.loc[1, "fecha_inicio_red"] == "2020-01-01"
    assert g.loc[2, "fecha_inicio_red"] == "Sin registro"


@pytest.mark.parametrize("n,nivel", [
    (1, "Monitoreo Basico (1-2 contaminantes)"), (2, "Monitoreo Basico (1-2 contaminantes)"),
    (3, "Monitoreo Medio (3-4 contaminantes)"), (4, "Monitoreo Medio (3-4 contaminantes)"),
    (5, "Red Completa (5+ contaminantes)"), (7, "Red Completa (5+ contaminantes)"),
])
def test_clasificar_cobertura(n, nivel):
    assert gold_india._clasificar_cobertura(n) == nivel


def test_promedios_y_maximos_diarios_coinciden_con_calculo_manual(gold, silver):
    _, aire = silver
    for lid in [1, 3]:
        serie = aire[aire["location_id"] == lid]
        fila = gold[gold["location_id"] == lid].iloc[0]
        assert fila["pm2_5_promedio_dia"] == pytest.approx(round(serie["pm2_5"].mean(), 2))
        assert fila["pm2_5_max_dia"] == pytest.approx(serie["pm2_5"].max())
        assert fila["aqi_promedio_dia"] == pytest.approx(round(serie["european_aqi"].mean(), 2))
        assert fila["aqi_max_dia"] == pytest.approx(serie["european_aqi"].max())
        assert fila["categoria_aqi_predominante"] == serie["categoria_aqi"].mode()[0]


def test_lectura_actual_es_la_ultima_hora_hasta_ahora(gold, silver):
    _, aire = silver
    fila = gold[gold["location_id"] == 3].iloc[0]
    assert fila["hora_lectura_actual"] == AHORA          # 03:00 es futura y se ignora
    esperado = aire[(aire["location_id"] == 3) & (aire["date"] == AHORA)]["pm2_5"].iloc[0]
    assert fila["pm2_5_actual"] == esperado


def test_estacion_sin_datos_de_api_queda_con_fecha_de_hoy(silver):
    sensores, aire = silver
    g = gold_india.generar_gold_india(sensores, aire[aire["location_id"] != 3], ahora=AHORA)
    fila = g[g["location_id"] == 3].iloc[0]
    assert pd.isna(fila["aqi_promedio_dia"])
    assert fila["fecha_lectura"] == "2026-10-14"


def test_ranking_1_es_el_peor_aire_y_empata_puntos_compartidos(gold):
    r = gold_india.calcular_ranking(gold)
    # C tiene base 20 (peor), A y B comparten punto con base 10 -> empatan en 2
    assert r.set_index("location_id")["ranking_peor_aire"].to_dict() == {3: 1, 1: 2, 2: 2}
    assert r.iloc[0]["location_id"] == 3


def test_ranking_se_calcula_por_fecha():
    df = pd.DataFrame({
        "location_id": [1, 2, 1, 2],
        "fecha_lectura": ["2026-10-14", "2026-10-14", "2026-10-15", "2026-10-15"],
        "aqi_promedio_dia": [50.0, 70.0, 90.0, 30.0],
    })
    r = gold_india.calcular_ranking(df)
    rk = r.set_index(["fecha_lectura", "location_id"])["ranking_peor_aire"]
    assert rk[("2026-10-14", 2)] == 1 and rk[("2026-10-15", 1)] == 1
    assert r.iloc[0]["fecha_lectura"] == "2026-10-15"   # fecha mas reciente primero


def test_metadatos_de_origen(gold):
    assert (gold["fuente_valores"].str.contains("modelo")).all()
    assert (gold["codigo_pais"] == "IN").all()


def test_location_id_inconsistente_en_sensores_falla(silver):
    sensores, aire = silver
    sensores = sensores.copy()
    sensores.loc[sensores["sensor_id"] == 102, "location_name"] = "Otro nombre"
    with pytest.raises(ValueError, match="location_id"):
        gold_india.generar_gold_india(sensores, aire, ahora=AHORA)


def test_resumen_gold_escribe_en_log(gold, caplog):
    import logging
    with caplog.at_level(logging.INFO):
        gold_india.resumen_gold(gold)
    assert "Top 5" in caplog.text

