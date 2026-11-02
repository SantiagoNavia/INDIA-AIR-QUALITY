"""Prueba de regresion con los datos REALES del proyecto.

Recalcula la capa Gold a partir de los archivos Silver entregados y verifica que
los indicadores coincidan con data/gold/gold_aire_india.csv (la misma verificacion
que hizo el docente). Las columnas *_actual dependen de la hora de ejecucion y se excluyen.
"""
import os

import pandas as pd
import pytest

from src.transform import gold_india
from src.utils import io

RAIZ = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
SILVER_SENSORES = os.path.join(RAIZ, "data/silver/silver_aire_india_sensores.csv")
SILVER_HORARIO = os.path.join(RAIZ, "data/silver/silver_aire_india_horario.csv")
GOLD = os.path.join(RAIZ, "data/gold/gold_aire_india.csv")

pytestmark = pytest.mark.skipif(
    not all(os.path.exists(p) for p in [SILVER_SENSORES, SILVER_HORARIO, GOLD]),
    reason="No estan los archivos Silver/Gold del proyecto",
)


@pytest.fixture(scope="module")
def datos():
    sensores = io.leer_silver_sensores(SILVER_SENSORES)
    horario = io.leer_silver_horario(SILVER_HORARIO)
    gold = pd.read_csv(GOLD)
    return sensores, horario, gold


def test_gold_se_reproduce_desde_silver(datos, tmp_path):
    sensores, horario, gold_entregado = datos
    recalculado = gold_india.calcular_ranking(gold_india.generar_gold_india(sensores, horario))
    ruta = tmp_path / "gold.csv"
    recalculado.to_csv(ruta, index=False)       # mismo ciclo de escritura/lectura que el archivo real
    recalculado = pd.read_csv(ruta)

    clave = ["location_id", "fecha_lectura"]
    cols = [c for c in gold_entregado.columns if not c.endswith("_actual") and c != "hora_lectura_actual"]
    a = gold_entregado[cols].sort_values(clave).reset_index(drop=True)
    b = recalculado[cols].sort_values(clave).reset_index(drop=True)
    # Correccion detectada por las pruebas: en el Gold entregado las estaciones sin historial
    # quedaron con la fecha vacia en vez de "Sin registro" (bug de astype(str) con pandas 3).
    for col in ["fecha_inicio_red", "fecha_ultima_medicion"]:
        a[col] = a[col].fillna("Sin registro")
    pd.testing.assert_frame_equal(a, b, check_dtype=False)


def test_integridad_de_los_archivos_entregados(datos):
    sensores, horario, gold = datos
    assert sensores["sensor_id"].is_unique
    assert not horario.duplicated(["location_id", "date"]).any()
    assert not gold.duplicated(["location_id", "fecha_lectura"]).any()
    # Todas las estaciones del Silver de sensores llegan al Gold
    assert set(sensores["location_id"]) == set(gold["location_id"])
    # Coordenadas dentro de India
    assert sensores["latitude"].between(6, 37).all() and sensores["longitude"].between(68, 98).all()
    # Sin concentraciones negativas
    cols = ["pm10", "pm2_5", "carbon_monoxide", "nitrogen_dioxide", "sulfur_dioxide", "ozone", "european_aqi"]
    assert (horario[cols].dropna() >= 0).all().all()
