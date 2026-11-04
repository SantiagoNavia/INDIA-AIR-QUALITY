"""Pruebas de controles de calidad de integracion (Fase 4) e indice NAQI de India (Fase 5)."""
import numpy as np
import pandas as pd
import pytest

from src.extract.extract_api import obtener_estaciones
from src.quality.checks import (
    distancia_haversine_km,
    generar_reporte_coordenadas_compartidas,
    validar_calidad_pipeline,
)
from src.transform import gold_india
from src.transform.clean_api import limpiar_aire_india_api
from src.transform.clean_excel import limpiar_aire_india
from src.transform.naqi_india import (
    calcular_naqi_dataframe,
    categoria_naqi,
    subindice_cpcb,
)
from test_clean_api import crudo_desde_respuestas


# FASE 5: Indice NAQI de India (CPCB)
class TestNaqiIndia:
    @pytest.mark.parametrize("conc,cont,esperado", [
        (0.0, "pm2_5", 0.0),
        (30.0, "pm2_5", 50.0),
        (60.0, "pm2_5", 100.0),
        (90.0, "pm2_5", 200.0),
        (120.0, "pm2_5", 300.0),
        (250.0, "pm2_5", 400.0),
        (600.0, "pm2_5", 500.0),         # por encima del corte superior se acota a 500
        (100.0, "pm10", 100.0),
        (80.0, "nitrogen_dioxide", 100.0),
        (80.0, "sulfur_dioxide", 100.0),
        (100.0, "ozone", 100.0),
        # CO: 1000 ug/m3 = 1.0 mg/m3 -> sub-indice 50
        (1000.0, "carbon_monoxide", 50.0),
        (2000.0, "carbon_monoxide", 100.0),
    ])
    def test_subindice_cpcb_en_puntos_de_corte(self, conc, cont, esperado):
        assert subindice_cpcb(conc, cont) == pytest.approx(esperado, abs=0.1)

    def test_subindice_nulo_o_negativo(self):
        assert np.isnan(subindice_cpcb(np.nan, "pm2_5"))
        assert np.isnan(subindice_cpcb(-5.0, "pm2_5"))

    @pytest.mark.parametrize("valor,cat", [
        (0, "Bueno"), (50, "Bueno"),
        (51, "Satisfactorio"), (100, "Satisfactorio"),
        (101, "Moderado"), (200, "Moderado"),
        (201, "Pobre"), (300, "Pobre"),
        (301, "Muy pobre"), (400, "Muy pobre"),
        (401, "Severo"), (500, "Severo"),
        (np.nan, "Sin dato"),
    ])
    def test_categoria_naqi(self, valor, cat):
        assert categoria_naqi(valor) == cat

    def test_calcular_naqi_dataframe_y_contaminante_dominante(self):
        df = pd.DataFrame({
            "pm2_5": [90.0, 15.0, np.nan],   # fila 0: PM2.5 domina (I=200); fila 1: O3 domina (I=100)
            "pm10": [50.0, 20.0, np.nan],
            "o3": [30.0, 100.0, np.nan],
        })
        res = calcular_naqi_dataframe(df, {"pm2_5": "pm2_5", "pm10": "pm10", "ozone": "o3"})
        assert res.loc[0, "naqi"] == pytest.approx(200.0)
        assert res.loc[0, "dominante"] == "pm2_5"
        assert res.loc[0, "categoria"] == "Moderado"

        assert res.loc[1, "naqi"] == pytest.approx(100.0)
        assert res.loc[1, "dominante"] == "ozone"
        assert res.loc[1, "categoria"] == "Satisfactorio"

        assert pd.isna(res.loc[2, "naqi"])
        assert res.loc[2, "dominante"] == "Sin dato"
        assert res.loc[2, "categoria"] == "Sin dato"


# FASE 4: Controles de Calidad de la Integracion y Coordenadas Compartidas
@pytest.fixture
def capas(bronze_excel):
    sensores = limpiar_aire_india(bronze_excel)
    est = obtener_estaciones(sensores)
    aire = limpiar_aire_india_api(crudo_desde_respuestas(est, horas=4), est)
    gold = gold_india.calcular_ranking(
        gold_india.generar_gold_india(sensores, aire, ahora=pd.Timestamp("2026-10-14 02:00"))
    )
    return sensores, aire, gold


class TestCalidadIntegracion:
    def test_distancia_haversine_km(self):
        # Mismo punto -> 0 km
        assert distancia_haversine_km(28.6, 77.2, 28.6, 77.2).iloc[0] == 0.0
        # 1 grado de latitud ~ 111.19 km
        d = distancia_haversine_km(28.0, 77.0, 29.0, 77.0).iloc[0]
        assert 110.5 <= d <= 112.0

    def test_reporte_coordenadas_compartidas(self, capas):
        sensores, aire, _ = capas
        rep = generar_reporte_coordenadas_compartidas(sensores, aire, decimales=1)
        # Estaciones 1 y 2 comparten el punto 28.6_77.2; la 3 esta sola en 19.1_72.9
        assert len(rep) == 2
        primero = rep.iloc[0]
        assert primero["id_punto_consulta"] == "28.6_77.2"
        assert primero["n_estaciones_mismo_punto"] == 2
        assert bool(primero["valor_compartido"]) is True
        assert "1, 2" in primero["location_ids"]

    def test_gold_incluye_columnas_de_integracion_y_naqi(self, capas):
        _, _, gold = capas
        esperadas = {
            "id_punto_consulta", "n_estaciones_mismo_punto", "valor_compartido",
            "distancia_modelo_km", "estacion_activa_reciente",
            "naqi_india_promedio_dia", "categoria_naqi_india", "contaminante_dominante_naqi",
            "naqi_india_actual", "categoria_naqi_actual",
            "ranking_peor_aire", "ranking_peor_aire_naqi", "ranking_peor_aire_celda",
        }
        assert esperadas <= set(gold.columns)
        # El ranking por celda no tiene empates artificiales: solo hay 2 celdas (rango 1 y 2)
        assert sorted(gold["ranking_peor_aire_celda"].unique()) == [1, 2]

    def test_validar_calidad_ok(self, capas):
        sensores, aire, gold = capas
        m = validar_calidad_pipeline(sensores, aire, gold)
        assert m["unicidad_claves_ok"] is True
        assert m["cobertura_join_pct"] == 100.0
        assert m["puntos_consulta_unicos"] == 2
        assert m["estaciones_en_punto_compartido"] == 2

    def test_falla_si_hay_sensor_duplicado(self, capas):
        sensores, aire, gold = capas
        dup = pd.concat([sensores, sensores.head(1)], ignore_index=True)
        with pytest.raises(ValueError, match="sensor_id duplicados"):
            validar_calidad_pipeline(dup, aire, gold)

    def test_falla_si_coordenada_fuera_de_india(self, capas):
        sensores, aire, gold = capas
        fuera = sensores.copy()
        fuera.loc[0, "latitude"] = 55.0
        with pytest.raises(ValueError, match="fuera del rango de India"):
            validar_calidad_pipeline(fuera, aire, gold)

    def test_falla_si_concentracion_negativa_o_absurda(self, capas):
        sensores, aire, gold = capas
        neg = aire.copy()
        neg.loc[0, "pm2_5"] = -1.0
        with pytest.raises(ValueError, match="concentraciones negativas"):
            validar_calidad_pipeline(sensores, neg, gold)

        absurdo = aire.copy()
        absurdo.loc[0, "pm10"] = 50_000.0
        with pytest.raises(ValueError, match="tope fisico"):
            validar_calidad_pipeline(sensores, absurdo, gold)

    def test_falla_si_cobertura_join_baja(self, capas):
        sensores, aire, gold = capas
        parcial = aire[aire["location_id"] == 1]  # solo 1 de 3 estaciones (33.3% < 95%)
        with pytest.raises(ValueError, match="Cobertura del JOIN insuficiente"):
            validar_calidad_pipeline(sensores, parcial, gold, umbral_cobertura_join=0.95)

