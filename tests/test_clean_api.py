"""Pruebas de la limpieza de la API (capa Silver horaria) y de la integracion Excel <-> API."""
import numpy as np
import pandas as pd
import pytest

from src.extract.extract_api import obtener_estaciones
from src.transform.clean_api import COLUMNAS_SILVER, _categoria_eaqi, limpiar_aire_india_api
from src.transform.clean_excel import limpiar_aire_india
from conftest import respuesta_api


def crudo_desde_respuestas(est: pd.DataFrame, horas: int = 4) -> pd.DataFrame:
    """Construye el DataFrame Bronze de la API (igual que extraer_api_india) sin red."""
    tablas = []
    puntos = est[["lat_consulta", "lon_consulta"]].drop_duplicates()
    for i, (lat, lon) in enumerate(puntos.itertuples(index=False, name=None)):
        r = respuesta_api(lat, lon, horas=horas, base=10.0 * (i + 1))
        t = pd.DataFrame(r["hourly"])
        t["lat_consulta"], t["lon_consulta"] = lat, lon
        t["lat_modelo"], t["lon_modelo"] = r["latitude"], r["longitude"]
        tablas.append(t)
    df = pd.concat(tablas, ignore_index=True).rename(columns={"time": "date"})
    df["date"] = pd.to_datetime(df["date"])
    return df


@pytest.fixture
def estaciones(bronze_excel):
    return obtener_estaciones(limpiar_aire_india(bronze_excel))


@pytest.fixture
def crudo(estaciones):
    return crudo_desde_respuestas(estaciones)


# ---------------------------------------------------------------- Categoria EAQI
@pytest.mark.parametrize("valor,categoria", [
    (0, "Muy bueno"), (19.99, "Muy bueno"), (20, "Bueno"), (40, "Moderado"),
    (60, "Pobre"), (80, "Muy pobre"), (99.9, "Muy pobre"), (100, "Extremadamente pobre"),
    (250, "Extremadamente pobre"), (np.nan, "Sin dato"), (None, "Sin dato"),
])
def test_categoria_eaqi_en_los_limites(valor, categoria):
    assert _categoria_eaqi(valor) == categoria


# ---------------------------------------------------------------- Limpieza
def test_esquema_y_renombre(crudo, estaciones):
    df = limpiar_aire_india_api(crudo, estaciones)
    assert list(df.columns) == COLUMNAS_SILVER
    assert "sulphur_dioxide" not in df.columns and "sulfur_dioxide" in df.columns


def test_elimina_duplicados_y_fechas_vacias(crudo, estaciones):
    sucio = pd.concat([crudo, crudo.head(3)], ignore_index=True)
    sucio.loc[len(sucio)] = sucio.iloc[0]
    sucio.loc[len(sucio) - 1, "date"] = pd.NaT
    df = limpiar_aire_india_api(sucio, estaciones)
    assert not df.duplicated(["location_id", "date"]).any()
    assert df["date"].notna().all()


def test_negativos_se_anulan_y_se_interpolan(crudo, estaciones):
    crudo = crudo.copy()
    punto_c = (crudo["lat_consulta"] == 19.1)
    idx = crudo[punto_c].index
    crudo.loc[idx[1], "pm2_5"] = -5          # imposible -> NaN -> interpolado
    crudo.loc[idx[2], "ozone"] = None        # nulo -> interpolado
    df = limpiar_aire_india_api(crudo, estaciones)
    c = df[df["location_id"] == 3].reset_index(drop=True)
    vecinos_pm = (crudo.loc[idx[0], "pm2_5"] + crudo.loc[idx[2], "pm2_5"]) / 2
    assert c.loc[1, "pm2_5"] == pytest.approx(vecinos_pm)
    assert c.loc[2, "ozone"] == pytest.approx((crudo.loc[idx[1], "ozone"] + crudo.loc[idx[3], "ozone"]) / 2)
    assert (df[["pm2_5", "ozone"]] >= 0).all().all()


def test_interpolacion_no_mezcla_estaciones(crudo, estaciones):
    crudo = crudo.copy()
    punto_c = crudo["lat_consulta"] == 19.1
    crudo.loc[punto_c, "pm10"] = np.nan        # la estacion C no tiene ningun pm10
    df = limpiar_aire_india_api(crudo, estaciones)
    # Queda vacia: no "hereda" valores de otra estacion
    assert df.loc[df["location_id"] == 3, "pm10"].isna().all()
    assert df.loc[df["location_id"] != 3, "pm10"].notna().all()


def test_columnas_derivadas(crudo, estaciones):
    df = limpiar_aire_india_api(crudo, estaciones)
    fila = df.iloc[1]
    assert fila["fecha"] == "2026-10-14"
    assert fila["hora"] == 1
    assert fila["categoria_aqi"] == _categoria_eaqi(fila["european_aqi"])
    assert (df["country_code"] == "IN").all()


# ---------------------------------------------------------------- Integracion (JOIN)
def test_cada_estacion_recibe_los_datos_de_su_punto(crudo, estaciones):
    df = limpiar_aire_india_api(crudo, estaciones)
    # Estaciones con contaminantes: 1, 2, 3 (la 4 solo mide clima y no se consulta)
    assert sorted(df["location_id"].unique()) == [1, 2, 3]
    # Una fila por estacion y hora
    assert df.groupby("location_id").size().tolist() == [4, 4, 4]
    # Conserva las coordenadas reales de la estacion y las del modelo
    a = df[df["location_id"] == 1].iloc[0]
    assert (a["latitude"], a["longitude"]) == (28.6139, 77.209)
    assert a["lat_modelo"] == pytest.approx(28.61)


def test_estaciones_en_el_mismo_punto_reciben_valores_identicos(crudo, estaciones):
    df = limpiar_aire_india_api(crudo, estaciones)
    a = df[df["location_id"] == 1].reset_index(drop=True)
    b = df[df["location_id"] == 2].reset_index(drop=True)
    cols = ["pm2_5", "pm10", "european_aqi"]
    pd.testing.assert_frame_equal(a[cols], b[cols])
    c = df[df["location_id"] == 3].reset_index(drop=True)
    assert not a["pm2_5"].equals(c["pm2_5"])


def test_estacion_sin_respuesta_de_api_se_reporta(crudo, estaciones, caplog):
    sin_c = crudo[crudo["lat_consulta"] != 19.1]
    df = limpiar_aire_india_api(sin_c, estaciones)
    assert 3 not in df["location_id"].values
    assert "no recibieron datos de la API" in caplog.text


def test_join_con_estaciones_duplicadas_falla(crudo, estaciones):
    dup = pd.concat([estaciones, estaciones.head(1)], ignore_index=True)
    with pytest.raises(ValueError, match="duplicadas"):
        limpiar_aire_india_api(crudo, dup)

