"""Pruebas de la limpieza del Excel (capa Silver de sensores)."""
import pandas as pd
import pytest

from src.transform.clean_excel import _normalizar_unidad, limpiar_aire_india


@pytest.fixture
def silver(bronze_excel):
    return limpiar_aire_india(bronze_excel)


def test_solo_quedan_contaminantes_del_aire(silver):
    assert set(silver["parameter"]) <= {"pm25", "pm10", "no2", "co", "o3"}
    assert "temperature" not in silver["parameter"].values
    assert "um003" not in silver["parameter"].values
    assert 4 not in silver["location_id"].values      # estacion que solo mide clima


def test_parametros_se_estandarizan(silver):
    mapa = dict(zip(silver["parameter"], silver["parameter_std"]))
    assert mapa == {"pm25": "pm2_5", "pm10": "pm10", "no2": "nitrogen_dioxide",
                    "co": "carbon_monoxide", "o3": "ozone"}


def test_mayusculas_y_espacios_en_parametro(silver):
    # "CO" del Excel se normaliza a "co" y no se pierde
    assert "co" in silver["parameter"].values


def test_elimina_sensores_duplicados(silver):
    assert silver["sensor_id"].is_unique
    assert (silver["sensor_id"] == 201).sum() == 1


@pytest.mark.parametrize("cruda,esperada", [
    ("\ufffdg/m\ufffd", "ug/m3"),
    ("µg/m³", "ug/m3"),
    ("particles/cm\ufffd", "particles/cm3"),
    ("ppb", "ppb"),
    (" ppm ", "ppm"),
])
def test_normaliza_unidades(cruda, esperada):
    assert _normalizar_unidad(cruda) == esperada


def test_tipos_y_columnas(silver):
    assert silver["location_id"].dtype == "int64"
    assert silver["sensor_id"].dtype == "int64"
    assert pd.api.types.is_datetime64_any_dtype(silver["datetime_first"])
    assert list(silver.columns[:3]) == ["location_id", "location_name", "country_name"]
    assert (silver["country_code"] == "IN").all()
    assert (silver["tipo_metrica"] == "calidad_del_aire").all()


def test_dias_monitoreo_y_flag_de_historial(silver):
    s = silver.set_index("sensor_id")
    assert s.loc[101, "dias_monitoreo"] == 10.0
    assert s.loc[103, "dias_monitoreo"] == 4.5
    assert bool(s.loc[101, "tiene_historial_fechas"]) is True
    # Sin fechas: se imputa 0.0 con flag explicito
    assert s.loc[201, "dias_monitoreo"] == 0.0
    assert bool(s.loc[201, "tiene_historial_fechas"]) is False


def test_nombres_se_limpian(silver):
    assert "Estacion C" in silver["location_name"].values


def test_ids_o_coordenadas_invalidas_se_descartan(bronze_excel):
    df = bronze_excel.copy()
    df["latitude"] = df["latitude"].astype(object)
    df.loc[0, "Location_id"] = None          # id invalido
    df.loc[1, "latitude"] = "sin dato"       # coordenada invalida
    silver = limpiar_aire_india(df)
    assert 0 not in silver["location_id"].values     # no se crea una estacion ficticia con id 0
    assert not silver[["latitude", "longitude"]].isna().any().any()
    assert {101, 102}.isdisjoint(silver["sensor_id"])


def test_no_modifica_el_dataframe_de_entrada(bronze_excel):
    copia = bronze_excel.copy()
    limpiar_aire_india(bronze_excel)
    pd.testing.assert_frame_equal(bronze_excel, copia)
