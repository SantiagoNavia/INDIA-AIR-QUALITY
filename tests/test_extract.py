"""Pruebas de la extraccion (capa Bronze): Excel y API REST (sin red, con respuestas simuladas)."""
import json

import pandas as pd
import pytest

import src.extract.extract_api as extract_api
import src.extract.extract_excel as extract_excel
from conftest import RespuestaFalsa, respuesta_api


# ---------------------------------------------------------------- Excel
class TestExtraerExcel:
    def test_lee_excel_sin_modificar(self, tmp_path, bronze_excel):
        ruta = tmp_path / "india.xlsx"
        bronze_excel.to_excel(ruta, index=False)

        df = extract_excel.extraer_excel_india({"path": str(ruta)})

        assert len(df) == len(bronze_excel)
        assert list(df.columns) == list(bronze_excel.columns)
        # Bronze no transforma: el encoding dañado y los nombres originales se conservan
        assert df.loc[0, "Unit"] == bronze_excel.loc[0, "Unit"]
        assert df.loc[6, "Location_name"] == " Estacion C "

    def test_archivo_inexistente_da_error_claro(self, tmp_path):
        with pytest.raises(FileNotFoundError, match="No se encontro"):
            extract_excel.extraer_excel_india({"path": str(tmp_path / "no_existe.xlsx")})

    def test_columnas_faltantes_da_error(self, tmp_path, bronze_excel):
        ruta = tmp_path / "incompleto.xlsx"
        bronze_excel.drop(columns=["Sensor_id"]).to_excel(ruta, index=False)
        with pytest.raises(ValueError, match="Sensor_id"):
            extract_excel.extraer_excel_india({"path": str(ruta)})


# ---------------------------------------------------------------- Estaciones / coordenadas
class TestObtenerEstaciones:
    def test_una_fila_por_estacion_y_coordenadas_redondeadas(self, bronze_excel):
        est = extract_api.obtener_estaciones(bronze_excel, decimales=1)

        assert est["location_id"].is_unique
        assert len(est) == bronze_excel["Location_id"].nunique()
        fila_a = est.set_index("location_id").loc[1]
        assert (fila_a["lat_consulta"], fila_a["lon_consulta"]) == (28.6, 77.2)

    def test_detecta_estaciones_que_comparten_punto(self, bronze_excel):
        est = extract_api.obtener_estaciones(bronze_excel, decimales=1).set_index("location_id")
        # A y B quedan en la misma celda (28.6, 77.2); C y D estan solas
        assert est.loc[1, "n_estaciones_mismo_punto"] == 2
        assert est.loc[2, "n_estaciones_mismo_punto"] == 2
        assert est.loc[3, "n_estaciones_mismo_punto"] == 1

    def test_acepta_columnas_del_silver(self, bronze_excel):
        silver_like = bronze_excel.rename(columns={"Location_id": "location_id", "Location_name": "location_name"})
        est = extract_api.obtener_estaciones(silver_like)
        assert {"location_id", "lat_consulta", "lon_consulta"} <= set(est.columns)


# ---------------------------------------------------------------- Consulta HTTP con reintentos
class TestConsultar:
    def test_devuelve_json_si_200(self, monkeypatch, sin_esperas):
        monkeypatch.setattr(extract_api.requests, "get", lambda *a, **k: RespuestaFalsa(200, {"ok": 1}))
        assert extract_api._consultar("u", {}) == {"ok": 1}
        assert sin_esperas == []

    def test_reintenta_ante_429_y_luego_responde(self, monkeypatch, sin_esperas):
        respuestas = iter([RespuestaFalsa(429), RespuestaFalsa(503), RespuestaFalsa(200, {"ok": 1})])
        monkeypatch.setattr(extract_api.requests, "get", lambda *a, **k: next(respuestas))

        assert extract_api._consultar("u", {}, reintentos=4, espera_base=5) == {"ok": 1}
        assert sin_esperas == [5, 10]   # espera creciente entre reintentos

    def test_reintenta_ante_error_de_red(self, monkeypatch, sin_esperas):
        def get_falla(*a, **k):
            raise extract_api.requests.ConnectionError("sin internet")
        monkeypatch.setattr(extract_api.requests, "get", get_falla)

        with pytest.raises(RuntimeError, match="no respondio tras 3 intentos"):
            extract_api._consultar("u", {}, reintentos=3)
        assert len(sin_esperas) == 3

    def test_error_400_no_se_reintenta(self, monkeypatch, sin_esperas):
        monkeypatch.setattr(extract_api.requests, "get",
                            lambda *a, **k: RespuestaFalsa(400, text="parametro invalido"))
        with pytest.raises(RuntimeError, match="HTTP 400"):
            extract_api._consultar("u", {})
        assert sin_esperas == []


# ---------------------------------------------------------------- Extraccion por lotes
class TestExtraerApi:
    def _api_falsa(self, monkeypatch, peticiones):
        def get(url, params, timeout):
            peticiones.append(params)
            pares = list(zip(params["latitude"], params["longitude"]))
            datos = [respuesta_api(lat, lon) for lat, lon in pares]
            return RespuestaFalsa(200, datos[0] if len(datos) == 1 else datos)
        monkeypatch.setattr(extract_api.requests, "get", get)

    def test_lotes_bronze_json_y_dataframe(self, tmp_path, monkeypatch, sin_esperas, bronze_excel, fuente_api):
        peticiones = []
        self._api_falsa(monkeypatch, peticiones)
        est = extract_api.obtener_estaciones(bronze_excel)      # 3 puntos unicos (A y B comparten)
        ruta_json = tmp_path / "bronze" / "api.json"

        df = extract_api.extraer_api_india(fuente_api, est, str(ruta_json))

        # batch_size=2 -> 2 peticiones (2 + 1 coordenadas); la de 1 coordenada devuelve un dict
        assert [len(p["latitude"]) for p in peticiones] == [2, 1]
        assert peticiones[0]["timezone"] == "Asia/Kolkata"
        # Una fila por punto y hora; las fechas quedan tipadas
        assert len(df) == 3 * 4
        assert pd.api.types.is_datetime64_any_dtype(df["date"])
        assert {"lat_consulta", "lon_consulta", "lat_modelo", "lon_modelo", "sulphur_dioxide"} <= set(df.columns)
        # El Bronze guarda las respuestas originales + metadatos de la extraccion
        bronze = json.loads(ruta_json.read_text(encoding="utf-8"))
        assert {"fuente", "extraido_utc", "parametros", "lotes"} <= set(bronze)
        assert len(bronze["lotes"]) == 2
        assert "hourly" in bronze["lotes"][0]["respuesta"][0]

    def test_respuesta_incompleta_da_error(self, tmp_path, monkeypatch, sin_esperas, bronze_excel, fuente_api):
        monkeypatch.setattr(extract_api.requests, "get",
                            lambda *a, **k: RespuestaFalsa(200, [respuesta_api(28.6, 77.2)]))
        est = extract_api.obtener_estaciones(bronze_excel)
        with pytest.raises(RuntimeError, match="devolvio 1 respuestas"):
            extract_api.extraer_api_india(fuente_api, est, str(tmp_path / "x.json"))

