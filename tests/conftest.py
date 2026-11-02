"""Fixtures compartidas: datos pequenos y controlados que imitan cada capa."""
import pandas as pd
import pytest


@pytest.fixture
def bronze_excel() -> pd.DataFrame:
    """Imita el Excel original (mismas columnas y encodings dañados)."""
    filas = [
        # Location_id, Location_name, lat, lon, Sensor_id, Parameter, Unit, first, last
        (1, "Estacion A", 28.6139, 77.2090, 101, "pm25", "\ufffdg/m\ufffd", "2020-01-01T00:00:00Z", "2020-01-11T00:00:00Z"),
        (1, "Estacion A", 28.6139, 77.2090, 102, "pm10", "\ufffdg/m\ufffd", "2020-01-01T00:00:00Z", "2020-01-21T00:00:00Z"),
        (1, "Estacion A", 28.6139, 77.2090, 103, "no2", "ppb", "2020-01-01T00:00:00Z", "2020-01-05T12:00:00Z"),
        (1, "Estacion A", 28.6139, 77.2090, 104, "temperature", "c", "2020-01-01T00:00:00Z", "2020-01-05T00:00:00Z"),
        (2, "Estacion B", 28.6390, 77.2310, 201, "CO", "ppm", None, None),           # misma celda que A
        (2, "Estacion B", 28.6390, 77.2310, 201, "co", "ppm", None, None),           # sensor duplicado
        (3, " Estacion C ", 19.0760, 72.8777, 301, "o3", "\ufffdg/m\ufffd", "2021-05-01T00:00:00Z", "2021-05-03T00:00:00Z"),
        (3, " Estacion C ", 19.0760, 72.8777, 302, "um003", "particles/cm\ufffd", None, None),  # no es contaminante mapeado
        (4, "Solo clima", 12.9716, 77.5946, 401, "relativehumidity", "%", None, None),  # estacion sin contaminantes
    ]
    cols = ["Location_id", "Location_name", "latitude", "longitude", "Sensor_id",
            "Parameter", "Unit", "datetime_first", "datetime_last"]
    df = pd.DataFrame(filas, columns=cols)
    df.insert(2, "Country_name", "India")
    df.insert(3, "Country_code", "IN")
    df.insert(7, "Sensor_name", df["Parameter"])
    return df


def respuesta_api(lat: float, lon: float, horas: int = 4, base: float = 10.0, fecha: str = "2026-10-14") -> dict:
    """Respuesta JSON con la misma forma que devuelve Open-Meteo para una coordenada."""
    tiempos = pd.date_range(f"{fecha} 00:00", periods=horas, freq="h").strftime("%Y-%m-%dT%H:%M").tolist()
    return {
        "latitude": lat + 0.01,   # el modelo responde con el punto de rejilla mas cercano
        "longitude": lon - 0.01,
        "hourly": {
            "time": tiempos,
            "pm10": [base * 2 + i for i in range(horas)],
            "pm2_5": [base + i for i in range(horas)],
            "carbon_monoxide": [200.0 + i for i in range(horas)],
            "nitrogen_dioxide": [5.0 + i for i in range(horas)],
            "sulphur_dioxide": [3.0 + i for i in range(horas)],
            "ozone": [50.0 + i for i in range(horas)],
            "european_aqi": [base * 3 + i for i in range(horas)],
        },
    }


@pytest.fixture
def fuente_api() -> dict:
    return {
        "url": "https://api.falsa/v1/air-quality",
        "batch_size": 2,
        "pausa_segundos": 0,
        "params": {"timezone": "Asia/Kolkata", "hourly": ["pm10", "pm2_5"]},
    }


@pytest.fixture
def sin_esperas(monkeypatch):
    """Evita esperas reales (time.sleep) en los reintentos y pausas de la API."""
    import src.extract.extract_api as extract_api
    llamadas = []
    monkeypatch.setattr(extract_api.time, "sleep", lambda s: llamadas.append(s))
    return llamadas


class RespuestaFalsa:
    def __init__(self, status_code: int, payload=None, text: str = ""):
        self.status_code = status_code
        self._payload = payload
        self.text = text

    def json(self):
        return self._payload

