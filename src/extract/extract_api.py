import json
import logging
import os
import time
from datetime import datetime, timezone

import pandas as pd
import requests

log = logging.getLogger(__name__)

CODIGOS_REINTENTABLES = (429, 500, 502, 503, 504)


def obtener_estaciones(df_excel: pd.DataFrame, decimales: int = 1) -> pd.DataFrame:
    """
    Lista de estaciones (una por location_id) tomada del Excel.
    Acepta tanto el Excel crudo (Location_id, Location_name) como el Silver (location_id, ...).

    Agrega las coordenadas redondeadas con las que se consultara la API:
    el modelo tiene ~0.1 grados de resolucion, asi que estaciones muy cercanas
    comparten el mismo punto y no hace falta consultarlas por separado.
    Tambien agrega cuantas estaciones comparten cada punto de consulta.
    """
    df = df_excel.rename(columns={"Location_id": "location_id", "Location_name": "location_name"})
    est = (
        df[["location_id", "location_name", "latitude", "longitude"]]
        .drop_duplicates("location_id")
        .reset_index(drop=True)
    )
    est["lat_consulta"] = est["latitude"].round(decimales)
    est["lon_consulta"] = est["longitude"].round(decimales)
    est["n_estaciones_mismo_punto"] = (
        est.groupby(["lat_consulta", "lon_consulta"])["location_id"].transform("size").astype("int64")
    )
    compartidas = est[est["n_estaciones_mismo_punto"] > 1]
    log.info(
        "Estaciones: %s | puntos de consulta unicos: %s | estaciones que comparten punto: %s (max %s por punto)",
        len(est), est[["lat_consulta", "lon_consulta"]].drop_duplicates().shape[0],
        len(compartidas), int(est["n_estaciones_mismo_punto"].max()) if len(est) else 0,
    )
    return est


def _consultar(url: str, params: dict, reintentos: int = 4, espera_base: int = 5):
    """GET con reintentos ante limite de uso (429), fallos del servidor (5xx) o errores de red."""
    ultimo_error = None
    for intento in range(reintentos):
        try:
            r = requests.get(url, params=params, timeout=60)
        except requests.RequestException as e:  # timeout, conexion caida, DNS...
            ultimo_error = f"error de red: {e}"
        else:
            if r.status_code == 200:
                return r.json()
            if r.status_code not in CODIGOS_REINTENTABLES:
                raise RuntimeError(f"La API respondio HTTP {r.status_code}: {r.text[:300]}")
            ultimo_error = f"HTTP {r.status_code}"

        espera = espera_base * (intento + 1)
        log.warning("%s. Reintento %s/%s en %ss...", ultimo_error, intento + 1, reintentos, espera)
        time.sleep(espera)
    raise RuntimeError(f"La API no respondio tras {reintentos} intentos (ultimo: {ultimo_error}).")


def extraer_api_india(fuente: dict, df_estaciones: pd.DataFrame, ruta_bronze_json: str) -> pd.DataFrame:
    """
    Capa BRONZE: consulta la API de calidad del aire con las coordenadas de las estaciones del Excel.
    - Consulta por lotes de coordenadas unicas.
    - Guarda las respuestas tal como llegaron (JSON) en la capa Bronze.
    - Devuelve un DataFrame crudo: una fila por coordenada consultada y hora.
    """
    url = fuente["url"]
    params_base = fuente["params"]
    tam_lote = fuente.get("batch_size", 50)
    pausa = fuente.get("pausa_segundos", 1)

    coords = df_estaciones[["lat_consulta", "lon_consulta"]].drop_duplicates().reset_index(drop=True)
    n_lotes = -(-len(coords) // tam_lote)
    log.info("[BRONZE - API REST] %s", url)
    log.info("Estaciones del Excel: %s -> coordenadas unicas a consultar: %s", len(df_estaciones), len(coords))
    log.info("Peticiones: %s lotes de hasta %s coordenadas", n_lotes, tam_lote)

    lotes_crudos, tablas = [], []
    for i in range(n_lotes):
        lote = coords.iloc[i * tam_lote:(i + 1) * tam_lote]
        params = {
            **params_base,
            "latitude": lote["lat_consulta"].tolist(),
            "longitude": lote["lon_consulta"].tolist(),
        }
        respuesta = _consultar(url, params)
        if isinstance(respuesta, dict):  # con una sola coordenada la API devuelve un objeto
            respuesta = [respuesta]
        if len(respuesta) != len(lote):
            raise RuntimeError(
                f"Lote {i + 1}: se enviaron {len(lote)} coordenadas y la API devolvio {len(respuesta)} respuestas."
            )

        lotes_crudos.append({
            "consulta": {
                "latitude": lote["lat_consulta"].tolist(),
                "longitude": lote["lon_consulta"].tolist(),
            },
            "respuesta": respuesta,
        })

        for (lat, lon), r in zip(lote.itertuples(index=False, name=None), respuesta):
            t = pd.DataFrame(r["hourly"])
            t["lat_consulta"] = lat
            t["lon_consulta"] = lon
            t["lat_modelo"] = r["latitude"]    # punto de rejilla que realmente respondio
            t["lon_modelo"] = r["longitude"]
            tablas.append(t)

        log.info("Lote %s/%s: %s coordenadas OK", i + 1, n_lotes, len(lote))
        if i < n_lotes - 1:
            time.sleep(pausa)

    # Bronze: respuestas originales + datos de la consulta, sin transformar
    os.makedirs(os.path.dirname(ruta_bronze_json) or ".", exist_ok=True)
    bronze = {
        "fuente": url,
        "extraido_utc": datetime.now(timezone.utc).isoformat(),
        "parametros": params_base,
        "lotes": lotes_crudos,
    }
    with open(ruta_bronze_json, "w", encoding="utf-8") as f:
        json.dump(bronze, f, ensure_ascii=False, indent=2)
    log.info("Respuestas originales guardadas en Bronze: %s", ruta_bronze_json)

    df_horario = pd.concat(tablas, ignore_index=True).rename(columns={"time": "date"})
    df_horario["date"] = pd.to_datetime(df_horario["date"])
    log.info("Serie horaria cruda: %s filas (%s a %s)",
             f"{len(df_horario):,}", df_horario["date"].min(), df_horario["date"].max())
    return df_horario
