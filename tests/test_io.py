"""Pruebas de la carga incremental (upsert) y de la escritura de archivos."""
import pandas as pd

from src.utils import io


def test_upsert_sin_archivo_previo(tmp_path):
    nuevo = pd.DataFrame({"id": [1, 2], "fecha": ["d1", "d1"], "v": [10, 20]})
    acumulado, previas = io.upsert_csv(nuevo, str(tmp_path / "x.csv"), ["id", "fecha"])
    assert previas == 0
    pd.testing.assert_frame_equal(acumulado, nuevo)


def test_upsert_reemplaza_claves_repetidas_y_conserva_historial(tmp_path):
    ruta = str(tmp_path / "gold.csv")
    io.guardar_csv(pd.DataFrame({"id": [1, 2, 1], "fecha": ["d1", "d1", "d0"], "v": [1, 2, 3]}), ruta)
    nuevo = pd.DataFrame({"id": [1, 3], "fecha": ["d1", "d1"], "v": [100, 300]})

    acumulado, previas = io.upsert_csv(nuevo, ruta, ["id", "fecha"])

    assert previas == 2                                   # (2,d1) y (1,d0) se conservan
    assert not acumulado.duplicated(["id", "fecha"]).any()
    v = acumulado.set_index(["id", "fecha"])["v"]
    assert v[(1, "d1")] == 100 and v[(1, "d0")] == 3 and v[(3, "d1")] == 300


def test_upsert_es_idempotente(tmp_path):
    ruta = str(tmp_path / "h.csv")
    nuevo = pd.DataFrame({"location_id": [1, 1], "date": pd.to_datetime(["2026-10-14 00:00", "2026-10-14 01:00"]),
                          "pm2_5": [1.0, 2.0]})
    for _ in range(3):
        acumulado, _ = io.upsert_csv(nuevo, ruta, ["location_id", "date"])
        io.guardar_csv(acumulado, ruta)
    final = pd.read_csv(ruta)
    assert len(final) == 2


def test_upsert_con_fechas_mezcla_tipos_correctamente(tmp_path):
    ruta = str(tmp_path / "h.csv")
    viejo = pd.DataFrame({"location_id": [1], "date": pd.to_datetime(["2026-10-13 23:00"]), "v": [5.0]})
    io.guardar_csv(viejo, ruta)
    nuevo = pd.DataFrame({"location_id": [1], "date": pd.to_datetime(["2026-10-14 00:00"]), "v": [6.0]})
    acumulado, previas = io.upsert_csv(nuevo, ruta, ["location_id", "date"])
    assert previas == 1
    assert pd.api.types.is_datetime64_any_dtype(acumulado["date"])
    acumulado.sort_values("date")                          # no falla por tipos mezclados


def test_upsert_con_archivo_corrupto_o_sin_claves(tmp_path):
    nuevo = pd.DataFrame({"id": [1], "fecha": ["d1"]})
    vacio = tmp_path / "vacio.csv"
    vacio.write_text("")
    assert io.upsert_csv(nuevo, str(vacio), ["id", "fecha"])[1] == 0
    otro = tmp_path / "otro.csv"
    otro.write_text("a,b\n1,2\n")
    acumulado, previas = io.upsert_csv(nuevo, str(otro), ["id", "fecha"])
    assert previas == 0 and len(acumulado) == 1


def test_lectura_de_silver_restaura_tipos(tmp_path):
    ruta_s = str(tmp_path / "s.csv")
    pd.DataFrame({"datetime_first": ["2020-01-01 00:00:00+00:00", None],
                  "datetime_last": ["2020-01-02 00:00:00+00:00", None]}).to_csv(ruta_s, index=False)
    s = io.leer_silver_sensores(ruta_s)
    assert pd.api.types.is_datetime64_any_dtype(s["datetime_first"])

    ruta_h = str(tmp_path / "h.csv")
    pd.DataFrame({"date": ["2026-10-14 00:00:00"]}).to_csv(ruta_h, index=False)
    assert pd.api.types.is_datetime64_any_dtype(io.leer_silver_horario(ruta_h)["date"])

