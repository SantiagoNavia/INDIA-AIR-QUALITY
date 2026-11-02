"""Pruebas unitarias de la carga a Data Warehouse (PostgreSQL / SQLite)."""
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from src.load import load_datawarehouse


@pytest.fixture
def muestra_gold():
    return pd.DataFrame({
        "location_id": [1, 2],
        "location_name": ["Estacion 1", "Estacion 2"],
        "latitude": [28.6, 19.1],
        "longitude": [77.2, 72.8],
        "fecha_lectura": ["2026-10-14", "2026-10-14"],
        "aqi_promedio_dia": [55.0, 75.0],
        "naqi_india_promedio_dia": [110.0, 160.0],
        "categoria_naqi_india": ["Moderado", "Moderado"],
        "contaminante_dominante_naqi": ["pm2_5", "pm10"],
        "nivel_cobertura": ["Red Completa", "Basico"],
        "total_contaminantes": [5, 2],
        "contaminantes_monitoreados": ["pm2_5, pm10", "pm2_5"],
        "mide_pm2_5": [True, True],
        "mide_pm10": [True, False],
        "mide_no2": [False, False],
        "mide_co": [False, False],
        "mide_o3": [False, False],
        "dias_operacion_max": [10.0, 5.0],
        "fecha_inicio_red": ["2020-01-01", "2020-01-01"],
        "fecha_ultima_medicion": ["2026-09-01", "2026-09-01"],
        "estacion_activa_reciente": [True, True],
        "distancia_modelo_km": [2.5, 3.1],
        "id_punto_consulta": ["28.6_77.2", "19.1_72.8"],
        "n_estaciones_mismo_punto": [1, 1],
        "valor_compartido": [False, False],
        "pm2_5_promedio_dia": [35.0, 45.0],
        "pm10_promedio_dia": [60.0, 80.0],
        "no2_promedio_dia": [15.0, 20.0],
        "so2_promedio_dia": [5.0, 8.0],
        "co_promedio_dia": [0.5, 0.7],
        "o3_promedio_dia": [40.0, 50.0],
        "categoria_aqi_predominante": ["Moderado", "Pobre"],
        "ranking_peor_aire": [2, 1],
        "ranking_peor_aire_naqi": [2, 1],
        "ranking_peor_aire_celda": [2, 1],
    })


def test_obtener_motor_postgres(monkeypatch):
    monkeypatch.setenv("DATABASE_URL", "postgresql+psycopg2://test:pass@localhost:5432/test_db")
    engine = load_datawarehouse.obtener_motor_postgres()
    assert str(engine.url).startswith("postgresql+psycopg2://test:***@localhost:5432/test_db")


def test_cargar_gold_a_postgres_flujo(muestra_gold):
    """Verifica que la funcion ejecute la preparacion de staging, upsert y vistas."""
    mock_engine = MagicMock()
    mock_conn = MagicMock()
    mock_engine.connect.return_value.__enter__.return_value = mock_conn
    mock_engine.begin.return_value.__enter__.return_value = mock_conn

    # Simular columnas devueltas por information_schema
    mock_conn.execute.return_value.fetchall.return_value = [
        ("location_id", "bigint"),
        ("fecha_lectura", "character varying"),
        ("aqi_promedio_dia", "double precision"),
    ]
    mock_conn.execute.return_value.scalar.return_value = 2

    with patch("src.load.load_datawarehouse.obtener_motor_postgres", return_value=mock_engine):
        total = load_datawarehouse.cargar_gold_a_postgres(muestra_gold)
        assert total == 2
        assert mock_conn.execute.call_count >= 3

