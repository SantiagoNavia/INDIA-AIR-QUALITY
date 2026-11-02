"""Calculo del Indice Nacional de Calidad del Aire de India (NAQI - CPCB).

A diferencia del indice europeo (EAQI) que entrega directamente Open-Meteo,
el Central Pollution Control Board (CPCB) de India define su propio indice
oficial (NAQI, escala 0-500) con puntos de corte especificos para las
condiciones atmosfericas y normativas de India.

Fórmula oficial CPCB (interpolación lineal por tramos):
    I_p = ((I_hi - I_lo) / (B_hi - B_lo)) * (C_p - B_lo) + I_lo
    NAQI = max(I_PM2.5, I_PM10, I_NO2, I_SO2, I_CO, I_O3)

Nota de unidades:
    Open-Meteo reporta todos los gases y particulas en ug/m3.
    Los puntos de corte del CPCB para CO estan en mg/m3, por lo que CO se
    divide entre 1000 antes de calcular su sub-indice.
"""
import numpy as np
import pandas as pd

# Puntos de corte oficiales del CPCB: (C_low, C_high, I_low, I_high)
BREAKPOINTS_CPCB = {
    "pm2_5": [
        (0.0, 30.0, 0.0, 50.0),
        (30.0, 60.0, 51.0, 100.0),
        (60.0, 90.0, 101.0, 200.0),
        (90.0, 120.0, 201.0, 300.0),
        (120.0, 250.0, 301.0, 400.0),
        (250.0, 500.0, 401.0, 500.0),
    ],
    "pm10": [
        (0.0, 50.0, 0.0, 50.0),
        (50.0, 100.0, 51.0, 100.0),
        (100.0, 250.0, 101.0, 200.0),
        (250.0, 350.0, 201.0, 300.0),
        (350.0, 430.0, 301.0, 400.0),
        (430.0, 600.0, 401.0, 500.0),
    ],
    "nitrogen_dioxide": [
        (0.0, 40.0, 0.0, 50.0),
        (40.0, 80.0, 51.0, 100.0),
        (80.0, 180.0, 101.0, 200.0),
        (180.0, 280.0, 201.0, 300.0),
        (280.0, 400.0, 301.0, 400.0),
        (400.0, 600.0, 401.0, 500.0),
    ],
    "sulfur_dioxide": [
        (0.0, 40.0, 0.0, 50.0),
        (40.0, 80.0, 51.0, 100.0),
        (80.0, 380.0, 101.0, 200.0),
        (380.0, 800.0, 201.0, 300.0),
        (800.0, 1600.0, 301.0, 400.0),
        (1600.0, 2400.0, 401.0, 500.0),
    ],
    "ozone": [
        (0.0, 50.0, 0.0, 50.0),
        (50.0, 100.0, 51.0, 100.0),
        (100.0, 168.0, 101.0, 200.0),
        (168.0, 208.0, 201.0, 300.0),
        (208.0, 748.0, 301.0, 400.0),
        (748.0, 1000.0, 401.0, 500.0),
    ],
    # CO en mg/m3 (se convierte de ug/m3 dividiendo entre 1000)
    "carbon_monoxide": [
        (0.0, 1.0, 0.0, 50.0),
        (1.0, 2.0, 51.0, 100.0),
        (2.0, 10.0, 101.0, 200.0),
        (10.0, 17.0, 201.0, 300.0),
        (17.0, 34.0, 301.0, 400.0),
        (34.0, 50.0, 401.0, 500.0),
    ],
}

CATEGORIAS_NAQI = [
    (50.0, "Bueno"),
    (100.0, "Satisfactorio"),
    (200.0, "Moderado"),
    (300.0, "Pobre"),
    (400.0, "Muy pobre"),
]


def subindice_cpcb(concentracion: float, contaminante: str) -> float:
    """Calcula el sub-indice CPCB (0-500) para una concentracion en ug/m3."""
    if pd.isna(concentracion) or concentracion < 0:
        return np.nan

    valor = float(concentracion)
    if contaminante == "carbon_monoxide":
        valor = valor / 1000.0  # ug/m3 -> mg/m3

    tramos = BREAKPOINTS_CPCB[contaminante]
    for c_lo, c_hi, i_lo, i_hi in tramos:
        if valor <= c_hi:
            return round(((i_hi - i_lo) / (c_hi - c_lo)) * (valor - c_lo) + i_lo, 2)
    return 500.0


def categoria_naqi(valor: float) -> str:
    """Clasifica un valor numerico NAQI (0-500) en la escala oficial del CPCB de India."""
    if pd.isna(valor):
        return "Sin dato"
    for tope, nombre in CATEGORIAS_NAQI:
        if valor <= tope:
            return nombre
    return "Severo"


def calcular_naqi_dataframe(df: pd.DataFrame, mapa_columnas: dict[str, str]) -> pd.DataFrame:
    """Calcula (naqi, categoria, contaminante_dominante) para cada fila de un DataFrame.

    mapa_columnas asocia el nombre estandar del contaminante (clave en BREAKPOINTS_CPCB)
    con el nombre de la columna en df, por ejemplo:
        {"pm2_5": "pm2_5_promedio_dia", "pm10": "pm10_promedio_dia", ...}
    """
    subindices = {}
    for cont, col in mapa_columnas.items():
        if col in df.columns:
            subindices[cont] = df[col].apply(lambda v, c=cont: subindice_cpcb(v, c))
    df_sub = pd.DataFrame(subindices, index=df.index)

    tiene_datos = df_sub.notna().any(axis=1)
    naqi = df_sub.max(axis=1).round(2)
    dominante = pd.Series("Sin dato", index=df.index, dtype="object")
    if tiene_datos.any():
        dominante.loc[tiene_datos] = df_sub.loc[tiene_datos].idxmax(axis=1)
    categoria = naqi.apply(categoria_naqi)
    return pd.DataFrame({
        "naqi": naqi,
        "categoria": categoria,
        "dominante": dominante,
    }, index=df.index)
