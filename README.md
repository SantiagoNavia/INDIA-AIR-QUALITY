# Proyecto ETL: Calidad del Aire en India (Arquitectura Medallion)

**Autor:** Santiago Navia Soto

---

## 1. ¿De qué trata este proyecto?

Este proyecto construye un pipeline **ETL (Extracción, Transformación y Carga)** que analiza la calidad del aire en **763 estaciones de monitoreo de India** combinando dos fuentes de datos independientes:

1. **Un archivo Excel (`AIR_QUALITY_INDIA_DATASET.xlsx`):** contiene el directorio de estaciones físicas en India (dónde están ubicadas y qué sensores tienen instalados).
2. **La API REST de Open-Meteo:** entrega los niveles horarios de contaminación del aire (`PM2.5`, `PM10`, `NO2`, `SO2`, `CO`, `O3` e índice `AQI`) en las coordenadas de esas estaciones.

---

## 2. ¿Cómo funciona la Arquitectura Medallion?

Los datos pasan por tres capas organizadas de menor a mayor calidad:

* **Capa Bronze (Datos Crudos):**
  Guarda los datos tal como vienen de la fuente original, sin modificarlos: el Excel original y el archivo `bronze_aire_india_api.json` con las respuestas de la API.
* **Capa Silver (Datos Limpios):**
  * **Sensores (`silver_aire_india_sensores.csv`):** filtra únicamente sensores de calidad del aire (descarta variables de clima como temperatura o humedad), corrige unidades dañadas (`ug/m3`), elimina sensores duplicados y calcula cuántos días ha operado cada sensor.
  * **Horario (`silver_aire_india_horario.csv`):** limpia los datos horarios de la API, elimina valores negativos imposibles, rellena huecos por interpolación temporal y une cada estación del Excel con su serie horaria.
* **Capa Gold (Indicadores Finales):**
  * **`gold_aire_india.csv`:** resume la información en **una fila por estación y día**, combinando la cobertura de sensores del Excel con los promedios/máximos diarios de contaminación, el índice europeo (**EAQI**), el índice oficial de India (**NAQI**) y los rankings de peor calidad del aire.
  * **`reporte_coordenadas_compartidas.csv`:** tabla de control que muestra qué estaciones cercanas comparten el mismo punto de consulta en la API.

---

## 3. Estructura de Carpetas

```text
medallon/
├── config/config.yaml          # Configuración de rutas, API y reglas de calidad
├── dags/                       # DAG de Apache Airflow para automatizar el pipeline
├── data/
│   ├── bronze/                 # Datos crudos (Excel + JSON de la API)
│   ├── silver/                 # Datos limpios (sensores + serie horaria)
│   └── gold/                   # Indicadores por estación y reporte de coordenadas
├── logs/
│   ├── log.txt                 # Registro paso a paso de cada ejecución
│   └── run_summary.json        # Resumen en JSON de la última corrida
├── notebooks/notebook.ipynb    # Análisis exploratorio y gráficos (EDA)
├── scripts/                    # Script para empaquetar la entrega en .zip
├── src/
│   ├── extract/                # Lectura del Excel y consumo de la API por lotes
│   ├── transform/              # Limpieza Silver, cálculo de NAQI India y capa Gold
│   ├── quality/                # Controles de calidad de los datos y del JOIN
│   ├── load/                   # Carga de la capa Gold a PostgreSQL (Data Warehouse)
│   └── utils/                  # Configuración, logs y guardado incremental de CSV
├── tests/                      # Pruebas automatizadas con pytest
├── main.py                     # Archivo principal para ejecutar todo el pipeline
└── requirements.txt            # Librerías necesarias
```

---

## 4. Cómo instalar y ejecutar el proyecto

### Paso 1: Instalar librerías
```powershell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
pip install -r requirements-dev.txt
```
*(No se necesita ninguna clave o `API_KEY` porque la API de Open-Meteo es pública y gratuita).*

### Paso 2: Ejecutar el pipeline
* **Para correr todo el proceso completo (Bronze $\rightarrow$ Silver $\rightarrow$ Gold $\rightarrow$ Calidad $\rightarrow$ PostgreSQL):**
  ```powershell
  python main.py
  ```
* **Para regenerar Gold, controles de calidad y actualizar PostgreSQL desde los archivos Silver (rápido, sin consultar la API):**
  ```powershell
  python main.py --solo-gold
  ```
*(Nota: siempre que exista tu archivo `.env` en `medallon/`, el pipeline actualizará PostgreSQL automáticamente al final sin necesidad de escribir comandos adicionales).*

Al terminar, puedes revisar el archivo `logs/log.txt` para ver todo lo que hizo el programa paso por paso.

### Paso 3: Ejecutar las pruebas automatizadas
```powershell
pytest
```
*(Si deseas ver el porcentaje de código cubierto por las pruebas: `pytest --cov=src --cov=main`).*

---

## 5. Aspectos importantes sobre los datos

### A. Diferencia entre los datos de Open-Meteo y un sensor físico real
* **El Excel** es un catálogo de estaciones reales en tierra, pero **no trae mediciones**, solo la ubicación y las fechas en que operó cada sensor.
* **Open-Meteo** no lee el sensor físico de la calle: usa un **modelo atmosférico por computadora (Copernicus CAMS)** que divide el mapa en cuadros (celdas) de aproximadamente **11 km $\times$ 11 km ($0.1^\circ$)**.
* **¿Qué significa esto?**
  1. El valor de Open-Meteo es el **promedio estimado para un cuadro de 11 km**, mientras que un sensor real mide una esquina o avenida específica (donde un camión o fábrica cercana puede disparar el dato).
  2. El modelo entrega datos todos los días aunque el sensor físico de esa estación lleve años apagado. Por eso en la capa Gold se agregó la columna `estacion_activa_reciente`, que indica si la estación tuvo actividad en el último año (**630 activas** y **133 inactivas**) y la columna `distancia_modelo_km` (distancia entre la estación y el punto del modelo, en promedio **4.19 km**).

### B. ¿Por qué varias estaciones comparten coordenadas y empatan en el ranking?
* Como el modelo de Open-Meteo cambia cada $0.1^\circ$ ($\sim 11\text{ km}$), las coordenadas se redondean a 1 decimal (`lat_consulta`, `lon_consulta`) para no pedirle a la API dos veces el mismo cuadro.
* En ciudades grandes (como Delhi o Mumbai) hay muchas estaciones separadas por pocos kilómetros que caen dentro del **mismo cuadro de 11 km**.
* En total:
  * De las **763 estaciones**, se consultan **460 puntos únicos**.
  * **427 estaciones** comparten su cuadro con al menos otra estación vecina.
* Como reciben el mismo pronóstico del cuadro, **tienen el mismo AQI y empatan en el ranking por estación (`ranking_peor_aire`)**. Para distinguirlas están las columnas `id_punto_consulta`, `n_estaciones_mismo_punto`, `valor_compartido`, el ranking por cuadro único (`ranking_peor_aire_celda`) y el archivo `data/gold/reporte_coordenadas_compartidas.csv`.

### C. Índice Europeo (EAQI) vs. Índice Oficial de India (NAQI)
* La API de Open-Meteo entrega por defecto el índice europeo (`aqi_promedio_dia`), pensado para niveles de contaminación más bajos.
* En este proyecto se calculó además el **índice oficial de India (`naqi_india_promedio_dia`, escala 0 a 500)** usando la fórmula del *Central Pollution Control Board (CPCB)* de India, junto con su categoría (`categoria_naqi_india`) y qué contaminante fue el más crítico del día (`contaminante_dominante_naqi`).

---

## 6. Integración con PostgreSQL y pgAdmin (Data Warehouse)

El módulo [`src/load/load_datawarehouse.py`](src/load/load_datawarehouse.py) toma la capa Gold y la sube de forma **idempotente** (UPSERT sobre `location_id` y `fecha_lectura`) a tu base de datos PostgreSQL (`DB_GOLD_INDIA`).

En **pgAdmin**, al desplegar `DB_GOLD_INDIA` $\rightarrow$ `Schemas` $\rightarrow$ `public`, encontrarás:
* **Tabla `gold_aire_india`:** tabla analítica principal con todos los registros y métricas.
* **Vista `vw_dim_estaciones`:** dimensión con el catálogo único de las 763 estaciones, sus sensores, cobertura y coordenadas.
* **Vista `vw_fact_calidad_aire_diaria`:** tabla de hechos con las concentraciones diarias, índices EAQI y NAQI y rankings.
* **Vista `vw_top_peor_aire`:** ranking de las estaciones con peor calidad del aire ordenadas por fecha.

---

## 7. Automatización con Apache Airflow (Docker)

Para no tener que ejecutar el pipeline manualmente todos los días, el proyecto se conectó con **Apache Airflow** corriendo en **Docker Compose**.

En el archivo [`dags/dag_calidad_aire_india.py`](dags/dag_calidad_aire_india.py) se creó el flujo automático (**DAG `etl_calidad_aire_india_medallion`**), que divide el trabajo en **7 pasos ordenados**:

1. **`t1_extraer_excel_bronze`:** revisa y carga el archivo Excel de estaciones.
2. **`t2_limpiar_excel_silver`:** limpia el catálogo de sensores y guarda el archivo Silver de sensores.
3. **`t3_extraer_api_bronze`:** pide a la API de Open-Meteo los datos del día y guarda el JSON en Bronze.
4. **`t4_limpiar_api_silver`:** limpia los datos horarios, los une con las estaciones y actualiza el archivo Silver horario.
5. **`t5_construir_capa_gold`:** calcula los promedios diarios, el índice europeo (EAQI), el índice de India (NAQI) y los rankings en la capa Gold.
6. **`t6_validar_calidad_integracion`:** revisa que no haya datos duplicados, vacíos ni fuera de rango antes de tocar la base de datos. Si algo está mal, detiene el proceso y avisa del error.
7. **`t7_cargar_postgres_datawarehouse`:** sube los resultados validados directamente a **PostgreSQL (`DB_GOLD_INDIA`)** para verlos en pgAdmin.

### ¿Cómo se usa desde el navegador?
1. Se levanta Airflow con Docker (`docker compose up -d` en la carpeta `airflow-medallon`).
2. Se entra a **[http://localhost:8080](http://localhost:8080)**.
3. Ahí aparece el flujo **`etl_calidad_aire_india_medallion`**, donde se puede ejecutar con un clic y ver en verde cada uno de los 7 pasos y sus registros de ejecución.

---

## 8. Columnas principales de la Capa Gold (`data/gold/gold_aire_india.csv`)

| Grupo | Columnas principales | Descripción sencilla |
| :--- | :--- | :--- |
| **Identificación** | `location_id`, `location_name`, `latitude`, `longitude` | ID, nombre y coordenadas reales de la estación. |
| **Red de sensores (Excel)** | `total_contaminantes`, `contaminantes_monitoreados`, `mide_pm2_5`...`mide_o3`, `nivel_cobertura`, `dias_operacion_max`, `estacion_activa_reciente` | Qué sensores tiene la estación, cuántos días operó y si estuvo activa en el último año. |
| **Control de celda y distancia** | `id_punto_consulta`, `n_estaciones_mismo_punto`, `valor_compartido`, `distancia_modelo_km` | En qué celda de 11 km cayó la estación, cuántas estaciones comparten esa celda y a cuántos km está del punto del modelo. |
| **Contaminantes del día (API)** | `fecha_lectura`, `pm2_5_promedio_dia`, `pm2_5_max_dia`, `pm10_promedio_dia`, `no2_promedio_dia`, `so2_promedio_dia`, `co_promedio_dia`, `o3_promedio_dia` | Promedios y máximos diarios modelados en $\mu\text{g/m}^3$. |
| **Índices de Calidad del Aire** | `aqi_promedio_dia`, `categoria_aqi_predominante`, `naqi_india_promedio_dia`, `categoria_naqi_india`, `contaminante_dominante_naqi` | Calidad del aire evaluada tanto con la norma europea (EAQI) como con la norma oficial de India (NAQI - CPCB). |
| **Lectura de la hora actual** | `hora_lectura_actual`, `pm2_5_actual`...`aqi_actual`, `naqi_india_actual` | Última lectura horaria disponible en el día. |
| **Rankings diarios** | `ranking_peor_aire`, `ranking_peor_aire_naqi`, `ranking_peor_aire_celda` | Posición de la estación ($1 = \text{peor aire del día}$) según EAQI, según NAQI India y por celda única. |
