# Evidencia P5 — Prueba de Idempotencia y Re-ejecución sin Duplicados

- **Fecha de verificación (UTC):** `2026-11-04T03:31:21+00:00`
- **Estrategia de idempotencia aplicada:**
  1. **Capa Bronze (`data/bronze/runs/<run_id>/`):** cada corrida escribe su snapshot inalterado en una subcarpeta propia identificada por `run_id`, sin corromper corridas previas, y actualiza el archivo puntero `bronze_aire_india_api.json`.
  2. **Capa Silver (`silver_aire_india_horario.csv`):** aplica `upsert_csv` sobre la clave compuesta `(location_id, date)` y `drop_duplicates(subset=["sensor_id"])` en sensores.
  3. **Capa Gold (`gold_final.csv`) y PostgreSQL (`gold_aire_india`):** aplica `upsert_csv` (y `ON CONFLICT (location_id, fecha_lectura) DO UPDATE` en PostgreSQL) sobre la clave primaria compuesta `(location_id, fecha_lectura)`, recalculando los rankings diarios de forma determinista.

## Comparación cuantitativa Antes vs. Después de Re-ejecutar el DAG

| Métrica / Indicador KPI | Antes de Re-ejecutar (Corrida 1) | Después de Re-ejecutar (Corrida 2) | Diferencia |
| :--- | :---: | :---: | :---: |
| Total de filas en gold_final.csv | `2289.0` | `2289.0` | `0.0` |
| Duplicados en clave (location_id, fecha_lectura) | `0.0` | `0.0` | `0.0` |
| Estaciones unicas monitoreadas | `763.0` | `763.0` | `0.0` |
| KPI_01 - NAQI India promedio nacional | `125.76` | `125.76` | `0.0` |
| KPI_02 - EAQI Europeo promedio nacional | `69.62` | `69.62` | `0.0` |
| KPI_03 - Excedencia umbral OMS PM2.5 > 15 ug/m3 (%) | `88.69` | `88.69` | `0.0` |
| KPI_04 - Cobertura Red Completa >= 5 contaminantes (%) | `89.91` | `89.91` | `0.0` |
| KPI_05 - Distancia media estacion vs celda CAMS (km) | `4.19` | `4.19` | `0.0` |

- **Tabla `tabla_kpi_resumen.csv` idéntica antes y después:** `True`
- **Conclusión:** La re-ejecución del DAG sobre el mismo intervalo de datos es **100 % idempotente** (`0` filas duplicadas y `0.0` desviación en todos los indicadores).
