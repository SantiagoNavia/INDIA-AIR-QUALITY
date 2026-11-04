# Evidencia P3 — Planificación Horaria (`0 * * * *`) y Zonas Horarias

- **Expresión Cron configurada en el DAG:** `schedule="0 * * * *"` (minuto `0` de cada hora: `00:00, 01:00, 02:00, ..., 23:00 UTC`).
- **Prevención de ejecuciones retroactivas:** `catchup=False` (al activar el DAG por primera vez no lanza cientos de corridas históricas desde `start_date=2026-10-01`).
- **Control de concurrencia:** `max_active_runs=1` (impide que dos ejecuciones horarias escriban simultáneamente sobre Silver/Gold si una corrida tarda más de lo previsto).
- **Zona horaria del planificador (Airflow Scheduler):** `UTC` (estándar en Docker Compose para evitar saltos por horario de verano y mantener identificadores `scheduled__YYYY-MM-DDTHH:00:00+00:00` estrictamente crecientes).
- **Zona horaria funcional de los datos:** `Asia/Kolkata` (`UTC+05:30`), configurada en `config/config.yaml` (`sources.aire_india_api.params.timezone`), de modo que los promedios diarios (`fecha_lectura`) reflejan el día calendario local en India.

> Para la captura visual del disparo automático en la interfaz de Airflow (`http://localhost:8080`), mantén el interruptor del DAG `etl_green_data_hourly` activo (**Unpaused**): al cumplirse el minuto `:00` de la hora en curso aparecerá automáticamente una corrida con prefijo `scheduled__...` en estado `success`.
