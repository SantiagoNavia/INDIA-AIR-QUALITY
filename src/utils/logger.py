"""Configuracion del registro (logging) del pipeline.

Cada ejecucion queda registrada en consola y en el archivo de log
(por defecto logs/log.txt), con el formato:
    fecha hora | NIVEL | modulo | mensaje
"""
import logging
import os

FORMATO = "%(asctime)s | %(levelname)-7s | %(name)-28s | %(message)s"
FORMATO_FECHA = "%Y-%m-%d %H:%M:%S"


def configurar_logger(archivo_log: str = "logs/log.txt", nivel: str = "INFO") -> logging.Logger:
    """Configura el logger raiz con salida a consola y a archivo (modo append)."""
    os.makedirs(os.path.dirname(archivo_log) or ".", exist_ok=True)

    raiz = logging.getLogger()
    raiz.setLevel(getattr(logging, str(nivel).upper(), logging.INFO))
    for h in list(raiz.handlers):  # evita handlers duplicados si se llama dos veces
        raiz.removeHandler(h)
        h.close()

    formato = logging.Formatter(FORMATO, FORMATO_FECHA)

    consola = logging.StreamHandler()
    consola.setFormatter(formato)
    raiz.addHandler(consola)

    archivo = logging.FileHandler(archivo_log, mode="a", encoding="utf-8")
    archivo.setFormatter(formato)
    raiz.addHandler(archivo)

    # Librerias externas solo reportan advertencias
    logging.getLogger("urllib3").setLevel(logging.WARNING)
    return logging.getLogger("etl")

