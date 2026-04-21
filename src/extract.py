import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

def extract(path: Path, columnas_necesarias: list) -> pd.DataFrame:
    logger.info(f'[EXTRACT] Iniciando extracción de datos desde: {path}')
    try:
        df = pd.read_parquet(path, engine='pyarrow', columns=columnas_necesarias)
        logger.info(f'[EXTRACT] Archivo cargado con éxito | Filas: {len(df):,} | Columnas: {df.shape[1]}')
        return df
    except FileNotFoundError:
        logger.error(f'[EXTRACT] No se encontró el archivo en: {path}')
        raise
    except Exception:
        logger.exception(f'[EXTRACT] Error durante la extracción')
        raise

def extract_csv(path: Path) -> pd.DataFrame:
    logger.info(f'[EXTRACT] Iniciando extraccion de datos desde: {path}')
    try:
        df = pd.read_csv(path, encoding='utf-8')
        logger.info(f'[EXTRACT] Archivo cargado con éxito | Filas: {len(df):,} | Columnas: {df.shape[1]}')
        return df
    except FileNotFoundError:
        logger.error(f'[EXTRACT] No se encontró el archivo en: {path}')
        raise
    except Exception:
        logger.exception(f'[EXTRACT] Error durante la extracción')
        raise
        