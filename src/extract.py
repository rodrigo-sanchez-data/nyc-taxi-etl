import pandas as pd
import logging

logger = logging.getLogger(__name__)

def extract(path: str) -> pd.DataFrame:
    logger.info(f'[EXTRACT] Iniciando extracción de datos desde: {path}')
    try:
        df = pd.read_parquet(path, engine='pyarrow')
        logger.info(f'[EXTRACT] Archivo cargado con éxito | Filas: {len(df):,} | Columnas: {df.shape[1]}')
        return df
    except FileNotFoundError:
        logger.error(f'[EXTRACT] No se encontró el archivo en: {path}')
        raise
    except Exception:
        logger.exception(f'[EXTRACT] Error durante la extracción')
        raise

def extract_csv(path: str) -> pd.DataFrame:
    logger.info(f'[EXTRACT] Iniciando extraccion de datos desde: {path}')
    try:
        df = pd.read_csv(path, sep=',', encoding='utf-8')
        logger.info(f'[EXTRACT] Archivo cargado con éxito | Filas: {len(df):,} | Columnas: {df.shape[1]}')
        return df
    except FileNotFoundError:
        logger.error(f'[EXTRACT] No se encontró el archivo en: {path}')
        raise
    except Exception:
        logger.exception(f'[EXTRACT] Error durante la extracción')
        raise
        