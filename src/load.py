import pandas as pd
import logging
from sqlalchemy import create_engine
from  pathlib import Path

logger = logging.getLogger(__name__)

def load(df: pd.DataFrame, path: Path) -> None:
    logger.info(f'[LOAD] Iniciando carga de datos en: {path}')
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
        df.to_parquet(path, engine='pyarrow', compression='snappy', index=False)
        logger.info(f'[LOAD] Carga exitosa | Archivo guardado con {len(df):,} registros')
    except Exception:
        logger.exception(f'[LOAD] Error al intentar guardar el archivo')
        raise

def load_to_postgres(df: pd.DataFrame, tabla: str, conn_string: str) -> None:
    if df.empty:
        logger.warning('[LOAD] DataFrame vacío, no se insertaron registros')
        return
    
    logger.info(f'[LOAD] Iniciando conexión al motor de base de datos para la tabla: {tabla}')
    try:
        engine = create_engine(conn_string)
        df.to_sql(tabla, engine, if_exists='replace', index=False, chunksize=10000, method='multi')
        logger.info(f'[LOAD] Carga exitosa | {len(df):,} registros insertados en la tabla: {tabla}')
    except Exception:
        logger.exception('[LOAD] Error al cargar en PostgreSQL')
        raise
    finally:
        engine.dispose()
