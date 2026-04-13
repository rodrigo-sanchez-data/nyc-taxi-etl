import pandas as pd
import logging
from sqlalchemy import create_engine

logger = logging.getLogger(__name__)

def load(df: pd.DataFrame, path) -> None:
    logger.info(f'[LOAD] Iniciando carga de datos en: {path}')
    try:
        df.to_parquet(path, engine='pyarrow', compression='snappy', index=False)
        logger.info(f'[LOAD] Carga exitosa | Archivo guardado con {len(df):,} registros')
    except Exception:
        logger.exception(f'[LOAD] Error inesperado al intentar guardar el archivo')
        raise

def load_to_postgres(df: pd.DataFrame, tabla: str, conn_string: str) -> None:
    logger.info(f'[LOAD] Iniciando conexión al motor de base de datos para la tabla: {tabla}')
    try:
        engine = create_engine(conn_string)
        df.to_sql(tabla, engine, if_exists='replace', index=False)
        logger.info(f'[LOAD] Carga exitosa | {len(df):,} registros insertados en base de datos')
    except Exception:
        logger.exception('[LOAD] Error al cargar en PostgreSQL')
        raise   

