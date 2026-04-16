import pandas as pd
import logging
import sys
from src.extract import extract, extract_csv
from src.load import load, load_to_postgres
from config import (
    PATH_LOG, PATH_RAW, PATH_PROCESSED, PATH_CSV_RAW, COLUMNAS_NECESARIAS, CAMPOS_CRITICOS, CAMPOS_MONETARIOS_AUXILIARES,
    COLUMNAS_CLAVE, PASSENGER_MIN, PASSENGER_MAX, DISTANCIA_MIN, FARE_MIN,TOTAL_MIN, DB_CONN
)
from src.transform import (
    estandarizar_columnas, validar_esquema, limpiar_texto, convertir_tipos, filtrar_nulos_criticos,
    imputar_nulos,filtrar_registros_invalidos, remover_duplicados, enriquecer_zonas, calcular_features, validar_resultado
)

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(PATH_LOG),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

def ejecutar_pipeline(df: pd.DataFrame, df_zonas: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    df_zonas = df_zonas.copy()

    logger.info('[PIPELINE] Iniciando capa de transformación...')
    df_clean = (
        df
        .pipe(estandarizar_columnas)
        .pipe(validar_esquema, COLUMNAS_NECESARIAS)
        .pipe(limpiar_texto)
        .pipe(convertir_tipos)
        .pipe(filtrar_nulos_criticos, CAMPOS_CRITICOS)
        .pipe(imputar_nulos, CAMPOS_MONETARIOS_AUXILIARES)
        .pipe(filtrar_registros_invalidos, CAMPOS_MONETARIOS_AUXILIARES, PASSENGER_MIN, PASSENGER_MAX, DISTANCIA_MIN, FARE_MIN, TOTAL_MIN)
        .pipe(remover_duplicados, COLUMNAS_CLAVE)
        .pipe(enriquecer_zonas, df_zonas)   
        .pipe(calcular_features)
        .pipe(validar_resultado, CAMPOS_CRITICOS, COLUMNAS_CLAVE, PASSENGER_MIN, PASSENGER_MAX, DISTANCIA_MIN, FARE_MIN, TOTAL_MIN)
    )
    return df_clean
    
def main() -> None:
    logger.info('[PIPELINE] === Iniciando pipeline ETL Taxi NYC ===')
    try:
        df = extract(PATH_RAW)
        df_zonas = extract_csv(PATH_CSV_RAW)
        n_antes = len(df)

        df_clean = ejecutar_pipeline(df, df_zonas)
        
        logger.info(f'[PIPELINE] Resumen | Iniciales: {n_antes:,} | Finales: {len(df_clean):,}')
        logger.info(f'[PIPELINE] Resumen | Reducción: {(n_antes - len(df_clean)) / n_antes:.2%}')

        load(df_clean, PATH_PROCESSED)
        load_to_postgres(df_clean, 'yellow_taxi_2023_01', DB_CONN)
        logger.info('[PIPELINE] === Pipeline completado con éxito ===')

    except Exception as e:
        logger.critical(f'[PIPELINE] Falla crítica: {e}', exc_info=True)
        sys.exit(1)

if __name__ == '__main__':
    main()