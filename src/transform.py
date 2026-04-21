import pandas as pd
import logging
from pathlib import Path

logger = logging.getLogger(__name__)

def estandarizar_columnas(df: pd.DataFrame) -> pd.DataFrame:

    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(' ', '_', regex=False)
        .str.replace(r'[^\w]', '_', regex=True)
        .str.replace(r'_+', '_', regex=True)
        .str.strip('_')
    )
    logger.info('[TRANSFORM] Nombres de columnas estandarizados')
    return df

def validar_esquema(df: pd.DataFrame, columnas_necesarias: list[str]) -> pd.DataFrame:

    faltantes = [col for col in columnas_necesarias if col not in df.columns]
    if faltantes:
        logger.error(f'[TRANSFORM] Faltan columnas requeridas: {faltantes}')
        raise ValueError(f'Faltan columnas requeridas: {faltantes}')
    logger.info(f'[TRANSFORM] Esquema validado con éxito')
    return df

def limpiar_texto(df: pd.DataFrame) -> pd.DataFrame:

    cols_texto = df.select_dtypes(include=['object','string']).columns.tolist()
    for col in cols_texto:
        df[col] = df[col].str.strip().astype('string')
    if cols_texto:
        logger.info(f'[TRANSFORM] Texto limpiado en {len(cols_texto)} columnas')
    return df

def convertir_tipos(df: pd.DataFrame) -> pd.DataFrame:

    df['tpep_pickup_datetime'] = pd.to_datetime(df['tpep_pickup_datetime'], errors='coerce')
    df['tpep_dropoff_datetime'] = pd.to_datetime(df['tpep_dropoff_datetime'], errors='coerce')
    df['vendorid'] = df['vendorid'].astype('category')
    df['payment_type'] = df['payment_type'].astype('category')
    df['passenger_count'] = pd.to_numeric(df['passenger_count'], errors='coerce').astype('Int64')
    df['ratecodeid'] = pd.to_numeric(df['ratecodeid'], errors='coerce').astype('Int64')
    df['store_and_fwd_flag'] = df['store_and_fwd_flag'].map({'Y': True, 'N': False}).astype('boolean')

    logger.info('[TRANSFORM] Tipos de datos convertidos')
    return df

def filtrar_nulos_criticos(df: pd.DataFrame, campos_criticos: list[str]) -> pd.DataFrame:
    n_antes = len(df)

    filas_con_nulos = df[campos_criticos].isna().any(axis=1).sum()
    if filas_con_nulos > 0:
        df = df.dropna(subset=campos_criticos)
        logger.warning(f'[TRANSFORM] Eliminadas {filas_con_nulos:,} filas por nulos en campos críticos')
    else:
        logger.info('[TRANSFORM] Validación exitosa: Cero nulos en campos críticos')

    mask_fecha = df['tpep_dropoff_datetime'] > df['tpep_pickup_datetime']
    fechas_inconsistentes = (~mask_fecha).sum()

    if fechas_inconsistentes > 0:
        logger.warning(f'[TRANSFORM] Eliminadas {fechas_inconsistentes:,} filas por fechas invertidas o viajes de 0 segundos')
        df = df[mask_fecha]

    logger.info(f'[TRANSFORM] Filtrar Nulos Criticos | Entrantes: {n_antes:,} -> Salientes: {len(df):,} ({n_antes-len(df):,} eliminados)')
    return df

def imputar_nulos(df: pd.DataFrame, campos_monetarios_aux: list[str]) -> pd.DataFrame:

    nulos_pass = df['passenger_count'].isna().sum()
    if nulos_pass > 0:
        moda_passenger = df['passenger_count'].mode()[0]
        logger.info(f'[TRANSFORM] Imputados {nulos_pass:,} nulos en passenger_count con la moda ({moda_passenger})')
        df['passenger_count'] = df['passenger_count'].fillna(moda_passenger).astype('Int64')

    nulos_rate = df['ratecodeid'].isna().sum()
    if nulos_rate > 0:
        logger.info(f'[TRANSFORM] Imputados {nulos_rate:,} nulos en ratecodeid con tarifa estándar (1)')
        df['ratecodeid'] = df['ratecodeid'].fillna(1).astype('Int64')

    nulos_flag = df['store_and_fwd_flag'].isna().sum()
    if nulos_flag > 0:
        logger.info(f'[TRANSFORM] Imputados {nulos_flag:,} nulos en store_and_fwd_flag con (False)')
        df['store_and_fwd_flag'] = df['store_and_fwd_flag'].fillna(False).astype('boolean')

    nulos_mone = df[campos_monetarios_aux].isna().sum().sum()
    if nulos_mone > 0:
        logger.info(f'[TRANSFORM] Imputados {nulos_mone:,} nulos en campos monetarios auxiliares con (0.0)')
        df[campos_monetarios_aux] = df[campos_monetarios_aux].fillna(0).astype('float64')

    logger.info(f'[TRANSFORM] Imputar Nulos | Total imputados: {nulos_pass + nulos_rate + nulos_flag + nulos_mone:,}')
    return df   

def filtrar_registros_invalidos(
    df: pd.DataFrame,
    campos_monetarios_aux: list[str],
    passenger_min: int,
    passenger_max: int,
    distancia_min: float,
    fare_min: float,
    total_min: float
) -> pd.DataFrame:

    n_antes = len(df)
    mask_passenger = df['passenger_count'].between(passenger_min,passenger_max)
    mask_distance = df['trip_distance'] > distancia_min
    mask_fare = df['fare_amount'] >= fare_min
    mask_total = df['total_amount'] >= total_min

    invalidos_passenger = (~mask_passenger).sum()
    invalidos_distance = (~mask_distance).sum()
    invalidos_fare = (~mask_fare).sum()
    invalidos_total = (~mask_total).sum()

    if invalidos_passenger > 0:
        logger.warning(f'[TRANSFORM] passenger_count: {invalidos_passenger:,} registros fuera de rango [{passenger_min} - {passenger_max}]')
    if invalidos_distance > 0:
        logger.warning(f'[TRANSFORM] trip_distance: {invalidos_distance:,} registros inválidos (<= {distancia_min})')
    if invalidos_fare > 0:
        logger.warning(f'[TRANSFORM] fare_amount: {invalidos_fare:,} registros por debajo del mínimo (${fare_min})')
    if invalidos_total > 0:
        logger.warning(f'[TRANSFORM] total_amount: {invalidos_total:,} registros por debajo del mínimo (${total_min})')

    df = df[mask_passenger & mask_distance & mask_fare & mask_total]

    valores_negativos = (df[campos_monetarios_aux] < 0).sum().sum()
    if valores_negativos > 0:
        logger.warning(f'[TRANSFORM] Campos monetarios: {valores_negativos:,} valores negativos ajustados a (0.0)')
        df[campos_monetarios_aux] = df[campos_monetarios_aux].clip(lower=0)
        
    logger.info(f'[TRANSFORM] Filtro Invalidados | Entrantes: {n_antes:,} -> Salientes: {len(df):,} ({n_antes-len(df):,} eliminados)')
    return df

def remover_duplicados(df: pd.DataFrame, columnas_claves: list[str]) -> pd.DataFrame:
    
    n_antes = len(df)
    n_duplicados = df.duplicated(subset=columnas_claves).sum()
    if n_duplicados > 0:
        logger.warning(f'[TRANSFORM] Eliminadas {n_duplicados:,} filas duplicadas')
        df = df.drop_duplicates(subset=columnas_claves, keep='first')
    else:
        logger.info('[TRANSFORM] Validación exitosa: Cero registros duplicados encontrados')

    logger.info(f'[TRANSFORM] Remover Duplicados | Entrantes: {n_antes:,} -> Salientes: {len(df):,} ({n_antes-len(df):,} eliminados)')
    return df

def enriquecer_zonas(df: pd.DataFrame, df_zonas: pd.DataFrame) -> pd.DataFrame:
    n_antes = len(df)

    logger.info('[TRANSFORM] Limpiando tabla de dimensiones (zonas)...')
    df_zonas = (
        df_zonas
        .pipe(estandarizar_columnas)
        .pipe(limpiar_texto)
    )

    logger.info('[TRANSFORM] Cruzando zonas de Pickup y Dropoff...')
    df = (
        df
        .merge(right=df_zonas, left_on='pulocationid', right_on='locationid', validate='many_to_one')
        .merge(right=df_zonas, left_on='dolocationid', right_on='locationid', validate='many_to_one', suffixes=('_pickup', '_dropoff'))
        .drop(columns=['locationid_pickup', 'locationid_dropoff'])
    )

    if len(df) != n_antes:
        logger.warning(f'[TRANSFORM] Merge eliminó filas inesperadas | Eliminadas: {n_antes - len(df):,}')

    logger.info(f'[TRANSFORM] Merge completado | Entrantes: {n_antes:,} -> Salientes: {len(df):,}')
    return df

def calcular_features(df: pd.DataFrame) -> pd.DataFrame:

    df['trip_duration_min'] = (df['tpep_dropoff_datetime'] - df['tpep_pickup_datetime']).dt.total_seconds() / 60

    df['speed_mph'] = df['trip_distance'] / (df['trip_duration_min'] / 60)

    mask_speed_fuera_rango = df['speed_mph'] >= 75
    n_fuera_rango = mask_speed_fuera_rango.sum()

    if mask_speed_fuera_rango.any():
        logger.warning(f'[TRANSFORM] speed_mph: {n_fuera_rango:,} viajes con velocidad irreal (>= 75 mph) reemplazados con NaN')
        df['speed_mph'] = df['speed_mph'].mask(mask_speed_fuera_rango)
    
    df['pickup_day_name'] = df['tpep_pickup_datetime'].dt.day_name().astype('category')
    df['pickup_hour'] = df['tpep_pickup_datetime'].dt.hour.astype('int8')

    logger.info('[TRANSFORM] Features calculadas: trip_duration_min, speed_mph, pickup_day_name, pickup_hour')
    return df

def validar_resultado(
    df: pd.DataFrame,
    campos_criticos: list[str],
    columnas_clave: list[str],
    passenger_min: int,
    passenger_max: int,
    distancia_min: float,
    fare_min: float,
    total_min: float

) -> pd.DataFrame:

    checks = {
        'sin_nulos_criticos': df[campos_criticos].notna().all().all(),
        'fechas_consistentes': (df['tpep_dropoff_datetime'] > df['tpep_pickup_datetime']).all(),

        'pasajeros_validos': df['passenger_count'].between(passenger_min,passenger_max).all(),
        'distancias_validas': (df['trip_distance'] > distancia_min).all(),
        'fare_validos': (df['fare_amount'] >= fare_min).all(),
        'total_validos': (df['total_amount'] >= total_min).all(),

        'sin_duplicados': not df[columnas_clave].duplicated().any(),

        'trip_duration_positiva': (df['trip_duration_min'] > 0).all(),
        'speed_valida': (df['speed_mph'].dropna() < 75).all(),
        'pickup_hour_valido': df['pickup_hour'].between(0, 23).all(),
        
        'vendorid_es_category': str(df['vendorid'].dtype) == 'category',
        'fechas_son_datetime': pd.api.types.is_datetime64_any_dtype(df['tpep_pickup_datetime']),
        'passenger_es_int': str(df['passenger_count'].dtype) == 'Int64',
    }

    fallos = [nombre for nombre, resultado in checks.items() if not resultado]
    if fallos:
        logger.error(f'[TRANSFORM] Validacion final fallida en los checks: {fallos}', )
        raise ValueError(f'Validacion final fallida en los checks: {fallos}')

    logger.info(f'[TRANSFORM] Validación final OK | Registros limpios: {len(df):,}')
    return df
