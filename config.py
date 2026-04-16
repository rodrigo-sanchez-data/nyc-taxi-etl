from pathlib import Path
from dotenv import load_dotenv
import os

load_dotenv()

BASE_DIR = Path(__file__).parent

PATH_LOG = BASE_DIR / 'etl_taxi.log'
PATH_RAW = BASE_DIR / 'data' / 'raw' / 'yellow_tripdata_2023-01.parquet'
PATH_CSV_RAW = BASE_DIR / 'data' / 'raw' / 'taxi_zone_lookup.csv'
PATH_PROCESSED = BASE_DIR / 'data' / 'processed' / 'clean_taxi.parquet'

COLUMNAS_NECESARIAS = [
    'vendorid', 'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'passenger_count', 'trip_distance',
    'ratecodeid', 'store_and_fwd_flag', 'pulocationid', 'dolocationid', 'payment_type', 'fare_amount',
    'extra', 'mta_tax', 'tip_amount', 'tolls_amount', 'improvement_surcharge', 'total_amount',
    'congestion_surcharge', 'airport_fee'
]
CAMPOS_CRITICOS = [
    'tpep_pickup_datetime', 'tpep_dropoff_datetime', 'trip_distance',
    'pulocationid', 'dolocationid', 'fare_amount', 'total_amount'
]
CAMPOS_MONETARIOS_AUXILIARES = [
    'congestion_surcharge', 'airport_fee', 'tip_amount',
    'tolls_amount', 'extra', 'mta_tax', 'improvement_surcharge'
]
COLUMNAS_CLAVE = [
    'vendorid','tpep_pickup_datetime','tpep_dropoff_datetime',
    'trip_distance','pulocationid','dolocationid'
]

DB_CONN = (
    f"postgresql://{os.getenv('DB_USER')}:{os.getenv('DB_PASSWORD')}"
    f"@{os.getenv('DB_HOST')}:{os.getenv('DB_PORT')}/{os.getenv('DB_NAME')}"
)

PASSENGER_MIN = 1
PASSENGER_MAX = 6
FARE_MIN = 3.0
TOTAL_MIN = 3.0
DISTANCIA_MIN = 0.0