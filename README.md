# 🚕 NYC Yellow Taxi — Pipeline ETL

Pipeline ETL end-to-end construido con Python y pandas sobre el dataset público de viajes en taxi amarillo de Nueva York (enero 2023). Procesa más de 3 millones de registros, aplica limpieza y validaciones de calidad de datos, enriquece con datos geográficos y carga el resultado en PostgreSQL.

---

## 📋 Descripción

Este proyecto implementa un pipeline ETL modular que:

- **Extrae** datos desde archivos Parquet y CSV (tabla de zonas)
- **Transforma** aplicando limpieza, imputación, reglas de negocio y feature engineering
- **Valida** la calidad del dato en cada etapa con checks automatizados
- **Carga** el resultado procesado tanto en formato Parquet local como en una base de datos PostgreSQL

**Resultado:** De ~3,000,000 registros iniciales se obtienen ~2,900,000 registros limpios y enriquecidos, listos para análisis.

---

## 🏗️ Arquitectura del Pipeline

```
Parquet (raw)           CSV (zonas)
      │                      │
      └──────────┬───────────┘
                 ▼
          [ EXTRACT ]
                 │
                 ▼
     [ estandarizar_columnas ]
     [ validar_esquema       ]
     [ limpiar_texto         ]
     [ convertir_tipos       ]
     [ filtrar_nulos_criticos]
     [ imputar_nulos         ]
     [ filtrar_invalidos     ]
     [ remover_duplicados    ]
     [ enriquecer_zonas  ]  ← merge con tabla de dimensiones
     [ calcular_features     ]
     [ validar_resultado     ]  ← 9 checks de calidad final
                 │
        ┌────────┴────────┐
        ▼                 ▼
   Parquet (local)   PostgreSQL
```

---

## 📁 Estructura del Proyecto

```
nyc-taxi-etl/
├── src/
│   ├── extract.py        # Carga de Parquet y CSV
│   ├── transform.py      # Todas las transformaciones del pipeline
│   └── load.py           # Carga a Parquet y PostgreSQL
├── data/
│   ├── raw/              # Datos originales (no versionados)
│   └── processed/        # Datos procesados (no versionados)
├── config.py             # Constantes, rutas y configuración de BD
├── main.py               # Punto de entrada — orquesta el pipeline
├── etl_taxi.log          # Log generado al correr el pipeline
├── .env.example          # Plantilla de variables de entorno
├── requirements.txt      # Dependencias del proyecto
└── .gitignore
```

---

## ⚙️ Tecnologías

| Herramienta | Uso |
|---|---|
| Python 3.13 | Lenguaje principal |
| Pandas | Transformación y limpieza de datos |
| PyArrow | Lectura/escritura de archivos Parquet |
| SQLAlchemy | Conexión y carga a PostgreSQL |
| Psycopg2 | Driver PostgreSQL |
| Python-dotenv | Manejo de variables de entorno |
| PostgreSQL 18 | Base de datos destino |

---

## 🚀 Cómo ejecutarlo

### 1. Clonar el repositorio

```bash
git clone https://github.com/rodrigo-sanchez-data/nyc-taxi-etl.git
cd nyc-taxi-etl
```

### 2. Crear entorno virtual e instalar dependencias

```bash
python -m venv venv
venv\Scripts\activate        # Windows
pip install -r requirements.txt
```

### 3. Configurar variables de entorno

Copiá el archivo de ejemplo y completá con tus credenciales:

```bash
copy .env.example .env
```

Contenido del `.env`:

```
DB_USER=tu_usuario
DB_PASSWORD=tu_contraseña
DB_HOST=localhost
DB_PORT=5432
DB_NAME=taxi_nyc
```

### 4. Descargar los datos

- **Trips:** [NYC TLC Trip Record Data — Enero 2023](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) → `yellow_tripdata_2023-01.parquet` → copiarlo en `data/raw/`
- **Zonas:** [Taxi Zone Lookup Table](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv) → `taxi_zone_lookup.csv` → copiarlo en `data/raw/`

### 5. Correr el pipeline

```bash
python main.py
```

---

## 🔄 Etapas del Pipeline

### Extract
Carga el archivo Parquet con PyArrow y el CSV de zonas con pandas. Valida que el archivo sea del formato correcto antes de procesar.

### Transform
El pipeline de transformación aplica las siguientes capas en orden:

| Etapa | Descripción |
|---|---|
| `estandarizar_columnas` | Normaliza nombres a snake_case |
| `validar_esquema` | Verifica que existan todas las columnas requeridas |
| `limpiar_texto` | Elimina espacios en columnas string |
| `convertir_tipos` | Convierte fechas, categorías e Int64 nullable |
| `filtrar_nulos_criticos` | Elimina filas con nulos en campos clave y fechas invertidas |
| `imputar_nulos` | Imputa campos recuperables con criterio de negocio |
| `filtrar_registros_invalidos` | Aplica reglas de negocio (pasajeros, tarifa, distancia) |
| `remover_duplicados` | Deduplica por combinación de campos clave |
| `enriquecer_zonas` | Merge con tabla de dimensiones de zonas geográficas |
| `calcular_features` | Crea `trip_duration_min`, `speed_mph`, `pickup_hour`, `pickup_day_name` |
| `validar_resultado` | 9 checks de calidad final antes de cargar |

### Load
Guarda el DataFrame procesado en Parquet local (compresión Snappy) y carga a PostgreSQL usando `to_sql` con `chunksize=10_000` para datasets grandes.

---

## 📊 Resultados

| Métrica | Valor |
|---|---|
| Registros iniciales | ~3,000,000 |
| Registros finales | ~2,900,000 |
| Reducción total | ~3% |
| Checks de calidad | 9 / 9 OK |
| Destinos de carga | Parquet + PostgreSQL |

---

## 📝 Notas

- Los archivos de datos (`data/`) no están versionados por su tamaño. Descargalos desde los links de la sección anterior.
- El archivo `.env` no está versionado. Usá `.env.example` como plantilla.
- El log completo de cada ejecución se guarda en `etl_taxi.log`.