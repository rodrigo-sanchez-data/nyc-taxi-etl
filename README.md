# NYC Yellow Taxi — Pipeline ETL

Pipeline ETL construido con Python sobre el dataset público de taxis amarillos de Nueva York (enero 2023). Procesa 3 millones de registros aplicando limpieza, validación de calidad, enriquecimiento geográfico y carga a PostgreSQL.

Construido como proyecto de portafolio en mi transición hacia Data Engineering.

---

## ¿Qué hace este proyecto?

Toma el archivo Parquet crudo que publica mensualmente la TLC de Nueva York y lo convierte en un dataset limpio y analizable. El pipeline recorre estas etapas en orden:

- Estandariza nombres de columnas y tipos de datos
- Elimina registros con campos críticos nulos, fechas invertidas y valores fuera de rango
- Imputa campos recuperables con criterio de negocio
- Deduplica por combinación de campos clave
- Enriquece cada viaje con el nombre del barrio y zona de pickup y dropoff
- Calcula features derivadas: duración del viaje, velocidad promedio, hora y día de la semana
- Valida 16 checks de calidad antes de cargar el resultado

**Resultado:** 3,066,766 registros iniciales → 2,940,315 registros limpios (reducción del 4.12%)

---

## Arquitectura

```
Parquet (raw)              CSV (zonas)
      │                         │
      └──────────┬──────────────┘
                 ▼
           [ Extract ]
                 │
                 ▼
    [ estandarizar_columnas   ]
    [ validar_esquema         ]
    [ limpiar_texto           ]
    [ convertir_tipos         ]
    [ filtrar_nulos_criticos  ]
    [ filtrar_fechas_invalidas]
    [ imputar_nulos           ]
    [ filtrar_invalidos       ]
    [ remover_duplicados      ]
    [ enriquecer_zonas        ]  ← left join con tabla de zonas TLC
    [ calcular_features       ]
    [ validar_resultado       ]  ← 16 checks de calidad final
                 │
        ┌────────┴────────┐
        ▼                 ▼
  Parquet (local)    PostgreSQL
```

Cada etapa es una función pura que recibe y retorna un DataFrame. El encadenamiento usa `.pipe()` de pandas para mantener el flujo legible.

---

## Estructura del proyecto

```
nyc-taxi-etl/
├── src/
│   ├── extract.py        # Carga de Parquet y CSV
│   ├── transform.py      # Transformaciones del pipeline
│   └── load.py           # Carga a Parquet y PostgreSQL
├── data/
│   ├── raw/              # Datos originales (no versionados)
│   └── processed/        # Resultado procesado (no versionado)
├── config.py             # Constantes, rutas y configuración de BD
├── main.py               # Orquestador del pipeline
├── .env.example          # Plantilla de variables de entorno
├── requirements.txt
└── .gitignore
```

> `etl_taxi.log` se genera automáticamente al ejecutar el pipeline.

---

## Tecnologías

| Herramienta | Versión | Uso |
|---|---|---|
| Python | 3.13 | Lenguaje principal |
| Pandas | 2.x | Transformación y limpieza |
| PyArrow | — | Lectura/escritura Parquet |
| SQLAlchemy | 2.x | Conexión a PostgreSQL |
| Psycopg2 | — | Driver PostgreSQL |
| Python-dotenv | — | Variables de entorno |
| PostgreSQL | 18.3 | Base de datos destino |

---

## Cómo ejecutarlo

### 1. Clonar el repositorio

```bash
git clone https://github.com/rodrigo-sanchez-data/nyc-taxi-etl.git
cd nyc-taxi-etl
```

### 2. Crear entorno virtual e instalar dependencias

```bash
python -m venv venv
source venv/bin/activate     # Mac/Linux
venv\Scripts\activate        # Windows CMD
pip install -r requirements.txt
```

### 3. Configurar variables de entorno (solo si usas PostgreSQL)

Si quieres cargar los datos a PostgreSQL, copia el archivo de ejemplo:

```bash
cp .env.example .env        # Mac/Linux
copy .env.example .env      # Windows CMD
```

Completar `.env` con las credenciales de tu base de datos:

```
DB_USER=tu_usuario
DB_PASSWORD=tu_contraseña
DB_HOST=localhost
DB_PORT=5432
DB_NAME=taxi_nyc
```
> Si solo quieres ejecutar el pipeline y obtener el Parquet local,
> puedes saltarte este paso.

### 4. Descargar los datos

Los archivos de datos no están en el repositorio por su tamaño. Hay que descargarlos manualmente:

- **Viajes:** [NYC TLC — Enero 2023](https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page) → guardar como `data/raw/yellow_tripdata_2023-01.parquet`
- **Zonas:** [Taxi Zone Lookup](https://d37ci6vzurychx.cloudfront.net/misc/taxi_zone_lookup.csv) → guardar como `data/raw/taxi_zone_lookup.csv`

### 5. Ejecutar

```bash
python main.py
```

Por defecto el pipeline procesa los datos y guarda el resultado 
en `data/processed/clean_taxi.parquet`.

Para cargar también a PostgreSQL, asegúrate de haber configurado
el `.env` y cambia la última línea de `main.py`:

```python
# Por defecto — solo Parquet local
main(cargar_postgres=False)

# Con carga a PostgreSQL
main(cargar_postgres=True)
```

El progreso se imprime en consola y el log completo se guarda 
en `etl_taxi.log`.

## Decisiones técnicas que tomé

Estas son algunas decisiones no obvias que encontré mientras construía el pipeline y que me obligaron a entender bien los datos:

**`how='left'` en el merge de zonas**
El dataset de TLC incluye los IDs 264 y 265 que representan zonas sin nombre ("Unknown"). Un `inner join` los eliminaba silenciosamente — perdía miles de viajes válidos sin ningún aviso. Cambié a `left join` y agregué imputación previa en el CSV de zonas para que esos registros lleguen con el valor `'Unknown'` en lugar de `NaN`.

**`get_db_conn()` como función, no como constante**
Originalmente `DB_CONN` era una constante que se construía al importar `config.py`. Si el `.env` no estaba configurado, la cadena de conexión quedaba como `postgresql://None:None@None:None/None` sin ningún error. Moví la lógica a una función que valida las variables antes de construir la URL y lanza `EnvironmentError` con un mensaje claro si falta alguna.

**`DISTANCIA_MIN = 0.1` en lugar de `0.0`**
Un viaje de 0 km es casi siempre un error de GPS o un registro corrupto. Dejarlo en 0.0 los filtraba solo si eran exactamente cero — cualquier valor de `0.001` pasaba. Cambié el umbral a `0.1` millas como mínimo razonable para un viaje real en NYC.

**Separar `filtrar_fechas_invalidas` de `filtrar_nulos_criticos`**
La validación de fechas invertidas estaba mezclada con el filtro de nulos. Son dos responsabilidades distintas — separándolas cada función hace exactamente una cosa, el pipeline es más fácil de debuggear y puedo desactivar una sin tocar la otra.

---

## Resultados

| Métrica | Valor |
|---|---|
| Registros iniciales | 3,066,766 |
| Registros finales | 2,940,315 |
| Reducción total | 4.12% |
| Checks de calidad | 16 / 16 OK |
| Destinos de carga | Parquet local + PostgreSQL |

---

## Notas

- Los datos no están versionados. Descargarlos desde los links de la sección anterior.
- El `.env` no está versionado. Usar `.env.example` como plantilla.
- El log de cada ejecución se guarda en `etl_taxi.log`.
