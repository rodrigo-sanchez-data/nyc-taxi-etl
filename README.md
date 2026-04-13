# ETL Pipeline — NYC Yellow Taxi 2023

Pipeline ETL desarrollado en Python como proyecto de aprendizaje 
de Data Engineering. Procesa el dataset público de viajes en taxi 
amarillo de Nueva York de enero 2023.

## ¿Qué hace el pipeline?

- Lee los datos crudos desde un archivo Parquet
- Estandariza y limpia las columnas
- Filtra registros con nulos críticos, fechas inválidas y valores fuera de rango
- Imputa nulos recuperables con valores de negocio
- Elimina duplicados
- Enriquece los viajes con información de zonas geográficas de NYC
- Calcula features nuevas como duración del viaje y velocidad promedio
- Valida el resultado final antes de cargar
- Carga los datos procesados en Parquet y PostgreSQL

## Tecnologías

- Python 3.13
- Pandas
- PyArrow
- SQLAlchemy
- PostgreSQL
- python-dotenv

## Estructura del proyecto

```
nyc-taxi-etl/
├── src/
│   ├── extract.py      # Lectura de archivos Parquet y CSV
│   ├── transform.py    # Limpieza, transformaciones y features
│   └── load.py         # Carga a Parquet y PostgreSQL
├── data/
│   ├── raw/            # Datos originales (ignorado por Git)
│   └── processed/      # Datos procesados (ignorado por Git)
├── config.py           # Rutas y constantes del proyecto
├── main.py             # Punto de entrada del pipeline
├── requirements.txt    
├── .env.example        
└── README.md 
```     

## Cómo ejecutarlo

1. Clonar el repositorio
   git clone https://github.com/rodrigo-sanchez-data/nyc-taxi-etl.git

2. Instalar dependencias
   pip install -r requirements.txt

3. Configurar variables de entorno
   Copiar .env.example a .env y completar con las credenciales
   de PostgreSQL

4. Descargar el dataset y colocarlo en data/raw/
   https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

5. Ejecutar el pipeline
   python main.py

## Dataset

NYC TLC Trip Record Data — Yellow Taxi January 2023  
Fuente oficial: https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page

## Notas

Este es mi primer proyecto de Data Engineering. 
El objetivo principal fue aprender a estructurar un pipeline ETL 
modular aplicando buenas prácticas como logging, manejo de errores 
y separación de responsabilidades.