# Análisis de Datos Agroindustriales en Argentina

Este repositorio contiene scripts y recursos utilizados en el análisis de datos agroindustriales en Argentina, desarrollado en el contexto de la carrera de Ciencias de Datos en la Universidad Tecnológica Nacional Facultad Regional Córdoba (UTN FRC).

## Contenidos

- **Scripts en Python**: Procesos ETL para la extracción, transformación y carga de datos agroindustriales.
- **Normalización de Datos**: Uso de la API GeoRef para estandarizar datos territoriales (provincias, departamentos, etc.).
- **Integración de Datos Climáticos**: Uso de la API Open Meteo para añadir datos climáticos (temperaturas, precipitaciones, etc.).

## APIs Utilizadas

- **GeoRef**: Normalización de unidades territoriales en Argentina.
- **Open Meteo**: Datos climáticos históricos y pronósticos.

## Estructura del Proyecto

- `data/`: Datos crudos y procesados.
- `scripts/`: Scripts de Python para el análisis y procesamiento de datos.
- `notebooks/`: Jupyter Notebooks con análisis exploratorios y visualizaciones.

## Cómo Empezar

1. Clonar el repositorio:
    ```bash
    git clone https://github.com/duiliomamani/cdd-agroindustria.git
    ```
2. Instalar las dependencias:
    ```bash
    pip install -r requirements.txt
    ```
3. Ejecutar los scripts en `scripts/` para procesar los datos.

## Contribuciones

Las contribuciones son bienvenidas. Por favor, abre un issue o envía un pull request.

## Licencia

Este proyecto está bajo la Licencia MIT.

