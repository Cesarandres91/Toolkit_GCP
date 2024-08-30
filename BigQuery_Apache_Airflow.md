# Automatización de Consultas en BigQuery con Apache Airflow

Este tutorial te guiará para automatizar la ejecución diaria de una consulta en BigQuery que actualiza una tabla añadiendo registros desde otra tabla, utilizando Apache Airflow.

## 1. Instalación de Apache Airflow

Primero, debes instalar Airflow en tu entorno. Esto se puede hacer de varias maneras, pero la más común es usar Docker o instalarlo directamente en un servidor o en tu máquina local.

### Usando Docker:

```bash
docker-compose -f https://raw.githubusercontent.com/apache/airflow/main/docs/apache-airflow/start/docker-compose.yaml up -d
```

### Instalación Local:

```bash
pip install apache-airflow
```

## 2. Configuración de Airflow

Una vez instalado, debes configurar Airflow para que se conecte a tu proyecto de Google Cloud y BigQuery.

### Configura las Credenciales de Google Cloud en Airflow:

1. Crea un archivo JSON con las credenciales de tu cuenta de servicio de Google Cloud.
2. Guarda el archivo en una ubicación segura y define la variable de entorno `GOOGLE_APPLICATION_CREDENTIALS` en tu máquina o servidor donde se ejecuta Airflow:

```bash
export GOOGLE_APPLICATION_CREDENTIALS="/path/to/your/credentials.json"
```

### Configura una Conexión en Airflow:

1. Ve a la interfaz web de Airflow (normalmente en `http://localhost:8080`).
2. Navega a `Admin > Connections` y crea una nueva conexión:
   - **Conn Id**: `google_cloud_default`
   - **Conn Type**: `Google Cloud`
   - **Project Id**: `tu-proyecto-id`
   - **Keyfile Path**: `/path/to/your/credentials.json`
   - **Scopes**: `https://www.googleapis.com/auth/cloud-platform`

## 3. Crear el DAG en Airflow

Crea un archivo Python en el directorio de DAGs de Airflow (normalmente en `/dags` o el que hayas configurado).

Un ejemplo de DAG que ejecuta una consulta en BigQuery todos los días a las 10:00 AM en la hora local de Chile se vería así:

```python
from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago
from pendulum import timezone

# Define la zona horaria de Chile
local_tz = timezone('America/Santiago')

# Define el DAG
dag = DAG(
    'actualiza_tabla_diaria',
    schedule_interval='0 10 * * *',  # 10:00 AM Chile (hora local)
    start_date=days_ago(1),
    catchup=False,
    default_args={'timezone': local_tz},  # Especifica la zona horaria
)

# Define la tarea para ejecutar la consulta en BigQuery
actualiza_tabla = BigQueryExecuteQueryOperator(
    task_id='actualiza_tabla',
    sql=\"\"\"
        INSERT INTO tu_tabla_destino (columna1, columna2, ...)
        SELECT columna1, columna2, ...
        FROM tu_tabla_origen
        WHERE fecha > (SELECT MAX(fecha) FROM tu_tabla_destino)
    \"\"\",
    use_legacy_sql=False,
    dag=dag
)

# Define el flujo del DAG
actualiza_tabla
```

## 4. Verificar y Ejecutar

Verifica que el DAG esté activo en la interfaz web de Airflow. Airflow ejecutará este DAG todos los días a la hora programada. Puedes monitorear las ejecuciones y ver los registros de cada tarea desde la misma interfaz web.

## 5. Despliegue Opcional en Google Cloud Composer

Si deseas llevar tu flujo de trabajo a la nube, puedes usar Google Cloud Composer, que es una versión gestionada de Apache Airflow en Google Cloud. Simplemente sube tu DAG a un bucket de Google Cloud y configura Cloud Composer para que lo ejecute.

## Resumen

1. **Instalar Apache Airflow**: Usa Docker o pip.
2. **Configurar Airflow**: Conectar a Google Cloud y BigQuery.
3. **Crear un DAG**: Define el flujo de trabajo que ejecutará la consulta.
4. **Verificar la Ejecución**: Asegúrate de que el DAG esté activo y funcionando correctamente.
