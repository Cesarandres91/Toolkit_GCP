from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.utils.dates import days_ago
from datetime import timedelta

# Definir los argumentos por defecto del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(hours=11),  # Reintento después de 11 horas
}

# Definir el DAG
dag = DAG(
    'update_bigquery_t2_from_t1',
    default_args=default_args,
    description='Actualiza la tabla t2 en el proyecto pf con datos de t1 del proyecto pi, reintenta después de 11 horas si falla',
    schedule_interval='@daily',  # Ajusta esto según tus necesidades
    catchup=False,  # Evita ejecuciones atrasadas
)

# Definir la consulta SQL
update_query = """
INSERT INTO `pf.t2` (column1, column2, column3)  -- Reemplaza con las columnas reales de tu tabla
SELECT column1, column2, column3  -- Reemplaza con las columnas reales que quieres seleccionar
FROM `pi.t1`
WHERE date = '10-09-2024'
"""

# Definir la tarea para ejecutar la consulta
update_t2_task = BigQueryExecuteQueryOperator(
    task_id='update_t2_from_t1',
    sql=update_query,
    use_legacy_sql=False,
    location='US',  # Reemplaza con tu región de BigQuery
    dag=dag,
    retries=1,  # Número de reintentos
    retry_delay=timedelta(hours=11),  # Reintento después de 11 horas
)

# Establecer el orden de las tareas
update_t2_task

# Si necesitas más tareas, puedes añadirlas aquí y establecer sus dependencias
