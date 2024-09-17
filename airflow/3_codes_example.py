from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 16),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'sql_insert_and_ml_analysis',
    default_args=default_args,
    description='A DAG to insert data into BigQuery and perform ML analysis',
    schedule_interval='0 15 * * 1-5',  # Run at 3 PM UTC, Monday to Friday
    catchup=False
)

# SQL tasks
insert_t1 = BigQueryExecuteQueryOperator(
    task_id='insert_t1',
    sql="""
    INSERT INTO `project.dataset.t1` (column1, column2, ...)
    SELECT ...
    FROM ...
    WHERE ...
    """,
    use_legacy_sql=False,
    dag=dag
)

insert_t2 = BigQueryExecuteQueryOperator(
    task_id='insert_t2',
    sql="""
    INSERT INTO `project.dataset.t2` (column1, column2, ...)
    SELECT ...
    FROM ...
    WHERE ...
    """,
    use_legacy_sql=False,
    dag=dag
)

# Python functions for ML analysis
def ml_analysis_1():
    # Your ML analysis code here
    print("Performing ML analysis 1")

def ml_analysis_2():
    # Your ML analysis code here
    print("Performing ML analysis 2")

# Python tasks
ml_task_1 = PythonOperator(
    task_id='ml_analysis_1',
    python_callable=ml_analysis_1,
    dag=dag
)

ml_task_2 = PythonOperator(
    task_id='ml_analysis_2',
    python_callable=ml_analysis_2,
    dag=dag
)

# Set task dependencies
insert_t1 >> insert_t2 >> [ml_task_1, ml_task_2]
