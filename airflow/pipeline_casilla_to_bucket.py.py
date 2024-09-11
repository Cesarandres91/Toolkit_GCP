from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
import pysftp
from google.cloud import storage
import os

# Parámetros SFTP
sftp_host = 'sftp.your-server.com'
sftp_username = 'your-username'
sftp_password = 'your-password'
remote_file_path = '/path/to/remote/file.csv'
local_file_path = 'file.csv'

# Parámetros Google Cloud Storage
bucket_name = 'your-bucket-name'
gcs_destination_path = 'path/in/bucket/file.csv'

# Funciones ETL
def extract_csv_from_sftp():
    """Extrae un archivo .csv desde un servidor SFTP y lo guarda localmente."""
    with pysftp.Connection(host=sftp_host, username=sftp_username, password=sftp_password) as sftp:
        print("Conexión SFTP exitosa.")
        sftp.get(remote_file_path, local_file_path)
        print(f"Archivo {remote_file_path} descargado exitosamente.")

def upload_to_gcs():
    """Sube el archivo local a un bucket de Google Cloud Storage."""
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)
    blob = bucket.blob(gcs_destination_path)
    blob.upload_from_filename(local_file_path)
    print(f"Archivo {local_file_path} subido exitosamente a gs://{bucket_name}/{gcs_destination_path}")

def clean_up_local_file():
    """Elimina el archivo local después de haber sido subido a GCS."""
    if os.path.exists(local_file_path):
        os.remove(local_file_path)
        print(f"Archivo local {local_file_path} eliminado.")

# Definir el DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1
}

with DAG(
    'etl_sftp_to_gcs',
    default_args=default_args,
    description='Pipeline ETL desde SFTP a GCS',
    schedule_interval='@daily',  # Programar para ejecución diaria
    start_date=days_ago(1),
    catchup=False
) as dag:

    # Tarea 1: Extraer archivo desde SFTP
    extract_task = PythonOperator(
        task_id='extract_csv_from_sftp',
        python_callable=extract_csv_from_sftp
    )

    # Tarea 2: Subir archivo a GCS
    upload_task = PythonOperator(
        task_id='upload_to_gcs',
        python_callable=upload_to_gcs
    )

    # Tarea 3: Limpiar archivo local
    cleanup_task = PythonOperator(
        task_id='clean_up_local_file',
        python_callable=clean_up_local_file
    )

    # Definir el orden de ejecución del ETL
    extract_task >> upload_task >> cleanup_task
