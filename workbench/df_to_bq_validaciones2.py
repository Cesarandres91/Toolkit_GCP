from google.cloud import bigquery
from google.cloud.exceptions import NotFound
import pandas as pd

# Parámetros
project_id = 'your-project-id'  # ID de tu proyecto de GCP
dataset_id = 'your-dataset-id'  # Dataset en BigQuery
table_id = 'your_table_name'  # Nombre de la tabla que quieres crear
full_table_id = f"{project_id}.{dataset_id}.{table_id}"

# Inicializa el cliente de BigQuery
client = bigquery.Client()

def check_table_exists(full_table_id):
    """Verifica si la tabla ya existe en BigQuery."""
    try:
        client.get_table(full_table_id)
        print(f"La tabla {full_table_id} ya existe.")
        return True
    except NotFound:
        print(f"La tabla {full_table_id} no existe, se procederá a crearla.")
        return False

def upload_dataframe_to_bigquery(df, full_table_id, project_id, append_data=False):
    """
    Sube un DataFrame a una tabla en BigQuery.
    Si append_data=True, añade datos a la tabla existente en lugar de reemplazarla.
    """
    if not isinstance(df, pd.DataFrame):
        raise ValueError("El argumento 'df' debe ser un DataFrame de pandas.")

    if df.empty:
        print("El DataFrame está vacío. No se realizará ninguna acción.")
        return

    job_config = bigquery.LoadJobConfig()
    job_config.autodetect = True  # Autodetectar el esquema

    if check_table_exists(full_table_id):
        if append_data:
            job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
            print(f"Añadiendo datos a la tabla existente {full_table_id}.")
        else:
            print(f"La tabla {full_table_id} ya existe y append_data=False. No se ha modificado.")
            return
    else:
        job_config.write_disposition = bigquery.WriteDisposition.WRITE_EMPTY
        print(f"Creando nueva tabla {full_table_id}.")

    try:
        job = client.load_table_from_dataframe(
            df, full_table_id, job_config=job_config
        )
        job.result()  # Espera a que el job termine
        print(f"Cargados {job.output_rows} registros en {full_table_id}")
    except Exception as e:
        print(f"Error al cargar datos en BigQuery: {str(e)}")

# Ejemplo de uso
if __name__ == "__main__":
    # Asegúrate de tener un DataFrame llamado 'df' antes de llamar a esta función
    # df = pd.DataFrame(...)  # Tu DataFrame aquí
    
    # Llamada a la función para subir el DataFrame
    upload_dataframe_to_bigquery(df, full_table_id, project_id, append_data=True)
