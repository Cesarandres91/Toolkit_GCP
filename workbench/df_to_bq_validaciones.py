from google.cloud import bigquery
from google.cloud.exceptions import NotFound

# Parámetros
project_id = 'your-project-id'  # ID de tu proyecto de GCP
dataset_id = 'your-dataset-id'  # Dataset en BigQuery
table_id = 'your_table_name'    # Nombre de la tabla que quieres crear
full_table_id = f"{project_id}.{dataset_id}.{table_id}"

# Inicializa el cliente de BigQuery
client = bigquery.Client()

# Verificar si la tabla ya existe
def check_table_exists(full_table_id):
    try:
        client.get_table(full_table_id)  # Intenta obtener la tabla
        print(f"La tabla {full_table_id} ya existe.")
        return True
    except NotFound:
        print(f"La tabla {full_table_id} no existe, se procederá a crearla.")
        return False

# Subir el DataFrame a BigQuery
def upload_dataframe_to_bigquery(df, full_table_id, project_id, append_data=False):
    """Sube un DataFrame a una tabla en BigQuery.
       Si append_data=True, añade datos a la tabla existente en lugar de reemplazarla.
    """
    # Verificar si la tabla existe
    if not check_table_exists(full_table_id):
        # Si no existe, se crea la tabla y se sube el DataFrame
        df.to_gbq(destination_table=full_table_id, 
                  project_id=project_id, 
                  if_exists='fail')  # 'fail' asegura que no sobrescriba una tabla existente
        print(f"Tabla {full_table_id} creada exitosamente.")
    else:
        if append_data:
            # Añadir datos a la tabla existente
            df.to_gbq(destination_table=full_table_id, 
                      project_id=project_id, 
                      if_exists='append')  # Añade datos a la tabla existente
            print(f"Datos añadidos a la tabla {full_table_id} exitosamente.")
        else:
            print(f"Ya existe una tabla con el nombre {full_table_id}. No se ha modificado.")

# Llamada a la función para subir el DataFrame, aquí `append_data=True` para añadir data
upload_dataframe_to_bigquery(df, full_table_id, project_id, append_data=True)
