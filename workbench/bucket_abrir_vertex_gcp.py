from google.cloud import storage
import pandas as pd
from io import BytesIO

# Define variables
bucket_name = 'your-bucket-name'  # Nombre del bucket
file_path = 'path/to/your/file.csv'  # Ruta del archivo dentro del bucket

def download_csv_from_gcs(bucket_name, file_path):
    """Descarga un archivo .csv desde Google Cloud Storage."""
    # Inicializa el cliente de Google Cloud Storage
    storage_client = storage.Client()

    # Obtén el bucket
    bucket = storage_client.get_bucket(bucket_name)

    # Obtén el blob (archivo) desde el bucket
    blob = bucket.blob(file_path)

    # Descarga el contenido del blob en la memoria
    content = blob.download_as_bytes()

    # Carga el archivo CSV en un DataFrame de pandas
    csv_data = pd.read_csv(BytesIO(content))

    return csv_data

# Llama a la función para obtener el DataFrame
df = download_csv_from_gcs(bucket_name, file_path)

# Procesa el DataFrame (ejemplo: mostrar las primeras filas)
print(df.head())
