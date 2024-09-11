import pandas as pd
import json
from pandas.io.json import json_normalize
import numpy as np

def flatten_json(nested_json, prefix=''):
    flattened = {}
    for key, value in nested_json.items():
        new_key = f"{prefix}{key}"
        if isinstance(value, dict):
            flattened.update(flatten_json(value, f"{new_key}_"))
        elif isinstance(value, list):
            for i, item in enumerate(value):
                if isinstance(item, dict):
                    flattened.update(flatten_json(item, f"{new_key}_{i}_"))
                else:
                    flattened[f"{new_key}_{i}"] = item
        else:
            flattened[new_key] = value
    return flattened

def unpack_json_column(df, column_name):
    # Convertir la columna JSON a diccionario
    df[column_name] = df[column_name].apply(lambda x: json.loads(x) if isinstance(x, str) else x)
    
    # Aplicar flatten_json a cada fila
    flattened_data = df[column_name].apply(flatten_json).apply(pd.Series)
    
    # Combinar el DataFrame original con los datos aplanados
    result = pd.concat([df.drop(columns=[column_name]), flattened_data], axis=1)
    
    return result

def process_dataframe(df, json_column):
    # Hacer una copia del DataFrame original
    df_copy = df.copy()
    
    # Desempaquetar la columna JSON
    df_unpacked = unpack_json_column(df_copy, json_column)
    
    # Eliminar columnas con todos los valores nulos
    df_unpacked = df_unpacked.dropna(axis=1, how='all')
    
    # Reemplazar los valores 'null' (como string) por None
    df_unpacked = df_unpacked.replace(['null', 'NULL', 'Null', ''], np.nan)
    
    # Intentar convertir columnas a tipos de datos apropiados
    for col in df_unpacked.columns:
        try:
            df_unpacked[col] = pd.to_numeric(df_unpacked[col])
        except:
            try:
                df_unpacked[col] = pd.to_datetime(df_unpacked[col])
            except:
                pass  # Mantener como objeto si no se puede convertir
    
    return df_unpacked

# Función principal
def main():
    # Cargar el DataFrame
    # Asegúrate de reemplazar 'tu_archivo.csv' con la ruta correcta a tu archivo
    df = pd.read_csv('tu_archivo.csv')
    
    # Nombre de la columna que contiene el JSON
    json_column = 'GGG'  # Reemplaza esto con el nombre real de tu columna JSON
    
    # Procesar el DataFrame
    df_processed = process_dataframe(df, json_column)
    
    # Mostrar información sobre el DataFrame procesado
    print("Información del DataFrame procesado:")
    print(df_processed.info())
    
    # Mostrar las primeras filas del DataFrame procesado
    print("\nPrimeras filas del DataFrame procesado:")
    print(df_processed.head())
    
    # Guardar el DataFrame procesado en un nuevo archivo CSV
    output_file = 'df_procesado.csv'
    df_processed.to_csv(output_file, index=False)
    print(f"\nDataFrame procesado guardado en: {output_file}")

if __name__ == "__main__":
    main()
