import pandas as pd
import json
from typing import Dict, Any

def flatten_json(obj: Dict[str, Any], prefix: str = '') -> Dict[str, Any]:
    """
    Aplana un objeto JSON anidado.
    """
    result = {}
    for key, value in obj.items():
        new_key = f"{prefix}.{key}" if prefix else key
        if isinstance(value, dict):
            result.update(flatten_json(value, new_key))
        else:
            result[new_key] = value
    return result

def process_json_dataframe(df: pd.DataFrame, json_column: str) -> pd.DataFrame:
    """
    Procesa un DataFrame que contiene JSONs en una columna específica.
    
    :param df: DataFrame de entrada
    :param json_column: Nombre de la columna que contiene los JSONs
    :return: Nuevo DataFrame con JSONs aplanados
    """
    flattened_data = []
    
    for _, row in df.iterrows():
        try:
            json_obj = json.loads(row[json_column])
            flat_obj = flatten_json(json_obj)
            flattened_data.append(flat_obj)
        except json.JSONDecodeError as e:
            print(f"Error al procesar JSON en la fila {_}: {e}")
            flattened_data.append({})  # Añadir un diccionario vacío para mantener el índice
        except Exception as e:
            print(f"Error inesperado en la fila {_}: {e}")
            flattened_data.append({})  # Añadir un diccionario vacío para mantener el índice
    
    # Crear el nuevo DataFrame con los datos aplanados
    result_df = pd.DataFrame(flattened_data)
    
    # Añadir las columnas originales que no sean la columna JSON
    for col in df.columns:
        if col != json_column:
            result_df[col] = df[col]
    
    return result_df

# Ejemplo de uso
if __name__ == "__main__":
    # Supongamos que tienes un DataFrame llamado 'df' con una columna 'json_data'
    # df = pd.read_csv('tu_archivo.csv')
    
    # Procesar el DataFrame
    result_df = process_json_dataframe(df, 'json_data')
  #  result_df = process_json_dataframe(df, 'GGG')
    
    print("Proceso completado. Nuevo DataFrame creado.")
    print(result_df.head())  # Muestra las primeras filas del nuevo DataFrame
