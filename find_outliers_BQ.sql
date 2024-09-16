from google.cloud import bigquery
import pandas as pd
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
from typing import List, Dict
import logging

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_bigquery_data(query: str) -> pd.DataFrame:
    """
    Ejecuta una consulta en BigQuery y devuelve los resultados como un DataFrame.
    """
    client = bigquery.Client()
    try:
        df = client.query(query).to_dataframe()
        logging.info(f"Datos extraídos de BigQuery. Shape: {df.shape}")
        return df
    except Exception as e:
        logging.error(f"Error al extraer datos de BigQuery: {str(e)}")
        raise

def preprocess_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Preprocesa los datos para el análisis de anomalías.
    """
    df['fecha'] = pd.to_datetime(df['fecha'])
    df['mes'] = df['fecha'].dt.to_period('M')
    df_pivot = df.pivot_table(values='cantidad', index=['cliente_id', 'mes'], 
                              columns='producto', aggfunc='sum').reset_index()
    df_pivot = df_pivot.sort_values(['cliente_id', 'mes'])
    logging.info(f"Datos preprocesados. Shape: {df_pivot.shape}")
    return df_pivot

def detect_anomalies(group: pd.Series, contamination: float = 0.1) -> pd.Series:
    """
    Detecta anomalías en una serie de datos usando Isolation Forest.
    """
    if len(group) < 3:
        return pd.Series([False] * len(group))
    
    scaler = StandardScaler()
    X = scaler.fit_transform(group.values.reshape(-1, 1))
    
    clf = IsolationForest(contamination=contamination, random_state=42, n_jobs=-1)
    anomalies = clf.fit_predict(X)
    return pd.Series(anomalies == -1, index=group.index)

def process_client_data(cliente_data: pd.DataFrame, productos: List[str]) -> List[Dict]:
    """
    Procesa los datos de un cliente y detecta anomalías para cada producto.
    """
    anomaly_results = []
    for producto in productos:
        if producto != 'cliente_id':
            serie_producto = cliente_data[producto].dropna()
            if not serie_producto.empty:
                anomalies = detect_anomalies(serie_producto)
                anomaly_results.extend([
                    {
                        'cliente_id': cliente_data['cliente_id'].iloc[0],
                        'producto': producto,
                        'mes': mes,
                        'cantidad': cantidad,
                        'es_anomalia': es_anomalia
                    }
                    for mes, cantidad, es_anomalia in zip(serie_producto.index, serie_producto, anomalies)
                    if es_anomalia
                ])
    return anomaly_results

def main():
    # Consulta SQL para extraer datos
    query = """
    SELECT cliente_id, fecha, producto, cantidad
    FROM `tu_proyecto.tu_dataset.tu_tabla`
    ORDER BY cliente_id, fecha
    """

    try:
        # Extraer y preprocesar datos
        df = get_bigquery_data(query)
        df_pivot = preprocess_data(df)

        # Obtener lista de productos
        productos = [col for col in df_pivot.columns if col not in ['cliente_id', 'mes']]

        # Detectar anomalías
        anomaly_results = []
        for _, group in df_pivot.groupby('cliente_id'):
            anomaly_results.extend(process_client_data(group, productos))

        # Crear DataFrame con los resultados
        anomalies_df = pd.DataFrame(anomaly_results)
        
        logging.info(f"Anomalías detectadas. Shape: {anomalies_df.shape}")
        print(anomalies_df)

        # Opcional: Guardar resultados en BigQuery
        # anomalies_df.to_gbq('tu_proyecto.tu_dataset.anomalias_detectadas', if_exists='replace')

    except Exception as e:
        logging.error(f"Error en el proceso principal: {str(e)}")

if __name__ == "__main__":
    main()
