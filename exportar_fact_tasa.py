import pandas as pd
import urllib
from sqlalchemy import create_engine
import os

def exportar_tabla():
    print("Iniciando exportación autónoma...")
    
    # Parámetros de conexión (basados en conexion.py)
    servidor = 'LCP-PAG-PRACTIC'
    base = 'ACP_DataWarehose_Proyecciones'
    driver = 'ODBC Driver 17 for SQL Server'
    
    cadena_pyodbc = (
        f'DRIVER={{{driver}}};'
        f'SERVER={servidor};'
        f'DATABASE={base};'
        f'Trusted_Connection=yes;'
        f'TrustServerCertificate=yes;'
    )
    
    cadena_url = (
        'mssql+pyodbc:///?odbc_connect='
        + urllib.parse.quote_plus(cadena_pyodbc)
    )
    
    try:
        print(f"Conectando a {servidor} / {base}...")
        engine = create_engine(cadena_url, fast_executemany=True)
        
        tabla = "Silver.Fact_Tasa_Crecimiento_Brotes"
        archivo_salida = "Fact_Tasa_Crecimiento_Export.csv"
        
        print(f"Leyendo datos de {tabla} (esto puede tomar un minuto)...")
        query = f"SELECT * FROM {tabla}"
        df = pd.read_sql(query, engine)
        
        print(f"Exportando {len(df)} filas a {archivo_salida}...")
        df.to_csv(archivo_salida, index=False, sep=',', encoding='utf-8-sig')
        
        print(f"\n¡EXITO! Archivo generado: {os.path.abspath(archivo_salida)}")
        
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    exportar_tabla()
