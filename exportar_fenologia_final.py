import pandas as pd
import urllib
from sqlalchemy import create_engine
import os

def exportar_conteo_fenologico():
    print("Iniciando exportacion de Conteo Fenologico...")
    servidor = 'LCP-PAG-PRACTIC'
    base = 'ACP_DataWarehose_Proyecciones'
    driver = 'ODBC Driver 17 for SQL Server'
    cadena_pyodbc = f'DRIVER={{{driver}}};SERVER={servidor};DATABASE={base};Trusted_Connection=yes;TrustServerCertificate=yes;'
    u = 'mssql+pyodbc:///?odbc_connect=' + urllib.parse.quote_plus(cadena_pyodbc)
    
    engine = create_engine(u)
    tabla = "Silver.Fact_Conteo_Fenologico"
    archivo = "Fact_Conteo_Fenologico.csv"
    
    try:
        print(f"Leyendo {tabla}...")
        df = pd.read_sql(f"SELECT * FROM {tabla}", engine)
        if not df.empty:
            df.to_csv(archivo, index=False, sep=',', encoding='utf-8-sig')
            print(f"EXITO: Exportado a {archivo} ({len(df)} filas)")
        else:
            print(f"La tabla {tabla} esta VACIA.")
    except Exception as e:
        print(f"ERROR: {e}")

if __name__ == "__main__":
    exportar_conteo_fenologico()
