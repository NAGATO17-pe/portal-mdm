import pandas as pd
import urllib
from sqlalchemy import create_engine
import os

def exportar_maestro():
    print("Iniciando exportacion maestra para el modelo...")
    
    # Parametros de conexion
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
    
    tablas = {
        "Silver.Dim_Variedad": "Dim_Variedad.csv",
        "Silver.Dim_Geografia": "Dim_Geografia.csv",
        "Silver.Dim_Tiempo": "Dim_Tiempo.csv",
        "Silver.Fact_Telemetria_Clima": "Fact_Clima.csv"
    }
    
    try:
        engine = create_engine(cadena_url, fast_executemany=True)
        
        for tabla, archivo in tablas.items():
            print(f"Procesando {tabla} -> {archivo}...")
            try:
                df = pd.read_sql(f"SELECT * FROM {tabla}", engine)
                df.to_csv(archivo, index=False, sep=',', encoding='utf-8-sig')
                print(f"   Listo! ({len(df)} filas)")
            except Exception as e_tabla:
                print(f"   Error en {tabla}: {e_tabla}")
        
        print(f"Exportacion completada.")
        
    except Exception as e:
        print(f"ERROR CRITICO: {e}")

if __name__ == "__main__":
    exportar_maestro()
