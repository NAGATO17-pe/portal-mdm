import pandas as pd
import urllib
from sqlalchemy import create_engine
import os

def exportar_clima():
    print("Iniciando exportacion de tablas de Clima...")
    servidor = 'LCP-PAG-PRACTIC'
    base = 'ACP_DataWarehose_Proyecciones'
    driver = 'ODBC Driver 17 for SQL Server'
    cadena_pyodbc = f'DRIVER={{{driver}}};SERVER={servidor};DATABASE={base};Trusted_Connection=yes;TrustServerCertificate=yes;'
    u = 'mssql+pyodbc:///?odbc_connect=' + urllib.parse.quote_plus(cadena_pyodbc)
    
    tablas_clima = ["Silver.Fact_Telemetria_Clima", "Bronce.Reporte_Clima", "Silver.Fact_Clima"]
    engine = create_engine(u)
    
    for t in tablas_clima:
        print(f"Probando exportar {t}...")
        try:
            df = pd.read_sql(f"SELECT * FROM {t}", engine)
            if not df.empty:
                nombre_archivo = t.replace(".", "_") + ".csv"
                df.to_csv(nombre_archivo, index=False, sep=',', encoding='utf-8-sig')
                print(f"   EXITO: Exportado {t} a {nombre_archivo} ({len(df)} filas)")
            else:
                print(f"   INFO: La tabla {t} existe pero esta VACIA.")
        except Exception as e:
            print(f"   ERROR: No se pudo leer {t}.")

if __name__ == "__main__":
    exportar_clima()
