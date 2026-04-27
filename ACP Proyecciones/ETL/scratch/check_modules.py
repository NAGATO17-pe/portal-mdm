import pandas as pd
from sqlalchemy import create_engine

def main():
    try:
        engine = create_engine('mssql+pyodbc:///?odbc_connect=DRIVER={ODBC Driver 17 for SQL Server};SERVER=LCP-PAG-PRACTIC;DATABASE=ACP_DataWarehose_Proyecciones;Trusted_Connection=yes;TrustServerCertificate=yes')
        
        print("\n--- Distinct Modulo_Raw in Evaluacion_Vegetativa (CARGADO) ---")
        query = "SELECT DISTINCT Modulo_Raw FROM Bronce.Evaluacion_Vegetativa WHERE Estado_Carga = 'CARGADO'"
        df = pd.read_sql(query, engine)
        print(df)

        print("\n--- Distinct Modulo_Raw in Evaluacion_Pesos (CARGADO) ---")
        query = "SELECT DISTINCT Modulo_Raw FROM Bronce.Evaluacion_Pesos WHERE Estado_Carga = 'CARGADO'"
        df = pd.read_sql(query, engine)
        print(df)

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
