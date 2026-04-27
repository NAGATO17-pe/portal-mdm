import pandas as pd
from sqlalchemy import create_engine

def main():
    try:
        engine = create_engine('mssql+pyodbc:///?odbc_connect=DRIVER={ODBC Driver 17 for SQL Server};SERVER=LCP-PAG-PRACTIC;DATABASE=ACP_DataWarehose_Proyecciones;Trusted_Connection=yes;TrustServerCertificate=yes')
        
        print("\n--- Columns in Bronce.Conteo_Fruta ---")
        query = "SELECT TOP 1 * FROM Bronce.Conteo_Fruta"
        df = pd.read_sql(query, engine)
        print(df.columns.tolist())

        print("\n--- Sample data for Valvula, Modulo, Punto ---")
        query_sample = "SELECT TOP 10 Modulo_Raw, Valvula_Raw, Valores_Raw FROM Bronce.Conteo_Fruta"
        df_sample = pd.read_sql(query_sample, engine)
        print(df_sample)

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
