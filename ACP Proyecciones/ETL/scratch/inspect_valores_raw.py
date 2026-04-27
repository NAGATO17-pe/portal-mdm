import pandas as pd
from sqlalchemy import create_engine

def main():
    try:
        engine = create_engine('mssql+pyodbc:///?odbc_connect=DRIVER={ODBC Driver 17 for SQL Server};SERVER=LCP-PAG-PRACTIC;DATABASE=ACP_DataWarehose_Proyecciones;Trusted_Connection=yes;TrustServerCertificate=yes')
        
        print("\n--- Deep inspection of Valores_Raw for Conteo_Fruta ---")
        query = "SELECT TOP 5 Valores_Raw FROM Bronce.Conteo_Fruta"
        df = pd.read_sql(query, engine)
        for i, row in df.iterrows():
            print(f"\nRow {i}:")
            print(row['Valores_Raw'])

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
