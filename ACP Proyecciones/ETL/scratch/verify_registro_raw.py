import pandas as pd
from sqlalchemy import create_engine

def main():
    try:
        engine = create_engine('mssql+pyodbc:///?odbc_connect=DRIVER={ODBC Driver 17 for SQL Server};SERVER=LCP-PAG-PRACTIC;DATABASE=ACP_DataWarehose_Proyecciones;Trusted_Connection=yes;TrustServerCertificate=yes')
        
        query = "SELECT TOP 10 Valores_Raw FROM Bronce.Conteo_Fruta WHERE Valores_Raw LIKE '%Registro_Raw=%'"
        df = pd.read_sql(query, engine)
        for i, val in enumerate(df['Valores_Raw']):
            print(f"Row {i}: {val[:50]}...")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
