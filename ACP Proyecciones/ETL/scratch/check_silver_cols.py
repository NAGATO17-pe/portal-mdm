import pandas as pd
from sqlalchemy import create_engine

def main():
    try:
        engine = create_engine('mssql+pyodbc:///?odbc_connect=DRIVER={ODBC Driver 17 for SQL Server};SERVER=LCP-PAG-PRACTIC;DATABASE=ACP_DataWarehose_Proyecciones;Trusted_Connection=yes;TrustServerCertificate=yes')
        
        print("\n--- Columns in Silver.Fact_Conteo_Fenologico ---")
        query = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'Silver' AND TABLE_NAME = 'Fact_Conteo_Fenologico'"
        df = pd.read_sql(query, engine)
        print(df['COLUMN_NAME'].tolist())

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
