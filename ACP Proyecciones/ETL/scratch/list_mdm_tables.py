import pandas as pd
from sqlalchemy import create_engine

def main():
    try:
        engine = create_engine('mssql+pyodbc:///?odbc_connect=DRIVER={ODBC Driver 17 for SQL Server};SERVER=LCP-PAG-PRACTIC;DATABASE=ACP_DataWarehose_Proyecciones;Trusted_Connection=yes;TrustServerCertificate=yes')
        
        print("\n--- All Tables in MDM Schema ---")
        query = "SELECT name FROM sys.objects WHERE schema_id = SCHEMA_ID('MDM') AND type = 'U' ORDER BY name"
        df = pd.read_sql(query, engine)
        print(df['name'].tolist())

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
