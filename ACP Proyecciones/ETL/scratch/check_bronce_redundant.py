import pandas as pd
from sqlalchemy import create_engine

def main():
    try:
        engine = create_engine('mssql+pyodbc:///?odbc_connect=DRIVER={ODBC Driver 17 for SQL Server};SERVER=LCP-PAG-PRACTIC;DATABASE=ACP_DataWarehose_Proyecciones;Trusted_Connection=yes;Trusted_Connection=yes;TrustServerCertificate=yes')
        
        print("\n--- Redundant Tables in Bronce Schema ---")
        query = """
            SELECT name 
            FROM sys.objects 
            WHERE schema_id = SCHEMA_ID('Bronce') 
              AND type = 'U' 
              AND (name LIKE '%Temp%' OR name LIKE '%Backup%' OR name LIKE '%Old%')
        """
        df = pd.read_sql(query, engine)
        print(df['name'].tolist())

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
