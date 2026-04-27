import pandas as pd
from sqlalchemy import create_engine

def main():
    try:
        engine = create_engine('mssql+pyodbc:///?odbc_connect=DRIVER={ODBC Driver 17 for SQL Server};SERVER=LCP-PAG-PRACTIC;DATABASE=ACP_DataWarehose_Proyecciones;Trusted_Connection=yes;TrustServerCertificate=yes')
        
        print("\n--- All Geography Related Objects (Full Names) ---")
        query = """
            SELECT s.name AS SchemaName, o.name AS ObjectName, o.type_desc
            FROM sys.objects o 
            INNER JOIN sys.schemas s ON o.schema_id = s.schema_id 
            WHERE o.name LIKE '%Geografia%'
            ORDER BY SchemaName, ObjectName
        """
        df = pd.read_sql(query, engine)
        print(df)

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
