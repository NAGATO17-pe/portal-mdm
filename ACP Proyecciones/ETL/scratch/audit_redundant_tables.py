import pandas as pd
from sqlalchemy import create_engine

def main():
    try:
        engine = create_engine('mssql+pyodbc:///?odbc_connect=DRIVER={ODBC Driver 17 for SQL Server};SERVER=LCP-PAG-PRACTIC;DATABASE=ACP_DataWarehose_Proyecciones;Trusted_Connection=yes;TrustServerCertificate=yes')
        
        print("\n--- Redundant Tables and Views Audit ---")
        query = """
            SELECT 
                s.name AS SchemaName, 
                o.name AS ObjectName, 
                o.type_desc, 
                o.create_date 
            FROM sys.objects o 
            INNER JOIN sys.schemas s ON o.schema_id = s.schema_id 
            WHERE o.type IN ('U', 'V') 
              AND (
                o.name LIKE '%Obsoleto%' 
                OR o.name LIKE '%Obsoleta%' 
                OR o.name LIKE '%_v1%' 
                OR o.name LIKE '%_v2%' 
                OR o.name LIKE '%_Old%' 
                OR o.name LIKE '%_New%' 
                OR o.name LIKE '%_Base%' 
                OR o.name LIKE '%Temp%'
                OR o.name LIKE '%Backup%'
              )
            ORDER BY SchemaName, ObjectName
        """
        df = pd.read_sql(query, engine)
        print(df)

        print("\n--- All Geography Related Objects ---")
        query_geo = """
            SELECT s.name AS SchemaName, o.name AS ObjectName, o.type_desc
            FROM sys.objects o 
            INNER JOIN sys.schemas s ON o.schema_id = s.schema_id 
            WHERE o.name LIKE '%Geografia%'
        """
        df_geo = pd.read_sql(query_geo, engine)
        print(df_geo)

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
