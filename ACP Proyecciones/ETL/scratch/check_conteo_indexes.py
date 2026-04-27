import pandas as pd
from sqlalchemy import create_engine

def main():
    try:
        engine = create_engine('mssql+pyodbc:///?odbc_connect=DRIVER={ODBC Driver 17 for SQL Server};SERVER=LCP-PAG-PRACTIC;DATABASE=ACP_DataWarehose_Proyecciones;Trusted_Connection=yes;TrustServerCertificate=yes')
        
        print("\n--- Unique Indexes for Silver.Fact_Conteo_Fenologico ---")
        query = """
            SELECT 
                i.name AS IndexName, 
                c.name AS ColumnName,
                ic.key_ordinal
            FROM sys.indexes i 
            INNER JOIN sys.index_columns ic ON i.object_id = ic.object_id AND i.index_id = ic.index_id 
            INNER JOIN sys.columns c ON ic.object_id = c.object_id AND ic.column_id = c.column_id 
            WHERE i.object_id = OBJECT_ID('Silver.Fact_Conteo_Fenologico') 
              AND i.is_unique = 1
            ORDER BY i.name, ic.key_ordinal
        """
        df = pd.read_sql(query, engine)
        print(df)

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
