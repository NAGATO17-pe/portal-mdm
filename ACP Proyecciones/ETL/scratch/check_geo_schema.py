import pandas as pd
from sqlalchemy import create_engine

def main():
    try:
        engine = create_engine('mssql+pyodbc:///?odbc_connect=DRIVER={ODBC Driver 17 for SQL Server};SERVER=LCP-PAG-PRACTIC;DATABASE=ACP_DataWarehose_Proyecciones;Trusted_Connection=yes;TrustServerCertificate=yes')
        
        print("\n--- Geography Tables/Views in Silver ---")
        query = "SELECT TABLE_NAME, TABLE_TYPE FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = 'Silver' AND TABLE_NAME LIKE 'Dim_Geografia%'"
        df = pd.read_sql(query, engine)
        print(df)

        print("\n--- Geography Views in Silver ---")
        query = "SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS WHERE TABLE_SCHEMA = 'Silver' AND TABLE_NAME LIKE 'Dim_Geografia%'"
        df = pd.read_sql(query, engine)
        print(df)

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
