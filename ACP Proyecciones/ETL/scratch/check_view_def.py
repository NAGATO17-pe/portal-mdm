import pandas as pd
from sqlalchemy import create_engine

def main():
    try:
        engine = create_engine('mssql+pyodbc:///?odbc_connect=DRIVER={ODBC Driver 17 for SQL Server};SERVER=LCP-PAG-PRACTIC;DATABASE=ACP_DataWarehose_Proyecciones;Trusted_Connection=yes;TrustServerCertificate=yes')
        
        print("\n--- View Definition for Silver.Dim_Geografia ---")
        query = "SELECT definition FROM sys.sql_modules WHERE object_id = OBJECT_ID('Silver.Dim_Geografia')"
        df = pd.read_sql(query, engine)
        if not df.empty:
            print(df['definition'].iloc[0])
        else:
            print("View definition not found.")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
