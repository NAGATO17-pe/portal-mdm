import pandas as pd
from sqlalchemy import create_engine

def main():
    try:
        engine = create_engine('mssql+pyodbc:///?odbc_connect=DRIVER={ODBC Driver 17 for SQL Server};SERVER=LCP-PAG-PRACTIC;DATABASE=ACP_DataWarehose_Proyecciones;Trusted_Connection=yes;TrustServerCertificate=yes')
        
        print("\n--- Distinct Fundo_Raw in Bronce.Evaluacion_Vegetativa ---")
        query = "SELECT DISTINCT Fundo_Raw FROM Bronce.Evaluacion_Vegetativa"
        df = pd.read_sql(query, engine)
        print(df)

    except Exception as e:
        # Check if column exists first
        print(f"Error: {e}")
        print("\n--- Schema of Bronce.Evaluacion_Vegetativa ---")
        query_schema = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'Bronce' AND TABLE_NAME = 'Evaluacion_Vegetativa'"
        df_schema = pd.read_sql(query_schema, engine)
        print(df_schema)

if __name__ == "__main__":
    main()
