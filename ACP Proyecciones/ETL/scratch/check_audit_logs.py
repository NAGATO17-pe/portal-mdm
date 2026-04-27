import pandas as pd
from sqlalchemy import create_engine

def main():
    try:
        engine = create_engine('mssql+pyodbc:///?odbc_connect=DRIVER={ODBC Driver 17 for SQL Server};SERVER=LCP-PAG-PRACTIC;DATABASE=ACP_DataWarehose_Proyecciones;Trusted_Connection=yes;TrustServerCertificate=yes')
        
        print("\n--- Official Audit Log (Auditoria.Log_Carga) ---")
        query = "SELECT TOP 10 * FROM Auditoria.Log_Carga ORDER BY Fecha_Inicio DESC"
        df = pd.read_sql(query, engine)
        print(df)

        print("\n--- Recent Errors in Audit Log ---")
        query_errors = "SELECT TOP 5 * FROM Auditoria.Log_Carga WHERE Estado_Proceso = 'ERROR' ORDER BY Fecha_Inicio DESC"
        df_errors = pd.read_sql(query_errors, engine)
        print(df_errors)

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
