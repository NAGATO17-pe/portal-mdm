import pandas as pd
from sqlalchemy import create_engine

def main():
    try:
        engine = create_engine('mssql+pyodbc:///?odbc_connect=DRIVER={ODBC Driver 17 for SQL Server};SERVER=LCP-PAG-PRACTIC;DATABASE=ACP_DataWarehose_Proyecciones;Trusted_Connection=yes;TrustServerCertificate=yes')
        
        print("\n--- ETL Execution Log (MDM.Log_Ejecucion_ETL) ---")
        try:
            query = "SELECT TOP 5 * FROM MDM.Log_Ejecucion_ETL ORDER BY Fecha_Inicio DESC"
            df = pd.read_sql(query, engine)
            print(df)
        except Exception as e:
            print(f"Table not found or error: {e}")

        print("\n--- MDM Rejections Summary (Cuarentena) ---")
        query_cuar = """
            SELECT Tabla_Origen, Motivo, COUNT(*) as Total 
            FROM MDM.Cuarentena 
            GROUP BY Tabla_Origen, Motivo 
            ORDER BY Total DESC
        """
        df_cuar = pd.read_sql(query_cuar, engine)
        print(df_cuar)

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
