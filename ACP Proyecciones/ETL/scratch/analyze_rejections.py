import pandas as pd
from sqlalchemy import create_engine, text

def main():
    try:
        engine = create_engine('mssql+pyodbc:///?odbc_connect=DRIVER={ODBC Driver 17 for SQL Server};SERVER=LCP-PAG-PRACTIC;DATABASE=ACP_DataWarehose_Proyecciones;Trusted_Connection=yes;TrustServerCertificate=yes')
        
        print("--- Rejection Summary for Evaluacion_Vegetativa ---")
        query_veg = """
            SELECT TOP 10 Motivo, COUNT(*) as Total 
            FROM MDM.Cuarentena 
            WHERE Tabla_Origen = 'Bronce.Evaluacion_Vegetativa' 
              AND Estado = 'PENDIENTE'
            GROUP BY Motivo 
            ORDER BY Total DESC
        """
        df_veg = pd.read_sql(query_veg, engine)
        print(df_veg)
        
        print("\n--- Rejection Summary for Evaluacion_Pesos ---")
        query_pesos = """
            SELECT TOP 10 Motivo, COUNT(*) as Total 
            FROM MDM.Cuarentena 
            WHERE Tabla_Origen = 'Bronce.Evaluacion_Pesos' 
              AND Estado = 'PENDIENTE'
            GROUP BY Motivo 
            ORDER BY Total DESC
        """
        df_pesos = pd.read_sql(query_pesos, engine)
        print(df_pesos)

        print("\n--- Last 5 Rejections for Evaluacion_Vegetativa ---")
        query_last = """
            SELECT TOP 5 Tabla_Origen, Campo_Origen, Valor_Recibido, Motivo 
            FROM MDM.Cuarentena 
            WHERE Tabla_Origen = 'Bronce.Evaluacion_Vegetativa'
            ORDER BY Fecha_Ingreso DESC
        """
        df_last = pd.read_sql(query_last, engine)
        print(df_last)

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
