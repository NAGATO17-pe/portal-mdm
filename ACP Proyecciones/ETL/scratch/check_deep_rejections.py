import pandas as pd
from sqlalchemy import create_engine

def main():
    try:
        engine = create_engine('mssql+pyodbc:///?odbc_connect=DRIVER={ODBC Driver 17 for SQL Server};SERVER=LCP-PAG-PRACTIC;DATABASE=ACP_DataWarehose_Proyecciones;Trusted_Connection=yes;TrustServerCertificate=yes')
        
        print("\n--- Rejection Summary for Tasa_Crecimiento_Brotes ---")
        query = """
            SELECT TOP 10 Motivo, COUNT(*) as Total 
            FROM MDM.Cuarentena 
            WHERE Tabla_Origen = 'Bronce.Tasa_Crecimiento_Brotes' 
            GROUP BY Motivo 
            ORDER BY Total DESC
        """
        df = pd.read_sql(query, engine)
        print(df)

        print("\n--- Rejection Summary for Evaluacion_Vegetativa (Cuarentena) ---")
        query_veg = """
            SELECT TOP 10 Motivo, COUNT(*) as Total 
            FROM MDM.Cuarentena 
            WHERE Tabla_Origen = 'Bronce.Evaluacion_Vegetativa' 
            GROUP BY Motivo 
            ORDER BY Total DESC
        """
        df_veg = pd.read_sql(query_veg, engine)
        print(df_veg)

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
