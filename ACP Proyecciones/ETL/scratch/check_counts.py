import pandas as pd
from sqlalchemy import create_engine

def main():
    try:
        engine = create_engine('mssql+pyodbc:///?odbc_connect=DRIVER={ODBC Driver 17 for SQL Server};SERVER=LCP-PAG-PRACTIC;DATABASE=ACP_DataWarehose_Proyecciones;Trusted_Connection=yes;TrustServerCertificate=yes')
        
        tables = [
            'Bronce.Evaluacion_Vegetativa',
            'Bronce.Evaluacion_Pesos',
            'Bronce.Variables_Meteorologicas',
            'MDM.Cuarentena'
        ]
        
        for table in tables:
            print(f"\n--- Counts for {table} ---")
            if table == 'MDM.Cuarentena':
                query = "SELECT Estado, COUNT(Estado) as Total FROM MDM.Cuarentena GROUP BY Estado"
            else:
                query = f"SELECT Estado_Carga, COUNT(Estado_Carga) as Total FROM {table} GROUP BY Estado_Carga"
            df = pd.read_sql(query, engine)
            print(df)

        print("\n--- Recent rejections in Cuarentena for Evaluacion_Vegetativa ---")
        query_recent = """
            SELECT TOP 5 Tabla_Origen, Campo_Origen, Valor_Recibido, Motivo, Fecha_Ingreso
            FROM MDM.Cuarentena 
            WHERE Tabla_Origen = 'Bronce.Evaluacion_Vegetativa'
            ORDER BY Fecha_Ingreso DESC
        """
        print(pd.read_sql(query_recent, engine))

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
