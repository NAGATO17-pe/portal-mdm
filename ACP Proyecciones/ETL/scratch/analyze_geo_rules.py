import pandas as pd
from sqlalchemy import create_engine, text

def main():
    try:
        engine = create_engine('mssql+pyodbc:///?odbc_connect=DRIVER={ODBC Driver 17 for SQL Server};SERVER=LCP-PAG-PRACTIC;DATABASE=ACP_DataWarehose_Proyecciones;Trusted_Connection=yes;TrustServerCertificate=yes')
        
        print("--- Sample Rejected Geographies from Evaluacion_Vegetativa ---")
        query_geo = """
            SELECT TOP 20 Modulo_Raw, Turno_Raw, Valvula_Raw, Cama_Raw 
            FROM Bronce.Evaluacion_Vegetativa 
            WHERE Estado_Carga = 'RECHAZADO'
        """
        df_geo = pd.read_sql(query_geo, engine)
        print(df_geo)
        
        print("\n--- Current Rules in MDM.Regla_Modulo_Raw ---")
        query_rules = "SELECT TOP 20 * FROM MDM.Regla_Modulo_Raw WHERE Es_Activa = 1"
        df_rules = pd.read_sql(query_rules, engine)
        print(df_rules)

        print("\n--- Checking for specific Modulo_Raw in Reglas ---")
        # Let's check some values from the rejected sample
        if not df_geo.empty:
            modules = df_geo['Modulo_Raw'].unique()
            print(f"Modules in rejection sample: {modules}")
            
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
