import pandas as pd
from sqlalchemy import create_engine

def main():
    try:
        engine = create_engine('mssql+pyodbc:///?odbc_connect=DRIVER={ODBC Driver 17 for SQL Server};SERVER=LCP-PAG-PRACTIC;DATABASE=ACP_DataWarehose_Proyecciones;Trusted_Connection=yes;TrustServerCertificate=yes')
        
        print("\n--- Sentinel records (ID=0) in Catalogs ---")
        for tab in ['Fundo', 'Sector', 'Modulo', 'Turno', 'Valvula', 'Cama']:
            query = f"SELECT * FROM Silver.Dim_{tab}_Catalogo WHERE ID_{tab}_Catalogo = 0"
            df = pd.read_sql(query, engine)
            print(f"\nTable: Dim_{tab}_Catalogo")
            print(df)

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
