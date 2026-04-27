import pandas as pd
from sqlalchemy import create_engine

def main():
    try:
        engine = create_engine('mssql+pyodbc:///?odbc_connect=DRIVER={ODBC Driver 17 for SQL Server};SERVER=LCP-PAG-PRACTIC;DATABASE=ACP_DataWarehose_Proyecciones;Trusted_Connection=yes;TrustServerCertificate=yes')
        
        print("\n--- Counting rows per Point/Day in Bronce.Conteo_Fruta ---")
        query = """
            SELECT Fecha_Raw, Modulo_Raw, Valvula_Raw, Valores_Raw
            FROM Bronce.Conteo_Fruta
            WHERE Estado_Carga = 'CARGADO'
        """
        df = pd.read_sql(query, engine)
        
        def get_punto(v):
            if not v: return '0'
            parts = str(v).split('|')
            for p in parts:
                if 'Punto_Raw=' in p or 'Punto=' in p:
                    return p.split('=')[1].strip()
            return '0'
            
        df['Punto'] = df['Valores_Raw'].apply(get_punto)
        
        counts = df.groupby(['Fecha_Raw', 'Modulo_Raw', 'Valvula_Raw', 'Punto']).size().reset_index(name='Total_Rows')
        dups = counts[counts['Total_Rows'] > 1]
        
        print(f"Total combinations: {len(counts)}")
        print(f"Combinations with multiple rows: {len(dups)}")
        print("\nSample of 'duplicates' at Point level:")
        print(dups.head(10))

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
