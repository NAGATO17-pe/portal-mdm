import sqlalchemy
import pandas as pd
from sqlalchemy import text

# Intentamos conectar usando el engine del proyecto si es posible, o uno genérico
engine = sqlalchemy.create_engine('mssql+pyodbc://LCP-PAG-PRACTIC/ACP_DataWarehose_Proyecciones?driver=ODBC+Driver+17+for+SQL+Server&Trusted_Connection=yes')

query_cols = "SELECT COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'MDM' AND TABLE_NAME = 'Cuarentena'"
try:
    with engine.connect() as conn:
        cols = [r[0] for r in conn.execute(text(query_cols)).fetchall()]
        print(f"Columnas en MDM.Cuarentena: {cols}")
        
        col_name = 'Campo_Origen'
        val_name = 'Valor_Recibido'
        motivo_name = 'Motivo'

        pd.set_option('display.max_columns', None)
        pd.set_option('display.max_colwidth', None)
        pd.set_option('display.width', 1000)

        query = f"""
        SELECT TOP 20 
            Tabla_Origen, 
            {col_name} as Columna, 
            {val_name} as Valor, 
            {motivo_name} as Motivo, 
            COUNT(*) as Frecuencia
        FROM MDM.Cuarentena
        GROUP BY Tabla_Origen, {col_name}, {val_name}, {motivo_name}
        ORDER BY Frecuencia DESC
        """
        df = pd.read_sql(text(query), conn)
        print(df)
except Exception as e:
    print(f"Error: {e}")
