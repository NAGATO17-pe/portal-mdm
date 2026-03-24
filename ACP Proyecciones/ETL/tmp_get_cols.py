from config.conexion import obtener_engine
import pandas as pd
engine = obtener_engine()
with open('bronce_schema.txt', 'w') as f:
    df = pd.read_sql("SELECT TABLE_NAME, COLUMN_NAME FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA = 'Bronce'", engine)
    for table in df['TABLE_NAME'].unique():
        cols = df[df['TABLE_NAME'] == table]['COLUMN_NAME'].tolist()
        f.write(f"{table}: {', '.join(cols)}\n")
