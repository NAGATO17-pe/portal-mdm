import sys
import os
import pandas as pd

# Add the parent directory to sys.path to import utils.db
sys.path.append(os.getcwd())

from utils.db import ejecutar_query

def inspect(table_name):
    print(f"\n--- {table_name} ---")
    try:
        df = ejecutar_query(f"SELECT TOP 0 * FROM {table_name}")
        print("Columns:", df.columns.tolist())
    except Exception as e:
        print(f"Error: {e}")

inspect("Silver.Dim_Personal")
inspect("MDM.Catalogo_Variedades")
inspect("MDM.Homologacion")
inspect("MDM.Cuarentena")
inspect("MDM.Log_Sugerencias_MDM") # Check if it really doesn't exist
inspect("MDM.Log_Decisiones_MDM")
