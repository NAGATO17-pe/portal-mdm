import sys
import os
import pandas as pd

# Add the parent directory to sys.path to import utils.db
sys.path.append(os.getcwd())

from utils.db import ejecutar_query

def inspect(table_name):
    res = f"\n--- {table_name} ---\n"
    try:
        df = ejecutar_query(f"SELECT TOP 0 * FROM {table_name}")
        res += f"Columns: {df.columns.tolist()}\n"
    except Exception as e:
        res += f"Error: {e}\n"
    return res

output = ""
output += inspect("Silver.Dim_Personal")
output += inspect("MDM.Catalogo_Variedades")
output += inspect("MDM.Homologacion")
output += inspect("MDM.Cuarentena")
output += inspect("MDM.Log_Sugerencias_MDM") 
output += inspect("MDM.Log_Decisiones_MDM")

with open("schema_info.txt", "w", encoding="utf-8") as f:
    f.write(output)

print("Done. Check schema_info.txt")
