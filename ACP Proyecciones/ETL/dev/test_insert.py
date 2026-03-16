import sqlite3
import pandas as pd

f = 'D:/Proyecto2026/ACP Proyecciones/ETL/data/entrada/evaluacion_vegetativa.xlsx'
df = pd.read_excel(f, skiprows=1)
dft = df.rename(columns={
    'Modulo': 'Modulo_Raw', 
    'Descripción': 'Variedad_Raw', 
    'Consumidor': 'Fundo_Raw', 
    'Evaluación': 'IDESTADOCICLO_Raw', 
    'Fecha': 'Fecha_Raw', 
    'Nombres': 'Evaluador_Raw'
})
cols = [c for c in dft.columns if c in ['Modulo_Raw', 'Variedad_Raw', 'Fundo_Raw', 'IDESTADOCICLO_Raw', 'Fecha_Raw', 'Evaluador_Raw']]
dfb = dft[cols].copy()

c = sqlite3.connect('D:/Proyecto2026/ACP Proyecciones/ETL/data/acp_dev.db')
r = dfb.iloc[0].to_dict()
print(f"Buscando insertar: {r}")

try:
    c.execute(
        'INSERT INTO "Bronce.Ciclos_Fenologicos" (Fecha_Raw, Evaluador_Raw, Fundo_Raw, Modulo_Raw, IDESTADOCICLO_Raw, Variedad_Raw, Archivo_Origen) VALUES (?,?,?,?,?,?,?)',
        (r['Fecha_Raw'], r['Evaluador_Raw'], r['Fundo_Raw'], r['Modulo_Raw'], r['IDESTADOCICLO_Raw'], r['Variedad_Raw'], 'evaluacion_vegetativa.xlsx')
    )
    print("Insert exitoso")
except Exception as e:
    print(f"Error: {e}")
finally:
    c.close()
