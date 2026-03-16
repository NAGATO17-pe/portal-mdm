import sqlite3
conn = sqlite3.connect('data/acp_dev.db')
conn.execute('UPDATE "Config.Reglas_Validacion" SET Activo=0')
conn.execute('DELETE FROM "MDM.Cuarentena"')
conn.execute('DELETE FROM "Bronce.Evaluacion_Pesos"')
conn.execute('DELETE FROM "Auditoria.Log_Carga"')
conn.commit()
conn.close()
print("Reglas desactivadas y BD limpia.")
