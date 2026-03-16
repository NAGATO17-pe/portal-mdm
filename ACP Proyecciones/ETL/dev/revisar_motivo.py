import sqlite3
import pprint

c = sqlite3.connect('data/acp_dev.db')
motivos = c.execute('SELECT Motivo, COUNT(*) FROM "MDM.Cuarentena" GROUP BY Motivo').fetchall()
print("Motivos de cuarentena:")
pprint.pprint(motivos)
c.close()
