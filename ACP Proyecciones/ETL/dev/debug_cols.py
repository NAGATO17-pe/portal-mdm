import sqlite3

c = sqlite3.connect('data/acp_dev.db')
cols = c.execute('PRAGMA table_info("Bronce.Ciclos_Fenologicos")').fetchall()
print("Columnas en Bronce.Ciclos_Fenologicos:")
for col in cols:
    print(f"  - {col[1]}")
c.close()
