import pandas as pd

def main():
    try:
        path = 'd:/Proyecto2026/ACP_DWH/ACP Proyecciones/ETL/data/procesados/conteo_fruta/Conteo frutos_20260422_103356_20260422_105132.xlsx'
        df = pd.read_excel(path, header=1, nrows=50)
        
        print("\n--- Full columns of Conteo Excel ---")
        print(df.columns.tolist())
        
        print("\n--- Rows for a 'duplicate' point (e.g. Modulo 03, Valvula 38, Punto 2) ---")
        # I'll try to find the rows I saw in Valores_Raw
        mask = (df['Modulo'].astype(str).str.contains('03')) & (df['Valvula'].astype(str).str.contains('38')) & (df['Punto'] == 2)
        print(df[mask][['Fecha', 'Registro', 'Punto', 'Botones Florales', 'Flores']])

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
