import pandas as pd

def main():
    try:
        path = 'd:/Proyecto2026/ACP_DWH/ACP Proyecciones/ETL/data/procesados/conteo_fruta/Conteo frutos_20260422_103356_20260422_105132.xlsx'
        df = pd.read_excel(path, header=1)
        
        # Group by grain keys and see if counts vary
        grain_cols = ['Fecha', 'Modulo', 'Valvula', 'Punto']
        
        counts = df.groupby(grain_cols).size().reset_index(name='NumRows')
        dups = counts[counts['NumRows'] > 1]
        
        print(f"Total combinations: {len(counts)}")
        print(f"Combinations with multiple rows: {len(dups)}")
        
        # Check if they have different counts
        print("\nChecking for different counts in 'duplicate' rows...")
        samples = []
        for _, row in dups.head(20).iterrows():
            mask = True
            for col in grain_cols:
                mask &= (df[col] == row[col])
            group = df[mask]
            
            unique_counts = group['Botones Florales'].nunique()
            if unique_counts > 1:
                samples.append(group[['Registro', 'Punto', 'Botones Florales', 'Flores']])
        
        if samples:
            for s in samples[:5]:
                print("\nSample Group:")
                print(s)
        else:
            print("No cases found with different counts > 0? (Wait, maybe they are all X and 0)")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
