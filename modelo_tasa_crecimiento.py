"""
Modelo de Tasa de Crecimiento - Arándanos
Objetivos:
  1. Predecir crecimiento (cm) dado días/semana desde poda y variables agronómicas
  2. Comparar curvas por variedad, condición, estado vegetativo, tipo de tallo
  3. Pronosticar semana en que el cultivo alcanza fenología cosechable
"""

import pandas as pd
import numpy as np
from scipy.optimize import curve_fit
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.model_selection import cross_val_score
from sklearn.metrics import mean_absolute_error, r2_score
from sklearn.preprocessing import LabelEncoder
import warnings
warnings.filterwarnings("ignore")

# ── 1. CARGA Y JOIN DE DATOS ──────────────────────────────────────────────────

print("Cargando datos...")

fact = pd.read_csv("D:/Proyecto2026/ACP_DWH/Fact_Tasa_Crecimiento_Export.csv", encoding="utf-8-sig")
dim_var = pd.read_csv("D:/Proyecto2026/ACP_DWH/Dim_Variedad.csv", encoding="utf-8-sig")
dim_geo = pd.read_csv("D:/Proyecto2026/ACP_DWH/Dim_Geografia.csv", encoding="utf-8-sig")
dim_tiempo = pd.read_csv("D:/Proyecto2026/ACP_DWH/Dim_Tiempo.csv", encoding="utf-8-sig")

df = fact.merge(dim_var, on="ID_Variedad", how="left")
df = df.merge(dim_geo[["ID_Geografia", "ID_Fundo_Catalogo", "ID_Sector_Catalogo", "Es_Test_Block"]],
              on="ID_Geografia", how="left")
df = df.merge(dim_tiempo[["ID_Tiempo", "Semana_Cosecha", "Semana_ISO", "Mes", "Anio"]],
              on="ID_Tiempo", how="left")

# Semana desde poda (variable principal del modelo)
df["Semana_Poda"] = (df["Dias_Desde_Poda"] // 7).astype(int)

# Filtrar solo datos de calidad OK y rangos razonables
df = df[df["Estado_DQ"] == "OK"].copy()
df = df[df["Medida_Crecimiento"] > 0].copy()
df = df[df["Dias_Desde_Poda"] >= 0].copy()

print(f"  Registros válidos: {len(df):,}")
print(f"  Variedades presentes: {df['Nombre_Variedad'].nunique()}")
print(f"  Rango semanas: {df['Semana_Poda'].min()} - {df['Semana_Poda'].max()}")


# ── 2. CURVA DE CRECIMIENTO PROMEDIO (Gompertz) ───────────────────────────────

print("\n[1/3] Ajustando curva de crecimiento promedio (Gompertz)...")

def gompertz(t, K, b, c):
    """K=asíntota, b=desplazamiento, c=tasa de crecimiento"""
    return K * np.exp(-b * np.exp(-c * t))

# Promedio por semana para ajuste de curva
curva_base = df.groupby("Semana_Poda")["Medida_Crecimiento"].median().reset_index()
curva_base = curva_base[curva_base["Semana_Poda"] <= 20]  # semanas con datos densos

t_data = curva_base["Semana_Poda"].values.astype(float)
y_data = curva_base["Medida_Crecimiento"].values.astype(float)

try:
    popt, _ = curve_fit(gompertz, t_data, y_data, p0=[35, 5, 0.5], maxfev=10000)
    K, b, c = popt
    print(f"  Parámetros Gompertz: K(asíntota)={K:.1f} cm, b={b:.3f}, c(tasa)={c:.3f}")
    print(f"  Crecimiento máximo esperado: {K:.1f} cm")
except Exception as e:
    print(f"  Ajuste Gompertz falló: {e}")
    K, b, c = 30, 5, 0.4


# ── 3. CURVAS POR GRUPO (variedad × condición × estado vegetativo) ─────────────

print("\n[2/3] Comparando curvas por grupo...")

grupos_resultados = []

grupos = ["Nombre_Variedad", "Condicion", "Estado_Vegetativo", "Tipo_Tallo"]

for grupo in grupos:
    print(f"\n  >> Por {grupo}:")
    resumen = (
        df[df["Semana_Poda"].between(2, 10)]
        .groupby([grupo, "Semana_Poda"])["Medida_Crecimiento"]
        .median()
        .reset_index()
    )
    # Promedio ponderado semanas 4-8 (pico de crecimiento)
    pico = df[df["Semana_Poda"].between(4, 8)].groupby(grupo)["Medida_Crecimiento"].agg(
        ["median", "mean", "std", "count"]
    ).round(2)
    pico.columns = ["Mediana_cm", "Promedio_cm", "Desv_std", "N_obs"]
    pico = pico.sort_values("Mediana_cm", ascending=False)
    print(pico.head(10).to_string())
    grupos_resultados.append((grupo, pico))


# ── 4. MODELO PREDICTIVO (Gradient Boosting) ──────────────────────────────────

print("\n\n[3/3] Entrenando modelo predictivo...")

features = ["Semana_Poda", "Dias_Desde_Poda", "Condicion", "Estado_Vegetativo",
            "Tipo_Tallo", "Nombre_Variedad", "Campana", "ID_Fundo_Catalogo",
            "Mes", "Anio"]

df_model = df[features + ["Medida_Crecimiento"]].dropna()

# Codificar categóricas
cat_cols = ["Condicion", "Estado_Vegetativo", "Tipo_Tallo", "Nombre_Variedad", "Campana"]
encoders = {}
for col in cat_cols:
    le = LabelEncoder()
    df_model[col] = le.fit_transform(df_model[col].astype(str))
    encoders[col] = le

X = df_model[features]
y = df_model["Medida_Crecimiento"]

modelo = GradientBoostingRegressor(
    n_estimators=200,
    max_depth=5,
    learning_rate=0.05,
    subsample=0.8,
    random_state=42
)

print("  Entrenando (puede tardar ~1 min)...")
modelo.fit(X, y)

y_pred = modelo.predict(X)
mae = mean_absolute_error(y, y_pred)
r2 = r2_score(y, y_pred)
print(f"  MAE: {mae:.2f} cm")
print(f"  R²:  {r2:.4f}")

# Importancia de variables
importancias = pd.Series(modelo.feature_importances_, index=features).sort_values(ascending=False)
print("\n  Importancia de variables:")
for feat, imp in importancias.items():
    bar = "|" * int(imp * 50)
    print(f"    {feat:<25} {imp:.4f}  {bar}")


# ── 5. PRONÓSTICO DE SEMANA COSECHABLE ────────────────────────────────────────

print("\n\n== PRONOSTICO POR VARIEDAD ==")
print("Semana estimada en que cada variedad alcanza crecimiento pico (>=25 cm mediana)\n")

pronostico = []
for variedad in df["Nombre_Variedad"].dropna().unique():
    sub = df[df["Nombre_Variedad"] == variedad]
    por_semana = sub.groupby("Semana_Poda")["Medida_Crecimiento"].median()
    semanas_ok = por_semana[por_semana >= 25.0]
    semana_pico = por_semana.idxmax() if not por_semana.empty else None
    semana_cosecha = semanas_ok.index.min() if not semanas_ok.empty else None
    n = len(sub)
    pronostico.append({
        "Variedad": variedad,
        "N_obs": n,
        "Semana_Pico_cm": semana_pico,
        "cm_en_pico": round(por_semana.max(), 1) if not por_semana.empty else None,
        "Semana_>=25cm": semana_cosecha
    })

df_pronostico = pd.DataFrame(pronostico).sort_values("Semana_>=25cm")
df_pronostico = df_pronostico[df_pronostico["N_obs"] >= 50]  # solo variedades con datos suficientes
print(df_pronostico.to_string(index=False))


# ── 6. EXPORTAR RESULTADOS ────────────────────────────────────────────────────

out_path = "D:/Proyecto2026/ACP_DWH/"

# Curva base
curva_export = pd.DataFrame({
    "Semana_Poda": t_data,
    "Mediana_Real_cm": y_data,
    "Gompertz_Ajustado_cm": gompertz(t_data, K, b, c).round(2)
})
curva_export.to_csv(out_path + "resultado_curva_base.csv", index=False)

# Pronóstico por variedad
df_pronostico.to_csv(out_path + "resultado_pronostico_variedades.csv", index=False)

# Comparativo por condición y estado vegetativo
for grupo, pico in grupos_resultados:
    nombre = grupo.replace(" ", "_").lower()
    pico.to_csv(out_path + f"resultado_comparativo_{nombre}.csv")

print(f"\nResultados exportados en {out_path}")
print("  - resultado_curva_base.csv")
print("  - resultado_pronostico_variedades.csv")
print("  - resultado_comparativo_*.csv")
print("\nModelo listo.")
