"""
datos_mock.py — Datos de prueba para todas las páginas del portal MDM.
Sin conexión a base de datos: se usan DataFrames estáticos.
"""
import pandas as pd
from datetime import datetime, timedelta
import random

# ─────────────────────────────────────────────
# INICIO — métricas y estado de tablas
# ─────────────────────────────────────────────

ESTADO_TABLAS = pd.DataFrame({
    "Tabla": [
        "Bronce.Evaluacion_Pesos",
        "Bronce.Conteo_Fruta",
        "Bronce.Data_SAP",
        "Bronce.Peladas",
        "Bronce.Ciclos_Fenologicos",
        "Bronce.Personal",
    ],
    "Última carga": [
        "2026-03-16 06:15", "2026-03-16 06:18", "2026-03-16 06:22",
        "2026-03-16 06:25", "2026-03-16 06:30", "2026-03-16 06:33",
    ],
    "Filas insertadas": [5212, 3870, 1204, 980, 2110, 450],
    "Filas rechazadas": [312, 148, 0, 5, 89, 12],
    "Estado": ["⚠️ Con errores", "⚠️ Con errores", "✅ OK", "✅ OK", "⚠️ Con errores", "✅ OK"],
})

LOG_CARGAS = pd.DataFrame({
    "Fecha": [f"2026-03-{16-i:02d} 06:20" for i in range(10)],
    "Tablas procesadas": [6, 6, 5, 6, 6, 4, 6, 6, 6, 5],
    "Total filas": [13826, 14102, 12980, 13450, 14200, 11000, 13700, 14050, 13600, 12800],
    "Rechazadas": [566, 421, 380, 502, 319, 890, 450, 612, 298, 740],
    "Resultado": ["⚠️ Con errores","✅ OK","✅ OK","⚠️ Con errores","✅ OK","❌ Falló","✅ OK","⚠️ Con errores","✅ OK","⚠️ Con errores"],
})

# ─────────────────────────────────────────────
# CUARENTENA
# ─────────────────────────────────────────────

CUARENTENA = pd.DataFrame({
    "ID": list(range(1, 31)),
    "Tabla Origen": (
        ["Bronce.Evaluacion_Pesos"] * 12 +
        ["Bronce.Conteo_Fruta"] * 10 +
        ["Bronce.Ciclos_Fenologicos"] * 8
    ),
    "Columna Origen": (
        ["PesoBaya_Raw"] * 12 +
        ["Variedad_Raw"] * 10 +
        ["IDESTADOCICLO_Raw"] * 8
    ),
    "Valor Raw": (
        ["25.0","0.1","99.9","30.5","0.2","28.1","0.0","35.0","0.3","22.0","0.05","45.0"] +
        ["FCM14-057","BILOXY","sekoya pop","H-BLOOM","Megacrysp","Draper?","O'Neil","blueray","Misty2","DUKE X"] +
        ["99","0","7","8","99","0","6","5"]
    ),
    "Motivo": (
        ["Fuera de rango (0.5–8.0 g)"] * 12 +
        ["Variedad no reconocida"] * 10 +
        ["Estado de ciclo no reconocido"] * 8
    ),
    "Severidad": (
        ["CRÍTICO"] * 5 + ["ALTO"] * 7 +
        ["ALTO"] * 10 +
        ["MEDIO"] * 8
    ),
    "Fecha ingreso": [f"2026-03-16 06:{15+i%20:02d}" for i in range(30)],
    "Estado": ["Pendiente"] * 30,
})

# ─────────────────────────────────────────────
# HOMOLOGACIÓN
# ─────────────────────────────────────────────

HOMOLOGACION_PENDIENTE = pd.DataFrame({
    "Texto crudo": ["FCM14-057","BILOXY","sekoya pop","Megacrysp","blueray","O'Neil","draper x","H-BLOOM"],
    "Valor canónico sugerido": ["Megacrisp","Biloxi","Sekoya Pop","Megacrisp","Blueray","O'Neal","Draper","H-Bloom"],
    "Score": [0.71, 0.88, 0.92, 0.68, 0.95, 0.83, 0.79, 0.87],
    "Tabla origen": ["Bronce.Conteo_Fruta"] * 8,
    "Veces visto": [29, 14, 7, 21, 45, 12, 33, 8],
    "Fecha": ["2026-03-16"] * 8,
})

HOMOLOGACION_HISTORIAL = pd.DataFrame({
    "Texto crudo": ["Draperr","misty 2","BluGold","emerald","Starburst","O-Neil"],
    "Valor canónico": ["Draper","Misty","Blu Gold","Emerald","StarBurst","O'Neal"],
    "Score": [0.82, 0.91, 0.76, 0.95, 0.80, 0.85],
    "Tabla": ["Bronce.Conteo_Fruta"] * 6,
    "Aprobado por": ["Carlos H.","Ana M.","Carlos H.","Luis P.","Ana M.","Carlos H."],
    "Fecha aprobación": ["2026-03-15","2026-03-14","2026-03-13","2026-03-12","2026-03-11","2026-03-10"],
})

# ─────────────────────────────────────────────
# CATÁLOGOS
# ─────────────────────────────────────────────

VARIEDADES = pd.DataFrame({
    "Nombre canónico": ["Draper","Biloxi","O'Neal","Misty","Blu Gold","Emerald","Megacrisp","Sekoya Pop","StarBurst","Snowchaser","Blueray","H-Bloom"],
    "Breeder": ["Fall Creek","Driscoll's","Sharpe-Nelson","Driscoll's","Fall Creek","Driscoll's","ACP Propio","ACP Propio","ACP Propio","ACP Propio","Corneille","Fall Creek"],
    "Activa": [True, True, True, True, True, True, True, True, False, True, True, True],
})

GEOGRAFIA = pd.DataFrame({
    "Fundo": ["Los Andes","Los Andes","Los Andes","San Miguel","San Miguel","El Retiro","El Retiro","El Retiro","La Esperanza","La Esperanza"],
    "Sector": ["Norte","Norte","Sur","Este","Este","Principal","Principal","Auxiliar","A","B"],
    "Módulo": ["M-01","M-02","M-10","M-05","M-06","M-03","M-04","M-07","M-08","M-09"],
    "Turno": ["Mañana","Tarde","Mañana","Mañana","Tarde","Mañana","Tarde","Mañana","Tarde","Mañana"],
    "Es Test Block": [False, False, True, False, False, False, True, False, False, False],
    "Activa": [True, True, True, True, True, True, True, False, True, True],
})

PERSONAL = pd.DataFrame({
    "DNI": ["12345678","23456789","34567890","45678901","56789012","67890123","78901234","89012345"],
    "Nombre completo": ["Ana María Torres","Luis Peralta","Carlos Quispe","Rosa Díaz","Juan Flores","María López","Pedro Ramos","Elena García"],
    "Rol": ["Evaluador","Operario","Supervisor","Evaluador","Operario","Operario","Evaluador","Supervisor"],
    "Sexo": ["F","M","M","F","M","F","M","F"],
    "Activo": [True, True, True, True, False, True, True, True],
})

# ─────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────

REGLAS_VALIDACION = pd.DataFrame({
    "Tabla destino": [
        "Silver.Evaluacion_Pesos","Silver.Evaluacion_Pesos","Silver.Evaluacion_Pesos",
        "Silver.Peladas","Silver.Conteo_Fruta","Silver.Ciclos_Fenologicos",
    ],
    "Columna": ["PesoBaya","Firmeza","Color_Brix","Muestras","ConteoFruta","DuracionCiclo"],
    "Tipo validación": ["rango","rango","rango","rango","rango","rango"],
    "Valor min": [0.5, 50.0, 5.0, 1, 0, 20],
    "Valor max": [8.0, 2000.0, 22.0, 500, 9999, 365],
    "Acción": ["QUARANTINE","QUARANTINE","QUARANTINE","QUARANTINE","QUARANTINE","QUARANTINE"],
    "Activa": [True, True, True, True, True, False],
})

PARAMETROS_PIPELINE = pd.DataFrame({
    "Parámetro": [
        "CAMPANA_ACTIVA","PESO_BAYA_MIN","PESO_BAYA_MAX",
        "LEVENSHTEIN_UMBRAL","CHUNK_SIZE_INSERT","SCORE_AUTO_APROBACION",
        "DIAS_RETENCION_CUARENTENA","LOG_NIVEL",
    ],
    "Valor actual": ["2026-A","0.5","8.0","0.65","500","0.90","90","INFO"],
    "Descripción": [
        "Identificador de la campaña agrícola activa",
        "Peso mínimo de baya aceptado (gramos)",
        "Peso máximo de baya aceptado (gramos)",
        "Score mínimo para sugerir homologación automática",
        "Tamaño de bloque para inserciones en BD",
        "Score mínimo para aprobar homologación sin revisión",
        "Días que se conservan registros en cuarentena",
        "Nivel de log del pipeline (DEBUG/INFO/WARNING)",
    ],
    "Última modificación": [
        "2026-01-10","2025-11-20","2025-11-20",
        "2026-02-15","2026-01-05","2026-02-15",
        "2025-12-01","2026-03-01",
    ],
})
