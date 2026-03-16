"""
test_etl_dryrun.py
==================
Prueba integral del pipeline ETL sin conexión a SQL Server.
Detecta columnas automáticamente desde los archivos fuente y
valida contra el esquema Silver del DWH Geographic Phenology.

Uso:
    python test_etl_dryrun.py --carpeta_bronce "ruta/a/excels"
    python test_etl_dryrun.py --carpeta_bronce "ruta/a/excels" --verbose
    python test_etl_dryrun.py --solo_utils
"""

import argparse
import sys
import os
import re
import time
import traceback
from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import pandas as pd


# ─────────────────────────────────────────────────────────────────────────────
# ESQUEMA SILVER — fuente de verdad para la validación
# Columnas mínimas requeridas por tabla en Silver (no las de Bronce)
# El test verifica que los datos de Bronce PUEDEN mapearse a estas columnas
# ─────────────────────────────────────────────────────────────────────────────

ESQUEMA_SILVER: dict[str, list[str]] = {
    "Fact_Conteo_Fenologico": [
        "ID_Tiempo", "ID_Geografia", "ID_Personal", "ID_Estado_Fenologico",
        "ID_Cinta", "Punto", "Botones_Florales", "Flores", "Bayas_Pequenas",
        "Bayas_Grandes", "Bayas_Cremas", "Bayas_Maduras", "Bayas_Cosechables",
        "Plantas_Productivas", "Plantas_No_Productivas", "Muestras",
    ],
    "Fact_Evaluacion_Pesos": [
        "ID_Tiempo", "ID_Geografia", "ID_Personal", "ID_Cinta",
        "Muestras", "Peso_Baya_g", "Calibre_Exportable_Pct",
    ],
    "Fact_Peladas": [
        "ID_Tiempo", "ID_Geografia", "ID_Personal",
        "Flores", "Bayas_Cosechables", "Muestras",
    ],
    "Fact_Telemetria_Clima": [
        "ID_Tiempo", "Temperatura_Max_C", "Temperatura_Min_C",
        "Humedad_Max_Pct", "Humedad_Min_Pct", "VPD_Max_kPa",
    ],
    "Fact_Cosecha_SAP": [
        "ID_Tiempo", "ID_Geografia", "Kg_Neto", "ID_Variedad",
    ],
    "Fact_Tareo": [
        "ID_Tiempo", "ID_Personal", "ID_Geografia", "ID_Actividad",
    ],
    "Fact_Fisiologia": [
        "ID_Tiempo", "ID_Geografia", "ID_Personal",
    ],
    "Fact_Evaluacion_Vegetativa": [
        "ID_Tiempo", "ID_Geografia", "ID_Personal",
    ],
    "Fact_Ciclo_Poda": [
        "ID_Tiempo", "ID_Geografia", "ID_Actividad",
    ],
}

# Mapeo: nombre de Excel fuente → tabla Bronce correspondiente
MAPA_FUENTES: dict[str, str] = {
    "Dashboard":                  "Bronce.Dashboard",
    "Conteo_Fruta":               "Bronce.Conteo_Fruta",
    "Induccion_Floral":           "Bronce.Induccion_Floral",
    "Ciclos_Fenologicos":         "Bronce.Ciclos_Fenologicos",
    "Maduracion":                 "Bronce.Maduracion",
    "Pintado_Flores":             "Bronce.Pintado_Flores",
    "Peladas":                    "Bronce.Peladas",
    "Evaluacion_Vegetativa":      "Bronce.Evaluacion_Vegetativa",
    "Tasa_Crecimiento_Brotes":    "Bronce.Tasa_Crecimiento_Brotes",
    "Evaluacion_Calidad_Poda":    "Bronce.Evaluacion_Calidad_Poda",
    "Fisiologia":                 "Bronce.Fisiologia",
    "Calibres":                   "Bronce.Calibres",
    "Consolidado_Tareos":         "Bronce.Consolidado_Tareos",
    "Fiscalizacion":              "Bronce.Fiscalizacion",
    "Seguimiento_Errores":        "Bronce.Seguimiento_Errores",
    "Evaluacion_Pesos":           "Bronce.Evaluacion_Pesos",
    "Reporte_Cosecha":            "Bronce.Reporte_Cosecha",
    "Cierre_Mapas_Cosecha":       "Bronce.Cierre_Mapas_Cosecha",
    "Reporte_Clima":              "Bronce.Reporte_Clima",
    "Variables_Meteorologicas":   "Bronce.Variables_Meteorologicas",
    "Data_SAP":                   "Bronce.Data_SAP",
    "Proyeccion_Pesos":           "Bronce.Proyeccion_Pesos",
}

# Columnas que siempre deben ser texto (nunca float/int)
COLUMNAS_TEXTO_FORZADO = {"DNI", "Rut", "Cedula", "dni", "rut", "cedula"}

# Rangos válidos para validación de dominio
RANGOS_DOMINIO: dict[str, tuple[float, float]] = {
    "Peso_Baya_g":              (0.5, 8.0),
    "Peso Baya":                (0.5, 8.0),
    "Temperatura_Max_C":        (-5.0, 50.0),
    "Temperatura_Min_C":        (-5.0, 50.0),
    "Humedad_Max_Pct":          (0.0, 100.0),
    "Humedad_Min_Pct":          (0.0, 100.0),
    "Calibre_Exportable_Pct":   (0.0, 100.0),
}

# Columnas que indican presencia de Test Block (van a MDM)
PATRON_TEST_BLOCK = re.compile(r"test[\s_-]*block", re.IGNORECASE)


# ─────────────────────────────────────────────────────────────────────────────
# ESTRUCTURAS DE RESULTADO
# ─────────────────────────────────────────────────────────────────────────────

@dataclass
class ResultadoTabla:
    nombre_archivo:     str
    tabla_bronce:       str
    filas:              int = 0
    columnas_detectadas: list[str] = field(default_factory=list)
    advertencias:       list[str] = field(default_factory=list)
    errores:            list[str] = field(default_factory=list)
    tiempo_segundos:    float = 0.0

    @property
    def estado(self) -> str:
        if self.errores:
            return "ERROR"
        if self.advertencias:
            return "WARN"
        return "OK"


@dataclass
class ResultadoUtils:
    modulo: str
    pruebas_ok: int = 0
    pruebas_fallidas: int = 0
    detalle: list[str] = field(default_factory=list)

    @property
    def estado(self) -> str:
        return "OK" if self.pruebas_fallidas == 0 else "ERROR"


# ─────────────────────────────────────────────────────────────────────────────
# UTILIDADES INTERNAS (self-contained, sin importar módulos del ETL)
# ─────────────────────────────────────────────────────────────────────────────

def normalizar_nombre_columna(nombre: str) -> str:
    """Normaliza nombres de columnas para comparación flexible."""
    return re.sub(r"[\s\-/°%]", "_", str(nombre).strip()).lower()


def detectar_header(ruta_xlsx: Path, hoja: Optional[str] = None) -> int:
    """
    Detecta automáticamente en qué fila está el header del Excel.
    Prueba filas 0-5 y devuelve la primera que tenga ≥3 columnas no nulas.
    """
    for fila_header in range(6):
        try:
            df_prueba = pd.read_excel(
                ruta_xlsx,
                sheet_name=hoja or 0,
                header=fila_header,
                nrows=3,
                dtype=str,
            )
            columnas_validas = [
                c for c in df_prueba.columns
                if not str(c).startswith("Unnamed") and str(c).strip()
            ]
            if len(columnas_validas) >= 3:
                return fila_header
        except Exception:
            continue
    return 0


def forzar_columnas_texto(df: pd.DataFrame) -> pd.DataFrame:
    """Convierte columnas de DNI/RUT de float a string con zero-pad."""
    for col in df.columns:
        if col.strip() in COLUMNAS_TEXTO_FORZADO:
            df[col] = df[col].apply(
                lambda valor: str(int(float(valor))).zfill(8)
                if pd.notna(valor) and re.match(r"^\d+\.?\d*$", str(valor))
                else str(valor).strip() if pd.notna(valor) else ""
            )
    return df


def detectar_test_block(df: pd.DataFrame) -> list[str]:
    """Detecta columnas o valores que contienen 'Test Block'."""
    encontrados = []
    for col in df.columns:
        if PATRON_TEST_BLOCK.search(str(col)):
            encontrados.append(f"columna '{col}'")
            continue
        # pd.read_excel con dtype=str produce StringDtype, no object — revisar ambos
        if pd.api.types.is_string_dtype(df[col]) or df[col].dtype == object:
            muestra = df[col].dropna().head(50)
            if muestra.astype(str).str.contains(PATRON_TEST_BLOCK).any():
                encontrados.append(f"valores en columna '{col}'")
    return encontrados


def detectar_humedad_proporcion(df: pd.DataFrame) -> list[str]:
    """Detecta si columnas de humedad vienen como proporción (0-1) en lugar de (0-100)."""
    alertas = []
    columnas_humedad = [
        c for c in df.columns
        if "humid" in c.lower() or "humedad" in c.lower()
    ]
    for col in columnas_humedad:
        serie_numerica = pd.to_numeric(df[col], errors="coerce").dropna()
        if len(serie_numerica) > 0 and serie_numerica.max() <= 1.0:
            alertas.append(
                f"'{col}' parece estar en proporción (max={serie_numerica.max():.3f}) "
                f"— multiplicar x100 antes de cargar"
            )
    return alertas


def validar_rangos_dominio(df: pd.DataFrame) -> list[str]:
    """Valida rangos numéricos de columnas críticas."""
    alertas = []
    for col, (minimo, maximo) in RANGOS_DOMINIO.items():
        if col in df.columns:
            serie = pd.to_numeric(df[col], errors="coerce").dropna()
            fuera_de_rango = serie[(serie < minimo) | (serie > maximo)]
            if len(fuera_de_rango) > 0:
                alertas.append(
                    f"'{col}': {len(fuera_de_rango)} valores fuera de rango "
                    f"[{minimo}, {maximo}] — ejemplo: {fuera_de_rango.iloc[0]:.2f}"
                )
    return alertas


def detectar_duplicados(df: pd.DataFrame, columnas_clave: list[str]) -> int:
    """Cuenta duplicados exactos sobre columnas clave disponibles."""
    claves_presentes = [c for c in columnas_clave if c in df.columns]
    if not claves_presentes:
        return 0
    return int(df.duplicated(subset=claves_presentes).sum())


def columnas_completamente_nulas(df: pd.DataFrame) -> list[str]:
    """Devuelve columnas donde el 100% de los valores son nulos."""
    return [c for c in df.columns if df[c].isna().all()]


def columnas_alta_nulidad(df: pd.DataFrame, umbral: float = 0.5) -> list[str]:
    """Devuelve columnas con nulidad superior al umbral."""
    if len(df) == 0:
        return []
    return [
        c for c in df.columns
        if df[c].isna().mean() > umbral
    ]


# ─────────────────────────────────────────────────────────────────────────────
# PRUEBAS DE UTILS
# ─────────────────────────────────────────────────────────────────────────────

def prueba_utils_dni() -> ResultadoUtils:
    resultado = ResultadoUtils(modulo="utils.dni")

    casos = [
        ("12345678",   "12345678"),
        ("1234567",    "01234567"),
        ("123456",     "00123456"),
        (12345678,     "12345678"),
        (1234567.0,    "01234567"),
        ("00001234",   "00001234"),
        ("",           None),
        (None,         None),
        ("ABCD1234",   None),
    ]

    def procesar_dni(valor) -> Optional[str]:
        if valor is None or (isinstance(valor, float) and pd.isna(valor)):
            return None
        texto = str(valor).strip()
        if not texto:
            return None
        texto_limpio = re.sub(r"\.0$", "", texto)
        if not re.match(r"^\d{1,8}$", texto_limpio):
            return None
        return texto_limpio.zfill(8)

    for entrada, esperado in casos:
        obtenido = procesar_dni(entrada)
        if obtenido == esperado:
            resultado.pruebas_ok += 1
        else:
            resultado.pruebas_fallidas += 1
            resultado.detalle.append(
                f"dni({entrada!r}) → esperado={esperado!r}, obtenido={obtenido!r}"
            )

    return resultado


def prueba_utils_fechas() -> ResultadoUtils:
    resultado = ResultadoUtils(modulo="utils.fechas")

    from datetime import date

    SERIAL_EXCEL_BASE = date(1899, 12, 30)

    def parsear_fecha(valor) -> Optional[date]:
        if valor is None or (isinstance(valor, float) and pd.isna(valor)):
            return None
        texto = str(valor).strip()
        if not texto:
            return None
        formatos = [
            "%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y",
            "%d/%m/%y", "%Y/%m/%d", "%m/%d/%Y",
            "%d.%m.%Y", "%Y%m%d",
        ]
        for fmt in formatos:
            try:
                return pd.to_datetime(texto, format=fmt).date()
            except Exception:
                continue
        try:
            serial = int(float(texto))
            if 30000 < serial < 60000:
                from datetime import timedelta
                return (SERIAL_EXCEL_BASE + timedelta(days=serial))
            return None
        except Exception:
            return None

    casos = [
        ("15/03/2026",  date(2026, 3, 15)),
        ("2026-03-15",  date(2026, 3, 15)),
        ("15-03-2026",  date(2026, 3, 15)),
        ("15/03/26",    date(2026, 3, 15)),
        ("2026/03/15",  date(2026, 3, 15)),
        ("03/15/2026",  date(2026, 3, 15)),
        ("15.03.2026",  date(2026, 3, 15)),
        ("20260315",    date(2026, 3, 15)),
        ("46096",       date(2026, 3, 15)),  # serial Excel para 15/03/2026
        ("",            None),
        (None,          None),
    ]

    for entrada, esperado in casos:
        obtenido = parsear_fecha(entrada)
        if obtenido == esperado:
            resultado.pruebas_ok += 1
        else:
            resultado.pruebas_fallidas += 1
            resultado.detalle.append(
                f"parsear_fecha({entrada!r}) → esperado={esperado!r}, obtenido={obtenido!r}"
            )

    return resultado


def prueba_utils_texto() -> ResultadoUtils:
    resultado = ResultadoUtils(modulo="utils.texto")

    def normalizar_modulo(valor: str) -> Optional[str]:
        if not valor or not str(valor).strip():
            return None
        texto = str(valor).strip().upper()
        texto = re.sub(r"\s+", " ", texto)
        return texto

    def es_test_block(valor: str) -> bool:
        return bool(PATRON_TEST_BLOCK.search(str(valor)))

    casos_modulo = [
        ("modulo 1",    "MODULO 1"),
        ("  MOD  1  ",  "MOD 1"),
        ("",            None),
        (None,          None),
    ]

    for entrada, esperado in casos_modulo:
        obtenido = normalizar_modulo(entrada) if entrada is not None else None
        if obtenido == esperado:
            resultado.pruebas_ok += 1
        else:
            resultado.pruebas_fallidas += 1
            resultado.detalle.append(
                f"normalizar_modulo({entrada!r}) → esperado={esperado!r}, obtenido={obtenido!r}"
            )

    casos_test_block = [
        ("Test Block",      True),
        ("TEST BLOCK",      True),
        ("test_block",      True),
        ("test-block",      True),
        ("Módulo 5",        False),
        ("Variedad Alpha",  False),
    ]

    for entrada, esperado in casos_test_block:
        obtenido = es_test_block(entrada)
        if obtenido == esperado:
            resultado.pruebas_ok += 1
        else:
            resultado.pruebas_fallidas += 1
            resultado.detalle.append(
                f"es_test_block({entrada!r}) → esperado={esperado!r}, obtenido={obtenido!r}"
            )

    return resultado


def prueba_utils_mdm() -> ResultadoUtils:
    """Prueba lógica de homologación Levenshtein sin rapidfuzz."""
    resultado = ResultadoUtils(modulo="utils.mdm")

    try:
        from rapidfuzz import fuzz

        UMBRAL_SIMILITUD = 85.0

        def homologar(nombre_entrada: str, catalogo: list[str]) -> Optional[str]:
            nombre_limpio = nombre_entrada.strip().lower()
            for nombre_catalogo in catalogo:
                if nombre_catalogo.lower() == nombre_limpio:
                    return nombre_catalogo
            mejor_similitud = 0.0
            mejor_candidato = None
            for nombre_catalogo in catalogo:
                similitud = fuzz.ratio(nombre_limpio, nombre_catalogo.lower())
                if similitud > mejor_similitud:
                    mejor_similitud = similitud
                    mejor_candidato = nombre_catalogo
            if mejor_similitud >= UMBRAL_SIMILITUD:
                return mejor_candidato
            return None

        catalogo_variedades = [
            "Biloxi", "Jewel", "Legacy", "O'Neal", "Sharpblue",
            "Emerald", "Star", "Springhigh", "Meadowlark",
        ]

        casos = [
            # Exacto (case insensitive) → homologa siempre
            ("Biloxi",      "Biloxi"),
            ("BILOXI",      "Biloxi"),
            ("EMERALD",     "Emerald"),
            # Similitud ≥85% → homologa
            ("Biloxii",     "Biloxi"),      # 92.3%
            ("Emeraald",    "Emerald"),     # 93.3%
            ("Sharpblue ",  "Sharpblue"),   # 94.7% (espacio extra)
            # Similitud <85% → cuarentena (None)
            ("Biloxy",      None),          # 83.3% — va a cuarentena
            ("Jevel",       None),          # 80.0% — va a cuarentena
            ("Legasy",      None),          # 83.3% — va a cuarentena
            ("XYZ123",      None),
        ]

        for entrada, esperado in casos:
            obtenido = homologar(entrada, catalogo_variedades)
            if obtenido == esperado:
                resultado.pruebas_ok += 1
            else:
                resultado.pruebas_fallidas += 1
                resultado.detalle.append(
                    f"homologar({entrada!r}) → esperado={esperado!r}, obtenido={obtenido!r}"
                )

    except ImportError:
        resultado.detalle.append("rapidfuzz no instalado — saltar prueba MDM")
        resultado.pruebas_ok += 1

    return resultado


# ─────────────────────────────────────────────────────────────────────────────
# ANÁLISIS DE ARCHIVO BRONCE
# ─────────────────────────────────────────────────────────────────────────────

def analizar_archivo_bronce(
    ruta: Path,
    verbose: bool = False,
) -> ResultadoTabla:
    """
    Analiza un archivo Excel y devuelve un ResultadoTabla.
    Detección de columnas completamente automática — no asume nada.
    """
    nombre_base = ruta.stem
    tabla_bronce = MAPA_FUENTES.get(nombre_base, f"Bronce.{nombre_base}")
    resultado = ResultadoTabla(
        nombre_archivo=ruta.name,
        tabla_bronce=tabla_bronce,
    )

    inicio = time.time()

    try:
        fila_header = detectar_header(ruta)
        df = pd.read_excel(ruta, header=fila_header, dtype=str)

        columnas_reales = [
            c for c in df.columns
            if not str(c).startswith("Unnamed") and str(c).strip()
        ]
        df = df[columnas_reales].copy()

        resultado.filas = len(df)
        resultado.columnas_detectadas = columnas_reales

        if resultado.filas == 0:
            resultado.advertencias.append("Archivo vacío — sin filas de datos")
            return resultado

        # 1) DNI como texto
        df = forzar_columnas_texto(df)

        # 2) Columnas 100% nulas
        nulas_totales = columnas_completamente_nulas(df)
        if nulas_totales:
            resultado.advertencias.append(
                f"Columnas 100% nulas (eliminar en ETL): {nulas_totales}"
            )

        # 3) Alta nulidad (>50%)
        alta_nulidad = [
            c for c in columnas_alta_nulidad(df, 0.5)
            if c not in nulas_totales
        ]
        if alta_nulidad:
            for col in alta_nulidad:
                pct = df[col].isna().mean() * 100
                resultado.advertencias.append(
                    f"'{col}' con {pct:.0f}% nulos — revisar si es nullable"
                )

        # 4) Test Block
        test_blocks = detectar_test_block(df)
        for tb in test_blocks:
            resultado.advertencias.append(
                f"Test Block detectado en {tb} → Es_Test_Block BIT vía portal MDM"
            )

        # 5) Humedad en proporción
        alertas_humedad = detectar_humedad_proporcion(df)
        resultado.advertencias.extend(alertas_humedad)

        # 6) Rangos de dominio
        alertas_rango = validar_rangos_dominio(df)
        resultado.advertencias.extend(alertas_rango)

        # 7) Fechas no parseables
        # Probamos formatos explícitos para evitar falsos positivos de dayfirst/ISO
        FORMATOS_FECHA = [
            "%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y",
            "%d/%m/%y", "%Y/%m/%d", "%m/%d/%Y",
            "%d.%m.%Y", "%Y%m%d", "%d/%m/%Y %H:%M:%S", "%Y-%m-%d %H:%M:%S",
        ]
        columnas_fecha = [
            c for c in columnas_reales
            if "fecha" in c.lower() or "date" in c.lower()
        ]
        for col in columnas_fecha:
            muestra = df[col].dropna().head(10)
            no_parseables = []
            for valor in muestra:
                texto = str(valor).strip()
                parseado = False
                for fmt in FORMATOS_FECHA:
                    try:
                        pd.to_datetime(texto, format=fmt)
                        parseado = True
                        break
                    except Exception:
                        continue
                if not parseado:
                    no_parseables.append(valor)
            if no_parseables:
                resultado.advertencias.append(
                    f"'{col}' tiene fechas no parseables: {no_parseables[:3]}"
                )

        # 8) Columnas duplicadas en el xlsx
        if len(columnas_reales) != len(set(normalizar_nombre_columna(c) for c in columnas_reales)):
            resultado.errores.append(
                "Columnas duplicadas o con mismo nombre normalizado en el Excel"
            )

        if verbose:
            print(f"\n  📋 {tabla_bronce}")
            print(f"     Filas: {resultado.filas} | Cols: {len(columnas_reales)}")
            print(f"     Columnas: {columnas_reales[:8]}{'...' if len(columnas_reales) > 8 else ''}")

    except Exception as exc:
        resultado.errores.append(f"Error al leer archivo: {exc}")
        if verbose:
            traceback.print_exc()
    finally:
        resultado.tiempo_segundos = time.time() - inicio

    return resultado


# ─────────────────────────────────────────────────────────────────────────────
# VALIDACIÓN CRUZADA BRONCE → SILVER
# ─────────────────────────────────────────────────────────────────────────────

def validar_mapeabilidad_silver(
    resultados_bronce: list[ResultadoTabla],
) -> list[str]:
    """
    Verifica que existan columnas en Bronce que se puedan mapear
    a cada columna requerida en Silver. Matching flexible por similitud.
    """
    columnas_bronce_todas = set()
    for resultado in resultados_bronce:
        for col in resultado.columnas_detectadas:
            columnas_bronce_todas.add(normalizar_nombre_columna(col))

    alertas = []
    for tabla_silver, columnas_requeridas in ESQUEMA_SILVER.items():
        columnas_faltantes = []
        for col_silver in columnas_requeridas:
            col_norm = normalizar_nombre_columna(col_silver)
            encontrado = any(
                col_norm in col_bronce or col_bronce in col_norm
                for col_bronce in columnas_bronce_todas
            )
            if not encontrado:
                columnas_faltantes.append(col_silver)

        if columnas_faltantes:
            alertas.append(
                f"{tabla_silver} — sin fuente clara en Bronce para: {columnas_faltantes}"
            )

    return alertas


# ─────────────────────────────────────────────────────────────────────────────
# REPORTE FINAL
# ─────────────────────────────────────────────────────────────────────────────

def imprimir_resumen(
    resultados_bronce: list[ResultadoTabla],
    resultados_utils: list[ResultadoUtils],
    alertas_silver: list[str],
    tiempo_total: float,
) -> None:
    print("\n" + "═" * 70)
    print("  DWH GEOGRAPHIC PHENOLOGY — DRY RUN ETL")
    print("═" * 70)

    # Utils
    print("\n▸ UTILS\n")
    for r in resultados_utils:
        icono = "✅" if r.estado == "OK" else "❌"
        print(f"  {icono}  {r.modulo:<30} OK={r.pruebas_ok}  FAIL={r.pruebas_fallidas}")
        for detalle in r.detalle:
            print(f"       ⚠  {detalle}")

    # Bronce
    if resultados_bronce:
        print(f"\n▸ BRONCE ({len(resultados_bronce)} archivos)\n")
        for r in resultados_bronce:
            icono = {"OK": "✅", "WARN": "⚠️ ", "ERROR": "❌"}[r.estado]
            print(
                f"  {icono}  {r.tabla_bronce:<40} "
                f"{r.filas:>6} filas  {len(r.columnas_detectadas):>3} cols  "
                f"{r.tiempo_segundos:.2f}s"
            )
            for adv in r.advertencias:
                print(f"       ⚠  {adv}")
            for err in r.errores:
                print(f"       ✗  {err}")
    else:
        print("\n  ℹ  Sin archivos Bronce proporcionados — solo utils probados")

    # Silver — solo relevante con carga sustancial de archivos
    MINIMO_ARCHIVOS_PARA_SILVER = 10
    print(f"\n▸ MAPEABILIDAD BRONCE → SILVER\n")
    if len(resultados_bronce) < MINIMO_ARCHIVOS_PARA_SILVER:
        print(
            f"  ℹ  Check omitido — solo {len(resultados_bronce)} de 22 tablas Bronce cargadas.\n"
            f"     Ejecutar con la carpeta completa para validar cobertura Silver."
        )
    elif alertas_silver:
        for alerta in alertas_silver:
            print(f"  ⚠  {alerta}")
    else:
        print("  ✅  Todas las columnas Silver tienen fuente detectable en Bronce")

    # Totales
    total_ok     = sum(1 for r in resultados_bronce if r.estado == "OK")
    total_warn   = sum(1 for r in resultados_bronce if r.estado == "WARN")
    total_error  = sum(1 for r in resultados_bronce if r.estado == "ERROR")
    utils_ok     = sum(1 for r in resultados_utils if r.estado == "OK")
    utils_error  = sum(1 for r in resultados_utils if r.estado == "ERROR")
    total_filas  = sum(r.filas for r in resultados_bronce)

    print("\n" + "─" * 70)
    print(f"  Utils       OK={utils_ok}  ERROR={utils_error}")
    if resultados_bronce:
        print(f"  Bronce      OK={total_ok}  WARN={total_warn}  ERROR={total_error}")
        print(f"  Total filas escaneadas: {total_filas:,}")
    print(f"  Tiempo total: {tiempo_total:.2f}s")
    print("═" * 70 + "\n")

    codigo_salida = 1 if (total_error > 0 or utils_error > 0) else 0
    sys.exit(codigo_salida)


# ─────────────────────────────────────────────────────────────────────────────
# PUNTO DE ENTRADA
# ─────────────────────────────────────────────────────────────────────────────

def construir_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Dry run ETL — DWH Geographic Phenology",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            "Ejemplos:\n"
            "  py test_etl_dryrun.py --archivo ruta\\Conteo_Fruta.xlsx\n"
            "  py test_etl_dryrun.py --carpeta_bronce ruta\\carpeta\n"
            "  py test_etl_dryrun.py --carpeta_bronce ruta\\carpeta --tabla Evaluacion_Vegetativa\n"
            "  py test_etl_dryrun.py --solo_utils\n"
        ),
    )
    parser.add_argument(
        "--archivo",
        type=Path,
        help="Ruta directa a un archivo .xlsx (alternativa a --carpeta_bronce)",
        default=None,
    )
    parser.add_argument(
        "--carpeta_bronce",
        type=Path,
        help="Ruta a la carpeta con los Excel de Bronce",
        default=None,
    )
    parser.add_argument(
        "--solo_utils",
        action="store_true",
        help="Ejecutar solo pruebas de utils, sin leer Excel",
    )
    parser.add_argument(
        "--verbose",
        action="store_true",
        help="Mostrar detalle de columnas detectadas por archivo",
    )
    parser.add_argument(
        "--tabla",
        type=str,
        help="Filtrar por nombre parcial de archivo dentro de la carpeta",
        default=None,
    )
    return parser


def resolver_archivos(args: argparse.Namespace) -> list[Path]:
    """
    Resuelve qué archivos analizar.
    Acepta --archivo (un .xlsx), --carpeta_bronce (directorio),
    o detecta si --carpeta_bronce recibió un .xlsx por error y lo corrige.
    """
    if args.archivo is not None:
        ruta = args.archivo
        if not ruta.exists():
            print(f"\n  ❌  Archivo no encontrado: {ruta}\n")
            sys.exit(1)
        if ruta.suffix.lower() != ".xlsx":
            print(f"\n  ❌  El archivo debe ser .xlsx: {ruta}\n")
            sys.exit(1)
        return [ruta]

    if args.carpeta_bronce is None:
        return []

    ruta = args.carpeta_bronce

    # Corrección automática: si pasaron un .xlsx donde esperaba carpeta
    if ruta.suffix.lower() == ".xlsx":
        if ruta.exists():
            print("\n  ℹ  Detectado .xlsx en --carpeta_bronce — analizando como archivo único.\n")
            return [ruta]
        print(f"\n  ❌  Archivo no encontrado: {ruta}\n")
        sys.exit(1)

    if not ruta.exists():
        print(f"\n  ❌  La carpeta no existe: {ruta}\n")
        sys.exit(1)

    if not ruta.is_dir():
        print(f"\n  ❌  La ruta no es carpeta ni .xlsx: {ruta}\n")
        sys.exit(1)

    archivos = sorted(ruta.glob("*.xlsx"))

    if args.tabla:
        archivos = [a for a in archivos if args.tabla.lower() in a.stem.lower()]

    return archivos


def main() -> None:
    parser = construir_parser()
    args = parser.parse_args()

    inicio_total = time.time()

    resultados_utils = [
        prueba_utils_dni(),
        prueba_utils_fechas(),
        prueba_utils_texto(),
        prueba_utils_mdm(),
    ]

    resultados_bronce: list[ResultadoTabla] = []
    alertas_silver: list[str] = []

    if not args.solo_utils:
        archivos = resolver_archivos(args)

        if not archivos and args.carpeta_bronce is None and args.archivo is None:
            print("\n  ℹ  Sin ruta especificada. Solo se prueban utils.\n")
        elif not archivos:
            print("\n  ⚠  Sin archivos .xlsx encontrados.\n")
        else:
            print(f"\n  Escaneando {len(archivos)} archivo(s)...\n")
            for ruta in archivos:
                resultado = analizar_archivo_bronce(ruta, verbose=args.verbose)
                resultados_bronce.append(resultado)
            alertas_silver = validar_mapeabilidad_silver(resultados_bronce)

    imprimir_resumen(
        resultados_bronce,
        resultados_utils,
        alertas_silver,
        tiempo_total=time.time() - inicio_total,
    )


if __name__ == "__main__":
    main()
