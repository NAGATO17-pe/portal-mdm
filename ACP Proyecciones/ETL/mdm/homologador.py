"""
homologador.py
==============
HomologaciÃ³n de texto libre via Levenshtein (rapidfuzz).
Resuelve aliases de variedades contra MDM.Diccionario_Homologacion.

Flujo:
  1. Match exacto en el diccionario (Aprobado_Por IS NOT NULL)
  2. Si no â†’ Levenshtein contra valores canÃ³nicos aprobados
  3. score >= 0.85 â†’ homologaciÃ³n automÃ¡tica
  4. score < 0.85 â†’ MDM.Cuarentena para revisiÃ³n humana

DDL v2 â€” MDM.Diccionario_Homologacion:
  Columnas reales: ID_Homologacion, Texto_Crudo, Valor_Canonico,
    Tabla_Origen, Campo_Origen, Score_Levenshtein,
    Aprobado_Por NVARCHAR(20), Fecha_Aprobacion, Veces_Aplicado
  NO existe columna Aprobado BIT â€” se usa Aprobado_Por para distinguir
    aprobados  : Aprobado_Por IS NOT NULL AND Aprobado_Por != 'PENDIENTE'
    pendientes : Aprobado_Por = 'PENDIENTE' o IS NULL
"""

import pandas as pd
from rapidfuzz import fuzz, process
from sqlalchemy.engine import Engine
from sqlalchemy import text
from datetime import datetime

from utils.texto import normalizar_variedad, quitar_tildes


UMBRAL_AUTO = 0.99


def _clave_variedad(valor: str | None) -> str | None:
    if valor is None:
        return None
    valor_normalizado = normalizar_variedad(valor)
    if not valor_normalizado:
        return None
    return quitar_tildes(valor_normalizado).lower()


def cargar_diccionario(engine: Engine,
                        tabla_origen: str) -> pd.DataFrame:
    """
    Carga entradas aprobadas del diccionario para una tabla origen.
    Aprobado = Aprobado_Por IS NOT NULL AND != 'PENDIENTE'
    """
    with engine.connect() as conexion:
        resultado = conexion.execute(text("""
            SELECT
                Texto_Crudo,
                Valor_Canonico,
                Score_Levenshtein,
                Veces_Aplicado
            FROM MDM.Diccionario_Homologacion
            WHERE Tabla_Origen  = :tabla_origen
              AND Aprobado_Por IS NOT NULL
              AND Aprobado_Por != 'PENDIENTE'
        """), {'tabla_origen': tabla_origen})

        df = pd.DataFrame(resultado.fetchall(), columns=resultado.keys())
        if df.empty:
            return df
        df['Clave_Texto_Crudo'] = df['Texto_Crudo'].map(_clave_variedad)
        df['Clave_Valor_Canonico'] = df['Valor_Canonico'].map(_clave_variedad)
        return df


def cargar_catalogo_variedades(engine: Engine) -> pd.DataFrame:
    """
    Carga el catalogo canonico de variedades.
    Fuente de verdad para homologacion inicial:
    - MDM.Catalogo_Variedades activas
    - fallback a Silver.Dim_Variedad
    """
    with engine.connect() as conexion:
        resultado = conexion.execute(text("""
            SELECT Nombre_Canonico AS Valor_Canonico
            FROM MDM.Catalogo_Variedades
            WHERE Es_Activa = 1

            UNION

            SELECT Nombre_Variedad AS Valor_Canonico
            FROM Silver.Dim_Variedad
        """))

        df = pd.DataFrame(resultado.fetchall(), columns=resultado.keys())
        if df.empty:
            return df

        df['Clave_Valor_Canonico'] = df['Valor_Canonico'].map(_clave_variedad)
        df = df.dropna(subset=['Clave_Valor_Canonico'])
        df = df.drop_duplicates(subset=['Clave_Valor_Canonico'], keep='first')
        return df


def buscar_match_exacto(valor: str,
                         diccionario: pd.DataFrame) -> str | None:
    if diccionario.empty:
        return None
    clave = _clave_variedad(valor)
    if clave is None:
        return None
    coincidencia = diccionario[diccionario['Clave_Texto_Crudo'] == clave]
    return coincidencia.iloc[0]['Valor_Canonico'] if not coincidencia.empty else None


def buscar_match_catalogo(valor: str,
                           catalogo: pd.DataFrame) -> str | None:
    if catalogo.empty:
        return None

    clave = _clave_variedad(valor)
    if clave is None:
        return None

    coincidencia = catalogo[catalogo['Clave_Valor_Canonico'] == clave]
    return coincidencia.iloc[0]['Valor_Canonico'] if not coincidencia.empty else None


def buscar_match_levenshtein(valor: str,
                              catalogo: pd.DataFrame) -> tuple[str | None, float]:
    if catalogo.empty:
        return None, 0.0

    valor_normalizado = normalizar_variedad(valor)
    if not valor_normalizado:
        return None, 0.0

    candidatos = catalogo['Valor_Canonico'].tolist()
    resultado  = process.extractOne(valor_normalizado, candidatos, scorer=fuzz.token_sort_ratio)

    if resultado is None:
        return None, 0.0

    match, score, _ = resultado
    score_norm = score / 100.0

    return (match, score_norm) if score_norm >= UMBRAL_AUTO else (None, score_norm)


def buscar_sugerencia_levenshtein(valor: str,
                                   catalogo: pd.DataFrame) -> tuple[str | None, float]:
    if catalogo.empty:
        return None, 0.0

    valor_normalizado = normalizar_variedad(valor)
    if not valor_normalizado:
        return None, 0.0

    candidatos = catalogo['Valor_Canonico'].tolist()
    resultado = process.extractOne(valor_normalizado, candidatos, scorer=fuzz.token_sort_ratio)
    if resultado is None:
        return None, 0.0

    match, score, _ = resultado
    return match, score / 100.0


def registrar_homologacion(engine: Engine,
                            tabla_origen: str,
                            campo_origen: str,
                            texto_crudo: str,
                            valor_canonico: str,
                            score: float,
                            aprobado: bool = True) -> None:
    """
    Registra o actualiza una entrada en MDM.Diccionario_Homologacion.
    aprobado=True  â†’ Aprobado_Por = 'SISTEMA'
    aprobado=False â†’ Aprobado_Por = 'PENDIENTE' (requiere revisiÃ³n humana)
    """
    aprobado_por = 'SISTEMA' if aprobado else 'PENDIENTE'

    with engine.begin() as conexion:
        existe = conexion.execute(text("""
            SELECT COUNT(*)
            FROM MDM.Diccionario_Homologacion
            WHERE Tabla_Origen = :tabla_origen
              AND Texto_Crudo  = :texto_crudo
        """), {
            'tabla_origen': tabla_origen,
            'texto_crudo':  texto_crudo,
        }).scalar()

        if existe:
            if aprobado:
                conexion.execute(text("""
                    UPDATE MDM.Diccionario_Homologacion
                    SET Valor_Canonico     = :valor_canonico,
                        Score_Levenshtein  = :score,
                        Aprobado_Por       = :aprobado_por,
                        Fecha_Aprobacion   = :fecha_aprobacion,
                        Veces_Aplicado     = Veces_Aplicado + 1
                    WHERE Tabla_Origen = :tabla_origen
                      AND Texto_Crudo  = :texto_crudo
                """), {
                    'tabla_origen':      tabla_origen,
                    'texto_crudo':       texto_crudo,
                    'valor_canonico':    valor_canonico,
                    'score':             round(score, 4),
                    'aprobado_por':      aprobado_por,
                    'fecha_aprobacion':  datetime.now(),
                })
            else:
                conexion.execute(text("""
                    UPDATE MDM.Diccionario_Homologacion
                    SET Valor_Canonico    = COALESCE(Valor_Canonico, :valor_canonico),
                        Score_Levenshtein = :score,
                        Veces_Aplicado    = Veces_Aplicado + 1
                    WHERE Tabla_Origen = :tabla_origen
                      AND Texto_Crudo  = :texto_crudo
                """), {
                    'tabla_origen':      tabla_origen,
                    'texto_crudo':       texto_crudo,
                    'valor_canonico':    valor_canonico,
                    'score':             round(score, 4),
                })
        else:
            conexion.execute(text("""
                INSERT INTO MDM.Diccionario_Homologacion (
                    Texto_Crudo, Valor_Canonico,
                    Tabla_Origen, Campo_Origen,
                    Score_Levenshtein,
                    Aprobado_Por, Fecha_Aprobacion,
                    Veces_Aplicado
                ) VALUES (
                    :texto_crudo, :valor_canonico,
                    :tabla_origen, :campo_origen,
                    :score,
                    :aprobado_por, :fecha_aprobacion,
                    1
                )
            """), {
                'texto_crudo':       texto_crudo,
                'valor_canonico':    valor_canonico,
                'tabla_origen':      tabla_origen,
                'campo_origen':      campo_origen,
                'score':             round(score, 4),
                'aprobado_por':      aprobado_por,
                'fecha_aprobacion':  datetime.now() if aprobado else None,
            })


def homologar_valor(valor: str | None,
                    tabla_origen: str,
                    campo_origen: str,
                    diccionario: pd.DataFrame,
                    catalogo: pd.DataFrame,
                    engine: Engine) -> tuple[str | None, str]:
    """
    Homologa un valor contra el diccionario canÃ³nico.
    Retorna (valor_homologado, estado):
      'EXACTO'      â†’ match exacto en diccionario aprobado
      'LEVENSHTEIN' â†’ similitud >= 0.85, auto-aprobado
      'CUARENTENA'  â†’ sin match â†’ revisiÃ³n humana
      'NULO'        â†’ valor vacÃ­o o None
    """
    if not valor or not str(valor).strip():
        return None, 'NULO'

    valor = str(valor).strip()

    canonico = buscar_match_exacto(valor, diccionario)
    if canonico:
        registrar_homologacion(engine, tabla_origen, campo_origen,
                               valor, canonico, 1.0, aprobado=True)
        return canonico, 'EXACTO'

    canonico = buscar_match_catalogo(valor, catalogo)
    if canonico:
        registrar_homologacion(engine, tabla_origen, campo_origen,
                               valor, canonico, 1.0, aprobado=True)
        return canonico, 'CATALOGO'

    canonico, score = buscar_match_levenshtein(valor, catalogo)
    if canonico:
        registrar_homologacion(engine, tabla_origen, campo_origen,
                               valor, canonico, score, aprobado=True)
        return canonico, 'LEVENSHTEIN'

    sugerencia, score = buscar_sugerencia_levenshtein(valor, catalogo)
    registrar_homologacion(engine, tabla_origen, campo_origen,
                           valor, sugerencia or normalizar_variedad(valor) or valor,
                           score, aprobado=False)
    return None, 'CUARENTENA'


def homologar_columna(df: pd.DataFrame,
                       columna_raw: str,
                       columna_destino: str,
                       tabla_origen: str,
                       engine: Engine,
                       columna_id_origen: str | None = None) -> tuple[pd.DataFrame, list[dict]]:
    """
    Homologa una columna completa del DataFrame.
    Retorna (df con columna_destino, lista de cuarentenas).
    """
    diccionario = cargar_diccionario(engine, tabla_origen)
    catalogo    = cargar_catalogo_variedades(engine)
    cuarentenas = []
    resultados  = []
    cache_resoluciones: dict[str, tuple[str | None, str]] = {}

    for _, fila in df.iterrows():
        valor = fila.get(columna_raw)
        clave_cache = _clave_variedad(valor)
        if clave_cache is None:
            valor_token = '' if valor is None else str(valor).strip().lower()
            clave_cache = f'__RAW__::{valor_token}'

        if clave_cache in cache_resoluciones:
            homologado, estado = cache_resoluciones[clave_cache]
        else:
            homologado, estado = homologar_valor(
                valor, tabla_origen, columna_raw, diccionario, catalogo, engine
            )
            cache_resoluciones[clave_cache] = (homologado, estado)

        resultados.append(homologado)

        if estado == 'CUARENTENA':
            id_registro_origen = None
            if columna_id_origen:
                valor_id = fila.get(columna_id_origen)
                if pd.notna(valor_id):
                    try:
                        id_registro_origen = int(valor_id)
                    except (TypeError, ValueError):
                        id_registro_origen = None

            cuarentenas.append({
                'columna':           columna_raw,
                'valor':             valor,
                'motivo':            'Variedad no reconocida — requiere revisión en MDM',
                'tipo_regla':        'CATALOGO',
                'score_levenshtein': None,
                'severidad':         'ALTO',
                'id_registro_origen': id_registro_origen,
            })

    df[columna_destino] = resultados
    return df, cuarentenas

