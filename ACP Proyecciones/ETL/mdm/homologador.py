"""
homologador.py
==============
Homologación de texto libre via Levenshtein (rapidfuzz).
Resuelve aliases de variedades, nombres de personal y geografía
contra el diccionario canónico en MDM.Diccionario_Homologacion.

Flujo:
  1. Busca match exacto en el diccionario
  2. Si no hay exacto → calcula similitud Levenshtein
  3. Si score >= umbral → homologación automática
  4. Si score < umbral → va a MDM.Cuarentena para revisión humana
"""

import pandas as pd
from rapidfuzz import fuzz, process
from sqlalchemy.engine import Engine
from sqlalchemy import text
from datetime import datetime


UMBRAL_AUTO = 0.85  # Score mínimo para homologación automática


def cargar_diccionario(engine: Engine,
                        tabla_origen: str) -> pd.DataFrame:
    """
    Carga el diccionario de homologación aprobado
    para una tabla origen específica.
    Solo trae entradas con Aprobado = 1.
    """
    with engine.connect() as conexion:
        resultado = conexion.execute(text("""
            SELECT
                Texto_Crudo,
                Valor_Canonico,
                Score_Levenshtein,
                Veces_Aplicado
            FROM MDM.Diccionario_Homologacion
            WHERE Tabla_Origen = :tabla_origen
              AND Aprobado     = 1
        """), {'tabla_origen': tabla_origen})

        return pd.DataFrame(resultado.fetchall(),
                            columns=resultado.keys())


def buscar_match_exacto(valor: str,
                         diccionario: pd.DataFrame) -> str | None:
    """
    Busca un match exacto (case-insensitive) en el diccionario.
    Retorna el valor canónico si existe, None si no.
    """
    if diccionario.empty:
        return None

    coincidencia = diccionario[
        diccionario['Texto_Crudo'].str.lower() == valor.lower()
    ]

    if not coincidencia.empty:
        return coincidencia.iloc[0]['Valor_Canonico']

    return None


def buscar_match_levenshtein(valor: str,
                              diccionario: pd.DataFrame) -> tuple[str | None, float]:
    """
    Busca el mejor match usando similitud Levenshtein.
    Retorna (valor_canonico, score) o (None, 0.0) si no hay match suficiente.
    """
    if diccionario.empty:
        return None, 0.0

    candidatos = diccionario['Valor_Canonico'].tolist()

    resultado = process.extractOne(
        valor,
        candidatos,
        scorer=fuzz.token_sort_ratio,
    )

    if resultado is None:
        return None, 0.0

    match, score, _ = resultado
    score_normalizado = score / 100.0

    if score_normalizado >= UMBRAL_AUTO:
        return match, score_normalizado

    return None, score_normalizado


def registrar_homologacion(engine: Engine,
                            tabla_origen: str,
                            texto_crudo: str,
                            valor_canonico: str,
                            score: float,
                            aprobado: bool = True) -> None:
    """
    Registra una homologación en MDM.Diccionario_Homologacion.
    Si aprobado=False → queda pendiente de revisión humana en Streamlit.
    """
    with engine.begin() as conexion:
        # Verificar si ya existe
        existe = conexion.execute(text("""
            SELECT COUNT(*) FROM MDM.Diccionario_Homologacion
            WHERE Tabla_Origen = :tabla_origen
              AND Texto_Crudo  = :texto_crudo
        """), {
            'tabla_origen': tabla_origen,
            'texto_crudo':  texto_crudo,
        }).scalar()

        if existe:
            # Incrementar contador de usos
            conexion.execute(text("""
                UPDATE MDM.Diccionario_Homologacion
                SET Veces_Aplicado = Veces_Aplicado + 1
                WHERE Tabla_Origen = :tabla_origen
                  AND Texto_Crudo  = :texto_crudo
            """), {
                'tabla_origen': tabla_origen,
                'texto_crudo':  texto_crudo,
            })
        else:
            # Insertar nueva entrada
            conexion.execute(text("""
                INSERT INTO MDM.Diccionario_Homologacion (
                    Tabla_Origen,
                    Campo_Origen,
                    Texto_Crudo,
                    Valor_Canonico,
                    Score_Levenshtein,
                    Aprobado,
                    Aprobado_Por,
                    Veces_Aplicado
                ) VALUES (
                    :tabla_origen,
                    :campo_origen,
                    :texto_crudo,
                    :valor_canonico,
                    :score,
                    :aprobado,
                    :aprobado_por,
                    1
                )
            """), {
                'tabla_origen':  tabla_origen,
                'campo_origen':  'Variedad_Raw',
                'texto_crudo':   texto_crudo,
                'valor_canonico': valor_canonico,
                'score':         round(score, 4),
                'aprobado':      1 if aprobado else 0,
                'aprobado_por':  'SISTEMA' if aprobado else 'PENDIENTE',
            })


def homologar_valor(valor: str | None,
                    tabla_origen: str,
                    diccionario: pd.DataFrame,
                    engine: Engine) -> tuple[str | None, str]:
    """
    Homologa un valor contra el diccionario canónico.

    Retorna tupla (valor_homologado, estado):
      - estado = 'EXACTO'       → match exacto en diccionario
      - estado = 'LEVENSHTEIN'  → match automático por similitud
      - estado = 'CUARENTENA'   → sin match suficiente → revisión humana
    """
    if not valor or not str(valor).strip():
        return None, 'NULO'

    valor = str(valor).strip()

    # 1. Match exacto
    canonico = buscar_match_exacto(valor, diccionario)
    if canonico:
        registrar_homologacion(engine, tabla_origen, valor, canonico, 1.0)
        return canonico, 'EXACTO'

    # 2. Levenshtein
    canonico, score = buscar_match_levenshtein(valor, diccionario)
    if canonico:
        registrar_homologacion(engine, tabla_origen, valor, canonico, score)
        return canonico, 'LEVENSHTEIN'

    # 3. Sin match → cuarentena
    registrar_homologacion(
        engine, tabla_origen, valor,
        valor,   # se guarda el crudo como candidato
        score,
        aprobado=False
    )
    return None, 'CUARENTENA'


def homologar_columna(df: pd.DataFrame,
                       columna_raw: str,
                       columna_destino: str,
                       tabla_origen: str,
                       engine: Engine) -> tuple[pd.DataFrame, list[dict]]:
    """
    Homologa una columna completa del DataFrame.
    Retorna (df con columna_destino añadida, lista de cuarentenas).
    """
    diccionario = cargar_diccionario(engine, tabla_origen)
    cuarentenas = []

    resultados = []
    for _, fila in df.iterrows():
        valor = fila.get(columna_raw)
        homologado, estado = homologar_valor(
            valor, tabla_origen, diccionario, engine
        )
        resultados.append(homologado)

        if estado == 'CUARENTENA':
            cuarentenas.append({
                'columna':   columna_raw,
                'valor':     valor,
                'motivo':    'Variedad no reconocida — requiere revisión en MDM',
                'severidad': 'ALTO',
            })

    df[columna_destino] = resultados
    return df, cuarentenas
