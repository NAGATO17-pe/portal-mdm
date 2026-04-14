from __future__ import annotations

import re
from collections.abc import MutableSet

from sqlalchemy import text
from sqlalchemy.engine import Engine


def a_entero_nulo(valor) -> int | None:
    try:
        if valor is None:
            return None
        texto = str(valor).strip()
        if texto in ("", "None", "nan"):
            return None
        return int(float(texto))
    except (ValueError, TypeError):
        return None


def a_entero_no_negativo(valor) -> int | None:
    numero = a_entero_nulo(valor)
    if numero is None or numero < 0:
        return None
    return numero


def texto_nulo(valor) -> str | None:
    if valor is None:
        return None
    texto = str(valor).strip()
    return texto if texto and texto.lower() not in ("none", "nan") else None


def motivo_cuarentena_geografia(resultado_geo: dict) -> str:
    estado = resultado_geo.get("estado")
    if estado in ("TEST_BLOCK_NO_MAPEADO", "TEST_BLOCK_AMBIGUO"):
        return "Test block (VI) sin mapeo unico en Dim_Geografia."
    if estado in ("PENDIENTE_CASO_ESPECIAL", "CASO_ESPECIAL_MODULO"):
        return "Geografia especial requiere catalogacion o regla en MDM_Geografia."
    if estado in ("PENDIENTE_CAMA_GENERICA", "CAMA_NO_RELACION"):
        return "Cama no relacionada a la geografia operativa."
    if estado in ("PENDIENTE_DIM_DUPLICADA", "GEOGRAFIA_AMBIGUA"):
        return "La clave geografica tiene mas de un registro vigente en Silver.Dim_Geografia."
    if estado == "CAMA_NO_VALIDA":
        return "Cama fuera de rango operativo permitido."
    if estado == "CAMA_NO_CATALOGO":
        return "Cama valida pero no registrada en el catalogo operativo."
    return "Geografia no encontrada en Silver.Dim_Geografia."


def registrar_rechazo(
    resumen: dict,
    ids_rechazados,
    id_origen: int | None,
    *,
    columna: str,
    valor,
    motivo: str,
    tipo_regla: str = 'DQ',
    severidad: str = 'ALTO',
) -> None:
    resumen['rechazados'] = int(resumen.get('rechazados', 0) or 0) + 1
    if id_origen is not None:
        if isinstance(ids_rechazados, MutableSet) or hasattr(ids_rechazados, 'add'):
            ids_rechazados.add(id_origen)
        else:
            ids_rechazados.append(id_origen)
    resumen.setdefault('cuarentena', []).append({
        'columna': columna,
        'valor': valor,
        'motivo': motivo,
        'tipo_regla': tipo_regla,
        'severidad': severidad,
        'id_registro_origen': id_origen,
    })


def finalizar_resumen_fact(resumen: dict) -> dict:
    from utils.metricas import normalizar_resultado_fact

    return normalizar_resultado_fact(resumen)


def obtener_columnas_tabla(engine: Engine, tabla_completa: str) -> set[str]:
    esquema, tabla = tabla_completa.split(".")
    with engine.connect() as conexion:
        resultado = conexion.execute(
            text(
                """
                SELECT COLUMN_NAME
                FROM INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = :esquema
                  AND TABLE_NAME = :tabla
                """
            ),
            {"esquema": esquema, "tabla": tabla},
        )
        return {str(fila[0]) for fila in resultado.fetchall()}


def validar_layout_migrado(
    engine: Engine,
    *,
    tabla_origen: str,
    tabla_destino: str,
    columna_id: str,
    columnas_bronce_requeridas: set[str],
    columnas_silver_requeridas: set[str],
    nombre_layout: str,
) -> str:
    columnas_bronce = obtener_columnas_tabla(engine, tabla_origen)
    columnas_silver = obtener_columnas_tabla(engine, tabla_destino)

    faltantes_bronce = sorted(columnas_bronce_requeridas - columnas_bronce)
    faltantes_silver = sorted(columnas_silver_requeridas - columnas_silver)

    if faltantes_bronce or faltantes_silver:
        raise RuntimeError(
            f"La migracion definitiva de {nombre_layout} no esta aplicada. "
            f"Bronce faltantes: {faltantes_bronce or 'ninguno'} | "
            f"Silver faltantes: {faltantes_silver or 'ninguno'}"
        )

    return columna_id


def parsear_valores_raw(texto: str | None) -> dict[str, str]:
    if texto is None:
        return {}

    crudo = str(texto).strip()
    if not crudo:
        return {}

    resultado: dict[str, str] = {}
    for parte in re.split(r"\s*\|\s*", crudo):
        if "=" not in parte:
            continue
        clave, valor = parte.split("=", 1)
        clave = str(clave).strip()
        valor = str(valor).strip()
        if clave:
            resultado[clave] = valor
    return resultado
