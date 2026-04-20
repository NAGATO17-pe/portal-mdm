"""
repositorios/repo_reinyeccion.py
=================================
Capa de datos para la Herramienta de Reinyección MDM.

Lógica:
  Para cada registro RESUELTO en MDM.Cuarentena que tenga un
  ID_Registro_Origen válido, localiza la fila correspondiente en la
  tabla Bronce y restablece su Estado_Carga → 'CARGADO'.

  Esto permite que el pipeline ETL procese de nuevo esos registros
  con las reglas MDM ya actualizadas, sin que el usuario tenga que
  re-subir archivos.

Contrato:
  - Retorna dicts simples o lanza ErrorBaseDatos.
  - Nunca expone SQLAlchemy al exterior.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from nucleo.conexion import obtener_engine
from nucleo.excepciones import ErrorBaseDatos
from nucleo.logging import obtener_logger

log = obtener_logger(__name__)

# Mapa canónico: nombre normalizado de origen → tabla Bronce real + columna PK
# Se usará para construir el UPDATE dinámicamente de forma segura.
_TABLAS_BRONCE: dict[str, dict[str, str]] = {
    "bronce.peladas":                  {"tabla": "Bronce.Peladas",                  "pk": "ID_Peladas"},
    "bronce.tasa_crecimiento_brotes":  {"tabla": "Bronce.Tasa_Crecimiento_Brotes",  "pk": "ID_Brote"},
    "bronce.evaluacion_pesos":         {"tabla": "Bronce.Evaluacion_Pesos",         "pk": "ID_Evaluacion"},
    "bronce.evaluacion_vegetativa":    {"tabla": "Bronce.Evaluacion_Vegetativa",    "pk": "ID_Evaluacion"},
    "bronce.conteo_fenologico":        {"tabla": "Bronce.Conteo_Fenologico",        "pk": "ID_Conteo"},
    "bronce.induccion_floral":         {"tabla": "Bronce.Induccion_Floral",         "pk": "ID_Induccion"},
    "bronce.maduracion":               {"tabla": "Bronce.Maduracion",               "pk": "ID_Maduracion"},
    "bronce.fisiologia":               {"tabla": "Bronce.Fisiologia",               "pk": "ID_Fisiologia"},
    "bronce.ciclo_poda":               {"tabla": "Bronce.Ciclo_Poda",               "pk": "ID_Ciclo"},
    "bronce.cosecha_sap":              {"tabla": "Bronce.Cosecha_SAP",              "pk": "ID_Cosecha"},
    "bronce.sanidad_activo":           {"tabla": "Bronce.Sanidad_Activo",           "pk": "ID_Sanidad"},
    "bronce.tareo":                    {"tabla": "Bronce.Consolidado_Tareos",       "pk": "ID_Tareo"},
    "bronce.telemetria_clima":         {"tabla": "Bronce.Telemetria_Clima",         "pk": "ID_Clima"},
}


def obtener_resueltos_pendientes(tabla_filtro: str | None = None) -> list[dict]:
    """
    Obtiene todos los registros RESUELTOS en MDM.Cuarentena que aún no han
    sido reinyectados al pipeline (es decir, ID_Registro_Origen IS NOT NULL).

    Opcional: filtrar por tabla_origen (parcial, case-insensitive).
    """
    clausula_filtro = ""
    params: dict = {}
    if tabla_filtro:
        clausula_filtro = "AND LOWER(Tabla_Origen) LIKE :filtro"
        params["filtro"] = f"%{tabla_filtro.lower()}%"

    try:
        with obtener_engine().connect() as con:
            filas = con.execute(
                text(f"""
                    SELECT
                        CAST(ID_Cuarentena AS VARCHAR(30)) AS id_cuarentena,
                        Tabla_Origen                       AS tabla_origen,
                        Campo_Origen                       AS campo_origen,
                        Valor_Recibido                     AS valor_raw,
                        Valor_Corregido                    AS valor_corregido,
                        ID_Registro_Origen                 AS id_registro_origen
                    FROM MDM.Cuarentena
                    WHERE Estado = 'RESUELTO'
                      AND ID_Registro_Origen IS NOT NULL
                    {clausula_filtro}
                    ORDER BY Tabla_Origen, ID_Cuarentena
                """),
                params,
            ).fetchall()

        return [dict(f._mapping) for f in filas]

    except SQLAlchemyError:
        log.exception("Error al obtener resueltos para reinyección")
        raise ErrorBaseDatos()


def reinyectar_en_bronce(registros: list[dict]) -> dict:
    """
    Para cada registro resuelto, actualiza Estado_Carga = 'CARGADO' en Bronce.

    Agrupa por tabla para hacer un solo UPDATE por conjunto de IDs.

    Retorna: {reinyectados: int, omitidos: int, detalle: list[str]}
    """
    reinyectados = 0
    omitidos = 0
    detalle: list[str] = []

    # Agrupar IDs por tabla
    por_tabla: dict[str, list[int]] = {}
    for reg in registros:
        tabla_key = reg["tabla_origen"].lower()
        id_origen = reg.get("id_registro_origen")

        if tabla_key not in _TABLAS_BRONCE:
            omitidos += 1
            detalle.append(f"⚠️ Tabla '{reg['tabla_origen']}' no mapeada — omitida.")
            continue

        if not id_origen:
            omitidos += 1
            detalle.append(f"⚠️ Registro #{reg['id_cuarentena']} sin ID_Registro_Origen — omitido.")
            continue

        por_tabla.setdefault(tabla_key, []).append(int(id_origen))

    if not por_tabla:
        return {"reinyectados": 0, "omitidos": omitidos, "detalle": detalle}

    try:
        with obtener_engine().begin() as con:
            for tabla_key, ids in por_tabla.items():
                meta = _TABLAS_BRONCE[tabla_key]
                tabla_sql = meta["tabla"]
                pk_sql = meta["pk"]

                # Construimos placeholders de forma segura
                placeholders = ", ".join(f":id_{i}" for i in range(len(ids)))
                params_ids = {f"id_{i}": v for i, v in enumerate(ids)}

                resultado = con.execute(
                    text(f"""
                        UPDATE {tabla_sql}
                        SET Estado_Carga = 'CARGADO'
                        WHERE {pk_sql} IN ({placeholders})
                          AND Estado_Carga = 'RECHAZADO'
                    """),
                    params_ids,
                )
                n = resultado.rowcount
                reinyectados += n
                detalle.append(f"✅ {tabla_sql}: {n} registros reactivados de {len(ids)} candidatos.")

                log.info(
                    "Reinyección completada para tabla",
                    extra={"tabla": tabla_sql, "actualizados": n, "candidatos": len(ids)},
                )

    except SQLAlchemyError:
        log.exception("Error durante reinyección en Bronce")
        raise ErrorBaseDatos()

    return {"reinyectados": reinyectados, "omitidos": omitidos, "detalle": detalle}


def contar_candidatos_reinyeccion(tabla_filtro: str | None = None) -> int:
    """Retorna el conteo rápido de registros RESUELTOS disponibles para reinyectar."""
    clausula_filtro = ""
    params: dict = {}
    if tabla_filtro:
        clausula_filtro = "AND LOWER(Tabla_Origen) LIKE :filtro"
        params["filtro"] = f"%{tabla_filtro.lower()}%"

    try:
        with obtener_engine().connect() as con:
            total = con.execute(
                text(f"""
                    SELECT COUNT(*)
                    FROM MDM.Cuarentena
                    WHERE Estado = 'RESUELTO'
                      AND ID_Registro_Origen IS NOT NULL
                    {clausula_filtro}
                """),
                params,
            ).scalar() or 0
        return int(total)
    except SQLAlchemyError:
        log.exception("Error al contar candidatos para reinyección")
        raise ErrorBaseDatos()
