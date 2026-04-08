"""
repositorios/repo_catalogos.py
================================
Todas las consultas SQL relacionadas con catálogos MDM y dimensiones Silver.
Todos los métodos son de sólo lectura.
"""

from __future__ import annotations

from sqlalchemy import text
from sqlalchemy.exc import SQLAlchemyError

from nucleo.conexion import obtener_engine
from nucleo.excepciones import ErrorBaseDatos
from nucleo.logging import obtener_logger

log = obtener_logger(__name__)

_CATALOGOS = {
    "variedades": {
        "sql_count": "SELECT COUNT(*) FROM MDM.Catalogo_Variedades WHERE Es_Activa = 1",
        "sql_datos": """
            SELECT
                Nombre_Canonico AS nombre_canonico,
                Breeder         AS breeder,
                Es_Activa       AS es_activa
            FROM MDM.Catalogo_Variedades
            WHERE Es_Activa = 1
            ORDER BY Nombre_Canonico
            OFFSET :offset ROWS FETCH NEXT :tamano ROWS ONLY
        """,
        "mensaje_error": "Error al listar variedades",
    },
    "geografia": {
        "sql_count": "SELECT COUNT(*) FROM Silver.Dim_Geografia WHERE Es_Vigente = 1",
        "sql_datos": """
            SELECT
                Fundo               AS fundo,
                Sector              AS sector,
                Modulo              AS modulo,
                Turno               AS turno,
                Valvula             AS valvula,
                Cama                AS cama,
                Es_Test_Block       AS es_test_block,
                Codigo_SAP_Campo    AS codigo_sap_campo,
                Es_Vigente          AS es_vigente
            FROM Silver.Dim_Geografia
            WHERE Es_Vigente = 1
            ORDER BY Fundo, Sector, Modulo, Turno, Valvula, Cama
            OFFSET :offset ROWS FETCH NEXT :tamano ROWS ONLY
        """,
        "mensaje_error": "Error al listar geografÃ­a",
    },
    "personal": {
        "sql_count": "SELECT COUNT(*) FROM Silver.Dim_Personal",
        "sql_datos": """
            SELECT
                DNI                 AS dni,
                Nombre_Completo     AS nombre_completo,
                Rol                 AS rol,
                Sexo                AS sexo,
                ID_Planilla         AS id_planilla,
                Pct_Asertividad     AS pct_asertividad,
                Dias_Ausentismo     AS dias_ausentismo
            FROM Silver.Dim_Personal
            ORDER BY Nombre_Completo
            OFFSET :offset ROWS FETCH NEXT :tamano ROWS ONLY
        """,
        "mensaje_error": "Error al listar personal",
    },
}


_TTL_CATALOGOS = 3600   # 1 hora — datos estáticos
_TTL_CUARENTENA = 120   # 2 minutos — datos operativos


def _paginar(
    con,
    sql_count: str,
    sql_datos: str,
    params: dict,
    pagina: int,
    tamano: int,
) -> dict:
    """
    Helper interno: ejecuta count + query paginada y retorna el envelope estándar.
    """
    total = con.execute(text(sql_count), params).scalar() or 0
    filas = con.execute(text(sql_datos), params).fetchall()
    return {
        "total":  total,
        "pagina": pagina,
        "tamano": tamano,
        "datos":  [dict(fila._mapping) for fila in filas],
    }


def _listar_catalogo(nombre_catalogo: str, pagina: int = 1, tamano: int = 20) -> dict:
    configuracion = _CATALOGOS[nombre_catalogo]
    desplazamiento = (pagina - 1) * tamano
    params = {"offset": desplazamiento, "tamano": tamano}
    try:
        with obtener_engine().connect() as con:
            return _paginar(
                con,
                sql_count=configuracion["sql_count"],
                sql_datos=configuracion["sql_datos"],
                params=params,
                pagina=pagina,
                tamano=tamano,
            )
    except SQLAlchemyError:
        log.exception(configuracion["mensaje_error"])
        raise ErrorBaseDatos()


def listar_variedades(pagina: int = 1, tamano: int = 20) -> dict:
    """Lee MDM.Catalogo_Variedades activas con paginación server-side."""
    return _listar_catalogo("variedades", pagina=pagina, tamano=tamano)


def listar_geografia(pagina: int = 1, tamano: int = 20) -> dict:
    """Lee Silver.Dim_Geografia vigente con paginación server-side."""
    return _listar_catalogo("geografia", pagina=pagina, tamano=tamano)
    if False:
        log.exception("Error al listar geografía")


def listar_personal(pagina: int = 1, tamano: int = 20) -> dict:
    """Lee Silver.Dim_Personal con paginación server-side."""
    return _listar_catalogo("personal", pagina=pagina, tamano=tamano)
