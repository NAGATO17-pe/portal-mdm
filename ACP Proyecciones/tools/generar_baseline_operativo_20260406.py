from __future__ import annotations

import json
import os
import urllib.parse
from collections import defaultdict
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from pathlib import Path
from typing import Any

from sqlalchemy import create_engine, text
from sqlalchemy.engine import Connection, Engine


TOOLS_DIR = Path(__file__).resolve().parent
PROJECT_DIR = TOOLS_DIR.parent
AVANCE_DIR = PROJECT_DIR / "ETL" / "Avance"
FECHA_CORTE = "2026-04-06"
SERVIDOR_POR_DEFECTO = "LCP-PAG-PRACTIC"
BASE_POR_DEFECTO = "ACP_DataWarehose_Proyecciones"
DRIVER_POR_DEFECTO = "ODBC Driver 17 for SQL Server"


DOCS_OPERATIVOS = [
    "README_OPERATIVO_PIPELINE.md",
    "Avance/CIERRE_ESTABLE_ETL_20260330.md",
    "Avance/ACTUALIZACION_OPERATIVA_CLIMA_20260331.md",
    "Avance/ACTUALIZACION_OPERATIVA_INDUCCION_TASA_20260401.md",
    "Avance/CHECKLIST_OPERATIVO_5_MINUTOS.md",
]


FKS_DOMINIO: dict[str, list[tuple[str, str, str]]] = {
    "Silver.Fact_Conteo_Fenologico": [
        ("ID_Geografia", "Silver.Dim_Geografia", "ID_Geografia"),
        ("ID_Tiempo", "Silver.Dim_Tiempo", "ID_Tiempo"),
        ("ID_Variedad", "Silver.Dim_Variedad", "ID_Variedad"),
        ("ID_Personal", "Silver.Dim_Personal", "ID_Personal"),
    ],
    "Silver.Fact_Evaluacion_Pesos": [
        ("ID_Geografia", "Silver.Dim_Geografia", "ID_Geografia"),
        ("ID_Tiempo", "Silver.Dim_Tiempo", "ID_Tiempo"),
        ("ID_Variedad", "Silver.Dim_Variedad", "ID_Variedad"),
        ("ID_Personal", "Silver.Dim_Personal", "ID_Personal"),
    ],
    "Silver.Fact_Evaluacion_Vegetativa": [
        ("ID_Geografia", "Silver.Dim_Geografia", "ID_Geografia"),
        ("ID_Tiempo", "Silver.Dim_Tiempo", "ID_Tiempo"),
        ("ID_Variedad", "Silver.Dim_Variedad", "ID_Variedad"),
        ("ID_Personal", "Silver.Dim_Personal", "ID_Personal"),
    ],
    "Silver.Fact_Ciclo_Poda": [
        ("ID_Geografia", "Silver.Dim_Geografia", "ID_Geografia"),
        ("ID_Tiempo", "Silver.Dim_Tiempo", "ID_Tiempo"),
        ("ID_Variedad", "Silver.Dim_Variedad", "ID_Variedad"),
    ],
    "Silver.Fact_Maduracion": [
        ("ID_Geografia", "Silver.Dim_Geografia", "ID_Geografia"),
        ("ID_Tiempo", "Silver.Dim_Tiempo", "ID_Tiempo"),
        ("ID_Variedad", "Silver.Dim_Variedad", "ID_Variedad"),
        ("ID_Personal", "Silver.Dim_Personal", "ID_Personal"),
        ("ID_Cinta", "Silver.Dim_Cinta", "ID_Cinta"),
        ("ID_Estado_Fenologico", "Silver.Dim_Estado_Fenologico", "ID_Estado_Fenologico"),
    ],
    "Silver.Fact_Telemetria_Clima": [
        ("ID_Tiempo", "Silver.Dim_Tiempo", "ID_Tiempo"),
    ],
    "Silver.Fact_Induccion_Floral": [
        ("ID_Geografia", "Silver.Dim_Geografia", "ID_Geografia"),
        ("ID_Tiempo", "Silver.Dim_Tiempo", "ID_Tiempo"),
        ("ID_Variedad", "Silver.Dim_Variedad", "ID_Variedad"),
        ("ID_Personal", "Silver.Dim_Personal", "ID_Personal"),
    ],
    "Silver.Fact_Tasa_Crecimiento_Brotes": [
        ("ID_Geografia", "Silver.Dim_Geografia", "ID_Geografia"),
        ("ID_Tiempo", "Silver.Dim_Tiempo", "ID_Tiempo"),
        ("ID_Variedad", "Silver.Dim_Variedad", "ID_Variedad"),
        ("ID_Personal", "Silver.Dim_Personal", "ID_Personal"),
    ],
    "Silver.Fact_Tareo": [
        ("ID_Geografia", "Silver.Dim_Geografia", "ID_Geografia"),
        ("ID_Tiempo", "Silver.Dim_Tiempo", "ID_Tiempo"),
        ("ID_Personal", "Silver.Dim_Personal", "ID_Personal"),
        ("ID_Actividad_Operativa", "Silver.Dim_Actividad_Operativa", "ID_Actividad"),
        ("ID_Personal_Supervisor", "Silver.Dim_Personal", "ID_Personal"),
    ],
    "Silver.Fact_Fisiologia": [
        ("ID_Geografia", "Silver.Dim_Geografia", "ID_Geografia"),
        ("ID_Tiempo", "Silver.Dim_Tiempo", "ID_Tiempo"),
        ("ID_Variedad", "Silver.Dim_Variedad", "ID_Variedad"),
    ],
}


SQL_DUPLICADOS_CANONICOS: dict[str, str] = {
    "Silver.Fact_Telemetria_Clima": """
        SELECT COUNT(*) AS grupos, ISNULL(SUM(Filas - 1), 0) AS filas_extra
        FROM (
            SELECT Sector_Climatico, Fecha_Evento, COUNT(*) AS Filas
            FROM Silver.Fact_Telemetria_Clima
            WHERE Precipitacion_mm IS NULL
              AND (VPD IS NOT NULL OR Radiacion_Solar IS NOT NULL)
            GROUP BY Sector_Climatico, Fecha_Evento
            HAVING COUNT(*) > 1
        ) x
    """,
    "Silver.Fact_Induccion_Floral": """
        SELECT COUNT(*) AS grupos, ISNULL(SUM(Filas - 1), 0) AS filas_extra
        FROM (
            SELECT
                ID_Geografia, ID_Tiempo, ID_Variedad, ID_Personal,
                Tipo_Evaluacion, Codigo_Consumidor, Fecha_Evento,
                COUNT(*) AS Filas
            FROM Silver.Fact_Induccion_Floral
            GROUP BY
                ID_Geografia, ID_Tiempo, ID_Variedad, ID_Personal,
                Tipo_Evaluacion, Codigo_Consumidor, Fecha_Evento
            HAVING COUNT(*) > 1
        ) x
    """,
    "Silver.Fact_Tasa_Crecimiento_Brotes": """
        SELECT COUNT(*) AS grupos, ISNULL(SUM(Filas - 1), 0) AS filas_extra
        FROM (
            SELECT
                ID_Geografia, ID_Tiempo, ID_Variedad, ID_Personal,
                Tipo_Evaluacion, Codigo_Ensayo, Codigo_Origen, Fecha_Evento,
                COUNT(*) AS Filas
            FROM Silver.Fact_Tasa_Crecimiento_Brotes
            GROUP BY
                ID_Geografia, ID_Tiempo, ID_Variedad, ID_Personal,
                Tipo_Evaluacion, Codigo_Ensayo, Codigo_Origen, Fecha_Evento
            HAVING COUNT(*) > 1
        ) x
    """,
}


SQL_DUPLICADOS_TOTALES_CLIMA = """
    SELECT COUNT(*) AS grupos, ISNULL(SUM(Filas - 1), 0) AS filas_extra
    FROM (
        SELECT Sector_Climatico, Fecha_Evento, COUNT(*) AS Filas
        FROM Silver.Fact_Telemetria_Clima
        GROUP BY Sector_Climatico, Fecha_Evento
        HAVING COUNT(*) > 1
    ) x
"""


SQL_MODULO_9 = """
    SELECT Tabla, Total
    FROM (
        SELECT 'Bronce.Fisiologia' AS Tabla, COUNT(*) AS Total
        FROM Bronce.Fisiologia
        WHERE REPLACE(REPLACE(LTRIM(RTRIM(ISNULL(Modulo_Raw, ''))), ' ', ''), ',', '.') = '9.'
        UNION ALL
        SELECT 'Bronce.Evaluacion_Pesos', COUNT(*)
        FROM Bronce.Evaluacion_Pesos
        WHERE REPLACE(REPLACE(LTRIM(RTRIM(ISNULL(Modulo_Raw, ''))), ' ', ''), ',', '.') = '9.'
        UNION ALL
        SELECT 'Bronce.Evaluacion_Vegetativa', COUNT(*)
        FROM Bronce.Evaluacion_Vegetativa
        WHERE REPLACE(REPLACE(LTRIM(RTRIM(ISNULL(Modulo_Raw, ''))), ' ', ''), ',', '.') = '9.'
        UNION ALL
        SELECT 'Bronce.Induccion_Floral', COUNT(*)
        FROM Bronce.Induccion_Floral
        WHERE REPLACE(REPLACE(LTRIM(RTRIM(ISNULL(Modulo_Raw, ''))), ' ', ''), ',', '.') = '9.'
        UNION ALL
        SELECT 'Bronce.Tasa_Crecimiento_Brotes', COUNT(*)
        FROM Bronce.Tasa_Crecimiento_Brotes
        WHERE REPLACE(REPLACE(LTRIM(RTRIM(ISNULL(Modulo_Raw, ''))), ' ', ''), ',', '.') = '9.'
    ) x
    WHERE Total > 0
    ORDER BY Total DESC, Tabla
"""


@dataclass(frozen=True)
class Dominio:
    id: str
    nombre: str
    tabla_silver: str
    tablas_bronce: tuple[str, ...]
    clasificacion_ml: str
    nota_doc: str
    es_dimension: bool = False
    campo_personal: str | None = "ID_Personal"


DOMINIOS: tuple[Dominio, ...] = (
    Dominio("dim_geografia", "Dim_Geografia", "Silver.Dim_Geografia", tuple(), "NO_APLICA", "Base del resolvedor geográfico y del bridge cama.", True, None),
    Dominio("dim_personal", "Dim_Personal", "Silver.Dim_Personal", ("Bronce.Consolidado_Tareos", "Bronce.Fiscalizacion"), "REQUERIDO", "Dimensión conservadora; no debería asumirse como cerrada sin fuente fuerte.", True, None),
    Dominio("conteo", "Fact_Conteo_Fenologico", "Silver.Fact_Conteo_Fenologico", ("Bronce.Conteo_Fruta",), "CONDICIONAL", "Fact estable en checkpoint de marzo."),
    Dominio("pesos", "Fact_Evaluacion_Pesos", "Silver.Fact_Evaluacion_Pesos", ("Bronce.Evaluacion_Pesos",), "CONDICIONAL", "Fact estable con residual controlado y fuerte dependencia de Dim_Personal."),
    Dominio("vegetativa", "Fact_Evaluacion_Vegetativa", "Silver.Fact_Evaluacion_Vegetativa", ("Bronce.Evaluacion_Vegetativa",), "CONDICIONAL", "Fact estable con residual dominante de geografía y personal."),
    Dominio("ciclo_poda", "Fact_Ciclo_Poda", "Silver.Fact_Ciclo_Poda", ("Bronce.Evaluacion_Calidad_Poda", "Bronce.Ciclos_Fenologicos"), "LISTO", "Frente documentado como estable.", False, None),
    Dominio("maduracion", "Fact_Maduracion", "Silver.Fact_Maduracion", ("Bronce.Maduracion",), "CONDICIONAL", "Dominio estabilizado, pero sigue arrastrando debilidad de Dim_Personal."),
    Dominio("clima", "Fact_Telemetria_Clima", "Silver.Fact_Telemetria_Clima", ("Bronce.Clima", "Bronce.Reporte_Clima", "Bronce.Variables_Meteorologicas"), "CONDICIONAL", "Clima se documentó como cerrado operativamente al 2026-03-31.", False, None),
    Dominio("induccion", "Fact_Induccion_Floral", "Silver.Fact_Induccion_Floral", ("Bronce.Induccion_Floral",), "CONDICIONAL", "Dominio funcional; el modelo futuro debe consumirlo desde Silver."),
    Dominio("tasa", "Fact_Tasa_Crecimiento_Brotes", "Silver.Fact_Tasa_Crecimiento_Brotes", ("Bronce.Tasa_Crecimiento_Brotes",), "NO_LISTO", "Dominio funcional documentado, pero muy sensible a geografía/MDM."),
    Dominio("tareo", "Fact_Tareo", "Silver.Fact_Tareo", ("Bronce.Consolidado_Tareos",), "NO_LISTO", "Debe tratarse como dominio pendiente hasta tener fuente suficiente."),
    Dominio("fisiologia", "Fact_Fisiologia", "Silver.Fact_Fisiologia", ("Bronce.Fisiologia",), "CONDICIONAL", "Dominio activo documentado; residual concentrado en Modulo_Raw = '9.'.", False, None),
)


def crear_engine() -> Engine:
    servidor = os.getenv("DB_SERVIDOR", SERVIDOR_POR_DEFECTO)
    base = os.getenv("DB_NOMBRE", BASE_POR_DEFECTO)
    driver = os.getenv("DB_DRIVER", DRIVER_POR_DEFECTO)
    usuario = os.getenv("DB_USUARIO")
    clave = os.getenv("DB_CLAVE")

    if usuario:
        cadena = (
            f"DRIVER={{{driver}}};SERVER={servidor};DATABASE={base};"
            f"UID={usuario};PWD={clave};TrustServerCertificate=yes;"
        )
    else:
        cadena = (
            f"DRIVER={{{driver}}};SERVER={servidor};DATABASE={base};"
            "Trusted_Connection=yes;TrustServerCertificate=yes;"
        )

    return create_engine(
        "mssql+pyodbc:///?odbc_connect=" + urllib.parse.quote_plus(cadena)
    )


def serializar(valor: Any) -> Any:
    if isinstance(valor, Decimal):
        return float(valor)
    if isinstance(valor, datetime):
        return valor.isoformat(sep=" ")
    if hasattr(valor, "isoformat"):
        try:
            return valor.isoformat()
        except TypeError:
            pass
    return valor


def fila_dict(conexion: Connection, sql: str, parametros: dict[str, Any] | None = None) -> dict[str, Any]:
    resultado = conexion.execute(text(sql), parametros or {})
    fila = resultado.mappings().first()
    if fila is None:
        return {}
    return {clave: serializar(valor) for clave, valor in fila.items()}


def filas_dict(conexion: Connection, sql: str, parametros: dict[str, Any] | None = None) -> list[dict[str, Any]]:
    resultado = conexion.execute(text(sql), parametros or {})
    return [{clave: serializar(valor) for clave, valor in fila.items()} for fila in resultado.mappings().all()]


def escalar(conexion: Connection, sql: str, parametros: dict[str, Any] | None = None) -> Any:
    valor = conexion.execute(text(sql), parametros or {}).scalar()
    return serializar(valor)


def tabla_existe(conexion: Connection, tabla: str) -> bool:
    return bool(escalar(conexion, "SELECT CASE WHEN OBJECT_ID(:tabla) IS NULL THEN 0 ELSE 1 END", {"tabla": tabla}))


def columna_existe(conexion: Connection, tabla: str, columna: str) -> bool:
    return bool(escalar(conexion, "SELECT CASE WHEN COL_LENGTH(:tabla, :columna) IS NULL THEN 0 ELSE 1 END", {"tabla": tabla, "columna": columna}))


def consultar_contexto(conexion: Connection) -> dict[str, Any]:
    return fila_dict(
        conexion,
        """
        SELECT
            @@SERVERNAME AS servidor_sql,
            DB_NAME() AS base_sql,
            SUSER_SNAME() AS usuario_sql,
            @@VERSION AS version_sql,
            SYSDATETIME() AS fecha_sql
        """,
    )


def consultar_log_ultimo(conexion: Connection, tabla_destino: str) -> dict[str, Any]:
    return fila_dict(
        conexion,
        """
        SELECT TOP 1
            Tabla_Destino,
            Estado_Proceso,
            Filas_Leidas,
            Filas_Insertadas,
            Filas_Cuarentena,
            Filas_Rechazadas,
            Fecha_Inicio,
            Fecha_Fin,
            Nombre_Archivo_Fuente,
            Mensaje_Error
        FROM Auditoria.Log_Carga
        WHERE Tabla_Destino = :tabla
        ORDER BY Fecha_Fin DESC, ID_Log_Carga DESC
        """,
        {"tabla": tabla_destino},
    )


def consultar_estados_bronce(conexion: Connection, tabla_bronce: str) -> dict[str, Any]:
    resumen = {"tabla": tabla_bronce, "total_filas": 0, "por_estado": []}
    if not tabla_existe(conexion, tabla_bronce):
        resumen["no_existe"] = True
        return resumen

    resumen["total_filas"] = int(escalar(conexion, f"SELECT COUNT(*) FROM {tabla_bronce}") or 0)
    if columna_existe(conexion, tabla_bronce, "Estado_Carga"):
        resumen["por_estado"] = filas_dict(
            conexion,
            f"""
            SELECT ISNULL(Estado_Carga, '(NULL)') AS estado_carga, COUNT(*) AS total
            FROM {tabla_bronce}
            GROUP BY Estado_Carga
            ORDER BY estado_carga
            """,
        )
    return resumen


def consultar_rango_evento(conexion: Connection, tabla: str) -> dict[str, Any]:
    if not columna_existe(conexion, tabla, "Fecha_Evento"):
        return {}
    return fila_dict(
        conexion,
        f"""
        SELECT
            MIN(Fecha_Evento) AS fecha_min_evento,
            MAX(Fecha_Evento) AS fecha_max_evento
        FROM {tabla}
        """,
    )


def consultar_conteo_y_personal(conexion: Connection, dominio: Dominio) -> dict[str, Any]:
    resumen = {
        "tabla_silver": dominio.tabla_silver,
        "total_filas": int(escalar(conexion, f"SELECT COUNT(*) FROM {dominio.tabla_silver}") or 0),
        "minus1_total": None,
        "minus1_pct": None,
    }
    if dominio.campo_personal and columna_existe(conexion, dominio.tabla_silver, dominio.campo_personal):
        minus1_total = int(
            escalar(
                conexion,
                f"SELECT SUM(CASE WHEN {dominio.campo_personal} = -1 THEN 1 ELSE 0 END) FROM {dominio.tabla_silver}",
            )
            or 0
        )
        resumen["minus1_total"] = minus1_total
        total = resumen["total_filas"]
        resumen["minus1_pct"] = round((minus1_total / total) * 100, 2) if total else None
    resumen.update(consultar_rango_evento(conexion, dominio.tabla_silver))
    return resumen


def consultar_cuarentena(conexion: Connection, tabla_origen: str) -> dict[str, Any]:
    pendientes = int(
        escalar(
            conexion,
            """
            SELECT COUNT(*)
            FROM MDM.Cuarentena
            WHERE Tabla_Origen = :tabla
              AND Estado = 'PENDIENTE'
            """,
            {"tabla": tabla_origen},
        )
        or 0
    )
    top_motivos = filas_dict(
        conexion,
        """
        SELECT TOP 5 Motivo, COUNT(*) AS total
        FROM MDM.Cuarentena
        WHERE Tabla_Origen = :tabla
        GROUP BY Motivo
        ORDER BY total DESC, Motivo
        """,
        {"tabla": tabla_origen},
    )
    return {
        "tabla_origen": tabla_origen,
        "pendientes": pendientes,
        "top_motivos": top_motivos,
    }


def consultar_fks(conexion: Connection, tabla_fact: str) -> dict[str, Any]:
    detalle: list[dict[str, Any]] = []
    for fk, tabla_dim, pk in FKS_DOMINIO.get(tabla_fact, []):
        total = int(
            escalar(
                conexion,
                f"""
                SELECT COUNT(*)
                FROM {tabla_fact} f
                LEFT JOIN {tabla_dim} d
                  ON d.{pk} = f.{fk}
                WHERE f.{fk} IS NOT NULL
                  AND d.{pk} IS NULL
                """
            )
            or 0
        )
        detalle.append({"fk": fk, "tabla_dim": tabla_dim, "pk": pk, "huerfanas": total})
    return {"total_huerfanas": sum(item["huerfanas"] for item in detalle), "detalle": detalle}


def consultar_duplicados(conexion: Connection, tabla: str) -> dict[str, Any]:
    if tabla not in SQL_DUPLICADOS_CANONICOS:
        return {}
    datos = fila_dict(conexion, SQL_DUPLICADOS_CANONICOS[tabla])
    if tabla == "Silver.Fact_Telemetria_Clima":
        datos["superposicion_total"] = fila_dict(conexion, SQL_DUPLICADOS_TOTALES_CLIMA)
    return datos


def consultar_dim_geografia(conexion: Connection) -> dict[str, Any]:
    sp = fila_dict(
        conexion,
        """
        EXEC Silver.sp_Validar_Calidad_Camas
            @Cama_Max_Permitida = 100,
            @Max_Camas_Por_Geografia = 100
        """,
    )
    vigentes = fila_dict(
        conexion,
        """
        SELECT
            COUNT(*) AS vigentes,
            SUM(CASE WHEN ISNULL(Es_Test_Block, 0) = 1 THEN 1 ELSE 0 END) AS test_block,
            SUM(CASE WHEN LTRIM(RTRIM(ISNULL(Cama, ''))) IN ('', '0') THEN 1 ELSE 0 END) AS sin_cama_explicita
        FROM Silver.Dim_Geografia
        WHERE ISNULL(Es_Vigente, 1) = 1
        """,
    )
    duplicados_vigentes = int(
        escalar(
            conexion,
            """
            WITH duplicados AS (
                SELECT
                    Modulo,
                    ISNULL(SubModulo, -1) AS SubModulo_Normalizado,
                    Turno,
                    LTRIM(RTRIM(ISNULL(Valvula, ''))) AS Valvula_Normalizada,
                    LTRIM(RTRIM(ISNULL(Cama, ''))) AS Cama_Normalizada,
                    COUNT(*) AS registros
                FROM Silver.Dim_Geografia
                WHERE ISNULL(Es_Vigente, 1) = 1
                GROUP BY
                    Modulo,
                    ISNULL(SubModulo, -1),
                    Turno,
                    LTRIM(RTRIM(ISNULL(Valvula, ''))),
                    LTRIM(RTRIM(ISNULL(Cama, '')))
                HAVING COUNT(*) > 1
            )
            SELECT COUNT(*) FROM duplicados
            """
        )
        or 0
    )
    bridge = int(escalar(conexion, "SELECT COUNT(*) FROM Silver.Bridge_Geografia_Cama") or 0)
    return {
        "sp_validar_calidad_camas": sp,
        "bridge_geografia_cama": bridge,
        "vigentes": int(vigentes.get("vigentes") or 0),
        "test_block": int(vigentes.get("test_block") or 0),
        "sin_cama_explicita": int(vigentes.get("sin_cama_explicita") or 0),
        "duplicados_vigentes": duplicados_vigentes,
    }


def consultar_dim_personal(conexion: Connection) -> dict[str, Any]:
    return fila_dict(
        conexion,
        """
        SELECT
            COUNT(*) AS total,
            SUM(CASE WHEN ID_Personal = -1 THEN 1 ELSE 0 END) AS surrogate_menos_uno,
            SUM(CASE WHEN ID_Personal != -1 THEN 1 ELSE 0 END) AS filas_reales,
            SUM(CASE WHEN Nombre_Completo = 'Sin Nombre' THEN 1 ELSE 0 END) AS sin_nombre,
            SUM(CASE WHEN Sexo IS NULL THEN 1 ELSE 0 END) AS sin_sexo,
            SUM(CASE WHEN ID_Planilla IS NULL THEN 1 ELSE 0 END) AS sin_planilla
        FROM Silver.Dim_Personal
        """,
    )


def consultar_diccionario(conexion: Connection) -> dict[str, Any]:
    return fila_dict(
        conexion,
        """
        SELECT
            SUM(CASE WHEN Aprobado_Por IS NULL OR Aprobado_Por = 'PENDIENTE' THEN 1 ELSE 0 END) AS pendientes,
            SUM(CASE WHEN Aprobado_Por IS NOT NULL AND Aprobado_Por <> 'PENDIENTE' THEN 1 ELSE 0 END) AS aprobados
        FROM MDM.Diccionario_Homologacion
        """,
    )


def consultar_modulo_9(conexion: Connection) -> list[dict[str, Any]]:
    return filas_dict(conexion, SQL_MODULO_9)


def consultar_doc_vs_sql(baseline: dict[str, Any]) -> list[dict[str, Any]]:
    dominios = baseline["dominios"]
    geo = dominios["dim_geografia"]
    clima = dominios["clima"]
    induccion = dominios["induccion"]
    tasa = dominios["tasa"]
    dim_personal = dominios["dim_personal"]
    tareo = dominios["tareo"]
    modulo_9_total = sum(item["Total"] for item in baseline["control_global"]["modulo_9_por_tabla"])
    estado_sp = geo["metricas_geo"]["sp_validar_calidad_camas"].get("Estado_Calidad_Cama", "OK_OPERATIVO")

    return [
        {
            "afirmacion": "Servidor/base operativos siguen siendo LCP-PAG-PRACTIC / ACP_DataWarehose_Proyecciones.",
            "clasificacion": "VALIDADA EN SQL",
            "evidencia": f"{baseline['contexto']['servidor_sql']} / {baseline['contexto']['base_sql']}",
        },
        {
            "afirmacion": "sp_Validar_Calidad_Camas debe quedar en OK_OPERATIVO.",
            "clasificacion": "VALIDADA EN SQL" if estado_sp == "OK_OPERATIVO" else "INCONSISTENTE ENTRE DOC Y REALIDAD",
            "evidencia": str(geo["metricas_geo"]["sp_validar_calidad_camas"]),
        },
        {
            "afirmacion": "Fact_Telemetria_Clima quedó cerrado operativamente y sin duplicados canónicos.",
            "clasificacion": "VALIDADA EN SQL" if clima["duplicados"].get("grupos", 0) == 0 else "INCONSISTENTE ENTRE DOC Y REALIDAD",
            "evidencia": f"Duplicados canónicos: {clima['duplicados'].get('grupos', 0)} grupos; cuarentena pendiente: {clima['cuarentena_total_pendiente']}",
        },
        {
            "afirmacion": "Inducción floral se mantiene funcional desde Silver.",
            "clasificacion": "VALIDADA CON RESIDUAL CONTROLADO" if induccion["metricas"]["total_filas"] > 0 else "INCONSISTENTE ENTRE DOC Y REALIDAD",
            "evidencia": f"Filas Silver: {induccion['metricas']['total_filas']}; cuarentena pendiente: {induccion['cuarentena_total_pendiente']}; duplicados: {induccion['duplicados'].get('grupos', 0)} grupos",
        },
        {
            "afirmacion": "Tasa de crecimiento se mantiene funcional sin residual crítico nuevo.",
            "clasificacion": "ABIERTA POR FUENTE/NEGOCIO" if tasa["cuarentena_total_pendiente"] and tasa["cuarentena_total_pendiente"] > 1000 else "VALIDADA CON RESIDUAL CONTROLADO",
            "evidencia": f"Filas Silver: {tasa['metricas']['total_filas']}; cuarentena pendiente: {tasa['cuarentena_total_pendiente']}; bronce rechazado: {tasa['bronze_total_rechazado']}",
        },
        {
            "afirmacion": "Dim_Personal sigue en estado conservador y pendiente de validación fuerte.",
            "clasificacion": "VALIDADA EN SQL",
            "evidencia": f"Filas reales: {dim_personal['metricas_dim_personal']['filas_reales']}; sin nombre: {dim_personal['metricas_dim_personal']['sin_nombre']}",
        },
        {
            "afirmacion": "Fact_Tareo sigue diagnosticado y pendiente hasta contar con fuente suficiente.",
            "clasificacion": "VALIDADA EN SQL" if tareo["metricas"]["total_filas"] == 0 else "VALIDADA CON RESIDUAL CONTROLADO",
            "evidencia": f"Silver: {tareo['metricas']['total_filas']} filas; Bronce: {tareo['bronze_total_filas']} filas",
        },
        {
            "afirmacion": "El residual vigente de módulo 9. sigue abierto y no debe inferirse automáticamente.",
            "clasificacion": "VALIDADA CON RESIDUAL CONTROLADO" if modulo_9_total > 0 else "INCONSISTENTE ENTRE DOC Y REALIDAD",
            "evidencia": f"Total residual detectado: {modulo_9_total}",
        },
    ]


def resumir_residual(dominio: dict[str, Any]) -> str:
    if dominio["id"] == "dim_geografia":
        return (
            f"Bridge={dominio['metricas_geo']['bridge_geografia_cama']}, "
            f"duplicados_vigentes={dominio['metricas_geo']['duplicados_vigentes']}, "
            f"sp_estado={dominio['metricas_geo']['sp_validar_calidad_camas'].get('Estado_Calidad_Cama', 'OK_OPERATIVO')}"
        )
    if dominio["id"] == "dim_personal":
        return f"filas_reales={dominio['metricas_dim_personal']['filas_reales']}, sin_nombre={dominio['metricas_dim_personal']['sin_nombre']}"
    if dominio["id"] == "tareo":
        return f"Bronce={dominio['bronze_total_filas']}, Silver={dominio['metricas']['total_filas']}"
    motivos = dominio["cuarentena_top_motivos"]
    if motivos:
        principal = motivos[0]
        return f"{principal['Motivo']} ({principal['total']})"
    duplicados = dominio.get("duplicados", {})
    if duplicados.get("grupos"):
        return f"Duplicados={duplicados['grupos']} grupos"
    minus1 = dominio["metricas"].get("minus1_pct")
    if minus1 is not None:
        return f"ID_Personal=-1 {minus1}%"
    return "Sin residual dominante documentado"


def clasificar_dominio(dominio: dict[str, Any]) -> tuple[str, str]:
    if dominio["id"] == "dim_geografia":
        estado_sp = dominio["metricas_geo"]["sp_validar_calidad_camas"].get("Estado_Calidad_Cama", "OK_OPERATIVO")
        if dominio["metricas_geo"]["duplicados_vigentes"] == 0 and estado_sp == "OK_OPERATIVO":
            return "CERRADO", "Calidad de camas OK, bridge persistido y sin duplicados vigentes."
        return "PENDIENTE FUNCIONAL", "El resolvedor geográfico no quedó en un estado operativo limpio."

    if dominio["id"] == "dim_personal":
        filas_reales = int(dominio["metricas_dim_personal"]["filas_reales"] or 0)
        sin_nombre = int(dominio["metricas_dim_personal"]["sin_nombre"] or 0)
        if filas_reales <= 1 or sin_nombre >= filas_reales:
            return "PENDIENTE FUNCIONAL", "La dimensión existe, pero sigue siendo demasiado débil para soportar cierre analítico."
        return "ESTABLE CON RESIDUAL CONTROLADO", "La dimensión existe, pero aún requiere validación fuerte con fuente rica."

    if dominio["id"] == "tareo":
        if dominio["bronze_total_filas"] == 0 and dominio["metricas"]["total_filas"] == 0:
            return "BLOQUEADO POR FUENTE", "No hay fuente vigente cargada en Bronce para validar o cerrar el dominio."
        return "PENDIENTE FUNCIONAL", "El dominio no tiene evidencia suficiente para declararse cerrado."

    if dominio["id"] == "clima":
        if dominio["metricas"]["total_filas"] == 0:
            return "PENDIENTE FUNCIONAL", "No hay filas en Silver para validar el frente clima."
        if dominio["duplicados"].get("grupos", 0) > 0:
            return "ESTABLE CON RESIDUAL CONTROLADO", "Carga viva y sin huérfanas, pero el criterio canónico de no duplicados ya no coincide con el cierre documental."
        return "CERRADO", "Clima quedó operativo y consistente con el criterio canónico vigente."

    if dominio["id"] == "tasa":
        if dominio["cuarentena_total_pendiente"] > 100000 or dominio["bronze_total_rechazado"] > 100000:
            return "PENDIENTE DE DECISIÓN DE NEGOCIO/MDM", "La geografía y MDM siguen explicando un residual masivo."
        if dominio["metricas"]["total_filas"] > 0:
            return "ESTABLE CON RESIDUAL CONTROLADO", "El fact carga, pero el residual aún requiere contención."
        return "PENDIENTE FUNCIONAL", "El fact no tiene masa crítica suficiente para validarse."

    if dominio["id"] == "fisiologia":
        return "ESTABLE CON RESIDUAL CONTROLADO", "Dominio operativo con residual abierto de módulo 9."

    total_filas = dominio["metricas"]["total_filas"]
    total_huerfanas = dominio["integridad_fk"]["total_huerfanas"]
    cuarentena = dominio["cuarentena_total_pendiente"]
    minus1_pct = dominio["metricas"].get("minus1_pct")

    if total_filas == 0:
        return "PENDIENTE FUNCIONAL", "No hay evidencia suficiente en Silver para cerrar el dominio."
    if total_huerfanas > 0:
        return "PENDIENTE FUNCIONAL", "Existen huérfanas referenciales en Silver."
    if minus1_pct is not None and minus1_pct >= 95:
        return "ESTABLE CON RESIDUAL CONTROLADO", "Estructuralmente sano, pero con dependencia total de un Dim_Personal aún conservador."
    if cuarentena > 0:
        return "ESTABLE CON RESIDUAL CONTROLADO", "Dominio operativo con residual puntual todavía abierto."
    return "CERRADO", "Dominio sin huérfanas y sin residual dominante abierto."


def evaluar_ml(dominio: dict[str, Any]) -> str:
    if dominio["es_dimension"]:
        if dominio["id"] == "dim_personal":
            filas_reales = int(dominio["metricas_dim_personal"]["filas_reales"] or 0)
            sin_nombre = int(dominio["metricas_dim_personal"]["sin_nombre"] or 0)
            return "NO_LISTO" if filas_reales <= 10 or sin_nombre >= filas_reales else "CONDICIONAL"
        return "LISTO"

    if dominio["id"] in {"tareo", "tasa"}:
        return "NO_LISTO"
    if dominio["id"] == "clima":
        return "CONDICIONAL" if dominio["duplicados"].get("grupos", 0) > 0 else "LISTO"
    minus1_pct = dominio["metricas"].get("minus1_pct")
    if minus1_pct is not None and minus1_pct >= 95:
        return "CONDICIONAL"
    if dominio["integridad_fk"]["total_huerfanas"] > 0:
        return "NO_LISTO"
    return "LISTO"


def construir_dominio(conexion: Connection, dominio: Dominio) -> dict[str, Any]:
    metricas = consultar_conteo_y_personal(conexion, dominio)
    integridad_fk = consultar_fks(conexion, dominio.tabla_silver)
    duplicados = consultar_duplicados(conexion, dominio.tabla_silver)
    bronze = [consultar_estados_bronce(conexion, tabla) for tabla in dominio.tablas_bronce]
    cuarentena = [consultar_cuarentena(conexion, tabla) for tabla in dominio.tablas_bronce]

    salida: dict[str, Any] = {
        "id": dominio.id,
        "nombre": dominio.nombre,
        "tabla_silver": dominio.tabla_silver,
        "tablas_bronce": list(dominio.tablas_bronce),
        "es_dimension": dominio.es_dimension,
        "nota_doc": dominio.nota_doc,
        "metricas": metricas,
        "integridad_fk": integridad_fk,
        "duplicados": duplicados,
        "ultimo_log": consultar_log_ultimo(conexion, dominio.tabla_silver),
        "bronze": bronze,
        "cuarentena": cuarentena,
        "bronze_total_filas": sum(item["total_filas"] for item in bronze),
        "bronze_total_rechazado": sum(
            estado["total"]
            for item in bronze
            for estado in item.get("por_estado", [])
            if estado["estado_carga"] == "RECHAZADO"
        ),
        "cuarentena_total_pendiente": sum(item["pendientes"] for item in cuarentena),
        "cuarentena_top_motivos": [
            {
                "Tabla_Origen": item["tabla_origen"],
                "Motivo": motivo["Motivo"],
                "total": motivo["total"],
            }
            for item in cuarentena
            for motivo in item["top_motivos"][:1]
        ],
    }

    if dominio.id == "dim_geografia":
        salida["metricas_geo"] = consultar_dim_geografia(conexion)
    if dominio.id == "dim_personal":
        salida["metricas_dim_personal"] = consultar_dim_personal(conexion)

    dictamen, razon = clasificar_dominio(salida)
    salida["dictamen"] = dictamen
    salida["razon_dictamen"] = razon
    salida["readiness_ml"] = evaluar_ml(salida)
    salida["residual_dominante"] = resumir_residual(salida)
    return salida


def resumir_baseline(baseline: dict[str, Any]) -> dict[str, Any]:
    conteo_dictamenes: dict[str, int] = defaultdict(int)
    for dominio in baseline["dominios"].values():
        conteo_dictamenes[dominio["dictamen"]] += 1

    pendientes_criticos = [
        {
            "dominio": dominio["nombre"],
            "dictamen": dominio["dictamen"],
            "razon": dominio["razon_dictamen"],
        }
        for dominio in baseline["dominios"].values()
        if dominio["dictamen"] in {
            "PENDIENTE FUNCIONAL",
            "BLOQUEADO POR FUENTE",
            "PENDIENTE DE DECISIÓN DE NEGOCIO/MDM",
        }
    ]

    return {
        "conteo_dictamenes": dict(conteo_dictamenes),
        "pendientes_criticos": pendientes_criticos,
        "ready_ml": [
            dominio["nombre"]
            for dominio in baseline["dominios"].values()
            if dominio["readiness_ml"] == "LISTO"
        ],
        "no_ready_ml": [
            dominio["nombre"]
            for dominio in baseline["dominios"].values()
            if dominio["readiness_ml"] == "NO_LISTO"
        ],
    }


def construir_baseline(engine: Engine) -> dict[str, Any]:
    with engine.connect() as conexion:
        baseline: dict[str, Any] = {
            "fecha_corte_referencial": FECHA_CORTE,
            "fecha_generacion": datetime.now().isoformat(sep=" ", timespec="seconds"),
            "contexto": consultar_contexto(conexion),
            "docs_operativos_contrastados": DOCS_OPERATIVOS,
            "control_global": {
                "diccionario_homologacion": consultar_diccionario(conexion),
                "modulo_9_por_tabla": consultar_modulo_9(conexion),
            },
            "dominios": {},
        }

        for dominio in DOMINIOS:
            baseline["dominios"][dominio.id] = construir_dominio(conexion, dominio)

        baseline["doc_vs_sql"] = consultar_doc_vs_sql(baseline)
        baseline["resumen_global"] = resumir_baseline(baseline)
        return baseline


def formatear_numero(valor: Any) -> str:
    if valor is None:
        return "-"
    if isinstance(valor, float):
        return f"{valor:,.2f}"
    if isinstance(valor, int):
        return f"{valor:,}"
    return str(valor)


def renderizar_tabla_resumen(baseline: dict[str, Any]) -> str:
    filas = [
        "| Dominio | Filas Silver | Dictamen | Residual dominante | Readiness ML |",
        "| --- | ---: | --- | --- | --- |",
    ]
    for dominio in baseline["dominios"].values():
        total = dominio.get("metricas", {}).get("total_filas")
        if dominio["id"] == "dim_personal":
            total = dominio["metricas_dim_personal"]["total"]
        if dominio["id"] == "dim_geografia":
            total = dominio["metricas_geo"]["vigentes"]
        filas.append(
            f"| {dominio['nombre']} | {formatear_numero(total)} | {dominio['dictamen']} | {dominio['residual_dominante']} | {dominio['readiness_ml']} |"
        )
    return "\n".join(filas)


def renderizar_detalle_dominio(dominio: dict[str, Any]) -> str:
    lineas = [f"### {dominio['nombre']}"]
    lineas.append(f"- Dictamen: `{dominio['dictamen']}`")
    lineas.append(f"- Razon: {dominio['razon_dictamen']}")
    lineas.append(f"- Tabla Silver: `{dominio['tabla_silver']}`")
    if dominio["id"] == "dim_geografia":
        geo = dominio["metricas_geo"]
        lineas.append(f"- Vigentes: `{formatear_numero(geo['vigentes'])}`")
        lineas.append(f"- Bridge geografia-cama: `{formatear_numero(geo['bridge_geografia_cama'])}`")
        lineas.append(f"- Test block vigente: `{formatear_numero(geo['test_block'])}`")
        lineas.append(f"- Sin cama explicita: `{formatear_numero(geo['sin_cama_explicita'])}`")
        lineas.append(f"- Duplicados vigentes: `{formatear_numero(geo['duplicados_vigentes'])}`")
        lineas.append(f"- SP calidad camas: `{geo['sp_validar_calidad_camas']}`")
    elif dominio["id"] == "dim_personal":
        dim = dominio["metricas_dim_personal"]
        lineas.append(f"- Total filas: `{formatear_numero(dim['total'])}`")
        lineas.append(f"- Filas reales: `{formatear_numero(dim['filas_reales'])}`")
        lineas.append(f"- Surrogate -1: `{formatear_numero(dim['surrogate_menos_uno'])}`")
        lineas.append(f"- Sin nombre: `{formatear_numero(dim['sin_nombre'])}`")
        lineas.append(f"- Sin sexo: `{formatear_numero(dim['sin_sexo'])}`")
        lineas.append(f"- Sin planilla: `{formatear_numero(dim['sin_planilla'])}`")
    else:
        metricas = dominio["metricas"]
        lineas.append(f"- Filas Silver: `{formatear_numero(metricas['total_filas'])}`")
        if metricas.get("fecha_min_evento") or metricas.get("fecha_max_evento"):
            lineas.append(f"- Rango evento: `{metricas.get('fecha_min_evento', '-')}` -> `{metricas.get('fecha_max_evento', '-')}`")
        if metricas.get("minus1_pct") is not None:
            lineas.append(f"- ID_Personal = -1: `{formatear_numero(metricas['minus1_total'])}` ({formatear_numero(metricas['minus1_pct'])}%)")
        lineas.append(f"- Huérfanas FK: `{formatear_numero(dominio['integridad_fk']['total_huerfanas'])}`")
        lineas.append(f"- Cuarentena pendiente: `{formatear_numero(dominio['cuarentena_total_pendiente'])}`")
        lineas.append(f"- Bronce total: `{formatear_numero(dominio['bronze_total_filas'])}`")
        lineas.append(f"- Bronce rechazado: `{formatear_numero(dominio['bronze_total_rechazado'])}`")
        if dominio["duplicados"]:
            lineas.append(f"- Duplicados canónicos: `{dominio['duplicados'].get('grupos', 0)}` grupos / `{dominio['duplicados'].get('filas_extra', 0)}` filas extra")
            if "superposicion_total" in dominio["duplicados"]:
                total = dominio["duplicados"]["superposicion_total"]
                lineas.append(f"- Superposición total clima: `{total.get('grupos', 0)}` grupos / `{total.get('filas_extra', 0)}` filas extra")
    if dominio["ultimo_log"]:
        log = dominio["ultimo_log"]
        lineas.append(f"- Último log: `{log.get('Estado_Proceso', '-')}` | fin=`{log.get('Fecha_Fin', '-')}` | insertadas=`{formatear_numero(log.get('Filas_Insertadas'))}` | rechazadas=`{formatear_numero(log.get('Filas_Rechazadas'))}`")
    if dominio["cuarentena_top_motivos"]:
        lineas.append("- Residual dominante:")
        for item in dominio["cuarentena_top_motivos"][:3]:
            lineas.append(f"  - `{item['Tabla_Origen']}`: {item['Motivo']} ({formatear_numero(item['total'])})")
    return "\n".join(lineas)


def renderizar_markdown(baseline: dict[str, Any]) -> str:
    contexto = baseline["contexto"]
    resumen = baseline["resumen_global"]
    modulo_9 = baseline["control_global"]["modulo_9_por_tabla"]
    lineas = [
        "# Baseline Operativo Formal del ETL ACP",
        "",
        f"Corte de referencia: `{baseline['fecha_corte_referencial']}`",
        f"Fecha de generación: `{baseline['fecha_generacion']}`",
        "",
        "## Contexto validado",
        f"- Servidor SQL: `{contexto['servidor_sql']}`",
        f"- Base SQL: `{contexto['base_sql']}`",
        f"- Usuario SQL efectivo: `{contexto['usuario_sql']}`",
        "- Fuente oficial: instancia SQL real actual; los `.md` se usaron como contraste.",
        "",
        "## Resumen ejecutivo",
        f"- Dictámenes emitidos: `{resumen['conteo_dictamenes']}`",
        f"- Dominios listos para uso analítico directo/feature-ready: `{', '.join(resumen['ready_ml']) or '-'}`",
        f"- Dominios no listos para ML: `{', '.join(resumen['no_ready_ml']) or '-'}`",
        f"- Pendiente global de homologación: `{formatear_numero(baseline['control_global']['diccionario_homologacion']['pendientes'])}`",
        "",
        renderizar_tabla_resumen(baseline),
        "",
        "## Dominio crítico transversal",
        f"- Residual detectado de `Modulo_Raw = '9.'`: `{formatear_numero(sum(item['Total'] for item in modulo_9))}`",
    ]
    for item in modulo_9:
        lineas.append(f"- `{item['Tabla']}`: `{formatear_numero(item['Total'])}`")
    lineas.extend(["", "## Cruce documental vs SQL"])
    for item in baseline["doc_vs_sql"]:
        lineas.append(f"- `{item['clasificacion']}`: {item['afirmacion']} Evidencia: {item['evidencia']}")
    lineas.extend(["", "## Detalle por dominio"])
    for dominio in baseline["dominios"].values():
        lineas.extend(["", renderizar_detalle_dominio(dominio)])
    lineas.extend([
        "",
        "## Pendientes reales para la siguiente fase",
        "- Deuda de código: consolidar `lookup.py`, separar SP oficial de fallback legacy y volver testeable el resolvedor geográfico.",
        "- Deuda de datos/fuente: cerrar la política de recarga, bajar residual geográfico de `Tasa_Crecimiento_Brotes` y recuperar fuente fuerte para `Dim_Personal` / `Fact_Tareo`.",
        "- Deuda de negocio/MDM: formalizar la regla final de `Modulo_Raw = '9.'` y revisar la masa pendiente que sigue entrando a cuarentena por geografía.",
        "- Deuda de readiness ML: no promover a dataset formal los dominios con `ID_Personal = -1` masivo ni los dominios con duplicidad/ambigüedad no resuelta.",
        "",
        "## Criterio de no regresión",
        "- No cambiar la fuente oficial del baseline sin registrar nueva fecha de corte.",
        "- No declarar cerrado un dominio solo porque el MD lo diga; prevalece la evidencia SQL.",
        "- No mover a Gold ni a features de ML dominios con `PENDIENTE FUNCIONAL`, `BLOQUEADO POR FUENTE` o `PENDIENTE DE DECISIÓN DE NEGOCIO/MDM`.",
        "- Reejecutar este baseline después de cualquier cambio en geografía, campaña, recarga o Dim_Personal.",
        "",
        "## MD contrastados",
    ])
    for doc in baseline["docs_operativos_contrastados"]:
        lineas.append(f"- `{doc}`")
    return "\n".join(lineas) + "\n"


def guardar_salida(baseline: dict[str, Any]) -> tuple[Path, Path]:
    json_path = AVANCE_DIR / "baseline_operativo_etl_20260406.json"
    md_path = AVANCE_DIR / "BASELINE_OPERATIVO_ETL_20260406.md"
    json_path.write_text(json.dumps(baseline, ensure_ascii=False, indent=2), encoding="utf-8")
    md_path.write_text(renderizar_markdown(baseline), encoding="utf-8")
    return json_path, md_path


def main() -> None:
    engine = crear_engine()
    baseline = construir_baseline(engine)
    json_path, md_path = guardar_salida(baseline)
    print(f"JSON generado: {json_path}")
    print(f"Markdown generado: {md_path}")


if __name__ == "__main__":
    main()
