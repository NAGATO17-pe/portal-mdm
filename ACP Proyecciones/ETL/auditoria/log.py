"""
log.py
======
Registro de auditoría del pipeline.
Todo INSERT exitoso o fallido queda registrado en Auditoria.Log_Carga.
Las tablas de auditoría son append-only — sin UPDATE ni DELETE.
"""

from datetime import datetime
from sqlalchemy.engine import Engine
from sqlalchemy import text

from config.conexion import obtener_engine


def registrar_inicio(tabla_destino: str,
                     nombre_archivo: str) -> int:
    engine = obtener_engine()

    with engine.begin() as conexion:
        resultado = conexion.execute(text("""
            INSERT INTO Auditoria.Log_Carga (
                Nombre_Proceso,
                Tabla_Destino,
                Nombre_Archivo_Fuente,
                Fecha_Inicio,
                Estado_Proceso,
                Filas_Leidas,
                Filas_Insertadas,
                Filas_Rechazadas,
                Duracion_Segundos,
                Mensaje_Error
            )
            OUTPUT INSERTED.ID_Log_Carga
            VALUES (
                :nombre_proceso,
                :tabla_destino,
                :nombre_archivo,
                :fecha_inicio,
                'EN_PROCESO',
                0, 0, 0, 0, NULL
            )
        """), {
            'nombre_proceso': 'ETL_Pipeline',
            'tabla_destino':  tabla_destino,
            'nombre_archivo': nombre_archivo,
            'fecha_inicio':   datetime.now(),
        })

        return resultado.fetchone()[0]


def registrar_fin(id_log: int, resultado: dict) -> None:
    engine  = obtener_engine()
    fin     = datetime.now()
    estado  = resultado.get('estado', 'ERROR')
    filas   = resultado.get('filas', 0)
    mensaje = resultado.get('mensaje', '')

    with engine.begin() as conexion:
        conexion.execute(text("""
            UPDATE Auditoria.Log_Carga
            SET
                Fecha_Fin             = :fecha_fin,
                Estado_Proceso        = :estado,
                Filas_Insertadas      = :filas_insertadas,
                Filas_Rechazadas      = :filas_rechazadas,
                Duracion_Segundos     = DATEDIFF(
                    SECOND, Fecha_Inicio, :fecha_fin
                ),
                Mensaje_Error         = :mensaje_error
            WHERE ID_Log_Carga = :id_log
        """), {
            'fecha_fin':        fin,
            'estado':           estado,
            'filas_insertadas': filas if estado == 'OK' else 0,
            'filas_rechazadas': resultado.get('rechazadas', 0),
            'mensaje_error':    mensaje if estado != 'OK' else None,
            'id_log':           id_log,
        })

def registrar_decision_mdm(tabla_origen: str,
                            texto_crudo: str,
                            valor_canonico: str,
                            decision: str,
                            analista_dni: str,
                            comentario: str = '') -> None:
    """
    Registra una decisión humana de homologación MDM
    en Auditoria.Log_Decisiones_MDM.
    Solo INSERT — el historial de decisiones no se modifica.
    """
    engine = obtener_engine()

    with engine.begin() as conexion:
        conexion.execute(text("""
            INSERT INTO Auditoria.Log_Decisiones_MDM (
                Tabla_Origen,
                Texto_Crudo,
                Valor_Canonico,
                Decision,
                Analista_DNI,
                Comentario,
                Fecha_Decision
            ) VALUES (
                :tabla_origen,
                :texto_crudo,
                :valor_canonico,
                :decision,
                :analista_dni,
                :comentario,
                :fecha_decision
            )
        """), {
            'tabla_origen':   tabla_origen,
            'texto_crudo':    texto_crudo,
            'valor_canonico': valor_canonico,
            'decision':       decision,
            'analista_dni':   analista_dni,
            'comentario':     comentario,
            'fecha_decision': datetime.now(),
        })


def obtener_ultimo_estado(tabla_destino: str) -> dict | None:
    """
    Retorna el último registro de Log_Carga para una tabla.
    Útil para el dashboard de inicio del portal Streamlit.
    """
    engine = obtener_engine()

    with engine.connect() as conexion:
        resultado = conexion.execute(text("""
            SELECT TOP 1
                Estado,
                Fecha_Inicio,
                Fecha_Fin,
                Filas_Insertadas,
                Filas_Rechazadas,
                Duracion_Segundos,
                Mensaje_Error
            FROM Auditoria.Log_Carga
            WHERE Tabla_Destino = :tabla_destino
            ORDER BY Fecha_Inicio DESC
        """), {'tabla_destino': tabla_destino})

        fila = resultado.fetchone()
        if not fila:
            return None

        return dict(zip(resultado.keys(), fila))
