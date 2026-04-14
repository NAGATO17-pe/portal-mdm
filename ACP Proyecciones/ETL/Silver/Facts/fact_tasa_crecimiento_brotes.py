"""
fact_tasa_crecimiento_brotes.py
===============================
Carga Silver.Fact_Tasa_Crecimiento_Brotes desde Bronce.Tasa_Crecimiento_Brotes.
"""

from __future__ import annotations

import pandas as pd
from sqlalchemy import text
from sqlalchemy.engine import Engine

from mdm.homologador import homologar_columna
from mdm.lookup import (
    obtener_id_personal,
    obtener_id_tiempo,
    obtener_id_variedad,
    resolver_geografia,
)
from utils.dni import procesar_dni
from utils.contexto_transaccional import ContextoTransaccionalETL
from utils.fechas import obtener_id_tiempo as construir_id_tiempo, procesar_fecha
from utils.sql_lotes import ejecutar_en_lotes
from utils.texto import es_test_block, normalizar_modulo
from silver.facts._helpers_fact_comunes import (
    a_entero_nulo as _a_entero_nulo,
    finalizar_resumen_fact as _finalizar_resumen_fact,
    texto_nulo as _texto_nulo,
    motivo_cuarentena_geografia as _motivo_cuarentena_geografia,
    validar_layout_migrado as _validar_layout_migrado_helper,
)


TABLA_ORIGEN = 'Bronce.Tasa_Crecimiento_Brotes'
TABLA_DESTINO = 'Silver.Fact_Tasa_Crecimiento_Brotes'

SQL_INSERT_FACT = text("""
    INSERT INTO Silver.Fact_Tasa_Crecimiento_Brotes (
        ID_Geografia, ID_Tiempo, ID_Variedad, ID_Personal,
        Tipo_Evaluacion, Condicion, Estado_Vegetativo,
        Tipo_Tallo, Codigo_Ensayo, Codigo_Origen,
        Campana, Observacion,
        Fecha_Poda_Aux, Dias_Desde_Poda, Medida_Crecimiento,
        Fecha_Evento, Fecha_Sistema, Estado_DQ
    ) VALUES (
        :id_geo, :id_tiempo, :id_variedad, :id_personal,
        :tipo_evaluacion, :condicion, :estado_vegetativo,
        :tipo_tallo, :codigo_ensayo, :codigo_origen,
        :campana, :observacion,
        :fecha_poda_aux, :dias_desde_poda, :medida_crecimiento,
        :fecha_evento, SYSDATETIME(), 'OK'
    )
""")


def _a_decimal_nulo(valor) -> float | None:
    try:
        if valor is None:
            return None
        texto = str(valor).strip().replace(',', '.')
        if texto in ('', 'None', 'nan'):
            return None
        return float(texto)
    except (ValueError, TypeError):
        return None


def _clave_cache_fecha(valor) -> str:
    return str(valor).strip() if valor is not None else ''


def _resolver_fecha_evento_cache(valor, engine: Engine, cache: dict[str, tuple]) -> tuple:
    clave = _clave_cache_fecha(valor)
    if clave not in cache:
        fecha_evento, valida = procesar_fecha(
            valor,
            dominio='tasa_crecimiento_brotes',
        )
        id_tiempo = None
        if valida and fecha_evento is not None:
            id_tiempo = obtener_id_tiempo(construir_id_tiempo(fecha_evento), engine)
        cache[clave] = (fecha_evento, valida, id_tiempo)
    return cache[clave]


def _resolver_fecha_poda_cache(valor, cache: dict[str, tuple]) -> tuple:
    clave = _clave_cache_fecha(valor)
    if clave not in cache:
        cache[clave] = procesar_fecha(valor, dominio='historico')
    return cache[clave]


def _registrar_rechazo(
    resumen: dict,
    ids_rechazados: set[int],
    id_origen: int | None,
    *,
    columna: str,
    valor,
    motivo: str,
    tipo_regla: str,
) -> None:
    resumen['rechazados'] += 1
    if id_origen is not None:
        ids_rechazados.add(id_origen)
    resumen['cuarentena'].append({
        'columna': columna,
        'valor': valor,
        'motivo': motivo,
        'tipo_regla': tipo_regla,
        'id_registro_origen': id_origen,
    })


def _validar_layout_migrado(engine: Engine) -> str:
    return _validar_layout_migrado_helper(
        engine,
        tabla_origen=TABLA_ORIGEN,
        tabla_destino=TABLA_DESTINO,
        columna_id='ID_Tasa_Crecimiento',
        columnas_bronce_requeridas={
            'ID_Tasa_Crecimiento',
            'Fecha_Raw',
            'DNI_Raw',
            'Modulo_Raw',
            'Turno_Raw',
            'Valvula_Raw',
            'Cama_Raw',
            'Condicion_Raw',
            'Estado_Vegetativo_Raw',
            'Variedad_Raw',
            'Tipo_Tallo_Raw',
            'Ensayo_Raw',
            'Medida_Raw',
            'Fecha_Poda_Aux_Raw',
            'Campana_Raw',
            'Tipo_Evaluacion_Raw',
            'Estado_Carga',
        },
        columnas_silver_requeridas={
            'ID_Geografia',
            'ID_Tiempo',
            'ID_Variedad',
            'ID_Personal',
            'Codigo_Ensayo',
            'Codigo_Origen',
            'Fecha_Poda_Aux',
            'Dias_Desde_Poda',
            'Medida_Crecimiento',
        },
        nombre_layout='Tasa_Crecimiento_Brotes',
    )


def _leer_bronce(engine: Engine, columna_id: str) -> pd.DataFrame:
    with engine.connect() as conexion:
        resultado = conexion.execute(text(f"""
            SELECT
                {columna_id} AS ID_Registro_Origen,
                Codigo_Origen_Raw,
                Semana_Raw,
                Dia_Raw,
                Fecha_Raw,
                DNI_Raw,
                Evaluador_Raw,
                Modulo_Raw,
                Turno_Raw,
                Valvula_Raw,
                Condicion_Raw,
                Estado_Vegetativo_Raw,
                Variedad_Raw,
                Cama_Raw,
                Tipo_Tallo_Raw,
                Ensayo_Raw,
                Medida_Raw,
                Fecha_Poda_Aux_Raw,
                Campana_Raw,
                Observacion_Raw,
                Tipo_Evaluacion_Raw
            FROM {TABLA_ORIGEN}
            WHERE Estado_Carga = 'CARGADO'
        """))
        return pd.DataFrame(resultado.fetchall(), columns=resultado.keys())


def cargar_fact_tasa_crecimiento_brotes(engine: Engine) -> dict:
    resumen = {'insertados': 0, 'rechazados': 0, 'cuarentena': []}

    columna_id = _validar_layout_migrado(engine)
    df = _leer_bronce(engine, columna_id)
    if df.empty:
        return _finalizar_resumen_fact(resumen)
    resumen['leidos'] = len(df)

    payload_inserts = []
    ids_insertados: set[int] = set()
    ids_rechazados: set[int] = set()
    cache_fechas_evento: dict[str, tuple] = {}
    cache_fechas_poda: dict[str, tuple] = {}

    with ContextoTransaccionalETL(engine) as contexto:
        conexion = contexto._conexion_activa()

        df, cuar_var = homologar_columna(
            df,
            'Variedad_Raw',
            'Variedad_Canonica',
            TABLA_ORIGEN,
            conexion,
            columna_id_origen='ID_Registro_Origen',
        )
        resumen['cuarentena'].extend(cuar_var)

        for _, fila in df.iterrows():
            id_origen = _a_entero_nulo(fila.get('ID_Registro_Origen'))

            fecha_evento, valida, id_tiempo = _resolver_fecha_evento_cache(
                fila.get('Fecha_Raw'),
                engine,
                cache_fechas_evento,
            )
            if not valida:
                _registrar_rechazo(
                    resumen,
                    ids_rechazados,
                    id_origen,
                    columna='Fecha_Raw',
                    valor=fila.get('Fecha_Raw'),
                    motivo='Fecha invalida en tasa de crecimiento',
                    tipo_regla='DQ',
                )
                continue

            modulo_raw = fila.get('Modulo_Raw')
            modulo = modulo_raw if es_test_block(modulo_raw) else normalizar_modulo(modulo_raw)
            resultado_geo = resolver_geografia(
                None,
                None,
                modulo,
                engine,
                turno=fila.get('Turno_Raw'),
                valvula=fila.get('Valvula_Raw'),
                cama=fila.get('Cama_Raw'),
            )
            id_geo = resultado_geo.get('id_geografia')
            if not id_geo:
                _registrar_rechazo(
                    resumen,
                    ids_rechazados,
                    id_origen,
                    columna='Modulo_Raw',
                    valor=(
                        f"Modulo={fila.get('Modulo_Raw')} | Turno={fila.get('Turno_Raw')} | "
                        f"Valvula={fila.get('Valvula_Raw')} | Cama={fila.get('Cama_Raw')}"
                    ),
                    motivo=_motivo_cuarentena_geografia(resultado_geo),
                    tipo_regla='MDM',
                )
                continue

            id_variedad = obtener_id_variedad(fila.get('Variedad_Canonica'), engine)
            if not id_variedad:
                _registrar_rechazo(
                    resumen,
                    ids_rechazados,
                    id_origen,
                    columna='Variedad_Raw',
                    valor=fila.get('Variedad_Raw'),
                    motivo='Variedad sin match en Dim_Variedad',
                    tipo_regla='MDM',
                )
                continue

            if id_tiempo is None:
                _registrar_rechazo(
                    resumen,
                    ids_rechazados,
                    id_origen,
                    columna='Fecha_Raw',
                    valor=fila.get('Fecha_Raw'),
                    motivo='Fecha valida pero fuera de Dim_Tiempo',
                    tipo_regla='DQ',
                )
                continue

            codigo_ensayo = _texto_nulo(fila.get('Ensayo_Raw'))
            if codigo_ensayo is None:
                _registrar_rechazo(
                    resumen,
                    ids_rechazados,
                    id_origen,
                    columna='Ensayo_Raw',
                    valor=fila.get('Ensayo_Raw'),
                    motivo='Codigo de ensayo vacio o invalido',
                    tipo_regla='DQ',
                )
                continue

            medida_crecimiento = _a_decimal_nulo(fila.get('Medida_Raw'))
            if medida_crecimiento is None or medida_crecimiento < 0:
                _registrar_rechazo(
                    resumen,
                    ids_rechazados,
                    id_origen,
                    columna='Medida_Raw',
                    valor=fila.get('Medida_Raw'),
                    motivo='Medida de crecimiento invalida o negativa',
                    tipo_regla='DQ',
                )
                continue

            fecha_poda_aux = None
            dias_desde_poda = None
            valor_fecha_poda = _texto_nulo(fila.get('Fecha_Poda_Aux_Raw'))
            if valor_fecha_poda is not None:
                fecha_poda_aux, valida_poda = _resolver_fecha_poda_cache(
                    valor_fecha_poda,
                    cache_fechas_poda,
                )
                if not valida_poda:
                    _registrar_rechazo(
                        resumen,
                        ids_rechazados,
                        id_origen,
                        columna='Fecha_Poda_Aux_Raw',
                        valor=fila.get('Fecha_Poda_Aux_Raw'),
                        motivo='Fecha de poda auxiliar invalida',
                        tipo_regla='DQ',
                    )
                    continue

                dias_desde_poda = (fecha_evento.date() - fecha_poda_aux.date()).days
                if dias_desde_poda < 0:
                    _registrar_rechazo(
                        resumen,
                        ids_rechazados,
                        id_origen,
                        columna='Fecha_Poda_Aux_Raw',
                        valor=fila.get('Fecha_Poda_Aux_Raw'),
                        motivo='Fecha de poda auxiliar posterior a la fecha de evaluacion',
                        tipo_regla='DQ',
                    )
                    continue

            dni, _ = procesar_dni(fila.get('DNI_Raw'))
            id_personal = obtener_id_personal(dni, engine)

            payload_inserts.append({
                'id_geo': id_geo,
                'id_tiempo': id_tiempo,
                'id_variedad': id_variedad,
                'id_personal': id_personal,
                'tipo_evaluacion': _texto_nulo(fila.get('Tipo_Evaluacion_Raw')),
                'condicion': _texto_nulo(fila.get('Condicion_Raw')),
                'estado_vegetativo': _texto_nulo(fila.get('Estado_Vegetativo_Raw')),
                'tipo_tallo': _texto_nulo(fila.get('Tipo_Tallo_Raw')),
                'codigo_ensayo': codigo_ensayo,
                'codigo_origen': _texto_nulo(fila.get('Codigo_Origen_Raw')),
                'campana': _texto_nulo(fila.get('Campana_Raw')),
                'observacion': _texto_nulo(fila.get('Observacion_Raw')),
                'fecha_poda_aux': None if fecha_poda_aux is None else fecha_poda_aux.date(),
                'dias_desde_poda': dias_desde_poda,
                'medida_crecimiento': medida_crecimiento,
                'fecha_evento': fecha_evento.date(),
            })
            if id_origen is not None:
                ids_insertados.add(id_origen)

        if payload_inserts:
            ejecutar_en_lotes(conexion, SQL_INSERT_FACT, payload_inserts)
        resumen['insertados'] = len(payload_inserts)

        if ids_insertados:
            contexto.marcar_estado_carga(TABLA_ORIGEN, columna_id, sorted(ids_insertados), estado='PROCESADO')
        if ids_rechazados:
            contexto.marcar_estado_carga(TABLA_ORIGEN, columna_id, sorted(ids_rechazados), estado='RECHAZADO')

        if resumen['cuarentena']:
            contexto.enviar_cuarentena(TABLA_ORIGEN, resumen['cuarentena'])

    return _finalizar_resumen_fact(resumen)
