"""
fact_peladas.py
===============
Carga Silver.Fact_Peladas desde Bronce.Peladas.

Grain: Fecha + Geo + Variedad + Punto
Validación crítica: Muestras >= 1 (evita división por cero)
"""

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text

from utils.contexto_transaccional import ContextoTransaccionalETL
from utils.fechas    import procesar_fecha, obtener_id_tiempo
from utils.texto     import normalizar_modulo, es_test_block
from utils.dni       import procesar_dni
from dq.validador    import validar_muestras
from dq.cuarentena   import enviar_a_cuarentena
from mdm.lookup      import resolver_geografia, obtener_id_variedad, obtener_id_personal
from mdm.homologador import homologar_columna
from silver.facts._helpers_fact_comunes import (
    finalizar_resumen_fact as _finalizar_resumen_fact,
    parsear_valores_raw as _parsear_valores_raw,
)


TABLA_ORIGEN  = 'Bronce.Peladas'
TABLA_DESTINO = 'Silver.Fact_Peladas'


def _registrar_rechazo(
    resumen: dict,
    ids_rechazados: list[int],
    id_origen: int,
    *,
    columna: str,
    valor,
    motivo: str,
    tipo_regla: str = 'DQ',
    severidad: str = 'ALTO',
):
    resumen['rechazados'] += 1
    ids_rechazados.append(id_origen)
    resumen['cuarentena'].append({
        'columna': columna,
        'valor': valor,
        'motivo': motivo,
        'severidad': severidad,
        'tipo_regla': tipo_regla,
        'id_registro_origen': id_origen,
    })


def _motivo_rechazo_geografia(resultado_geo: dict | None) -> str:
    if not resultado_geo:
        return 'Geografia no encontrada en Silver.Dim_Geografia.'

    detalle = str(resultado_geo.get('detalle') or '').strip()
    estado = str(resultado_geo.get('estado') or '').strip().upper()
    if detalle:
        return detalle
    if estado:
        return f'Geografia no resuelta: {estado}.'
    return 'Geografia no encontrada en Silver.Dim_Geografia.'


def _leer_bronce(engine: Engine) -> pd.DataFrame:
    with engine.connect() as conexion:
        columnas_resultado = conexion.execute(text("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = 'Bronce'
              AND TABLE_NAME = 'Peladas'
        """)).fetchall()
        columnas_disponibles = {str(fila[0]) for fila in columnas_resultado}

        def _columna_sql(nombre_columna: str) -> str:
            if nombre_columna in columnas_disponibles:
                return nombre_columna
            return f"CAST(NULL AS NVARCHAR(MAX)) AS {nombre_columna}"

        filtro_estado = "WHERE Estado_Carga = 'CARGADO'" if 'Estado_Carga' in columnas_disponibles else ""
        columnas_select = [
            'ID_Peladas',
            _columna_sql('Fecha_Raw'),
            _columna_sql('Fundo_Raw'),
            _columna_sql('Modulo_Raw'),
            _columna_sql('Turno_Raw'),
            _columna_sql('Valvula_Raw'),
            _columna_sql('Variedad_Raw'),
            _columna_sql('DNI_Raw'),
            _columna_sql('Evaluador_Raw'),
            _columna_sql('Punto_Raw'),
            _columna_sql('Muestras_Raw'),
            _columna_sql('BotonesFlorales_Raw'),
            _columna_sql('Flores_Raw'),
            _columna_sql('BayasPequenas_Raw'),
            _columna_sql('BayasGrandes_Raw'),
            _columna_sql('Fase1_Raw'),
            _columna_sql('Fase2_Raw'),
            _columna_sql('BayasCremas_Raw'),
            _columna_sql('BayasMaduras_Raw'),
            _columna_sql('BayasCosechables_Raw'),
            _columna_sql('PlantasProductivas_Raw'),
            _columna_sql('PlantasNoProductivas_Raw'),
            _columna_sql('Valores_Raw'),
        ]

        resultado = conexion.execute(text(f"""
            SELECT
                {", ".join(columnas_select)}
            FROM {TABLA_ORIGEN}
            {filtro_estado}
        """))
        return pd.DataFrame(resultado.fetchall(), columns=resultado.keys())


def _a_int(valor, default: int = 0) -> int:
    try:
        return max(0, int(float(str(valor))))
    except (ValueError, TypeError):
        return default


def _obtener_valor_raw(
    fila: pd.Series,
    nombre_columna: str,
    valores_raw: dict[str, str] | None = None,
):
    valor = fila.get(nombre_columna)
    if valor is not None and str(valor).strip() not in ('', 'None', 'nan'):
        return valor

    if valores_raw is None:
        valores_raw = _parsear_valores_raw(fila.get('Valores_Raw'))

    valor_serializado = valores_raw.get(nombre_columna)
    if valor_serializado is None or str(valor_serializado).strip() in ('', 'None', 'nan'):
        return None
    return valor_serializado


def cargar_fact_peladas(engine: Engine) -> dict:
    resumen = {'insertados': 0, 'rechazados': 0, 'cuarentena': []}

    df = _leer_bronce(engine)
    if df.empty:
        return _finalizar_resumen_fact(resumen)
    resumen['leidos'] = len(df)

    df, cuar_var = homologar_columna(
        df, 'Variedad_Raw', 'Variedad_Canonica', TABLA_ORIGEN, engine
    )
    resumen['cuarentena'].extend(cuar_var)

    ids_procesados = []
    ids_rechazados = []

    with ContextoTransaccionalETL(engine) as contexto:
        conexion = contexto._conexion_activa()
        for _, fila in df.iterrows():
            id_origen = int(fila['ID_Peladas'])
            valores_raw = _parsear_valores_raw(fila.get('Valores_Raw'))

            fecha, valida = procesar_fecha(
                _obtener_valor_raw(fila, 'Fecha_Raw', valores_raw),
                dominio='peladas',
            )
            if not valida:
                _registrar_rechazo(
                    resumen,
                    ids_rechazados,
                    id_origen,
                    columna='Fecha_Raw',
                    valor=_obtener_valor_raw(fila, 'Fecha_Raw', valores_raw),
                    motivo='Fecha invalida o fuera de campana en peladas',
                    tipo_regla='DQ',
                )
                continue

            modulo_raw = _obtener_valor_raw(fila, 'Modulo_Raw', valores_raw)
            turno_raw = _obtener_valor_raw(fila, 'Turno_Raw', valores_raw)
            valvula_raw = _obtener_valor_raw(fila, 'Valvula_Raw', valores_raw)
            fundo_raw = _obtener_valor_raw(fila, 'Fundo_Raw', valores_raw)
            modulo = None if es_test_block(modulo_raw) else normalizar_modulo(modulo_raw)
            resultado_geo = resolver_geografia(
                fundo_raw,
                None,
                modulo,
                engine,
                turno=turno_raw,
                valvula=valvula_raw,
                cama=None,
            )
            id_geo = resultado_geo.get('id_geografia') if resultado_geo else None
            id_var  = obtener_id_variedad(fila.get('Variedad_Canonica'), engine)

            if not id_geo:
                _registrar_rechazo(
                    resumen,
                    ids_rechazados,
                    id_origen,
                    columna='Modulo_Raw',
                    valor=(
                        f"Fundo={fundo_raw} | Modulo={modulo_raw} | "
                        f"Turno={turno_raw} | Valvula={valvula_raw}"
                    ),
                    motivo=_motivo_rechazo_geografia(resultado_geo),
                    tipo_regla='MDM',
                )
                continue

            if not id_var:
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

            dni, _      = procesar_dni(_obtener_valor_raw(fila, 'DNI_Raw', valores_raw))
            id_personal = obtener_id_personal(dni, engine)

            # Validación crítica — Muestras >= 1
            muestras, error_muestras = validar_muestras(
                _obtener_valor_raw(fila, 'Muestras_Raw', valores_raw)
            )
            if error_muestras:
                _registrar_rechazo(
                    resumen,
                    ids_rechazados,
                    id_origen,
                    columna=error_muestras.get('columna', 'Muestras'),
                    valor=error_muestras.get('valor'),
                    motivo=error_muestras.get('motivo', 'Muestras invalidas'),
                    tipo_regla='DQ',
                    severidad=error_muestras.get('severidad', 'ALTO'),
                )
                continue

            try:
                punto = int(float(str(_obtener_valor_raw(fila, 'Punto_Raw', valores_raw) or 1)))
            except (ValueError, TypeError):
                punto = 1

            conexion.execute(text("""
                INSERT INTO Silver.Fact_Peladas (
                    ID_Geografia, ID_Tiempo, ID_Variedad, ID_Personal,
                    Punto, Muestras,
                    Botones_Florales, Flores,
                    Bayas_Pequenas, Bayas_Grandes,
                    Fase_1, Fase_2,
                    Bayas_Cremas, Bayas_Maduras, Bayas_Cosechables,
                    Plantas_Productivas, Plantas_No_Productivas,
                    Fecha_Evento, Fecha_Sistema, Estado_DQ
                ) VALUES (
                    :id_geo, :id_tiempo, :id_variedad, :id_personal,
                    :punto, :muestras,
                    :botones, :flores,
                    :pequenas, :grandes,
                    :fase1, :fase2,
                    :cremas, :maduras, :cosechables,
                    :productivas, :no_productivas,
                    :fecha_evento, SYSDATETIME(), 'OK'
                )
            """), {
                'id_geo':          id_geo,
                'id_tiempo':       obtener_id_tiempo(fecha),
                'id_variedad':     id_var,
                'id_personal':     id_personal,
                'punto':           punto,
                'muestras':        muestras,
                'botones':         _a_int(_obtener_valor_raw(fila, 'BotonesFlorales_Raw', valores_raw)),
                'flores':          _a_int(_obtener_valor_raw(fila, 'Flores_Raw', valores_raw)),
                'pequenas':        _a_int(_obtener_valor_raw(fila, 'BayasPequenas_Raw', valores_raw)),
                'grandes':         _a_int(_obtener_valor_raw(fila, 'BayasGrandes_Raw', valores_raw)),
                'fase1':           _a_int(_obtener_valor_raw(fila, 'Fase1_Raw', valores_raw)),
                'fase2':           _a_int(_obtener_valor_raw(fila, 'Fase2_Raw', valores_raw)),
                'cremas':          _a_int(_obtener_valor_raw(fila, 'BayasCremas_Raw', valores_raw)),
                'maduras':         _a_int(_obtener_valor_raw(fila, 'BayasMaduras_Raw', valores_raw)),
                'cosechables':     _a_int(_obtener_valor_raw(fila, 'BayasCosechables_Raw', valores_raw)),
                'productivas':     _a_int(_obtener_valor_raw(fila, 'PlantasProductivas_Raw', valores_raw)),
                'no_productivas':  _a_int(_obtener_valor_raw(fila, 'PlantasNoProductivas_Raw', valores_raw)),
                'fecha_evento':    fecha,
            })

            ids_procesados.append(id_origen)
            resumen['insertados'] += 1

        if ids_procesados:
            contexto.marcar_estado_carga(TABLA_ORIGEN, 'ID_Peladas', ids_procesados)
        if ids_rechazados:
            contexto.marcar_estado_carga(TABLA_ORIGEN, 'ID_Peladas', ids_rechazados, estado='RECHAZADO')

        if resumen['cuarentena']:
            contexto.enviar_cuarentena(TABLA_ORIGEN, resumen['cuarentena'])

    return _finalizar_resumen_fact(resumen)
