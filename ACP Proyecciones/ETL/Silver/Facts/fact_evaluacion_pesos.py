"""
fact_evaluacion_pesos.py
========================
Carga Silver.Fact_Evaluacion_Pesos desde Bronce.Evaluacion_Pesos.

Grain: Fecha + Geo + Personal + Variedad
FKs obligatorias: ID_Tiempo, ID_Geografia, ID_Variedad, ID_Personal
ValidaciÃƒÂ³n crÃƒÂ­tica: Peso_Baya_g BETWEEN 0.5 AND 8.0
"""

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text

from utils.fechas    import procesar_fecha, obtener_id_tiempo as construir_id_tiempo
from utils.contexto_transaccional import ContextoTransaccionalETL
from utils.texto     import normalizar_modulo, es_test_block
from utils.dni       import procesar_dni
from dq.validador    import validar_peso_baya
from mdm.lookup      import (
    obtener_id_tiempo,
    resolver_geografia,
    obtener_id_variedad,
    obtener_id_personal,
)
from mdm.homologador import homologar_columna
from silver.facts._helpers_fact_comunes import (
    finalizar_resumen_fact as _finalizar_resumen_fact,
    motivo_cuarentena_geografia as _motivo_cuarentena_geografia,
    registrar_rechazo as _registrar_rechazo,
)


TABLA_ORIGEN  = 'Bronce.Evaluacion_Pesos'
TABLA_DESTINO = 'Silver.Fact_Evaluacion_Pesos'


def _leer_bronce(engine: Engine) -> pd.DataFrame:
    with engine.connect() as conexion:
        resultado = conexion.execute(text(f"""
            SELECT
                ID_Evaluacion_Pesos,
                Fecha_Raw,
                Fundo_Raw,
                Modulo_Raw,
                Valvula_Raw,
                Turno_Raw,
                Cama_Raw,
                Variedad_Raw,
                Evaluacion_Raw,
                DNI_Raw,
                PesoBaya_Raw,
                CantMuestra_Raw,
                BayasPequenas_Raw,
                PesoBayasPequenas_Raw,
                BayasGrandes_Raw,
                BayasFase1_Raw,
                PesoBayasFase1_Raw,
                BayasFase2_Raw,
                PesoBayasFase2_Raw,
                Cremas_Raw,
                PesoCremas_Raw,
                Maduras_Raw,
                PesoMaduras_Raw,
                Cosechables_Raw,
                PesoCosechables_Raw
            FROM {TABLA_ORIGEN}
            WHERE Estado_Carga = 'CARGADO'
        """))
        return pd.DataFrame(resultado.fetchall(), columns=resultado.keys())


def _calcular_peso_ponderado(fila) -> float | None:
    """
    Calcula el peso promedio ponderado de baya en gramos.
    Usa todas las categorÃƒÂ­as de bayas disponibles en el reporte horizontal.
    Retorna None si no hay datos suficientes para calcular.
    """
    def safe_float(val, default=0.0):
        try:
            return float(str(val)) if val is not None and str(val).strip() not in ('', 'None', 'nan') else default
        except (ValueError, TypeError):
            return default

    # Si ya viene un PesoBaya_Raw directo (otros formatos), usarlo
    peso_directo = safe_float(fila.get('PesoBaya_Raw'))
    cant_directo = safe_float(fila.get('CantMuestra_Raw'))
    if peso_directo > 0 and cant_directo > 0:
        return round(peso_directo / cant_directo, 4)

    # Calcular desde columnas horizontales
    pares = [
        ('BayasPequenas_Raw',  'PesoBayasPequenas_Raw'),
        ('BayasGrandes_Raw',   None),
        ('BayasFase1_Raw',     'PesoBayasFase1_Raw'),
        ('BayasFase2_Raw',     'PesoBayasFase2_Raw'),
        ('Cremas_Raw',         'PesoCremas_Raw'),
        ('Maduras_Raw',        'PesoMaduras_Raw'),
        ('Cosechables_Raw',    'PesoCosechables_Raw'),
    ]

    total_bayas = 0.0
    total_peso  = 0.0
    for col_cnt, col_peso in pares:
        cnt  = safe_float(fila.get(col_cnt))
        peso = safe_float(fila.get(col_peso)) if col_peso else 0.0
        total_bayas += cnt
        total_peso  += peso

    if total_bayas > 0 and total_peso > 0:
        return round(total_peso / total_bayas, 4)

    return None


def cargar_fact_evaluacion_pesos(engine: Engine) -> dict:
    """
    Lee Bronce.Evaluacion_Pesos y carga Silver.Fact_Evaluacion_Pesos.
    """
    resumen = {'insertados': 0, 'rechazados': 0, 'cuarentena': []}

    df = _leer_bronce(engine)
    if df.empty:
        return _finalizar_resumen_fact(resumen)
    resumen['leidos'] = len(df)

    ids_procesados = []
    ids_rechazados = []

    with ContextoTransaccionalETL(engine) as contexto:
        conexion = contexto._conexion_activa()

        # Homologar variedades dentro de misma transaccion del fact.
        df, cuarentenas_var = homologar_columna(
            df, 'Variedad_Raw', 'Variedad_Canonica',
            TABLA_ORIGEN, conexion,
            columna_id_origen='ID_Evaluacion_Pesos'
        )
        resumen['cuarentena'].extend(cuarentenas_var)

        for _, fila in df.iterrows():
            id_origen = None
            try:
                id_origen = int(fila['ID_Evaluacion_Pesos'])
            except (ValueError, TypeError):
                pass

            # Ã¢â€â‚¬Ã¢â€â‚¬ Fecha Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
            fecha, fecha_valida = procesar_fecha(
                fila.get('Fecha_Raw'),
                dominio='evaluacion_pesos',
            )
            if not fecha_valida:
                _registrar_rechazo(
                    resumen,
                    ids_rechazados,
                    id_origen,
                    columna='Fecha_Raw',
                    valor=fila.get('Fecha_Raw'),
                    motivo='Fecha invalida o fuera de campana',
                )
                continue

            id_tiempo = obtener_id_tiempo(construir_id_tiempo(fecha), engine)
            if not id_tiempo:
                _registrar_rechazo(
                    resumen,
                    ids_rechazados,
                    id_origen,
                    columna='Fecha_Raw',
                    valor=fila.get('Fecha_Raw'),
                    motivo='Fecha valida pero fuera de Dim_Tiempo',
                )
                continue

            # Ã¢â€â‚¬Ã¢â€â‚¬ GeografÃƒÂ­a Ã¢â‚¬â€ Este reporte usa Valvula como mÃƒÂ³dulo Ã¢â€â‚¬Ã¢â€â‚¬
            valvula_raw = fila.get('Valvula_Raw')
            modulo_raw  = fila.get('Modulo_Raw')
            geo_modulo_raw = modulo_raw if modulo_raw and str(modulo_raw).strip() not in ('None', '', 'nan') else valvula_raw
            modulo = None if es_test_block(geo_modulo_raw) else normalizar_modulo(geo_modulo_raw)
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
                resumen['rechazados'] += 1
                resumen['cuarentena'].append({
                    'columna':   'Modulo_Raw',
                    'valor':     f"Modulo={fila.get('Modulo_Raw')} | Turno={fila.get('Turno_Raw')} | "
                                 f"Valvula={fila.get('Valvula_Raw')} | Cama={fila.get('Cama_Raw')}",
                    'motivo':    _motivo_cuarentena_geografia(resultado_geo),
                    'tipo_regla': 'MDM',
                    'severidad': 'ALTO',
                    'id_registro_origen': id_origen,
                })
                continue

            # Ã¢â€â‚¬Ã¢â€â‚¬ Variedad Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
            id_variedad = obtener_id_variedad(
                fila.get('Variedad_Canonica'), engine
            )
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

            # Ã¢â€â‚¬Ã¢â€â‚¬ Personal Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
            dni, _ = procesar_dni(fila.get('DNI_Raw'))
            id_personal = obtener_id_personal(dni, engine)

            # Ã¢â€â‚¬Ã¢â€â‚¬ Peso baya ponderado Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
            peso = _calcular_peso_ponderado(fila)
            if peso is None:
                _registrar_rechazo(
                    resumen,
                    ids_rechazados,
                    id_origen,
                    columna='PesoBaya',
                    valor=None,
                    motivo='No se pudo calcular peso promedio de baya',
                    severidad='MEDIO',
                )
                continue

            # Validar rango
            peso_val, error_peso = validar_peso_baya(peso)
            if error_peso:
                error_peso['id_registro_origen'] = id_origen
                _registrar_rechazo(
                    resumen,
                    ids_rechazados,
                    id_origen,
                    columna=error_peso.get('columna', 'Peso_Baya_g'),
                    valor=error_peso.get('valor'),
                    motivo=error_peso.get('motivo', 'Peso invalido'),
                    severidad=error_peso.get('severidad', 'ALTO'),
                )
                continue

            # Ã¢â€â‚¬Ã¢â€â‚¬ Cantidad bayas total Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
            def safe_int(v):
                try:
                    return max(0, int(float(str(v)))) if v is not None and str(v).strip() not in ('', 'None', 'nan') else 0
                except (ValueError, TypeError):
                    return 0

            cantidad = (
                safe_int(fila.get('BayasPequenas_Raw')) +
                safe_int(fila.get('BayasGrandes_Raw'))  +
                safe_int(fila.get('BayasFase1_Raw'))     +
                safe_int(fila.get('BayasFase2_Raw'))     +
                safe_int(fila.get('Cremas_Raw'))         +
                safe_int(fila.get('Maduras_Raw'))        +
                safe_int(fila.get('Cosechables_Raw'))
            ) or safe_int(fila.get('CantMuestra_Raw'))

            # Ã¢â€â‚¬Ã¢â€â‚¬ INSERT Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬Ã¢â€â‚¬
            conexion.execute(text("""
                INSERT INTO Silver.Fact_Evaluacion_Pesos (
                    ID_Geografia, ID_Tiempo, ID_Variedad, ID_Personal,
                    Peso_Promedio_Baya_g, Cantidad_Bayas_Muestra,
                    Fecha_Evento, Fecha_Sistema, Estado_DQ
                ) VALUES (
                    :id_geo, :id_tiempo, :id_variedad, :id_personal,
                    :peso, :cantidad,
                    :fecha_evento, SYSDATETIME(), 'OK'
                )
            """), {
                'id_geo':      id_geo,
                'id_tiempo':   id_tiempo,
                'id_variedad': id_variedad,
                'id_personal': id_personal,
                'peso':        peso_val,
                'cantidad':    cantidad,
                'fecha_evento': fecha,
            })

            resumen['insertados'] += 1
            if id_origen is not None:
                ids_procesados.append(id_origen)

        if ids_procesados:
            contexto.marcar_estado_carga(
                TABLA_ORIGEN, 'ID_Evaluacion_Pesos', ids_procesados
            )
        if ids_rechazados:
            contexto.marcar_estado_carga(
                TABLA_ORIGEN, 'ID_Evaluacion_Pesos', ids_rechazados, estado='RECHAZADO'
            )

        if resumen['cuarentena']:
            contexto.enviar_cuarentena(TABLA_ORIGEN, resumen['cuarentena'])

    return _finalizar_resumen_fact(resumen)


