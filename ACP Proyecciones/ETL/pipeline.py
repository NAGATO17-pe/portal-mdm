"""
pipeline.py
===========
Orquestador principal del ETL Geographic Phenology.
Unico punto de entrada; nada se corre por separado en produccion.

Uso:
    py pipeline.py
    py pipeline.py --modo-ejecucion facts --facts Fact_Telemetria_Clima
"""

import argparse
import sys
from datetime import datetime

from sqlalchemy import text

from config.conexion import verificar_conexion, obtener_engine
from config.parametros import limpiar_cache as limpiar_params
from bronce.cargador import ejecutar_carga_bronce
from mdm.lookup import limpiar_cache as limpiar_lookup

from silver.dims.dim_personal import cargar_dim_personal
from silver.dims.dim_geografia import cargar_dim_geografia

from silver.facts.fact_cosecha_sap import cargar_fact_cosecha_sap
from silver.facts.fact_conteo_fenologico import cargar_fact_conteo_fenologico
from silver.facts.fact_maduracion import cargar_fact_maduracion
from silver.facts.fact_peladas import cargar_fact_peladas
from silver.facts.fact_telemetria_clima import cargar_fact_telemetria_clima
from silver.facts.fact_evaluacion_pesos import cargar_fact_evaluacion_pesos
from silver.facts.fact_tareo import cargar_fact_tareo
from silver.facts.fact_fisiologia import cargar_fact_fisiologia
from silver.facts.fact_evaluacion_vegetativa import cargar_fact_evaluacion_vegetativa
from silver.facts.fact_induccion_floral import cargar_fact_induccion_floral
from silver.facts.fact_tasa_crecimiento_brotes import cargar_fact_tasa_crecimiento_brotes
from silver.facts.fact_sanidad_activo import cargar_fact_sanidad_activo
from silver.facts.fact_ciclo_poda import cargar_fact_ciclo_poda

from gold.marts import refrescar_marts_seleccionados, refrescar_todos_los_marts
from auditoria.log import registrar_inicio, registrar_fin
from utils.ejecucion import (
    CONFIG_FACTS,
    DEPENDENCIA_DIM_GEOGRAFIA,
    DEPENDENCIA_DIM_PERSONAL,
    DEPENDENCIA_SP_CAMA_SYNC,
    DEPENDENCIA_SP_CAMA_VALIDACION,
    MODO_EJECUCION_COMPLETO,
    MODO_EJECUCION_FACTS,
    obtener_facts_disponibles,
    resolver_plan_reproceso,
)
from utils.metricas import formatear_resumen_fact, normalizar_resultado_fact


CAMA_MIN_PERMITIDA = 0
CAMA_MAX_PERMITIDA = 100
MAX_CAMAS_POR_GEOGRAFIA = 100
TABLAS_BRONCE_SP_CAMA = {
    'Bronce.Evaluacion_Pesos',
    'Bronce.Evaluacion_Vegetativa',
}

CATALOGO_FACTS = {
    'Fact_Cosecha_SAP': {**CONFIG_FACTS['Fact_Cosecha_SAP'], 'funcion': cargar_fact_cosecha_sap},
    'Fact_Conteo_Fenologico': {**CONFIG_FACTS['Fact_Conteo_Fenologico'], 'funcion': cargar_fact_conteo_fenologico},
    'Fact_Maduracion': {**CONFIG_FACTS['Fact_Maduracion'], 'funcion': cargar_fact_maduracion},
    'Fact_Peladas': {**CONFIG_FACTS['Fact_Peladas'], 'funcion': cargar_fact_peladas},
    'Fact_Telemetria_Clima': {**CONFIG_FACTS['Fact_Telemetria_Clima'], 'funcion': cargar_fact_telemetria_clima},
    'Fact_Evaluacion_Pesos': {**CONFIG_FACTS['Fact_Evaluacion_Pesos'], 'funcion': cargar_fact_evaluacion_pesos},
    'Fact_Tareo': {**CONFIG_FACTS['Fact_Tareo'], 'funcion': cargar_fact_tareo},
    'Fact_Fisiologia': {**CONFIG_FACTS['Fact_Fisiologia'], 'funcion': cargar_fact_fisiologia},
    'Fact_Evaluacion_Vegetativa': {**CONFIG_FACTS['Fact_Evaluacion_Vegetativa'], 'funcion': cargar_fact_evaluacion_vegetativa},
    'Fact_Induccion_Floral': {**CONFIG_FACTS['Fact_Induccion_Floral'], 'funcion': cargar_fact_induccion_floral},
    'Fact_Tasa_Crecimiento_Brotes': {**CONFIG_FACTS['Fact_Tasa_Crecimiento_Brotes'], 'funcion': cargar_fact_tasa_crecimiento_brotes},
    'Fact_Sanidad_Activo': {**CONFIG_FACTS['Fact_Sanidad_Activo'], 'funcion': cargar_fact_sanidad_activo},
    'Fact_Ciclo_Poda': {**CONFIG_FACTS['Fact_Ciclo_Poda'], 'funcion': cargar_fact_ciclo_poda},
}


class ErrorEjecucionPipeline(RuntimeError):
    def __init__(self, errores: list[str]) -> None:
        self.errores = list(errores)
        super().__init__(' | '.join(self.errores))


def _encabezado() -> datetime:
    inicio = datetime.now()
    print()
    print('=' * 60)
    print('  DWH Geographic Phenology - ETL Pipeline')
    print(f'  Inicio: {inicio.strftime("%Y-%m-%d %H:%M:%S")}')
    print('=' * 60)
    print()
    return inicio


def _paso(numero: int, total: int, descripcion: str) -> None:
    print(f'[{numero:02d}/{total}] {descripcion}')


def _resumen_fact(resultado: dict) -> None:
    for linea in formatear_resumen_fact(resultado):
        print(linea)


def _resumen_final(inicio: datetime, resumen: dict) -> None:
    fin = datetime.now()
    duracion = round((fin - inicio).total_seconds(), 2)
    print()
    print('=' * 60)
    print('  RESUMEN FINAL')
    print('=' * 60)
    for clave, valor in resumen.items():
        print(f'  {clave:45} {valor}')
    print(f'  {"Duracion total":45} {duracion}s')
    print('=' * 60)
    print()


def _ejecutar_sp_upsert_cama(
    engine,
    modo_aplicar: int = 1,
    cama_min_permitida: int = CAMA_MIN_PERMITIDA,
    cama_max_permitida: int = CAMA_MAX_PERMITIDA,
) -> dict:
    with engine.begin() as conexion:
        fila = conexion.execute(text("""
            EXEC Silver.sp_Upsert_Cama_Desde_Bronce
                @Modo_Aplicar = :modo_aplicar,
                @Cama_Min_Permitida = :cama_min_permitida,
                @Cama_Max_Permitida = :cama_max_permitida
        """), {
            'modo_aplicar': int(modo_aplicar),
            'cama_min_permitida': int(cama_min_permitida),
            'cama_max_permitida': int(cama_max_permitida),
        }).fetchone()

    if not fila:
        return {
            'Filas_Bronce_Leidas': 0,
            'Filas_Evaluadas': 0,
            'Combinaciones_Aptas_Distintas': 0,
            'Insert_Catalogo_Real': 0,
            'Insert_Bridge_Real': 0,
        }
    return dict(fila._mapping)


def _ejecutar_sp_validar_camas(
    engine,
    cama_max_permitida: int = CAMA_MAX_PERMITIDA,
    max_camas_por_geografia: int = MAX_CAMAS_POR_GEOGRAFIA,
) -> dict:
    with engine.connect() as conexion:
        fila = conexion.execute(text("""
            EXEC Silver.sp_Validar_Calidad_Camas
                @Cama_Max_Permitida = :cama_max_permitida,
                @Max_Camas_Por_Geografia = :max_camas_por_geografia
        """), {
            'cama_max_permitida': int(cama_max_permitida),
            'max_camas_por_geografia': int(max_camas_por_geografia),
        }).fetchone()

    if not fila:
        return {
            'Cama_Fuera_Regla': None,
            'Geografias_Saturadas': None,
            'Estado_Calidad_Cama': 'SIN_RESULTADO',
        }
    return dict(fila._mapping)


def _obtener_contexto_sql(engine) -> dict:
    with engine.connect() as conexion:
        fila = conexion.execute(text("""
            SELECT
                CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(128)) AS servidor,
                DB_NAME() AS base_datos
        """)).fetchone()
    if not fila:
        return {'servidor': 'DESCONOCIDO', 'base_datos': 'DESCONOCIDA'}
    return dict(fila._mapping)


def _contar_bridge_geografia_cama(engine) -> int:
    with engine.connect() as conexion:
        return int(conexion.execute(text("""
            SELECT COUNT(*)
            FROM Silver.Bridge_Geografia_Cama
        """)).scalar() or 0)


def _parsear_argumentos() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description='Pipeline ETL ACP')
    parser.add_argument(
        '--modo-ejecucion',
        choices=[MODO_EJECUCION_COMPLETO, MODO_EJECUCION_FACTS],
        default=MODO_EJECUCION_COMPLETO,
    )
    parser.add_argument(
        '--facts',
        nargs='+',
        default=None,
        help=f"Facts a reprocesar. Disponibles: {', '.join(obtener_facts_disponibles())}",
    )
    parser.add_argument(
        '--sin-dependencias',
        action='store_true',
        help='Omite dependencias previas del reproceso parcial.',
    )
    parser.add_argument(
        '--sin-gold',
        action='store_true',
        help='Omite el refresh de marts Gold afectados.',
    )
    parser.add_argument(
        '--sin-relectura-bronce',
        action='store_true',
        help='No reabre filas PROCESADO/RECHAZADO en Bronce antes del reproceso.',
    )
    return parser.parse_args()


def _contar_filas_tabla(engine, tabla: str) -> int:
    with engine.connect() as conexion:
        return int(conexion.execute(text(f'SELECT COUNT(*) FROM {tabla}')).scalar() or 0)


def _limpiar_fact_destino(engine, tabla_destino: str) -> int:
    filas_antes = _contar_filas_tabla(engine, tabla_destino)
    with engine.begin() as conexion:
        conexion.execute(text(f'DELETE FROM {tabla_destino}'))
    return filas_antes


def _reiniciar_estado_carga_bronce(engine, tabla_bronce: str) -> int:
    with engine.begin() as conexion:
        resultado = conexion.execute(text(f"""
            UPDATE {tabla_bronce}
            SET Estado_Carga = 'CARGADO'
            WHERE Estado_Carga IN ('PROCESADO', 'RECHAZADO')
        """))
        return int(resultado.rowcount or 0)


def _ejecutar_dependencia_reproceso(engine, dependencia: str, resumen: dict) -> None:
    if dependencia == DEPENDENCIA_DIM_PERSONAL:
        r = cargar_dim_personal(engine)
        resumen['Dim_Personal insertados'] = r['insertados']
        resumen['Dim_Personal actualizados'] = r['actualizados']
        return

    if dependencia == DEPENDENCIA_DIM_GEOGRAFIA:
        r = cargar_dim_geografia(engine)
        resumen['Dim_Geografia vigentes'] = r.get('vigentes', 0)
        resumen['Dim_Geografia operativos'] = r.get('operativos', 0)
        resumen['Dim_Geografia test_block'] = r.get('test_block', 0)
        resumen['Dim_Geografia sin cama explicita'] = r.get('sin_cama_explicita', 0)
        resumen['Dim_Geografia duplicados'] = r.get('duplicados', 0)
        return

    if dependencia == DEPENDENCIA_SP_CAMA_SYNC:
        bridge_antes = _contar_bridge_geografia_cama(engine)
        r = _ejecutar_sp_upsert_cama(
            engine,
            modo_aplicar=1,
            cama_min_permitida=CAMA_MIN_PERMITIDA,
            cama_max_permitida=CAMA_MAX_PERMITIDA,
        )
        bridge_despues = _contar_bridge_geografia_cama(engine)
        resumen['SP_Cama filas evaluadas'] = r.get('Filas_Evaluadas', 0)
        resumen['SP_Cama aptas'] = r.get('Combinaciones_Aptas_Distintas', 0)
        resumen['SP_Cama insert catalogo'] = r.get('Insert_Catalogo_Real', 0)
        resumen['SP_Cama insert bridge'] = r.get('Insert_Bridge_Real', 0)
        resumen['Bridge camas antes'] = bridge_antes
        resumen['Bridge camas despues'] = bridge_despues
        if r.get('Combinaciones_Aptas_Distintas', 0) > 0 and bridge_despues == 0:
            raise RuntimeError(
                'SP_Cama inconsistente: hay combinaciones aptas pero Silver.Bridge_Geografia_Cama sigue en 0.'
            )
        return

    if dependencia == DEPENDENCIA_SP_CAMA_VALIDACION:
        r = _ejecutar_sp_validar_camas(
            engine,
            cama_max_permitida=CAMA_MAX_PERMITIDA,
            max_camas_por_geografia=MAX_CAMAS_POR_GEOGRAFIA,
        )
        resumen['SP_Cama fuera regla'] = r.get('Cama_Fuera_Regla')
        resumen['SP_Cama geo saturadas'] = r.get('Geografias_Saturadas')
        resumen['SP_Cama estado calidad'] = r.get('Estado_Calidad_Cama')
        if r.get('Estado_Calidad_Cama') == 'RIESGO_CONTAMINACION':
            raise RuntimeError('Calidad cama en estado RIESGO_CONTAMINACION.')
        return

    raise ValueError(f'Dependencia no soportada: {dependencia}')


def _preparar_fact_reproceso(engine, nombre_fact: str, meta_fact: dict, forzar_relectura_bronce: bool) -> dict:
    if not forzar_relectura_bronce and meta_fact.get('releer_bronce_por_estado', True):
        raise ValueError(
            f'{nombre_fact} requiere relectura de Bronce para reconstruirse sin perder historia.'
        )

    filas_bronce_reabiertas = 0
    if forzar_relectura_bronce and meta_fact.get('releer_bronce_por_estado', True):
        for tabla_bronce in meta_fact.get('fuentes_bronce', ()):
            filas_bronce_reabiertas += _reiniciar_estado_carga_bronce(engine, tabla_bronce)

    filas_destino_eliminadas = _limpiar_fact_destino(engine, meta_fact['tabla_destino'])
    return {
        'filas_bronce_reabiertas': filas_bronce_reabiertas,
        'filas_destino_eliminadas': filas_destino_eliminadas,
    }


def _registrar_errores_resumen(resumen: dict, errores: list[str]) -> None:
    resumen['Errores pipeline'] = len(errores)
    for indice, error in enumerate(errores, start=1):
        resumen[f'Pipeline error {indice}'] = error


def _ejecutar_fact(nombre: str, tabla_destino: str, funcion, engine, resumen: dict) -> str | None:
    id_log = registrar_inicio(tabla_destino, f'PIPELINE_FACT:{nombre}')
    r = {'insertados': 0, 'rechazados': 0, 'cuarentena': []}

    try:
        r = funcion(engine)
        r = normalizar_resultado_fact(r)
        _resumen_fact(r)
        resumen[nombre] = r.get('insertados', 0)
        resumen[f'{nombre} leidos'] = r.get('leidos', 0)
        resumen[f'{nombre} rechazados'] = r.get('rechazados', 0)
        resumen[f'{nombre} rechazo pct'] = f"{r.get('tasa_rechazo_pct', 0.0)}%"
        for indice, item in enumerate(r.get('motivos_principales', []), start=1):
            resumen[f'{nombre} motivo {indice}'] = f"{item['motivo']} ({item['cantidad']})"
        registrar_fin(id_log, {
            'estado': 'OK',
            'filas': r.get('insertados', 0),
            'filas_leidas': r.get('leidos', 0),
            'rechazadas': r.get('rechazados', 0),
            'mensaje': '',
        })
        return None
    except Exception as error:
        mensaje_error = f'{nombre}: {error}'
        print(f'  ERROR en {mensaje_error}')
        resumen[f'{nombre} ERROR'] = str(error)
        registrar_fin(id_log, {
            'estado': 'ERROR',
            'filas': 0,
            'rechazadas': r.get('rechazados', 0),
            'mensaje': str(error),
        })
        return mensaje_error


def ejecutar_reproceso_facts(
    facts_solicitadas: list[str] | tuple[str, ...],
    incluir_dependencias: bool = True,
    refrescar_gold: bool = True,
    forzar_relectura_bronce: bool = True,
) -> None:
    inicio = _encabezado()
    engine = obtener_engine()
    plan = resolver_plan_reproceso(
        facts_solicitadas=facts_solicitadas,
        incluir_dependencias=incluir_dependencias,
        refrescar_gold=refrescar_gold,
    )
    if not forzar_relectura_bronce:
        facts_incrementales = [
            nombre_fact
            for nombre_fact in plan['facts']
            if CATALOGO_FACTS[nombre_fact].get('releer_bronce_por_estado', True)
        ]
        if facts_incrementales:
            raise ValueError(
                'El reproceso sin relectura de Bronce solo aplica a facts full-read. '
                f'No permitido para: {facts_incrementales}'
            )
    total = 3 + len(plan['dependencias']) + len(plan['facts']) + (1 if plan['marts'] else 0)
    resumen = {
        'Modo ejecucion': 'REPROCESO_FACTS',
        'Facts solicitadas': ', '.join(plan['facts']),
    }
    errores_pipeline: list[str] = []

    paso_actual = 1
    _paso(paso_actual, total, 'Verificando conexion SQL Server...')
    if not verificar_conexion():
        print('Sin conexion. Pipeline detenido.')
        sys.exit(1)
    contexto_sql = _obtener_contexto_sql(engine)
    resumen['Servidor SQL'] = contexto_sql.get('servidor')
    resumen['Base SQL'] = contexto_sql.get('base_datos')

    paso_actual += 1
    _paso(paso_actual, total, 'Limpiando cache...')
    limpiar_lookup()
    limpiar_params()

    for dependencia in plan['dependencias']:
        paso_actual += 1
        _paso(paso_actual, total, f'Ejecutando dependencia {dependencia}...')
        _ejecutar_dependencia_reproceso(engine, dependencia, resumen)

    for nombre_fact in plan['facts']:
        paso_actual += 1
        meta_fact = CATALOGO_FACTS[nombre_fact]
        _paso(paso_actual, total, f'Reprocesando {nombre_fact}...')
        preparacion = _preparar_fact_reproceso(
            engine,
            nombre_fact,
            meta_fact,
            forzar_relectura_bronce=forzar_relectura_bronce,
        )
        resumen[f'{nombre_fact} bronce reabiertas'] = preparacion['filas_bronce_reabiertas']
        resumen[f'{nombre_fact} destino limpiado'] = preparacion['filas_destino_eliminadas']
        error_fact = _ejecutar_fact(
            nombre_fact,
            meta_fact['tabla_destino'],
            meta_fact['funcion'],
            engine,
            resumen,
        )
        if error_fact:
            errores_pipeline.append(error_fact)

    if plan['marts']:
        paso_actual += 1
        if errores_pipeline:
            _paso(paso_actual, total, 'Omitiendo Marts Gold por errores previos...')
            resumen['Gold estado'] = 'OMITIDO_POR_ERROR_EN_FACTS'
        else:
            _paso(paso_actual, total, 'Refrescando Marts Gold impactados...')
            try:
                resumen_marts = refrescar_marts_seleccionados(engine, plan['marts'], resumen_etl=resumen)
                for mart, valor in resumen_marts.items():
                    resumen[mart] = valor if isinstance(valor, int) else valor
            except Exception as error:
                mensaje_error = f'Gold: {error}'
                print(f'  ERROR en {mensaje_error}')
                resumen['Gold ERROR'] = str(error)
                errores_pipeline.append(mensaje_error)

    paso_actual += 1
    _paso(paso_actual, total, 'Finalizando...')
    if errores_pipeline:
        _registrar_errores_resumen(resumen, errores_pipeline)
    _resumen_final(inicio, resumen)
    if errores_pipeline:
        raise ErrorEjecucionPipeline(errores_pipeline)


def ejecutar() -> None:
    inicio = _encabezado()
    engine = obtener_engine()
    total = 22
    resumen = {}
    errores_pipeline: list[str] = []

    _paso(1, total, 'Verificando conexion SQL Server...')
    if not verificar_conexion():
        print('Sin conexion. Pipeline detenido.')
        sys.exit(1)
    contexto_sql = _obtener_contexto_sql(engine)
    resumen['Servidor SQL'] = contexto_sql.get('servidor')
    resumen['Base SQL'] = contexto_sql.get('base_datos')

    _paso(2, total, 'Limpiando cache...')
    limpiar_lookup()
    limpiar_params()

    _paso(3, total, 'Cargando archivos Excel a Bronce...')
    resultados_bronce = ejecutar_carga_bronce()
    resumen['Bronce archivos'] = len(resultados_bronce)
    resumen['Bronce filas'] = sum(r['filas'] for r in resultados_bronce)
    tablas_bronce_ok = {
        r.get('tabla')
        for r in resultados_bronce
        if r.get('estado') == 'OK'
    }
    error_critico_bronce = next((r for r in resultados_bronce if r.get('critico')), None)
    if error_critico_bronce:
        resumen['Bronce error critico'] = error_critico_bronce.get('codigo', 'ERROR_CRITICO')
        resumen['Bronce detalle'] = error_critico_bronce.get('mensaje', '')
        print('  ERROR CRITICO EN BRONCE. Pipeline detenido antes de Silver/Gold.')
        print(f'  {error_critico_bronce.get("mensaje", "")}')
        _resumen_final(inicio, resumen)
        sys.exit(1)

    _paso(4, total, 'Cargando Dim_Personal (SCD1)...')
    r = cargar_dim_personal(engine)
    resumen['Dim_Personal insertados'] = r['insertados']
    resumen['Dim_Personal actualizados'] = r['actualizados']

    _paso(5, total, 'Validando Dim_Geografia...')
    r = cargar_dim_geografia(engine)
    resumen['Dim_Geografia vigentes'] = r.get('vigentes', 0)
    resumen['Dim_Geografia operativos'] = r.get('operativos', 0)
    resumen['Dim_Geografia test_block'] = r.get('test_block', 0)
    resumen['Dim_Geografia sin cama explicita'] = r.get('sin_cama_explicita', 0)
    resumen['Dim_Geografia duplicados'] = r.get('duplicados', 0)

    _paso(6, total, 'Sincronizando catalogo/bridge de cama via SP...')
    try:
        if tablas_bronce_ok & TABLAS_BRONCE_SP_CAMA:
            bridge_antes = _contar_bridge_geografia_cama(engine)
            r = _ejecutar_sp_upsert_cama(
                engine,
                modo_aplicar=1,
                cama_min_permitida=CAMA_MIN_PERMITIDA,
                cama_max_permitida=CAMA_MAX_PERMITIDA,
            )
            bridge_despues = _contar_bridge_geografia_cama(engine)
            resumen['SP_Cama filas evaluadas'] = r.get('Filas_Evaluadas', 0)
            resumen['SP_Cama aptas'] = r.get('Combinaciones_Aptas_Distintas', 0)
            resumen['SP_Cama insert catalogo'] = r.get('Insert_Catalogo_Real', 0)
            resumen['SP_Cama insert bridge'] = r.get('Insert_Bridge_Real', 0)
            resumen['Bridge camas antes'] = bridge_antes
            resumen['Bridge camas despues'] = bridge_despues

            if r.get('Combinaciones_Aptas_Distintas', 0) > 0 and bridge_despues == 0:
                mensaje = (
                    'SP_Cama inconsistente: hay combinaciones aptas pero '
                    'Silver.Bridge_Geografia_Cama sigue en 0.'
                )
                print(f'  ERROR: {mensaje}')
                resumen['SP_Cama inconsistencia'] = mensaje
                sys.exit(1)
        else:
            resumen['SP_Cama estado'] = 'OMITIDO_SIN_TABLAS_CON_CAMA_EN_ESTA_CORRIDA'
            print('  SP_Cama omitido: no ingresaron tablas Bronce con cama en esta corrida.')
    except Exception as error:
        print(f'  ERROR en SP_Cama_Upsert: {error}')
        resumen['SP_Cama_Upsert ERROR'] = str(error)
        sys.exit(1)

    _paso(7, total, 'Validando calidad de cama via SP...')
    try:
        r = _ejecutar_sp_validar_camas(
            engine,
            cama_max_permitida=CAMA_MAX_PERMITIDA,
            max_camas_por_geografia=MAX_CAMAS_POR_GEOGRAFIA,
        )
        resumen['SP_Cama fuera regla'] = r.get('Cama_Fuera_Regla')
        resumen['SP_Cama geo saturadas'] = r.get('Geografias_Saturadas')
        resumen['SP_Cama estado calidad'] = r.get('Estado_Calidad_Cama')
        if r.get('Estado_Calidad_Cama') == 'RIESGO_CONTAMINACION':
            print('  ERROR: Calidad cama en estado RIESGO_CONTAMINACION. Pipeline detenido.')
            sys.exit(1)
    except Exception as error:
        print(f'  ERROR en SP_Cama_Validacion: {error}')
        resumen['SP_Cama_Validacion ERROR'] = str(error)
        sys.exit(1)

    facts = [
        (8, 'Fact_Cosecha_SAP', 'Silver.Fact_Cosecha_SAP', cargar_fact_cosecha_sap),
        (9, 'Fact_Conteo_Fenologico', 'Silver.Fact_Conteo_Fenologico', cargar_fact_conteo_fenologico),
        (10, 'Fact_Maduracion', 'Silver.Fact_Maduracion', cargar_fact_maduracion),
        (11, 'Fact_Peladas', 'Silver.Fact_Peladas', cargar_fact_peladas),
        (12, 'Fact_Telemetria_Clima', 'Silver.Fact_Telemetria_Clima', cargar_fact_telemetria_clima),
        (13, 'Fact_Evaluacion_Pesos', 'Silver.Fact_Evaluacion_Pesos', cargar_fact_evaluacion_pesos),
        (14, 'Fact_Tareo', 'Silver.Fact_Tareo', cargar_fact_tareo),
        (15, 'Fact_Fisiologia', 'Silver.Fact_Fisiologia', cargar_fact_fisiologia),
        (16, 'Fact_Evaluacion_Vegetativa', 'Silver.Fact_Evaluacion_Vegetativa', cargar_fact_evaluacion_vegetativa),
        (17, 'Fact_Induccion_Floral', 'Silver.Fact_Induccion_Floral', cargar_fact_induccion_floral),
        (18, 'Fact_Tasa_Crecimiento_Brotes', 'Silver.Fact_Tasa_Crecimiento_Brotes', cargar_fact_tasa_crecimiento_brotes),
        (19, 'Fact_Sanidad_Activo', 'Silver.Fact_Sanidad_Activo', cargar_fact_sanidad_activo),
        (20, 'Fact_Ciclo_Poda', 'Silver.Fact_Ciclo_Poda', cargar_fact_ciclo_poda),
    ]

    for numero, nombre, tabla_destino, funcion in facts:
        _paso(numero, total, f'Cargando {nombre}...')
        error_fact = _ejecutar_fact(nombre, tabla_destino, funcion, engine, resumen)
        if error_fact:
            errores_pipeline.append(error_fact)

    if errores_pipeline:
        _paso(21, total, 'Omitiendo Marts Gold por errores previos...')
        resumen['Gold estado'] = 'OMITIDO_POR_ERROR_EN_FACTS'
    else:
        _paso(21, total, 'Refrescando Marts Gold...')
        try:
            resumen_marts = refrescar_todos_los_marts(engine, resumen_etl=resumen)
            for mart, valor in resumen_marts.items():
                resumen[mart] = valor if isinstance(valor, int) else valor
        except Exception as error:
            mensaje_error = f'Gold: {error}'
            print(f'  ERROR en {mensaje_error}')
            resumen['Gold ERROR'] = str(error)
            errores_pipeline.append(mensaje_error)

    _paso(22, total, 'Finalizando...')
    if errores_pipeline:
        _registrar_errores_resumen(resumen, errores_pipeline)
    _resumen_final(inicio, resumen)
    if errores_pipeline:
        raise ErrorEjecucionPipeline(errores_pipeline)


if __name__ == '__main__':
    argumentos = _parsear_argumentos()

    try:
        if argumentos.modo_ejecucion == MODO_EJECUCION_FACTS:
            ejecutar_reproceso_facts(
                facts_solicitadas=argumentos.facts,
                incluir_dependencias=not argumentos.sin_dependencias,
                refrescar_gold=not argumentos.sin_gold,
                forzar_relectura_bronce=not argumentos.sin_relectura_bronce,
            )
        else:
            ejecutar()
    except ValueError as error:
        print(f'ERROR DE VALIDACION: {error}')
        sys.exit(1)
    except Exception as error:
        print(f'ERROR: {error}')
        sys.exit(1)
