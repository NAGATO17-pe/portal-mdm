"""
pipeline.py
===========
Orquestador principal del ETL Geographic Phenology.
Unico punto de entrada; nada se corre por separado en produccion.

Uso:
    py pipeline.py
"""

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
from silver.facts.fact_peladas import cargar_fact_peladas
from silver.facts.fact_telemetria_clima import cargar_fact_telemetria_clima
from silver.facts.fact_evaluacion_pesos import cargar_fact_evaluacion_pesos
from silver.facts.fact_tareo import cargar_fact_tareo
from silver.facts.fact_fisiologia import cargar_fact_fisiologia
from silver.facts.fact_evaluacion_vegetativa import cargar_fact_evaluacion_vegetativa
from silver.facts.fact_sanidad_activo import cargar_fact_sanidad_activo
from silver.facts.fact_ciclo_poda import cargar_fact_ciclo_poda

from gold.marts import refrescar_todos_los_marts
from auditoria.log import registrar_inicio, registrar_fin


CAMA_MIN_PERMITIDA = 1
CAMA_MAX_PERMITIDA = 100
MAX_CAMAS_POR_GEOGRAFIA = 100


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
    ins = resultado.get('insertados', 0)
    rec = resultado.get('rechazados', 0)
    cuar = len(resultado.get('cuarentena', []))
    print(f'       -> {ins} insertados | {rec} rechazados | {cuar} cuarentena')


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
    with engine.connect() as conexion:
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


def ejecutar() -> None:
    inicio = _encabezado()
    engine = obtener_engine()
    total = 19
    resumen = {}

    _paso(1, total, 'Verificando conexion SQL Server...')
    if not verificar_conexion():
        print('Sin conexion. Pipeline detenido.')
        sys.exit(1)

    _paso(2, total, 'Limpiando cache...')
    limpiar_lookup()
    limpiar_params()

    _paso(3, total, 'Cargando archivos Excel a Bronce...')
    resultados_bronce = ejecutar_carga_bronce()
    resumen['Bronce archivos'] = len(resultados_bronce)
    resumen['Bronce filas'] = sum(r['filas'] for r in resultados_bronce)

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
        r = _ejecutar_sp_upsert_cama(
            engine,
            modo_aplicar=1,
            cama_min_permitida=CAMA_MIN_PERMITIDA,
            cama_max_permitida=CAMA_MAX_PERMITIDA,
        )
        resumen['SP_Cama filas evaluadas'] = r.get('Filas_Evaluadas', 0)
        resumen['SP_Cama aptas'] = r.get('Combinaciones_Aptas_Distintas', 0)
        resumen['SP_Cama insert catalogo'] = r.get('Insert_Catalogo_Real', 0)
        resumen['SP_Cama insert bridge'] = r.get('Insert_Bridge_Real', 0)
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
        (10, 'Fact_Peladas', 'Silver.Fact_Peladas', cargar_fact_peladas),
        (11, 'Fact_Telemetria_Clima', 'Silver.Fact_Telemetria_Clima', cargar_fact_telemetria_clima),
        (12, 'Fact_Evaluacion_Pesos', 'Silver.Fact_Evaluacion_Pesos', cargar_fact_evaluacion_pesos),
        (13, 'Fact_Tareo', 'Silver.Fact_Tareo', cargar_fact_tareo),
        (14, 'Fact_Fisiologia', 'Silver.Fact_Fisiologia', cargar_fact_fisiologia),
        (15, 'Fact_Evaluacion_Vegetativa', 'Silver.Fact_Evaluacion_Vegetativa', cargar_fact_evaluacion_vegetativa),
        (16, 'Fact_Sanidad_Activo', 'Silver.Fact_Sanidad_Activo', cargar_fact_sanidad_activo),
        (17, 'Fact_Ciclo_Poda', 'Silver.Fact_Ciclo_Poda', cargar_fact_ciclo_poda),
    ]

    for numero, nombre, tabla_destino, funcion in facts:
        _paso(numero, total, f'Cargando {nombre}...')
        id_log = registrar_inicio(tabla_destino, f'PIPELINE_FACT:{nombre}')
        r = {'insertados': 0, 'rechazados': 0, 'cuarentena': []}

        try:
            r = funcion(engine)
            _resumen_fact(r)
            resumen[nombre] = r.get('insertados', 0)
            registrar_fin(id_log, {
                'estado': 'OK',
                'filas': r.get('insertados', 0),
                'rechazadas': r.get('rechazados', 0),
                'mensaje': '',
            })
        except Exception as error:
            print(f'  ERROR en {nombre}: {error}')
            resumen[f'{nombre} ERROR'] = str(error)
            registrar_fin(id_log, {
                'estado': 'ERROR',
                'filas': 0,
                'rechazadas': r.get('rechazados', 0),
                'mensaje': str(error),
            })

    _paso(18, total, 'Refrescando Marts Gold...')
    try:
        resumen_marts = refrescar_todos_los_marts(engine, resumen_etl=resumen)
        for mart, valor in resumen_marts.items():
            resumen[mart] = valor if isinstance(valor, int) else valor
    except Exception as error:
        print(f'  ERROR en Gold: {error}')

    _paso(19, total, 'Finalizando...')
    _resumen_final(inicio, resumen)


if __name__ == '__main__':
    ejecutar()
