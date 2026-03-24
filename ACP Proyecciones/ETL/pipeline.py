"""
pipeline.py
===========
Orquestador principal del ETL Geographic Phenology.
Único punto de entrada — nada se corre por separado en producción.

Uso:
    py pipeline.py
"""

import sys
from datetime import datetime

from config.conexion   import verificar_conexion, obtener_engine
from config.parametros import limpiar_cache as limpiar_params
from bronce.cargador   import ejecutar_carga_bronce
from mdm.lookup        import limpiar_cache as limpiar_lookup

from silver.dims.dim_personal  import cargar_dim_personal
from silver.dims.dim_geografia import cargar_dim_geografia

from silver.facts.fact_cosecha_sap           import cargar_fact_cosecha_sap
from silver.facts.fact_conteo_fenologico     import cargar_fact_conteo_fenologico
from silver.facts.fact_peladas               import cargar_fact_peladas
from silver.facts.fact_telemetria_clima      import cargar_fact_telemetria_clima
from silver.facts.fact_evaluacion_pesos      import cargar_fact_evaluacion_pesos
from silver.facts.fact_tareo                 import cargar_fact_tareo
from silver.facts.fact_fisiologia            import cargar_fact_fisiologia
from silver.facts.fact_evaluacion_vegetativa import cargar_fact_evaluacion_vegetativa
from silver.facts.fact_sanidad_activo        import cargar_fact_sanidad_activo
from silver.facts.fact_ciclo_poda            import cargar_fact_ciclo_poda

from gold.marts import refrescar_todos_los_marts


def _encabezado() -> datetime:
    inicio = datetime.now()
    print()
    print('=' * 60)
    print('  DWH Geographic Phenology — ETL Pipeline')
    print(f'  Inicio: {inicio.strftime("%Y-%m-%d %H:%M:%S")}')
    print('=' * 60)
    print()
    return inicio


def _paso(numero: int, total: int, descripcion: str) -> None:
    print(f'[{numero:02d}/{total}] {descripcion}')


def _resumen_fact(resultado: dict) -> None:
    ins  = resultado.get('insertados', 0)
    rec  = resultado.get('rechazados', 0)
    cuar = len(resultado.get('cuarentena', []))
    print(f'       -> {ins} insertados | {rec} rechazados | {cuar} cuarentena')


def _resumen_final(inicio: datetime, resumen: dict) -> None:
    fin      = datetime.now()
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


def ejecutar() -> None:
    inicio  = _encabezado()
    engine  = obtener_engine()
    total   = 17
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
    resumen['Bronce filas']    = sum(r['filas'] for r in resultados_bronce)

    _paso(4, total, 'Cargando Dim_Personal (SCD1)...')
    r = cargar_dim_personal(engine)
    resumen['Dim_Personal insertados']   = r['insertados']
    resumen['Dim_Personal actualizados'] = r['actualizados']

    _paso(5, total, 'Cargando Dim_Geografia (SCD2)...')
    r = cargar_dim_geografia(engine)
    resumen['Dim_Geografia insertados'] = r['insertados']
    resumen['Dim_Geografia cerrados']   = r['cerrados']

    facts = [
        (6,  'Fact_Cosecha_SAP',           cargar_fact_cosecha_sap),
        (7,  'Fact_Conteo_Fenologico',      cargar_fact_conteo_fenologico),
        (8,  'Fact_Peladas',               cargar_fact_peladas),
        (9,  'Fact_Telemetria_Clima',       cargar_fact_telemetria_clima),
        (10, 'Fact_Evaluacion_Pesos',       cargar_fact_evaluacion_pesos),
        (11, 'Fact_Tareo',                 cargar_fact_tareo),
        (12, 'Fact_Fisiologia',            cargar_fact_fisiologia),
        (13, 'Fact_Evaluacion_Vegetativa', cargar_fact_evaluacion_vegetativa),
        (14, 'Fact_Sanidad_Activo',        cargar_fact_sanidad_activo),
        (15, 'Fact_Ciclo_Poda',            cargar_fact_ciclo_poda),
    ]

    for numero, nombre, funcion in facts:
        _paso(numero, total, f'Cargando {nombre}...')
        try:
            r = funcion(engine)
            _resumen_fact(r)
            resumen[nombre] = r.get('insertados', 0)
        except Exception as error:
            print(f'  ❌ Error en {nombre}: {error}')
            # FIX: marcar con sufijo ERROR para que el gate de Gold lo detecte
            resumen[f'{nombre} ERROR'] = str(error)

    # FIX: pasar resumen completo — marts.py decide si publicar o bloquear
    _paso(16, total, 'Refrescando Marts Gold...')
    try:
        resumen_marts = refrescar_todos_los_marts(engine, resumen_etl=resumen)
        for mart, valor in resumen_marts.items():
            resumen[mart] = valor if isinstance(valor, int) else valor
    except Exception as error:
        print(f'  ❌ Error en Gold: {error}')

    _paso(17, total, 'Finalizando...')
    _resumen_final(inicio, resumen)


if __name__ == '__main__':
    ejecutar()
