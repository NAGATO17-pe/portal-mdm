"""
fase15_reproceso_pesos_vegetativa.py
===================================
Reproceso dirigido de Fase 1:
- Silver.Fact_Evaluacion_Pesos
- Silver.Fact_Evaluacion_Vegetativa

No ejecuta otros facts ni refresca marts.
"""

from datetime import datetime

from config.conexion import verificar_conexion, obtener_engine
from config.parametros import limpiar_cache as limpiar_parametros
from mdm.lookup import limpiar_cache as limpiar_lookup
from auditoria.log import registrar_inicio, registrar_fin

from silver.facts.fact_evaluacion_pesos import cargar_fact_evaluacion_pesos
from silver.facts.fact_evaluacion_vegetativa import cargar_fact_evaluacion_vegetativa


def _encabezado() -> datetime:
    inicio = datetime.now()
    print()
    print('=' * 70)
    print('  FASE 15 - Reproceso Dirigido Pesos/Vegetativa')
    print(f'  Inicio: {inicio.strftime("%Y-%m-%d %H:%M:%S")}')
    print('=' * 70)
    print()
    return inicio


def _resumen_fact(nombre: str, resultado: dict) -> None:
    insertados = int(resultado.get('insertados', 0) or 0)
    rechazados = int(resultado.get('rechazados', 0) or 0)
    cuarentena = len(resultado.get('cuarentena', []))
    print(f'  {nombre}: {insertados} insertados | {rechazados} rechazados | {cuarentena} cuarentena')


def ejecutar() -> None:
    inicio = _encabezado()

    print('[01/04] Verificando conexion SQL Server...')
    if not verificar_conexion():
        print('Sin conexion. Reproceso detenido.')
        raise SystemExit(1)

    print('[02/04] Limpiando cache...')
    limpiar_lookup()
    limpiar_parametros()

    engine = obtener_engine()
    resumen = {}

    facts = [
        ('Fact_Evaluacion_Pesos', 'Silver.Fact_Evaluacion_Pesos', cargar_fact_evaluacion_pesos),
        ('Fact_Evaluacion_Vegetativa', 'Silver.Fact_Evaluacion_Vegetativa', cargar_fact_evaluacion_vegetativa),
    ]

    print('[03/04] Ejecutando loaders dirigidos...')
    for nombre, tabla_destino, funcion in facts:
        resultado = {'insertados': 0, 'rechazados': 0, 'cuarentena': []}
        id_log = registrar_inicio(tabla_destino, f'REPROCESO_FASE15:{nombre}')

        try:
            resultado = funcion(engine)
            registrar_fin(id_log, {
                'estado': 'OK',
                'filas': int(resultado.get('insertados', 0) or 0),
                'rechazadas': int(resultado.get('rechazados', 0) or 0),
                'mensaje': '',
            })
        except Exception as error:
            registrar_fin(id_log, {
                'estado': 'ERROR',
                'filas': 0,
                'rechazadas': int(resultado.get('rechazados', 0) or 0),
                'mensaje': str(error),
            })
            print(f'  ERROR en {nombre}: {error}')

        resumen[nombre] = resultado
        _resumen_fact(nombre, resultado)

    print('[04/04] Finalizando...')
    fin = datetime.now()
    duracion = round((fin - inicio).total_seconds(), 2)

    total_insertados = sum(int(v.get('insertados', 0) or 0) for v in resumen.values())
    total_rechazados = sum(int(v.get('rechazados', 0) or 0) for v in resumen.values())
    total_cuarentena = sum(len(v.get('cuarentena', [])) for v in resumen.values())

    print()
    print('=' * 70)
    print('  RESUMEN REPROCESO FASE 15')
    print('=' * 70)
    print(f'  Total insertados: {total_insertados}')
    print(f'  Total rechazados: {total_rechazados}')
    print(f'  Total cuarentena: {total_cuarentena}')
    print(f'  Duracion total : {duracion}s')
    print('=' * 70)
    print()


if __name__ == '__main__':
    ejecutar()
