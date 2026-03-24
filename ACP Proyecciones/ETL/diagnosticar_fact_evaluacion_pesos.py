"""
diagnosticar_fact_evaluacion_pesos.py
=====================================
Diagnostico de rechazo para Silver.Fact_Evaluacion_Pesos.

No inserta, no actualiza y no escribe en tablas MDM.
Solo lee Bronce y dimensiones para responder:
  - cuantas filas son insertables hoy
  - cuantas caen por fecha, geografia, variedad o peso
  - que valores concretos estan bloqueando la carga
"""

from collections import Counter

from config.conexion import verificar_conexion, obtener_engine
from mdm.homologador import (
    buscar_match_exacto,
    buscar_match_levenshtein,
    cargar_diccionario,
)
from mdm.lookup import (
    limpiar_cache,
    obtener_id_geografia,
    obtener_id_personal,
    obtener_id_variedad,
)
from silver.facts.fact_evaluacion_pesos import (
    _calcular_peso_ponderado,
    _leer_bronce,
)
from utils.dni import procesar_dni
from utils.fechas import obtener_id_tiempo, procesar_fecha
from utils.texto import es_test_block, normalizar_modulo
from dq.validador import validar_peso_baya


def _top_valores(contador: Counter, limite: int = 10) -> list[tuple[str, int]]:
    return [(str(valor), cantidad) for valor, cantidad in contador.most_common(limite)]


def diagnosticar() -> None:
    if not verificar_conexion():
        print('Sin conexion. Diagnostico detenido.')
        return

    engine = obtener_engine()
    limpiar_cache()

    df = _leer_bronce(engine)
    total = len(df)

    if df.empty:
        print('No hay filas CARGADO en Bronce.Evaluacion_Pesos.')
        return

    diccionario = cargar_diccionario(engine, 'Bronce.Evaluacion_Pesos')

    resumen = Counter()
    modulos_sin_geo = Counter()
    variedades_sin_match = Counter()
    variedades_sin_dim = Counter()
    pesos_invalidos = Counter()
    fechas_invalidas = Counter()

    for _, fila in df.iterrows():
        fecha_raw = fila.get('Fecha_Raw')
        fecha, fecha_valida = procesar_fecha(fecha_raw)
        if not fecha_valida:
            resumen['fecha_invalida'] += 1
            fechas_invalidas[str(fecha_raw)] += 1
            continue

        id_tiempo = obtener_id_tiempo(fecha)
        if id_tiempo is None:
            resumen['id_tiempo_nulo'] += 1
            continue

        modulo_raw = fila.get('Modulo_Raw')
        valvula_raw = fila.get('Valvula_Raw')
        geo_modulo_raw = (
            modulo_raw
            if modulo_raw and str(modulo_raw).strip() not in ('None', '', 'nan')
            else valvula_raw
        )
        modulo = None if es_test_block(geo_modulo_raw) else normalizar_modulo(geo_modulo_raw)
        id_geo = obtener_id_geografia(None, None, modulo, engine)
        if not id_geo:
            resumen['geografia_no_encontrada'] += 1
            modulos_sin_geo[str(geo_modulo_raw)] += 1
            continue

        variedad_raw = fila.get('Variedad_Raw')
        variedad_canonica = buscar_match_exacto(variedad_raw, diccionario)
        if not variedad_canonica:
            variedad_canonica, _ = buscar_match_levenshtein(variedad_raw, diccionario)

        if not variedad_canonica:
            resumen['variedad_sin_homologacion'] += 1
            variedades_sin_match[str(variedad_raw)] += 1
            continue

        id_variedad = obtener_id_variedad(variedad_canonica, engine)
        if not id_variedad:
            resumen['variedad_fuera_de_dim'] += 1
            variedades_sin_dim[str(variedad_canonica)] += 1
            continue

        dni, _ = procesar_dni(fila.get('DNI_Raw'))
        obtener_id_personal(dni, engine)

        peso = _calcular_peso_ponderado(fila)
        if peso is None:
            resumen['peso_no_calculable'] += 1
            pesos_invalidos['NO_CALCULABLE'] += 1
            continue

        _, error_peso = validar_peso_baya(peso)
        if error_peso:
            resumen['peso_fuera_rango'] += 1
            pesos_invalidos[str(round(peso, 4))] += 1
            continue

        resumen['insertables'] += 1

    rechazadas = total - resumen['insertables']

    print()
    print('=' * 72)
    print('DIAGNOSTICO FACT_EVALUACION_PESOS')
    print('=' * 72)
    print(f'Total filas evaluadas                : {total}')
    print(f'Entradas diccionario homologacion   : {len(diccionario)}')
    print(f'Filas insertables hoy               : {resumen["insertables"]}')
    print(f'Filas bloqueadas hoy                : {rechazadas}')
    print()
    print('Causas de bloqueo')
    print(f'  Fecha invalida                    : {resumen["fecha_invalida"]}')
    print(f'  ID_Tiempo nulo                    : {resumen["id_tiempo_nulo"]}')
    print(f'  Geografia no encontrada           : {resumen["geografia_no_encontrada"]}')
    print(f'  Variedad sin homologacion         : {resumen["variedad_sin_homologacion"]}')
    print(f'  Variedad fuera de Dim_Variedad    : {resumen["variedad_fuera_de_dim"]}')
    print(f'  Peso no calculable                : {resumen["peso_no_calculable"]}')
    print(f'  Peso fuera de rango               : {resumen["peso_fuera_rango"]}')

    if fechas_invalidas:
        print()
        print('Fechas invalidas mas frecuentes')
        for valor, cantidad in _top_valores(fechas_invalidas):
            print(f'  {cantidad:5} | {valor}')

    if modulos_sin_geo:
        print()
        print('Modulos/valvulas sin geografia')
        for valor, cantidad in _top_valores(modulos_sin_geo):
            print(f'  {cantidad:5} | {valor}')

    if variedades_sin_match:
        print()
        print('Variedades sin homologacion')
        for valor, cantidad in _top_valores(variedades_sin_match):
            print(f'  {cantidad:5} | {valor}')

    if variedades_sin_dim:
        print()
        print('Variedades homologadas pero fuera de Dim_Variedad')
        for valor, cantidad in _top_valores(variedades_sin_dim):
            print(f'  {cantidad:5} | {valor}')

    if pesos_invalidos:
        print()
        print('Pesos invalidos mas frecuentes')
        for valor, cantidad in _top_valores(pesos_invalidos):
            print(f'  {cantidad:5} | {valor}')

    print('=' * 72)
    print()


if __name__ == '__main__':
    diagnosticar()
