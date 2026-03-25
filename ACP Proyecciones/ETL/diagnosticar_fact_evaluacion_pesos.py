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
from sqlalchemy import text

from config.conexion import verificar_conexion, obtener_engine
from mdm.homologador import (
    buscar_match_exacto,
    buscar_match_levenshtein,
    cargar_diccionario,
)
from mdm.lookup import (
    limpiar_cache,
    obtener_id_tiempo as buscar_id_tiempo,
    resolver_geografia,
    obtener_id_personal,
    obtener_id_variedad,
)
from silver.facts.fact_evaluacion_pesos import (
    _calcular_peso_ponderado,
)
from utils.dni import procesar_dni
from utils.fechas import obtener_id_tiempo as construir_id_tiempo, procesar_fecha
from utils.texto import es_test_block, normalizar_modulo
from dq.validador import validar_peso_baya


ESTADOS_GEO = {
    'PENDIENTE_CASO_ESPECIAL': {
        'titulo': 'Caso especial',
        'descripcion': 'Modulo o codigo geografico especial; revisar catalogacion en MDM_Geografia.',
        'accion': 'Evaluar si corresponde a Test_Block o a una regla especial de homologacion.',
    },
    'PENDIENTE_CAMA_GENERICA': {
        'titulo': 'Cama generica',
        'descripcion': 'El origen viene a nivel valvula/cama generica y no existe una geografia generica vigente en la dimension.',
        'accion': 'Decidir si la combinacion debe existir a nivel generico o si el origen debe corregirse.',
    },
    'PENDIENTE_GEOGRAFIA_NO_EXISTE': {
        'titulo': 'Geografia inexistente',
        'descripcion': 'La combinacion exacta no existe en Silver.Dim_Geografia.',
        'accion': 'Mandar a cuarentena para evaluacion y alta controlada si corresponde.',
    },
    'PENDIENTE_DIM_DUPLICADA': {
        'titulo': 'Dimension duplicada',
        'descripcion': 'La misma clave geografica tiene mas de un registro vigente en Silver.Dim_Geografia.',
        'accion': 'Corregir la dimension antes de volver a cargar facts.',
    },
}


def _top_valores(contador: Counter, limite: int = 10) -> list[tuple[str, int]]:
    return [(str(valor), cantidad) for valor, cantidad in contador.most_common(limite)]


def _porcentaje(cantidad: int, total: int) -> str:
    if total <= 0:
        return '0.00%'
    return f'{(cantidad / total) * 100:,.2f}%'


def _imprimir_metrica(etiqueta: str, cantidad: int, total: int, sangria: str = '  ') -> None:
    print(f'{sangria}{etiqueta:34} {cantidad:6} ({_porcentaje(cantidad, total)})')


def _imprimir_top(titulo: str, contador: Counter, limite: int = 10) -> None:
    if not contador:
        return
    print()
    print(titulo)
    for valor, cantidad in _top_valores(contador, limite):
        print(f'  {cantidad:5} | {valor}')


def _imprimir_bloque_geografia(estados_geografia: Counter,
                               geos_por_estado: dict[str, Counter],
                               total: int) -> None:
    if not estados_geografia:
        return

    print()
    print('Lectura operativa de geografia')
    for estado, meta in ESTADOS_GEO.items():
        cantidad = estados_geografia[estado]
        if cantidad == 0:
            continue
        _imprimir_metrica(meta['titulo'], cantidad, total)
        print(f'    Significado: {meta["descripcion"]}')
        print(f'    Accion     : {meta["accion"]}')
        ejemplos = geos_por_estado.get(estado)
        if ejemplos:
            print('    Ejemplos:')
            for valor, veces in _top_valores(ejemplos, 5):
                print(f'      {veces:5} | {valor}')


def _imprimir_recomendaciones(resumen: Counter, estados_geografia: Counter) -> None:
    recomendaciones = []
    if estados_geografia['PENDIENTE_DIM_DUPLICADA'] > 0:
        recomendaciones.append('Corregir duplicados vigentes en Silver.Dim_Geografia antes de cualquier nueva carga.')
    if estados_geografia['PENDIENTE_CASO_ESPECIAL'] > 0:
        recomendaciones.append('Revisar catalogacion de modulos especiales en MDM_Geografia para automatizar Test_Block y excepciones.')
    if estados_geografia['PENDIENTE_CAMA_GENERICA'] > 0:
        recomendaciones.append('Definir politica operativa para reportes a nivel valvula/cama generica.')
    if estados_geografia['PENDIENTE_GEOGRAFIA_NO_EXISTE'] > 0:
        recomendaciones.append('Enviar combinaciones nuevas a cuarentena para evaluacion y aprobacion controlada.')
    if resumen['variedad_sin_homologacion'] > 0:
        recomendaciones.append('Priorizar homologacion de variedades antes de seguir expandiendo reglas geograficas.')
    if resumen['peso_no_calculable'] > 0 or resumen['peso_fuera_rango'] > 0:
        recomendaciones.append('Revisar calidad de captura de pesos y conteos de muestra en origen.')

    if not recomendaciones:
        return

    print()
    print('Acciones sugeridas')
    for indice, recomendacion in enumerate(recomendaciones, start=1):
        print(f'  {indice}. {recomendacion}')


def _leer_ultimo_lote(engine) -> tuple[object | None, object | None, list[tuple], list[str]]:
    with engine.connect() as conexion:
        meta = conexion.execute(text("""
            SELECT TOP (1)
                Fecha_Sistema,
                Nombre_Archivo
            FROM Bronce.Evaluacion_Pesos
            ORDER BY Fecha_Sistema DESC, ID_Evaluacion_Pesos DESC
        """)).fetchone()

        if not meta:
            return None, None, [], []

        fecha_sistema = meta.Fecha_Sistema
        nombre_archivo = meta.Nombre_Archivo

        resultado = conexion.execute(text("""
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
                PesoCosechables_Raw,
                Estado_Carga,
                Fecha_Sistema,
                Nombre_Archivo
            FROM Bronce.Evaluacion_Pesos
            WHERE Fecha_Sistema = :fecha_sistema
              AND Nombre_Archivo = :nombre_archivo
        """), {
            'fecha_sistema': fecha_sistema,
            'nombre_archivo': nombre_archivo,
        })
        filas = resultado.fetchall()
        columnas = list(resultado.keys())
        return fecha_sistema, nombre_archivo, filas, columnas


def diagnosticar() -> None:
    if not verificar_conexion():
        print('Sin conexion. Diagnostico detenido.')
        return

    engine = obtener_engine()
    limpiar_cache()

    fecha_lote, archivo_lote, filas, columnas = _leer_ultimo_lote(engine)
    if not filas:
        print('No hay filas en Bronce.Evaluacion_Pesos.')
        return

    import pandas as pd
    df = pd.DataFrame(filas, columns=columnas)
    total = len(df)

    if df.empty:
        print('No hay filas en el ultimo lote de Bronce.Evaluacion_Pesos.')
        return

    diccionario = cargar_diccionario(engine, 'Bronce.Evaluacion_Pesos')

    resumen = Counter()
    estados_geografia = Counter()
    geos_sin_match = Counter()
    geos_por_estado = {estado: Counter() for estado in ESTADOS_GEO}
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

        id_tiempo = buscar_id_tiempo(construir_id_tiempo(fecha), engine)
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
            estado_geo = resultado_geo.get('estado', 'PENDIENTE_GEOGRAFIA_NO_EXISTE')
            resumen['geografia_no_encontrada'] += 1
            estados_geografia[estado_geo] += 1
            descripcion_geo = (
                f"Modulo={fila.get('Modulo_Raw')} | Turno={fila.get('Turno_Raw')} | "
                f"Valvula={fila.get('Valvula_Raw')} | Cama={fila.get('Cama_Raw')}"
            )
            geos_sin_match[f"[{estado_geo}] {descripcion_geo}"] += 1
            if estado_geo in geos_por_estado:
                geos_por_estado[estado_geo][descripcion_geo] += 1
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
    print(f'Archivo ultimo lote                 : {archivo_lote}')
    print(f'Fecha_Sistema lote                  : {fecha_lote}')
    print(f'Total filas evaluadas               : {total}')
    print(f'Entradas diccionario homologacion   : {len(diccionario)}')
    print(f'Filas insertables hoy               : {resumen["insertables"]} ({_porcentaje(resumen["insertables"], total)})')
    print(f'Filas bloqueadas hoy                : {rechazadas} ({_porcentaje(rechazadas, total)})')
    print()
    print('Causas de bloqueo')
    _imprimir_metrica('Fecha invalida', resumen['fecha_invalida'], total)
    _imprimir_metrica('ID_Tiempo nulo', resumen['id_tiempo_nulo'], total)
    _imprimir_metrica('Geografia no encontrada', resumen['geografia_no_encontrada'], total)
    _imprimir_metrica('Variedad sin homologacion', resumen['variedad_sin_homologacion'], total)
    _imprimir_metrica('Variedad fuera de Dim_Variedad', resumen['variedad_fuera_de_dim'], total)
    _imprimir_metrica('Peso no calculable', resumen['peso_no_calculable'], total)
    _imprimir_metrica('Peso fuera de rango', resumen['peso_fuera_rango'], total)

    _imprimir_bloque_geografia(estados_geografia, geos_por_estado, total)
    _imprimir_recomendaciones(resumen, estados_geografia)

    _imprimir_top('Fechas invalidas mas frecuentes', fechas_invalidas)
    _imprimir_top('Combinaciones geograficas sin match', geos_sin_match)
    _imprimir_top('Variedades sin homologacion', variedades_sin_match)
    _imprimir_top('Variedades homologadas pero fuera de Dim_Variedad', variedades_sin_dim)
    _imprimir_top('Pesos invalidos mas frecuentes', pesos_invalidos)

    print('=' * 72)
    print()


if __name__ == '__main__':
    diagnosticar()
