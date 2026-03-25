"""
diagnosticar_lookup_geografia_paralela.py
=========================================
Diagnostica el impacto del nuevo resolver geografico paralelo
sin reemplazar el lookup actual.

Compara:
  - resolver_geografia() actual
  - resolver_geografia_paralela() nuevo

Objetivo:
  - medir cuantas filas bloqueadas hoy por geografia
    quedarian resueltas con el modelo base + cama
"""

from collections import Counter
from sqlalchemy import text

from config.conexion import verificar_conexion, obtener_engine
from mdm.lookup import limpiar_cache, resolver_geografia, resolver_geografia_paralela
from utils.texto import es_test_block, normalizar_modulo


PILOTOS = [
    {
        'nombre': 'Evaluacion_Pesos',
        'tabla': 'Bronce.Evaluacion_Pesos',
        'columna_id': 'ID_Evaluacion_Pesos',
    },
    {
        'nombre': 'Evaluacion_Vegetativa',
        'tabla': 'Bronce.Evaluacion_Vegetativa',
        'columna_id': 'ID_Evaluacion_Vegetativa',
    },
]


def _porcentaje(cantidad: int, total: int) -> str:
    if total <= 0:
        return '0.00%'
    return f'{(cantidad / total) * 100:,.2f}%'


def _leer_ultimo_lote(engine, tabla: str, columna_id: str):
    with engine.connect() as conexion:
        meta = conexion.execute(text(f"""
            SELECT TOP (1)
                Fecha_Sistema,
                Nombre_Archivo
            FROM {tabla}
            ORDER BY Fecha_Sistema DESC, {columna_id} DESC
        """)).fetchone()

        if not meta:
            return None, None, []

        fecha_sistema = meta.Fecha_Sistema
        nombre_archivo = meta.Nombre_Archivo

        resultado = conexion.execute(text(f"""
            SELECT
                {columna_id} AS ID_Registro,
                Modulo_Raw,
                Turno_Raw,
                Valvula_Raw,
                Cama_Raw
            FROM {tabla}
            WHERE Fecha_Sistema = :fecha_sistema
              AND Nombre_Archivo = :nombre_archivo
        """), {
            'fecha_sistema': fecha_sistema,
            'nombre_archivo': nombre_archivo,
        })
        return fecha_sistema, nombre_archivo, resultado.fetchall()


def _resolver_modulo_entrada(modulo_raw, valvula_raw):
    geo_modulo_raw = (
        modulo_raw
        if modulo_raw and str(modulo_raw).strip() not in ('', 'None', 'nan')
        else valvula_raw
    )
    return None if es_test_block(geo_modulo_raw) else normalizar_modulo(geo_modulo_raw)


def _tiene_cama_real(cama_raw) -> bool:
    if cama_raw is None:
        return False
    texto = str(cama_raw).strip()
    if texto in ('', 'None', 'nan'):
        return False
    try:
        return float(texto.replace(',', '.')) != 0
    except ValueError:
        return True


def _descripcion_geo(fila) -> str:
    return (
        f"Modulo={fila.Modulo_Raw} | Turno={fila.Turno_Raw} | "
        f"Valvula={fila.Valvula_Raw} | Cama={fila.Cama_Raw}"
    )


def _diagnosticar_piloto(engine, piloto: dict) -> dict:
    fecha_lote, archivo_lote, filas = _leer_ultimo_lote(
        engine,
        piloto['tabla'],
        piloto['columna_id'],
    )

    if not filas:
        return {
            'nombre': piloto['nombre'],
            'archivo': None,
            'fecha': None,
        'total': 0,
        'actual_resueltas': 0,
        'actual_bloqueadas': 0,
        'paralela_base_resuelta': 0,
        'paralela_total_resuelta': 0,
        'paralela_base_sin_cama': 0,
        'paralela_bloqueadas': 0,
        'mejora_neta_base': 0,
        'mejora_neta_total': 0,
        'estados_actuales': Counter(),
        'estados_paralelos': Counter(),
        'base_recuperada_sin_cama': Counter(),
    }

    estados_actuales = Counter()
    estados_paralelos = Counter()
    base_recuperada_sin_cama = Counter()

    actual_resueltas = 0
    paralela_base_resuelta = 0
    paralela_total_resuelta = 0

    for fila in filas:
        modulo = _resolver_modulo_entrada(fila.Modulo_Raw, fila.Valvula_Raw)
        tiene_cama_real = _tiene_cama_real(fila.Cama_Raw)

        resultado_actual = resolver_geografia(
            None,
            None,
            modulo,
            engine,
            turno=fila.Turno_Raw,
            valvula=fila.Valvula_Raw,
            cama=fila.Cama_Raw,
        )
        estado_actual = resultado_actual.get('estado', 'SIN_ESTADO')
        estados_actuales[estado_actual] += 1
        if resultado_actual.get('id_geografia') is not None:
            actual_resueltas += 1

        resultado_paralelo = resolver_geografia_paralela(
            None,
            None,
            modulo,
            engine,
            turno=fila.Turno_Raw,
            valvula=fila.Valvula_Raw,
            cama=fila.Cama_Raw,
        )
        estado_paralelo = resultado_paralelo.get('estado', 'SIN_ESTADO')
        estados_paralelos[estado_paralelo] += 1
        if resultado_paralelo.get('id_geografia_base') is not None:
            paralela_base_resuelta += 1
        if (
            resultado_paralelo.get('id_geografia_base') is not None
            and (not tiene_cama_real or resultado_paralelo.get('id_cama') is not None)
        ):
            paralela_total_resuelta += 1

        if (
            resultado_actual.get('id_geografia') is None
            and resultado_paralelo.get('id_geografia_base') is not None
            and tiene_cama_real
            and resultado_paralelo.get('id_cama') is None
        ):
            base_recuperada_sin_cama[
                f"[{estado_paralelo}] {_descripcion_geo(fila)}"
            ] += 1

    total = len(filas)

    return {
        'nombre': piloto['nombre'],
        'archivo': archivo_lote,
        'fecha': fecha_lote,
        'total': total,
        'actual_resueltas': actual_resueltas,
        'actual_bloqueadas': total - actual_resueltas,
        'paralela_base_resuelta': paralela_base_resuelta,
        'paralela_total_resuelta': paralela_total_resuelta,
        'paralela_base_sin_cama': paralela_base_resuelta - paralela_total_resuelta,
        'paralela_bloqueadas': total - paralela_total_resuelta,
        'mejora_neta_base': paralela_base_resuelta - actual_resueltas,
        'mejora_neta_total': paralela_total_resuelta - actual_resueltas,
        'estados_actuales': estados_actuales,
        'estados_paralelos': estados_paralelos,
        'base_recuperada_sin_cama': base_recuperada_sin_cama,
    }


def _imprimir_top(titulo: str, contador: Counter, limite: int = 10) -> None:
    if not contador:
        return
    print()
    print(titulo)
    for valor, cantidad in contador.most_common(limite):
        print(f'  {cantidad:5} | {valor}')


def _imprimir_resumen_piloto(resultado: dict) -> None:
    print()
    print('=' * 72)
    print(f'DIAGNOSTICO LOOKUP PARALELO - {resultado["nombre"]}')
    print('=' * 72)
    print(f'Archivo ultimo lote                 : {resultado["archivo"]}')
    print(f'Fecha_Sistema lote                  : {resultado["fecha"]}')
    print(f'Total filas evaluadas               : {resultado["total"]}')
    print(
        f'Resueltas lookup actual             : '
        f'{resultado["actual_resueltas"]} ({_porcentaje(resultado["actual_resueltas"], resultado["total"])})'
    )
    print(
        f'Base resuelta lookup paralelo       : '
        f'{resultado["paralela_base_resuelta"]} ({_porcentaje(resultado["paralela_base_resuelta"], resultado["total"])})'
    )
    print(
        f'Resuelta completa lookup paralelo   : '
        f'{resultado["paralela_total_resuelta"]} ({_porcentaje(resultado["paralela_total_resuelta"], resultado["total"])})'
    )
    print(
        f'Base encontrada pero cama faltante  : '
        f'{resultado["paralela_base_sin_cama"]} ({_porcentaje(resultado["paralela_base_sin_cama"], resultado["total"])})'
    )
    print(
        f'Mejora neta por base encontrada     : '
        f'{resultado["mejora_neta_base"]} ({_porcentaje(resultado["mejora_neta_base"], resultado["total"])})'
    )
    print(
        f'Mejora neta realmente cargable      : '
        f'{resultado["mejora_neta_total"]} ({_porcentaje(resultado["mejora_neta_total"], resultado["total"])})'
    )

    _imprimir_top('Estados lookup actual', resultado['estados_actuales'])
    _imprimir_top('Estados lookup paralelo', resultado['estados_paralelos'])
    _imprimir_top('Geografias con base encontrada pero cama faltante', resultado['base_recuperada_sin_cama'])


def diagnosticar() -> None:
    if not verificar_conexion():
        print('Sin conexion. Diagnostico detenido.')
        return

    engine = obtener_engine()
    limpiar_cache()

    resultados = []
    for piloto in PILOTOS:
        resultados.append(_diagnosticar_piloto(engine, piloto))

    for resultado in resultados:
        _imprimir_resumen_piloto(resultado)

    total_filas = sum(r['total'] for r in resultados)
    total_actual = sum(r['actual_resueltas'] for r in resultados)
    total_base = sum(r['paralela_base_resuelta'] for r in resultados)
    total_paralelo = sum(r['paralela_total_resuelta'] for r in resultados)
    total_base_sin_cama = sum(r['paralela_base_sin_cama'] for r in resultados)
    total_mejora_base = total_base - total_actual
    total_mejora = total_paralelo - total_actual

    estados_actuales = Counter()
    estados_paralelos = Counter()
    base_recuperada_sin_cama = Counter()

    for resultado in resultados:
        estados_actuales.update(resultado['estados_actuales'])
        estados_paralelos.update(resultado['estados_paralelos'])
        base_recuperada_sin_cama.update(resultado['base_recuperada_sin_cama'])

    print()
    print('=' * 72)
    print('CONSOLIDADO PILOTOS')
    print('=' * 72)
    print(f'Total filas evaluadas               : {total_filas}')
    print(f'Resueltas lookup actual             : {total_actual} ({_porcentaje(total_actual, total_filas)})')
    print(f'Base resuelta lookup paralelo       : {total_base} ({_porcentaje(total_base, total_filas)})')
    print(f'Resuelta completa lookup paralelo   : {total_paralelo} ({_porcentaje(total_paralelo, total_filas)})')
    print(f'Base encontrada pero cama faltante  : {total_base_sin_cama} ({_porcentaje(total_base_sin_cama, total_filas)})')
    print(f'Mejora neta por base encontrada     : {total_mejora_base} ({_porcentaje(total_mejora_base, total_filas)})')
    print(f'Mejora neta realmente cargable      : {total_mejora} ({_porcentaje(total_mejora, total_filas)})')

    _imprimir_top('Estados lookup actual consolidado', estados_actuales)
    _imprimir_top('Estados lookup paralelo consolidado', estados_paralelos)
    _imprimir_top('Geografias con base encontrada pero cama faltante', base_recuperada_sin_cama, limite=20)


if __name__ == '__main__':
    diagnosticar()
