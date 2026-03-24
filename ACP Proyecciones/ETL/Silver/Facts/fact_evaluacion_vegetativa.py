"""
fact_evaluacion_vegetativa.py
==============================
Carga Silver.Fact_Evaluacion_Vegetativa desde Bronce.Evaluacion_Vegetativa.

Grain: Fecha + Geo + DNI
"""

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text

from utils.fechas    import procesar_fecha, obtener_id_tiempo
from utils.texto     import normalizar_modulo, es_test_block
from dq.cuarentena   import enviar_a_cuarentena
from mdm.lookup      import obtener_id_geografia, obtener_id_variedad
from mdm.homologador import homologar_columna


TABLA_ORIGEN  = 'Bronce.Evaluacion_Vegetativa'
SQL_INSERT_FACT = text("""
    INSERT INTO Silver.Fact_Evaluacion_Vegetativa (
        ID_Geografia, ID_Tiempo, ID_Variedad,
        Semanas_Despues_Poda,
        Promedio_Altura, Promedio_Tallos_Basales,
        Promedio_Tallos_Basales_Nuevos,
        Promedio_Brotes_Generales, Promedio_Brotes_Productivos,
        Promedio_Diametro_Brote,
        Fecha_Evento, Fecha_Sistema, Estado_DQ
    ) VALUES (
        :id_geo, :id_tiempo, :id_variedad,
        :semanas,
        :altura, :tallos_bas,
        :tallos_nuevos,
        :brotes_gen, :brotes_prod,
        :diametro,
        :fecha_evento, SYSDATETIME(), 'OK'
    )
""")


def _a_decimal(valor) -> float | None:
    try:
        return float(str(valor).replace(',', '.'))
    except (ValueError, TypeError):
        return None


def _a_entero_nulo(valor) -> int | None:
    try:
        if valor is None:
            return None
        texto = str(valor).strip()
        if texto in ('', 'None', 'nan'):
            return None
        return int(float(texto))
    except (ValueError, TypeError):
        return None


def _resolver_columna_id_bronce(engine: Engine) -> str | None:
    esquema, tabla = TABLA_ORIGEN.split('.')
    with engine.connect() as conexion:
        resultado = conexion.execute(text("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = :esquema
              AND TABLE_NAME = :tabla
        """), {'esquema': esquema, 'tabla': tabla})
        columnas = {str(fila[0]) for fila in resultado.fetchall()}

    for candidata in ('ID_Evaluacion_Vegetativa', 'ID_Evaluacion_Veg'):
        if candidata in columnas:
            return candidata
    return None


def _leer_bronce(engine: Engine, columna_id: str | None) -> pd.DataFrame:
    id_select = (
        f"{columna_id} AS ID_Registro_Origen"
        if columna_id
        else "CAST(NULL AS BIGINT) AS ID_Registro_Origen"
    )
    with engine.connect() as conexion:
        resultado = conexion.execute(text(f"""
            SELECT
                {id_select},
                Fecha_Raw, Fundo_Raw, Modulo_Raw, Variedad_Raw,
                Semanas_Poda_Raw,
                Altura_Raw, TallosBasales_Raw, TallosBasalesNuevos_Raw,
                BrotesGenerales_Raw, BrotesProductivos_Raw, DiametroBrote_Raw
            FROM {TABLA_ORIGEN}
            WHERE Estado_Carga = 'CARGADO'
        """))
        return pd.DataFrame(resultado.fetchall(), columns=resultado.keys())


def _marcar_procesado_por_firma(engine: Engine, claves: list[dict]) -> None:
    if not claves:
        return

    sentencia = text(f"""
        UPDATE TOP (1) {TABLA_ORIGEN}
        SET Estado_Carga = 'PROCESADO'
        WHERE Estado_Carga = 'CARGADO'
          AND ISNULL(Fecha_Raw, '') = ISNULL(:fecha_raw, '')
          AND ISNULL(Fundo_Raw, '') = ISNULL(:fundo_raw, '')
          AND ISNULL(Modulo_Raw, '') = ISNULL(:modulo_raw, '')
          AND ISNULL(Turno_Raw, '') = ISNULL(:turno_raw, '')
          AND ISNULL(Valvula_Raw, '') = ISNULL(:valvula_raw, '')
          AND ISNULL(Variedad_Raw, '') = ISNULL(:variedad_raw, '')
          AND ISNULL(Semanas_Poda_Raw, '') = ISNULL(:semanas_poda_raw, '')
    """)

    tam_lote = 2000
    with engine.begin() as conexion:
        for inicio in range(0, len(claves), tam_lote):
            conexion.execute(sentencia, claves[inicio:inicio + tam_lote])


def cargar_fact_evaluacion_vegetativa(engine: Engine) -> dict:
    resumen = {'insertados': 0, 'rechazados': 0, 'cuarentena': []}

    columna_id = _resolver_columna_id_bronce(engine)
    df = _leer_bronce(engine, columna_id)
    if df.empty:
        return resumen

    df, cuar_var = homologar_columna(
        df, 'Variedad_Raw', 'Variedad_Canonica', TABLA_ORIGEN, engine
    )
    resumen['cuarentena'].extend(cuar_var)

    ids_procesados = []
    claves_procesadas = []
    payload_inserts = []

    for _, fila in df.iterrows():
        fecha, valida = procesar_fecha(fila.get('Fecha_Raw'))
        if not valida:
            resumen['rechazados'] += 1
            continue

        modulo = None if es_test_block(fila.get('Modulo_Raw')) else normalizar_modulo(fila.get('Modulo_Raw'))
        id_geo = obtener_id_geografia(fila.get('Fundo_Raw'), None, modulo, engine)
        id_var = obtener_id_variedad(fila.get('Variedad_Canonica'), engine)

        if not id_geo or not id_var:
            resumen['rechazados'] += 1
            continue

        try:
            semanas = int(float(str(fila.get('Semanas_Poda_Raw', 0))))
        except (ValueError, TypeError):
            semanas = None

        id_tiempo = obtener_id_tiempo(fecha)
        if id_tiempo is None:
            resumen['rechazados'] += 1
            continue

        payload_inserts.append({
            'id_geo':       id_geo,
            'id_tiempo':    id_tiempo,
            'id_variedad':  id_var,
            'semanas':      semanas,
            'altura':       _a_decimal(fila.get('Altura_Raw')),
            'tallos_bas':   _a_decimal(fila.get('TallosBasales_Raw')),
            'tallos_nuevos':_a_decimal(fila.get('TallosBasalesNuevos_Raw')),
            'brotes_gen':   _a_decimal(fila.get('BrotesGenerales_Raw')),
            'brotes_prod':  _a_decimal(fila.get('BrotesProductivos_Raw')),
            'diametro':     _a_decimal(fila.get('DiametroBrote_Raw')),
            'fecha_evento': fecha,
        })

        id_origen = _a_entero_nulo(fila.get('ID_Registro_Origen'))
        if id_origen is not None:
            ids_procesados.append(id_origen)
        else:
            claves_procesadas.append({
                'fecha_raw':        fila.get('Fecha_Raw'),
                'fundo_raw':        fila.get('Fundo_Raw'),
                'modulo_raw':       fila.get('Modulo_Raw'),
                'turno_raw':        fila.get('Turno_Raw'),
                'valvula_raw':      fila.get('Valvula_Raw'),
                'variedad_raw':     fila.get('Variedad_Raw'),
                'semanas_poda_raw': fila.get('Semanas_Poda_Raw'),
            })

    if payload_inserts:
        tam_lote = 2000
        with engine.begin() as conexion:
            for inicio in range(0, len(payload_inserts), tam_lote):
                conexion.execute(SQL_INSERT_FACT, payload_inserts[inicio:inicio + tam_lote])
    resumen['insertados'] = len(payload_inserts)

    if ids_procesados and columna_id:
        with engine.begin() as conexion:
            conexion.execute(
                text(f"UPDATE {TABLA_ORIGEN} SET Estado_Carga = 'PROCESADO' WHERE {columna_id} = :id_origen"),
                [{'id_origen': id_origen} for id_origen in ids_procesados]
            )
    if claves_procesadas:
        _marcar_procesado_por_firma(engine, claves_procesadas)

    if resumen['cuarentena']:
        enviar_a_cuarentena(engine, TABLA_ORIGEN, resumen['cuarentena'])

    return resumen
