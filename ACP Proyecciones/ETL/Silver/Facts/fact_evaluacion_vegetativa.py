"""
fact_evaluacion_vegetativa.py
==============================
Carga Silver.Fact_Evaluacion_Vegetativa desde Bronce.Evaluacion_Vegetativa.

Layout definitivo:
- DNI / evaluador
- modulo / turno / valvula / cama
- descripcion como variedad fuente
- plantas evaluadas / plantas en floracion
"""

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text

from utils.dni       import procesar_dni
from utils.fechas    import procesar_fecha, obtener_id_tiempo as construir_id_tiempo
from utils.texto     import normalizar_modulo, es_test_block
from utils.sql_lotes import ejecutar_en_lotes_con_engine, marcar_estado_carga_por_ids
from dq.cuarentena   import enviar_a_cuarentena
from mdm.lookup      import obtener_id_tiempo, resolver_geografia, obtener_id_personal, obtener_id_variedad
from mdm.homologador import homologar_columna


TABLA_ORIGEN  = 'Bronce.Evaluacion_Vegetativa'
TABLA_DESTINO = 'Silver.Fact_Evaluacion_Vegetativa'

SQL_INSERT_FACT = text("""
    INSERT INTO Silver.Fact_Evaluacion_Vegetativa (
        ID_Geografia, ID_Tiempo, ID_Variedad, ID_Personal,
        Tipo_Evaluacion,
        Cantidad_Plantas_Evaluadas, Cantidad_Plantas_en_Floracion,
        Fecha_Evento, Fecha_Sistema, Estado_DQ
    ) VALUES (
        :id_geo, :id_tiempo, :id_variedad, :id_personal,
        :tipo_evaluacion,
        :plantas_evaluadas, :plantas_en_floracion,
        :fecha_evento, SYSDATETIME(), 'OK'
    )
""")


def _motivo_cuarentena_geografia(resultado_geo: dict) -> str:
    estado = resultado_geo.get('estado')
    if estado in ('TEST_BLOCK_NO_MAPEADO', 'TEST_BLOCK_AMBIGUO'):
        return 'Test block (VI) sin mapeo unico en Dim_Geografia.'
    if estado in ('PENDIENTE_CASO_ESPECIAL', 'CASO_ESPECIAL_MODULO'):
        return 'Geografia especial requiere catalogacion o regla en MDM_Geografia.'
    if estado in ('PENDIENTE_CAMA_GENERICA', 'CAMA_NO_RELACION'):
        return 'Cama no relacionada a la geografia operativa.'
    if estado in ('PENDIENTE_DIM_DUPLICADA', 'GEOGRAFIA_AMBIGUA'):
        return 'La clave geografica tiene mas de un registro vigente en Silver.Dim_Geografia.'
    if estado == 'CAMA_NO_VALIDA':
        return 'Cama fuera de rango operativo permitido.'
    if estado == 'CAMA_NO_CATALOGO':
        return 'Cama valida pero no registrada en el catalogo operativo.'
    return 'Geografia no encontrada en Silver.Dim_Geografia.'


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


def _a_entero_positivo(valor) -> int | None:
    numero = _a_entero_nulo(valor)
    if numero is None or numero < 0:
        return None
    return numero


def _obtener_columnas_tabla(engine: Engine, tabla_completa: str) -> set[str]:
    esquema, tabla = tabla_completa.split('.')
    with engine.connect() as conexion:
        resultado = conexion.execute(text("""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = :esquema
              AND TABLE_NAME = :tabla
        """), {'esquema': esquema, 'tabla': tabla})
        return {str(fila[0]) for fila in resultado.fetchall()}


def _validar_layout_migrado(engine: Engine) -> str:
    columnas_bronce = _obtener_columnas_tabla(engine, TABLA_ORIGEN)
    columnas_silver = _obtener_columnas_tabla(engine, TABLA_DESTINO)

    columnas_bronce_requeridas = {
        'ID_Evaluacion_Vegetativa',
        'Fecha_Raw',
        'DNI_Raw',
        'Modulo_Raw',
        'Turno_Raw',
        'Valvula_Raw',
        'Cama_Raw',
        'Descripcion_Raw',
        'Evaluacion_Raw',
        'N_Plantas_Evaluadas_Raw',
        'N_Plantas_en_Floracion_Raw',
    }
    columnas_silver_requeridas = {
        'ID_Personal',
        'Tipo_Evaluacion',
        'Cantidad_Plantas_Evaluadas',
        'Cantidad_Plantas_en_Floracion',
    }

    faltantes_bronce = sorted(columnas_bronce_requeridas - columnas_bronce)
    faltantes_silver = sorted(columnas_silver_requeridas - columnas_silver)

    if faltantes_bronce or faltantes_silver:
        raise RuntimeError(
            'La migracion definitiva de Evaluacion_Vegetativa no esta aplicada. '
            f'Bronce faltantes: {faltantes_bronce or "ninguno"} | '
            f'Silver faltantes: {faltantes_silver or "ninguno"}'
        )

    return 'ID_Evaluacion_Vegetativa'


def _leer_bronce(engine: Engine, columna_id: str) -> pd.DataFrame:
    with engine.connect() as conexion:
        resultado = conexion.execute(text(f"""
            SELECT
                {columna_id} AS ID_Registro_Origen,
                Fecha_Raw,
                DNI_Raw,
                Fecha_Subida_Raw,
                Nombres_Raw,
                Consumidor_Raw,
                Modulo_Raw,
                Turno_Raw,
                Valvula_Raw,
                Evaluacion_Raw,
                Cama_Raw,
                Descripcion_Raw,
                N_Plantas_Evaluadas_Raw,
                N_Plantas_en_Floracion_Raw
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
          AND ISNULL(DNI_Raw, '') = ISNULL(:dni_raw, '')
          AND ISNULL(Modulo_Raw, '') = ISNULL(:modulo_raw, '')
          AND ISNULL(Turno_Raw, '') = ISNULL(:turno_raw, '')
          AND ISNULL(Valvula_Raw, '') = ISNULL(:valvula_raw, '')
          AND ISNULL(Cama_Raw, '') = ISNULL(:cama_raw, '')
          AND ISNULL(Descripcion_Raw, '') = ISNULL(:descripcion_raw, '')
          AND ISNULL(N_Plantas_Evaluadas_Raw, '') = ISNULL(:plantas_evaluadas_raw, '')
          AND ISNULL(N_Plantas_en_Floracion_Raw, '') = ISNULL(:plantas_floracion_raw, '')
    """)
    ejecutar_en_lotes_con_engine(engine, sentencia, claves)


def cargar_fact_evaluacion_vegetativa(engine: Engine) -> dict:
    resumen = {'insertados': 0, 'rechazados': 0, 'cuarentena': []}

    columna_id = _validar_layout_migrado(engine)
    df = _leer_bronce(engine, columna_id)
    if df.empty:
        return resumen

    df, cuar_var = homologar_columna(
        df, 'Descripcion_Raw', 'Variedad_Canonica', TABLA_ORIGEN, engine,
        columna_id_origen='ID_Registro_Origen'
    )
    resumen['cuarentena'].extend(cuar_var)

    ids_leidos = []
    claves_procesadas = []
    payload_inserts = []

    for _, fila in df.iterrows():
        id_origen = _a_entero_nulo(fila.get('ID_Registro_Origen'))
        if id_origen is not None:
            ids_leidos.append(id_origen)

        fecha, valida = procesar_fecha(fila.get('Fecha_Raw'))
        if not valida:
            resumen['rechazados'] += 1
            resumen['cuarentena'].append({
                'columna': 'Fecha_Raw',
                'valor': fila.get('Fecha_Raw'),
                'motivo': 'Fecha invalida o fuera de campana',
                'tipo_regla': 'DQ',
                'id_registro_origen': id_origen,
            })
            continue

        modulo = None if es_test_block(fila.get('Modulo_Raw')) else normalizar_modulo(fila.get('Modulo_Raw'))
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
        id_var = obtener_id_variedad(fila.get('Variedad_Canonica'), engine)

        if not id_geo:
            resumen['rechazados'] += 1
            resumen['cuarentena'].append({
                'columna': 'Modulo_Raw',
                'valor': (
                    f"Modulo={fila.get('Modulo_Raw')} | Turno={fila.get('Turno_Raw')} | "
                    f"Valvula={fila.get('Valvula_Raw')} | Cama={fila.get('Cama_Raw')}"
                ),
                'motivo': _motivo_cuarentena_geografia(resultado_geo),
                'tipo_regla': 'MDM',
                'id_registro_origen': id_origen,
            })
            continue

        if not id_var:
            resumen['rechazados'] += 1
            continue

        id_tiempo = obtener_id_tiempo(construir_id_tiempo(fecha), engine)
        if id_tiempo is None:
            resumen['rechazados'] += 1
            resumen['cuarentena'].append({
                'columna': 'Fecha_Raw',
                'valor': fila.get('Fecha_Raw'),
                'motivo': 'Fecha valida pero fuera de Dim_Tiempo',
                'tipo_regla': 'DQ',
                'id_registro_origen': id_origen,
            })
            continue

        plantas_evaluadas = _a_entero_positivo(fila.get('N_Plantas_Evaluadas_Raw'))
        plantas_floracion = _a_entero_positivo(fila.get('N_Plantas_en_Floracion_Raw'))

        if plantas_evaluadas is None or plantas_evaluadas == 0:
            resumen['rechazados'] += 1
            resumen['cuarentena'].append({
                'columna': 'N_Plantas_Evaluadas_Raw',
                'valor': fila.get('N_Plantas_Evaluadas_Raw'),
                'motivo': 'Cantidad de plantas evaluadas invalida',
                'tipo_regla': 'DQ',
                'id_registro_origen': id_origen,
            })
            continue

        if plantas_floracion is None or plantas_floracion > plantas_evaluadas:
            resumen['rechazados'] += 1
            resumen['cuarentena'].append({
                'columna': 'N_Plantas_en_Floracion_Raw',
                'valor': fila.get('N_Plantas_en_Floracion_Raw'),
                'motivo': 'Plantas en floracion invalida o mayor al total evaluado',
                'tipo_regla': 'DQ',
                'id_registro_origen': id_origen,
            })
            continue

        dni, _ = procesar_dni(fila.get('DNI_Raw'))
        id_personal = obtener_id_personal(dni, engine)

        payload_inserts.append({
            'id_geo':               id_geo,
            'id_tiempo':            id_tiempo,
            'id_variedad':          id_var,
            'id_personal':          id_personal,
            'tipo_evaluacion':      fila.get('Evaluacion_Raw'),
            'plantas_evaluadas':    plantas_evaluadas,
            'plantas_en_floracion': plantas_floracion,
            'fecha_evento':         fecha,
        })

        if id_origen is None:
            claves_procesadas.append({
                'fecha_raw':             fila.get('Fecha_Raw'),
                'dni_raw':               fila.get('DNI_Raw'),
                'modulo_raw':            fila.get('Modulo_Raw'),
                'turno_raw':             fila.get('Turno_Raw'),
                'valvula_raw':           fila.get('Valvula_Raw'),
                'cama_raw':              fila.get('Cama_Raw'),
                'descripcion_raw':       fila.get('Descripcion_Raw'),
                'plantas_evaluadas_raw': fila.get('N_Plantas_Evaluadas_Raw'),
                'plantas_floracion_raw': fila.get('N_Plantas_en_Floracion_Raw'),
            })

    if payload_inserts:
        ejecutar_en_lotes_con_engine(engine, SQL_INSERT_FACT, payload_inserts)
    resumen['insertados'] = len(payload_inserts)

    if ids_leidos:
        marcar_estado_carga_por_ids(engine, TABLA_ORIGEN, columna_id, ids_leidos)
    if claves_procesadas:
        _marcar_procesado_por_firma(engine, claves_procesadas)

    if resumen['cuarentena']:
        enviar_a_cuarentena(engine, TABLA_ORIGEN, resumen['cuarentena'])

    return resumen

