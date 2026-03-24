"""
dim_geografia.py
================
Carga y actualización de Silver.Dim_Geografia.
Fuente: MDM.Catalogo_Geografia (carga manual vía Streamlit)

Lógica SCD Tipo 2:
  - Si combinación Fundo+Sector+Modulo no existe → INSERT nuevo
  - Si existe y no cambió → no hace nada
  - Si existe y cambió → cierra registro viejo + INSERT nuevo
  - Test Block → Es_Test_Block = 1, Modulo = NULL
"""

import pandas as pd
from datetime import date
from sqlalchemy.engine import Engine
from sqlalchemy import text

from utils.texto import titulo, es_test_block


def _cargar_catalogo_geografia(engine: Engine) -> pd.DataFrame:
    """
    Lee la geografía canónica desde MDM.Catalogo_Geografia.
    Esta tabla la mantiene el analista vía Streamlit.
    """
    with engine.connect() as conexion:
        resultado = conexion.execute(text("""
            SELECT
                Fundo,
                Sector,
                Modulo,
                Turno,
                Valvula,
                Cama,
                Codigo_SAP_Campo,
                Es_Test_Block
            FROM MDM.Catalogo_Geografia
            WHERE Es_Activa = 1
        """))
        return pd.DataFrame(resultado.fetchall(), columns=resultado.keys())


def _obtener_vigentes(engine: Engine) -> pd.DataFrame:
    """
    Carga los registros vigentes de Dim_Geografia (Es_Vigente = 1).
    """
    with engine.connect() as conexion:
        resultado = conexion.execute(text("""
            SELECT
                ID_Geografia,
                Fundo, Sector, Modulo,
                Turno, Valvula, Cama,
                Codigo_SAP_Campo,
                Es_Test_Block
            FROM Silver.Dim_Geografia
            WHERE Es_Vigente = 1
        """))
        return pd.DataFrame(resultado.fetchall(), columns=resultado.keys())


def _clave_geo(fundo: str, sector: str | None,
               modulo) -> str:
    """
    Genera clave de comparación para detectar cambios SCD2.
    """
    return f'{str(fundo).lower()}|{str(sector).lower()}|{str(modulo).lower()}'


def _cerrar_registro(conexion, id_geografia: int) -> None:
    """
    Cierra un registro SCD2 — marca como no vigente.
    """
    hoy = date.today()
    conexion.execute(text("""
        UPDATE Silver.Dim_Geografia
        SET Fecha_Fin_Vigencia = :hoy,
            Es_Vigente         = 0
        WHERE ID_Geografia = :id_geografia
    """), {'hoy': hoy, 'id_geografia': id_geografia})


def _insertar_registro(conexion, fila: pd.Series) -> None:
    """
    Inserta un nuevo registro en Dim_Geografia.
    """
    hoy = date.today()
    conexion.execute(text("""
        INSERT INTO Silver.Dim_Geografia (
            Fundo, Sector, Modulo, Turno,
            Valvula, Cama, Es_Test_Block,
            Codigo_SAP_Campo,
            Fecha_Inicio_Vigencia, Fecha_Fin_Vigencia, Es_Vigente
        ) VALUES (
            :fundo, :sector, :modulo, :turno,
            :valvula, :cama, :es_test_block,
            :codigo_sap,
            :fecha_inicio, NULL, 1
        )
    """), {
        'fundo':         fila.get('Fundo'),
        'sector':        fila.get('Sector'),
        'modulo':        fila.get('Modulo'),
        'turno':         fila.get('Turno'),
        'valvula':       fila.get('Valvula'),
        'cama':          fila.get('Cama'),
        'es_test_block': int(fila.get('Es_Test_Block', 0)),
        'codigo_sap':    fila.get('Codigo_SAP_Campo'),
        'fecha_inicio':  hoy,
    })


def cargar_dim_geografia(engine: Engine) -> dict:
    """
    Sincroniza Silver.Dim_Geografia con MDM.Catalogo_Geografia.
    Aplica SCD Tipo 2.
    Retorna resumen de operaciones.
    """
    resumen = {'insertados': 0, 'cerrados': 0, 'sin_cambios': 0}

    df_catalogo = _cargar_catalogo_geografia(engine)
    df_vigentes = _obtener_vigentes(engine)

    # Construir índice de vigentes por clave
    indice_vigentes: dict[str, pd.Series] = {}
    for _, fila in df_vigentes.iterrows():
        clave = _clave_geo(fila['Fundo'], fila['Sector'], fila['Modulo'])
        indice_vigentes[clave] = fila

    with engine.begin() as conexion:
        for _, fila_catalogo in df_catalogo.iterrows():
            fundo   = titulo(str(fila_catalogo.get('Fundo', '')))
            sector  = titulo(str(fila_catalogo.get('Sector', ''))) if fila_catalogo.get('Sector') else None
            modulo  = fila_catalogo.get('Modulo')
            clave   = _clave_geo(fundo, sector, modulo)

            if clave in indice_vigentes:
                existente = indice_vigentes[clave]

                # Detectar cambio en campos no clave
                cambio = (
                    str(existente.get('Turno'))         != str(fila_catalogo.get('Turno')) or
                    str(existente.get('Valvula', ''))   != str(fila_catalogo.get('Valvula', '')) or
                    str(existente.get('Cama', ''))      != str(fila_catalogo.get('Cama', '')) or
                    int(existente.get('Es_Test_Block')) != int(fila_catalogo.get('Es_Test_Block', 0))
                )

                if cambio:
                    _cerrar_registro(conexion, int(existente['ID_Geografia']))
                    _insertar_registro(conexion, fila_catalogo)
                    resumen['cerrados']   += 1
                    resumen['insertados'] += 1
                else:
                    resumen['sin_cambios'] += 1
            else:
                _insertar_registro(conexion, fila_catalogo)
                resumen['insertados'] += 1

    return resumen


# Implementacion corregida: clave natural completa + comparacion estable.
def _geo_es_nulo(valor) -> bool:
    import pandas as _pd
    if valor is None:
        return True
    if isinstance(valor, float) and _pd.isna(valor):
        return True
    return str(valor).strip() in ('', 'None', 'nan')


def _geo_normalizar_entero(valor) -> int | None:
    if _geo_es_nulo(valor):
        return None
    if isinstance(valor, bool):
        return 1 if valor else 0
    texto = str(valor).strip().lower()
    if texto in ('true', 'verdadero', 'si', 'yes'):
        return 1
    if texto in ('false', 'falso', 'no'):
        return 0
    return int(float(str(valor).strip()))


def _geo_normalizar_texto(valor, usar_titulo: bool = False) -> str | None:
    from utils.texto import normalizar_espacio, titulo as _titulo

    if _geo_es_nulo(valor):
        return None

    texto = normalizar_espacio(str(valor))
    return _titulo(texto) if usar_titulo else texto


def _geo_normalizar_fila(fila: pd.Series | dict) -> dict:
    from utils.texto import limpiar_numerico_texto

    return {
        'Fundo': _geo_normalizar_texto(fila.get('Fundo'), usar_titulo=True),
        'Sector': _geo_normalizar_texto(fila.get('Sector'), usar_titulo=True),
        'Modulo': _geo_normalizar_entero(fila.get('Modulo')),
        'Turno': _geo_normalizar_entero(fila.get('Turno')),
        'Valvula': limpiar_numerico_texto(_geo_normalizar_texto(fila.get('Valvula'))),
        'Cama': limpiar_numerico_texto(_geo_normalizar_texto(fila.get('Cama'))),
        'Codigo_SAP_Campo': _geo_normalizar_texto(fila.get('Codigo_SAP_Campo')),
        'Es_Test_Block': _geo_normalizar_entero(fila.get('Es_Test_Block')) or 0,
    }


def _geo_clave(fila: dict) -> tuple:
    return (
        (fila.get('Fundo') or '').lower(),
        (fila.get('Sector') or '').lower(),
        fila.get('Modulo'),
        fila.get('Turno'),
        (fila.get('Valvula') or '').lower(),
        (fila.get('Cama') or '').lower(),
    )


def _geo_hay_cambio(existente: dict, actual: dict) -> bool:
    return (
        (existente.get('Codigo_SAP_Campo') or None) != (actual.get('Codigo_SAP_Campo') or None)
        or int(existente.get('Es_Test_Block', 0)) != int(actual.get('Es_Test_Block', 0))
    )


def _geo_insertar_registro(conexion, fila: dict) -> None:
    hoy = date.today()
    conexion.execute(text("""
        INSERT INTO Silver.Dim_Geografia (
            Fundo, Sector, Modulo, Turno,
            Valvula, Cama, Es_Test_Block,
            Codigo_SAP_Campo,
            Fecha_Inicio_Vigencia, Fecha_Fin_Vigencia, Es_Vigente
        ) VALUES (
            :fundo, :sector, :modulo, :turno,
            :valvula, :cama, :es_test_block,
            :codigo_sap,
            :fecha_inicio, NULL, 1
        )
    """), {
        'fundo': fila.get('Fundo'),
        'sector': fila.get('Sector'),
        'modulo': fila.get('Modulo'),
        'turno': fila.get('Turno'),
        'valvula': fila.get('Valvula'),
        'cama': fila.get('Cama'),
        'es_test_block': int(fila.get('Es_Test_Block', 0)),
        'codigo_sap': fila.get('Codigo_SAP_Campo'),
        'fecha_inicio': hoy,
    })


def cargar_dim_geografia(engine: Engine) -> dict:
    resumen = {'insertados': 0, 'cerrados': 0, 'sin_cambios': 0}

    df_catalogo = _cargar_catalogo_geografia(engine)
    if df_catalogo.empty:
        return resumen

    df_vigentes = _obtener_vigentes(engine)

    registros_catalogo = []
    claves_catalogo = set()
    for _, fila_catalogo in df_catalogo.iterrows():
        fila_normalizada = _geo_normalizar_fila(fila_catalogo)
        clave = _geo_clave(fila_normalizada)
        if clave in claves_catalogo:
            continue
        claves_catalogo.add(clave)
        registros_catalogo.append(fila_normalizada)

    indice_vigentes: dict[tuple, dict] = {}
    for _, fila_vigente in df_vigentes.iterrows():
        fila_normalizada = _geo_normalizar_fila(fila_vigente)
        fila_normalizada['ID_Geografia'] = int(fila_vigente['ID_Geografia'])
        clave = _geo_clave(fila_normalizada)
        if (
            clave not in indice_vigentes
            or fila_normalizada['ID_Geografia'] > indice_vigentes[clave]['ID_Geografia']
        ):
            indice_vigentes[clave] = fila_normalizada

    with engine.begin() as conexion:
        for fila_catalogo in registros_catalogo:
            clave = _geo_clave(fila_catalogo)
            if clave in indice_vigentes:
                existente = indice_vigentes[clave]
                if _geo_hay_cambio(existente, fila_catalogo):
                    _cerrar_registro(conexion, int(existente['ID_Geografia']))
                    _geo_insertar_registro(conexion, fila_catalogo)
                    resumen['cerrados'] += 1
                    resumen['insertados'] += 1
                else:
                    resumen['sin_cambios'] += 1
            else:
                _geo_insertar_registro(conexion, fila_catalogo)
                resumen['insertados'] += 1

    return resumen
