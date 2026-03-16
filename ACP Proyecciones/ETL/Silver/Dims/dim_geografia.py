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
            WHERE Activo = 1
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
