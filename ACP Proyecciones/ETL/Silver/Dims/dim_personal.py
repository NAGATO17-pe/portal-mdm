"""
dim_personal.py
===============
Carga y actualización de Silver.Dim_Personal.
Fuentes: Bronce.Consolidado_Tareos + Bronce.Fiscalizacion

Lógica:
  - SCD Tipo 1 — actualiza sin historial
  - DNI nulo → ID_Personal = -1 (surrogate ya existe en seed)
  - Si DNI existe → UPDATE nombre si cambió
  - Si DNI no existe → INSERT nuevo registro
"""

import pandas as pd
from sqlalchemy.engine import Engine
from sqlalchemy import text

from utils.dni   import procesar_dni
from utils.texto import normalizar_nombre_persona, mayusculas


def _cargar_personal_bronce(engine: Engine) -> pd.DataFrame:
    """
    Lee personal único desde Bronce.Consolidado_Tareos
    y Bronce.Fiscalizacion.
    Une ambas fuentes y deduplica por DNI.
    """
    with engine.connect() as conexion:

        tareos = conexion.execute(text("""
            SELECT DISTINCT
                DNIResponsable_Raw  AS dni_raw,
                Evaluador_Raw       AS nombre_raw,
                NULL                AS sexo_raw,
                IDPlanilla_Raw      AS planilla_raw,
                'Operario'          AS rol
            FROM Bronce.Consolidado_Tareos
            WHERE DNIResponsable_Raw IS NOT NULL
        """))

        fiscalizacion = conexion.execute(text("""
            SELECT DISTINCT
                DNI_Raw             AS dni_raw,
                Evaluador_Raw       AS nombre_raw,
                NULL                AS sexo_raw,
                NULL                AS planilla_raw,
                'Evaluador'         AS rol
            FROM Bronce.Fiscalizacion
            WHERE DNI_Raw IS NOT NULL
        """))

    df_tareos = pd.DataFrame(tareos.fetchall(), columns=tareos.keys())
    df_fisca  = pd.DataFrame(fiscalizacion.fetchall(), columns=fiscalizacion.keys())

    df = pd.concat([df_tareos, df_fisca], ignore_index=True)

    # Limpiar DNI
    df['dni_limpio'] = df['dni_raw'].apply(
        lambda v: procesar_dni(v)[0]
    )

    # Descartar registros sin DNI válido
    df = df[df['dni_limpio'].notna()].copy()

    # Normalizar nombre
    df['nombre_limpio'] = df['nombre_raw'].apply(normalizar_nombre_persona)

    # Normalizar sexo
    def normalizar_sexo(valor: str | None) -> str | None:
        if not valor:
            return None
        v = str(valor).strip().upper()
        if v in ('M', 'MASCULINO', 'HOMBRE'):
            return 'M'
        if v in ('F', 'FEMENINO', 'MUJER'):
            return 'F'
        return None

    df['sexo_limpio'] = df['sexo_raw'].apply(normalizar_sexo)

    # Deduplicar — priorizar Fiscalizacion sobre Tareos
    df = df.sort_values('rol', ascending=False)
    df = df.drop_duplicates(subset='dni_limpio', keep='first')

    return df.reset_index(drop=True)


def _obtener_personal_existente(engine: Engine) -> pd.DataFrame:
    """
    Carga el personal ya registrado en Silver.Dim_Personal.
    """
    with engine.connect() as conexion:
        resultado = conexion.execute(text("""
            SELECT ID_Personal, DNI, Nombre_Completo, Rol
            FROM Silver.Dim_Personal
            WHERE ID_Personal != -1
        """))
        return pd.DataFrame(resultado.fetchall(), columns=resultado.keys())


def cargar_dim_personal(engine: Engine) -> dict:
    """
    Sincroniza Silver.Dim_Personal con las fuentes Bronce.
    Retorna resumen de operaciones realizadas.
    """
    resumen = {'insertados': 0, 'actualizados': 0, 'sin_cambios': 0}

    df_bronce    = _cargar_personal_bronce(engine)
    df_existente = _obtener_personal_existente(engine)

    dnis_existentes = set(df_existente['DNI'].tolist())

    with engine.begin() as conexion:
        for _, fila in df_bronce.iterrows():
            dni     = fila['dni_limpio']
            nombre  = fila['nombre_limpio'] or 'Sin Nombre'
            rol     = fila['rol']
            sexo    = fila.get('sexo_limpio')
            planilla= fila.get('planilla_raw')

            if dni in dnis_existentes:
                # SCD1 — actualizar si el nombre cambió
                existente = df_existente[df_existente['DNI'] == dni].iloc[0]
                if existente['Nombre_Completo'] != nombre:
                    conexion.execute(text("""
                        UPDATE Silver.Dim_Personal
                        SET Nombre_Completo = :nombre,
                            Rol             = :rol
                        WHERE DNI = :dni
                    """), {'nombre': nombre, 'rol': rol, 'dni': dni})
                    resumen['actualizados'] += 1
                else:
                    resumen['sin_cambios'] += 1
            else:
                # Nuevo registro
                conexion.execute(text("""
                    INSERT INTO Silver.Dim_Personal (
                        DNI, Nombre_Completo, Rol, Sexo,
                        ID_Planilla, Pct_Asertividad, Dias_Ausentismo,
                        Fecha_Evento, Fecha_Sistema, Estado_DQ
                    ) VALUES (
                        :dni, :nombre, :rol, :sexo,
                        :planilla, NULL, 0,
                        SYSDATETIME(), SYSDATETIME(), 'OK'
                    )
                """), {
                    'dni':      dni,
                    'nombre':   nombre,
                    'rol':      rol,
                    'sexo':     sexo,
                    'planilla': planilla,
                })
                dnis_existentes.add(dni)
                resumen['insertados'] += 1

    return resumen
