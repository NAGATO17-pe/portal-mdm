"""
marts.py
========
Refresca todos los Marts Gold desde Silver.
Operacion: TRUNCATE + INSERT, siempre desde cero.
Power BI solo conecta a estos Marts.

FIX: Gold no se publica si alguna fact critica fallo en el pipeline.
     refrescar_todos_los_marts() recibe el resumen del ETL y aborta
     si hay errores en las facts bloqueantes.
"""

from sqlalchemy.engine import Engine
from sqlalchemy import text


MARTS = [
    'Gold.Mart_Cosecha',
    'Gold.Mart_Proyecciones',
    'Gold.Mart_Fenologia',
    'Gold.Mart_Clima',
    'Gold.Mart_Pesos_Calibres',
    'Gold.Mart_Administrativo',
]

# Facts cuya falla bloquea la publicacion de Gold
FACTS_BLOQUEANTES = {
    'Fact_Cosecha_SAP',
    'Fact_Conteo_Fenologico',
    'Fact_Evaluacion_Pesos',
    'Fact_Telemetria_Clima',
}


def _hay_fallas_criticas(resumen_etl: dict) -> list[str]:
    """
    Detecta facts bloqueantes que terminaron en ERROR.
    Retorna lista de nombres con error (vacia = todo OK).
    """
    fallas = []
    for nombre in FACTS_BLOQUEANTES:
        clave_error = f'{nombre} ERROR'
        if clave_error in resumen_etl:
            fallas.append(nombre)
    return fallas


def _truncar(conexion, mart: str) -> None:
    conexion.execute(text(f'TRUNCATE TABLE {mart}'))


def refrescar_mart_cosecha(conexion) -> int:
    conexion.execute(text("""
        INSERT INTO Gold.Mart_Cosecha (
            ID_Tiempo, ID_Geografia, ID_Variedad,
            Fundo, Variedad, Fecha_Evento,
            Kg_Brutos, Kg_Neto_MP,
            Kg_Proyectado, Condicion
        )
        SELECT
            cs.ID_Tiempo,
            cs.ID_Geografia,
            cs.ID_Variedad,
            g.Fundo,
            v.Nombre_Variedad,
            cs.Fecha_Evento,
            cs.Kg_Brutos,
            cs.Kg_Neto_MP,
            p.Kg_Proyectados,
            c.Sustrato
        FROM Silver.Fact_Cosecha_SAP cs
        JOIN Silver.Dim_Tiempo             t ON t.ID_Tiempo = cs.ID_Tiempo
        JOIN Silver.Dim_Geografia          g ON g.ID_Geografia = cs.ID_Geografia AND g.Es_Vigente = 1
        JOIN Silver.Dim_Variedad           v ON v.ID_Variedad = cs.ID_Variedad
        JOIN Silver.Dim_Condicion_Cultivo  c ON c.ID_Condicion = cs.ID_Condicion_Cultivo
        LEFT JOIN Silver.Fact_Proyecciones p
            ON  p.ID_Tiempo = cs.ID_Tiempo
            AND p.ID_Variedad = cs.ID_Variedad
            AND p.ID_Geografia = cs.ID_Geografia
            AND p.ID_Escenario = 4
    """))
    return _contar(conexion, 'Gold.Mart_Cosecha')


def refrescar_mart_proyecciones(conexion) -> int:
    conexion.execute(text("""
        INSERT INTO Gold.Mart_Proyecciones (
            ID_Tiempo, ID_Geografia, ID_Variedad, ID_Escenario,
            Fundo, Variedad, Fecha_Cutoff,
            Kg_Proyectados, MAPE, Version_Modelo,
            Flag_Override, Estado_Workflow
        )
        SELECT
            p.ID_Tiempo,
            p.ID_Geografia,
            p.ID_Variedad,
            p.ID_Escenario,
            g.Fundo,
            v.Nombre_Variedad,
            p.Fecha_Cutoff,
            p.Kg_Proyectados,
            p.MAPE,
            p.Version_Modelo,
            p.Flag_Override,
            w.Estado
        FROM Silver.Fact_Proyecciones p
        JOIN Silver.Dim_Tiempo                t ON t.ID_Tiempo = p.ID_Tiempo
        JOIN Silver.Dim_Geografia             g ON g.ID_Geografia = p.ID_Geografia AND g.Es_Vigente = 1
        JOIN Silver.Dim_Variedad              v ON v.ID_Variedad = p.ID_Variedad
        JOIN Silver.Dim_Escenario_Proyeccion  e ON e.ID_Escenario = p.ID_Escenario
        JOIN Silver.Dim_Estado_Workflow       w ON w.ID_Workflow = p.ID_Estado_Workflow
    """))
    return _contar(conexion, 'Gold.Mart_Proyecciones')


def refrescar_mart_fenologia(conexion) -> int:
    conexion.execute(text("""
        INSERT INTO Gold.Mart_Fenologia (
            ID_Tiempo, ID_Geografia, ID_Variedad,
            Fundo, Variedad, Semana_ISO,
            Estado_Fenologico, Orden_Estado,
            Cantidad_Organos
        )
        SELECT
            cf.ID_Tiempo,
            cf.ID_Geografia,
            cf.ID_Variedad,
            g.Fundo,
            v.Nombre_Variedad,
            t.Semana_ISO,
            ef.Nombre_Estado,
            ef.Orden_Estado,
            SUM(cf.Cantidad_Organos)
        FROM Silver.Fact_Conteo_Fenologico cf
        JOIN Silver.Dim_Tiempo            t  ON t.ID_Tiempo = cf.ID_Tiempo
        JOIN Silver.Dim_Geografia         g  ON g.ID_Geografia = cf.ID_Geografia AND g.Es_Vigente = 1
        JOIN Silver.Dim_Variedad          v  ON v.ID_Variedad = cf.ID_Variedad
        JOIN Silver.Dim_Estado_Fenologico ef ON ef.ID_Estado_Fenologico = cf.ID_Estado_Fenologico
        GROUP BY
            cf.ID_Tiempo, cf.ID_Geografia, cf.ID_Variedad,
            g.Fundo, v.Nombre_Variedad, t.Semana_ISO,
            ef.Nombre_Estado, ef.Orden_Estado
    """))
    return _contar(conexion, 'Gold.Mart_Fenologia')


def refrescar_mart_clima(conexion) -> int:
    conexion.execute(text("""
        INSERT INTO Gold.Mart_Clima (
            ID_Tiempo, Sector_Climatico,
            Semana_ISO,
            Temp_Max_Promedio, Temp_Min_Promedio,
            VPD_Promedio, Humedad_Promedio,
            Precipitacion_Total
        )
        SELECT
            cl.ID_Tiempo,
            cl.Sector_Climatico,
            t.Semana_ISO,
            AVG(cl.Temperatura_Max_C),
            AVG(cl.Temperatura_Min_C),
            AVG(cl.VPD),
            AVG(cl.Humedad_Relativa_Pct),
            SUM(cl.Precipitacion_mm)
        FROM Silver.Fact_Telemetria_Clima cl
        JOIN Silver.Dim_Tiempo t ON t.ID_Tiempo = cl.ID_Tiempo
        GROUP BY cl.ID_Tiempo, cl.Sector_Climatico, t.Semana_ISO
    """))
    return _contar(conexion, 'Gold.Mart_Clima')


def refrescar_mart_pesos_calibres(conexion) -> int:
    conexion.execute(text("""
        INSERT INTO Gold.Mart_Pesos_Calibres (
            ID_Tiempo, ID_Geografia, ID_Variedad,
            Fundo, Variedad, Semana_ISO,
            Peso_Promedio_Baya_g, Cant_Bayas_Muestra
        )
        SELECT
            ep.ID_Tiempo,
            ep.ID_Geografia,
            ep.ID_Variedad,
            g.Fundo,
            v.Nombre_Variedad,
            t.Semana_ISO,
            AVG(ep.Peso_Promedio_Baya_g),
            SUM(ep.Cantidad_Bayas_Muestra)
        FROM Silver.Fact_Evaluacion_Pesos ep
        JOIN Silver.Dim_Tiempo    t ON t.ID_Tiempo = ep.ID_Tiempo
        JOIN Silver.Dim_Geografia g ON g.ID_Geografia = ep.ID_Geografia AND g.Es_Vigente = 1
        JOIN Silver.Dim_Variedad  v ON v.ID_Variedad = ep.ID_Variedad
        GROUP BY
            ep.ID_Tiempo, ep.ID_Geografia, ep.ID_Variedad,
            g.Fundo, v.Nombre_Variedad, t.Semana_ISO
    """))
    return _contar(conexion, 'Gold.Mart_Pesos_Calibres')


def refrescar_mart_administrativo(conexion) -> int:
    conexion.execute(text("""
        INSERT INTO Gold.Mart_Administrativo (
            ID_Tiempo, ID_Personal, ID_Actividad,
            Supervisor, Semana_ISO,
            Horas_Trabajadas_Total, Registros_Observados_SAP
        )
        SELECT
            ta.ID_Tiempo,
            ta.ID_Personal,
            ta.ID_Actividad_Operativa,
            COALESCE(sp.Nombre_Completo, 'Sin Supervisor'),
            t.Semana_ISO,
            SUM(ta.Horas_Trabajadas),
            SUM(CAST(ta.Es_Observado_SAP AS INT))
        FROM Silver.Fact_Tareo ta
        JOIN Silver.Dim_Tiempo      t  ON t.ID_Tiempo = ta.ID_Tiempo
        LEFT JOIN Silver.Dim_Personal sp ON sp.ID_Personal = ta.ID_Personal_Supervisor
        GROUP BY
            ta.ID_Tiempo, ta.ID_Personal, ta.ID_Actividad_Operativa,
            sp.Nombre_Completo, t.Semana_ISO
    """))
    return _contar(conexion, 'Gold.Mart_Administrativo')


def _contar(conexion, mart: str) -> int:
    resultado = conexion.execute(text(f'SELECT COUNT(*) FROM {mart}'))
    return resultado.scalar()


FUNCIONES_MARTS = {
    'Gold.Mart_Cosecha': refrescar_mart_cosecha,
    'Gold.Mart_Proyecciones': refrescar_mart_proyecciones,
    'Gold.Mart_Fenologia': refrescar_mart_fenologia,
    'Gold.Mart_Clima': refrescar_mart_clima,
    'Gold.Mart_Pesos_Calibres': refrescar_mart_pesos_calibres,
    'Gold.Mart_Administrativo': refrescar_mart_administrativo,
}


def refrescar_marts_seleccionados(
    engine: Engine,
    marts: list[str] | tuple[str, ...],
    resumen_etl: dict | None = None,
) -> dict:
    """
    Refresca solo los marts solicitados.
    """
    marts_set = set(marts)
    marts_solicitados = [mart for mart in MARTS if mart in marts_set]
    if not marts_solicitados:
        return {}

    if resumen_etl is not None:
        fallas = _hay_fallas_criticas(resumen_etl)
        if fallas:
            msg = f'Gold bloqueado - facts con error: {fallas}'
            print(f'  [BLOCK] {msg}')
            return {'BLOQUEADO': msg}

    resumen = {}
    with engine.begin() as conexion:
        for mart in marts_solicitados:
            _truncar(conexion, mart)

        for mart in marts_solicitados:
            filas = FUNCIONES_MARTS[mart](conexion)
            resumen[mart] = filas
            print(f'  [OK] {mart}: {filas} filas')

    return resumen


def refrescar_todos_los_marts(
    engine: Engine,
    resumen_etl: dict | None = None,
) -> dict:
    """
    Refresca todos los Marts Gold en orden.
    TRUNCATE + INSERT, siempre desde cero.

    FIX: si resumen_etl contiene errores en facts bloqueantes,
    aborta sin tocar Gold y retorna dict con la causa.
    """
    if resumen_etl is not None:
        fallas = _hay_fallas_criticas(resumen_etl)
        if fallas:
            msg = f'Gold bloqueado - facts con error: {fallas}'
            print(f'  [BLOCK] {msg}')
            return {'BLOQUEADO': msg}

    resumen = {}

    with engine.begin() as conexion:
        for mart in MARTS:
            _truncar(conexion, mart)

        for mart, funcion in FUNCIONES_MARTS.items():
            filas = funcion(conexion)
            resumen[mart] = filas
            print(f'  [OK] {mart}: {filas} filas')

    return resumen
