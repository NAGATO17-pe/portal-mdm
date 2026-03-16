"""
rutas.py
========
Mapeo carpeta de entrada → tabla Bronce destino.
El ETL usa este diccionario para saber a qué tabla va cada archivo.
"""

from pathlib import Path


# Carpeta raíz donde el usuario deposita los Excel de campo
CARPETA_ENTRADA = Path(__file__).parent.parent / 'data' / 'entrada'

# Carpeta donde se archivan los Excel ya procesados
CARPETA_PROCESADOS = Path(__file__).parent.parent / 'data' / 'procesados'


# Mapeo carpeta → tabla Bronce
# Clave   : nombre exacto de la subcarpeta en data/entrada/
# Valor   : nombre completo de la tabla destino en SQL Server
RUTAS: dict[str, str] = {
    'conteo_fruta':              'Bronce.Conteo_Fruta',
    'peladas':                   'Bronce.Peladas',
    'ciclos_fenologicos':        'Bronce.Ciclos_Fenologicos',
    'evaluacion_pesos':          'Bronce.Evaluacion_Pesos',
    'induccion_floral':          'Bronce.Induccion_Floral',
    'evaluacion_vegetativa':     'Bronce.Evaluacion_Vegetativa',
    'tasa_crecimiento_brotes':   'Bronce.Tasa_Crecimiento_Brotes',
    'evaluacion_calidad_poda':   'Bronce.Evaluacion_Calidad_Poda',
    'fisiologia':                'Bronce.Fisiologia',
    'calibres':                  'Bronce.Calibres',
    'tareos':                    'Bronce.Consolidado_Tareos',
    'fiscalizacion':             'Bronce.Fiscalizacion',
    'seguimiento_errores':       'Bronce.Seguimiento_Errores',
    'maduracion':                'Bronce.Maduracion',
    'pintado_flores':            'Bronce.Pintado_Flores',
    'reporte_cosecha':           'Bronce.Reporte_Cosecha',
    'cierre_mapas_cosecha':      'Bronce.Cierre_Mapas_Cosecha',
    'reporte_clima':             'Bronce.Reporte_Clima',
    'variables_meteorologicas':  'Bronce.Variables_Meteorologicas',
    'data_sap':                  'Bronce.Data_SAP',
    'proyeccion_pesos':          'Bronce.Proyeccion_Pesos',
}


def obtener_archivo_mas_reciente(carpeta: Path) -> Path | None:
    """
    Retorna el archivo .xlsx más reciente de una carpeta.
    Si no hay archivos, retorna None.
    """
    archivos = sorted(
        carpeta.glob('*.xlsx'),
        key=lambda archivo: archivo.stat().st_mtime,
        reverse=True
    )
    return archivos[0] if archivos else None


def listar_carpetas_con_archivos() -> list[tuple[str, Path, str]]:
    """
    Recorre CARPETA_ENTRADA y retorna solo las carpetas
    que tienen al menos un archivo .xlsx pendiente.

    Retorna lista de tuplas: (nombre_carpeta, ruta_archivo, tabla_destino)
    """
    pendientes = []

    for nombre_carpeta, tabla_destino in RUTAS.items():
        ruta_carpeta = CARPETA_ENTRADA / nombre_carpeta
        if not ruta_carpeta.exists():
            continue
        archivo = obtener_archivo_mas_reciente(ruta_carpeta)
        if archivo:
            pendientes.append((nombre_carpeta, archivo, tabla_destino))

    return pendientes


def crear_estructura_carpetas() -> None:
    """
    Crea las carpetas de entrada y procesados si no existen.
    Se llama una sola vez al inicializar el proyecto.
    """
    for nombre_carpeta in RUTAS:
        (CARPETA_ENTRADA / nombre_carpeta).mkdir(parents=True, exist_ok=True)
        (CARPETA_PROCESADOS / nombre_carpeta).mkdir(parents=True, exist_ok=True)

    print(f'Estructura de carpetas creada en: {CARPETA_ENTRADA}')
