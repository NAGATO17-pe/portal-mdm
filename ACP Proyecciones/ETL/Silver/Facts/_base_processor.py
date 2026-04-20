"""
ETL/silver/facts/_base_processor.py
===================================
Base processor para componentes Silver (Facts).

Implementa des-duplicacion nativa en SQL Server via #Temp Tables
usando CREATE TABLE + executemany, que es el unico metodo confiable
para tablas temporales de sesion en pyodbc/SQLAlchemy.

-- POR QUE NO usamos pandas.to_sql() para #Temp Tables --
pandas.to_sql() crea la tabla en una conexion interna nueva que
SQL Server no puede ver desde la conexion activa del pipeline.
El warning "not found exactly as such" confirma este problema.
La solucion correcta es CREATE TABLE #Temp + executemany en la
misma conexion de la transaccion activa.

-- METODOS DE VALIDACION CON CACHE --
_validar_y_resolver_fecha, _validar_y_resolver_geografia,
_validar_y_resolver_variedad, _validar_y_resolver_personal
encapsulan la logica de lookup + cache que estaba duplicada en
cada fact. Las clases hijas los llaman directamente en _construir_payload.
"""

from __future__ import annotations

import logging
from typing import Any

from sqlalchemy import text
from sqlalchemy.engine import Engine

from utils.contexto_transaccional import ContextoTransaccionalETL

_log = logging.getLogger("ETL_Pipeline")


class BaseFactProcessor:
    LIMITE_RECHAZO = 5.0  # Umbral por defecto para alertas de calidad

    def __init__(self, engine: Engine, tabla_origen: str, tabla_destino: str, columna_id: str = None):
        self.engine = engine
        self.tabla_origen = tabla_origen
        self.tabla_destino = tabla_destino
        
        # Prioridad: 1. Parametro explicito, 2. Guess por nombre de tabla
        self.columna_id = columna_id or f"ID_{self.tabla_origen.split('.')[-1]}"
        
        self.columnas_clave_unica: list[str] = []
        self.ids_procesados: list[int] = []
        self.ids_rechazados: list[int] = []
        
        self.resumen = {
            'leidos': 0,
            'insertados': 0,
            'rechazados': 0,
            'cuarentena': [],
            'rechazados_ids': [] # Nueva estructura para reporte de calidad refinado
        }
        
        # Cache interna para lookups frecuentes
        self._cache_personal: dict[str, int | None] = {}
        self._cache_variedades: dict[str, int | None] = {}
        self._cache_geografia: dict[tuple, dict | None] = {}
        self._cache_tiempo: dict[Any, int | None] = {}

    def registrar_rechazo(
        self,
        id_origen: int,
        columna: str,
        valor: Any,
        motivo: str,
        tipo_regla: str = 'DQ',
        severidad: str = 'ALTO',
        fila: dict | None = None
    ) -> None:
        """
        Registra un rechazo en el resumen y lo prepara para la tabla MDM.Cuarentena.
        """
        # Si tenemos la fila, intentamos extraer el nombre del archivo para dar contexto
        contexto_archivo = ""
        if fila and fila.get('Nombre_Archivo'):
            contexto_archivo = f"[{fila.get('Nombre_Archivo')}] "

        self.resumen['rechazados'] = self.resumen.get('rechazados', 0) + 1
        self.resumen['rechazados_ids'].append({
            'id': id_origen,
            'es_duplicado_externo': False
        })
        self.resumen['cuarentena'].append({
            'columna': columna,
            'valor': str(valor) if valor is not None else 'NULL',
            'motivo': f"{contexto_archivo}{motivo}",
            'severidad': severidad,
            'tipo_regla': tipo_regla,
            'id_registro_origen': id_origen,
        })

    # ── validaciones con cache (reutilizables por todas las clases hijas) ────────

    def _validar_y_resolver_fecha(
        self,
        id_origen: int,
        valor_fecha: Any,
        dominio: str,
    ) -> Any | None:
        """
        Llama procesar_fecha() + obtener_id_tiempo() con cache interno.
        Registra rechazo automaticamente si la fecha es invalida.
        Retorna el objeto fecha (date/datetime) o None si falla.
        """
        from utils.fechas import procesar_fecha, obtener_id_tiempo

        cache_key = (str(valor_fecha), dominio)
        if cache_key in self._cache_tiempo:
            resultado = self._cache_tiempo[cache_key]
            if resultado is None:
                self.registrar_rechazo(
                    id_origen,
                    columna='Fecha_Raw',
                    valor=valor_fecha,
                    motivo='Fecha invalida o fuera de campana',
                )
            return resultado

        fecha, valida = procesar_fecha(valor_fecha, dominio=dominio)
        if not valida:
            self._cache_tiempo[cache_key] = None
            self.registrar_rechazo(
                id_origen,
                columna='Fecha_Raw',
                valor=valor_fecha,
                motivo='Fecha invalida o fuera de campana',
            )
            return None

        self._cache_tiempo[cache_key] = fecha
        return fecha

    def _validar_y_resolver_geografia(
        self,
        id_origen: int,
        fundo: Any,
        modulo_raw: Any,
        turno: Any = None,
        valvula: Any = None,
        cama: Any = None,
    ) -> dict | None:
        """
        Llama es_test_block() + normalizar_modulo() + resolver_geografia() con cache.
        Registra rechazo automaticamente si la geografia no se resuelve.
        Retorna el dict resultado_geo (con 'id_geografia') o None si falla.
        """
        from utils.texto import es_test_block, normalizar_modulo
        from mdm.lookup import resolver_geografia
        from silver.facts._helpers_fact_comunes import motivo_cuarentena_geografia

        modulo = None if es_test_block(modulo_raw) else normalizar_modulo(modulo_raw)
        cache_key = (str(fundo), str(modulo), str(turno), str(valvula), str(cama))

        if cache_key in self._cache_geografia:
            resultado = self._cache_geografia[cache_key]
            if resultado is None or not resultado.get('id_geografia'):
                self.registrar_rechazo(
                    id_origen,
                    columna='Modulo_Raw',
                    valor=f"Fundo={fundo} | Modulo={modulo_raw} | Turno={turno} | Valvula={valvula}",
                    motivo=motivo_cuarentena_geografia(resultado or {}),
                    tipo_regla='MDM',
                )
                return None
            return resultado

        resultado = resolver_geografia(fundo, None, modulo, self.engine, turno=turno, valvula=valvula, cama=cama)
        self._cache_geografia[cache_key] = resultado

        if not resultado or not resultado.get('id_geografia'):
            self.registrar_rechazo(
                id_origen,
                columna='Modulo_Raw',
                valor=f"Fundo={fundo} | Modulo={modulo_raw} | Turno={turno} | Valvula={valvula}",
                motivo=motivo_cuarentena_geografia(resultado or {}),
                tipo_regla='MDM',
            )
            return None

        return resultado

    def _validar_y_resolver_variedad(
        self,
        id_origen: int,
        variedad_canonica: Any,
        variedad_raw: Any = None,
    ) -> int | None:
        """
        Llama obtener_id_variedad() con cache interno.
        Registra rechazo automaticamente si no hay match en Dim_Variedad.
        Retorna ID_Variedad (int) o None si falla.
        """
        from mdm.lookup import obtener_id_variedad

        cache_key = str(variedad_canonica)
        if cache_key in self._cache_variedades:
            id_var = self._cache_variedades[cache_key]
            if id_var is None:
                self.registrar_rechazo(
                    id_origen,
                    columna='Variedad_Raw',
                    valor=variedad_raw if variedad_raw is not None else variedad_canonica,
                    motivo='Variedad sin match en Dim_Variedad',
                    tipo_regla='MDM',
                )
            return id_var

        id_var = obtener_id_variedad(variedad_canonica, self.engine)
        self._cache_variedades[cache_key] = id_var

        if not id_var:
            self.registrar_rechazo(
                id_origen,
                columna='Variedad_Raw',
                valor=variedad_raw if variedad_raw is not None else variedad_canonica,
                motivo='Variedad sin match en Dim_Variedad',
                tipo_regla='MDM',
            )
            return None

        return id_var

    def _validar_y_resolver_personal(
        self,
        valor_dni: Any,
    ) -> int | None:
        """
        Llama procesar_dni() + obtener_id_personal() con cache interno.
        NO registra rechazo: personal ausente es aceptable (retorna None/-1).
        Retorna ID_Personal (int) o None si el DNI no es resolvible.
        """
        from utils.dni import procesar_dni
        from mdm.lookup import obtener_id_personal

        cache_key = str(valor_dni)
        if cache_key in self._cache_personal:
            return self._cache_personal[cache_key]

        dni, _ = procesar_dni(valor_dni)
        id_personal = obtener_id_personal(dni, self.engine)
        self._cache_personal[cache_key] = id_personal
        return id_personal

    # ── helpers internos ───────────────────────────────────────────────────────

    def _tipo_sql_para_valor(self, valor: Any) -> str:
        """Infiere el tipo SQL Server para la columna del batch."""
        if isinstance(valor, bool):
            return "BIT"
        if isinstance(valor, int):
            return "BIGINT"
        if isinstance(valor, float):
            return "FLOAT"
        # fecha/datetime
        try:
            import datetime
            if isinstance(valor, (datetime.date, datetime.datetime)):
                return "DATETIME2"
        except ImportError:
            pass
        return "NVARCHAR(MAX)"

    def _crear_tabla_temp_en_sesion(self, conexion, nombre_temp: str, columnas_con_tipos: list[tuple[str, str]]) -> None:
        """
        Crea una #Temp table en la MISMA sesion/conexion activa del pipeline.
        Esta es la unica forma correcta de usar tablas temporales de SQL Server
        con SQLAlchemy+pyodbc: si usas pandas.to_sql() crea la tabla en una
        conexion interna separada que SQL Server NO comparte con la transaccion.
        """
        defs = ", ".join([f"[{col}] {tipo}" for col, tipo in columnas_con_tipos])
        conexion.execute(text(f"IF OBJECT_ID('tempdb..{nombre_temp}') IS NOT NULL DROP TABLE {nombre_temp}"))
        conexion.execute(text(f"CREATE TABLE {nombre_temp} ({defs})"))

    def _insertar_en_temp(self, conexion, nombre_temp: str, lista_dicts: list[dict], columnas: list[str]) -> None:
        """Inserta filas en la tabla temporal via executemany (eficiente para lotes grandes)."""
        placeholders = ", ".join(["?" for _ in columnas])
        cols_quoted = ", ".join([f"[{c}]" for c in columnas])
        sql = f"INSERT INTO {nombre_temp} ({cols_quoted}) VALUES ({placeholders})"

        raw_conn = conexion.connection
        cursor = raw_conn.cursor()
        cursor.fast_executemany = True
        datos = [tuple(row.get(c) for c in columnas) for row in lista_dicts]
        cursor.executemany(sql, datos)
        cursor.close()

    def _limpiar_duplicados_internos(self, lista_dicts: list[dict]) -> list[dict]:
        """
        Detecta y rechaza registros que están duplicados dentro del mismo batch (Excel).
        Retorna la lista de diccionarios limpios (únicos).
        """
        if not self.columnas_clave_unica:
            return lista_dicts

        vistos = set()
        lista_limpia = []

        for row in lista_dicts:
            # Construimos la tupla clave para este row
            clave = tuple(row.get(c) for c in self.columnas_clave_unica)
            
            # Verificamos si ya esta clave ya fue procesada en este batch
            if clave in vistos:
                id_origen = row.get("id_origen_rastreo")
                if id_origen is not None:
                    # Construimos un string resumen de los valores que fallan para que el usuario sepa qué registro es
                    valor_resumen = " | ".join([str(v) for v in clave])
                    
                    self.registrar_rechazo(
                        id_origen=id_origen,
                        columna=",".join(self.columnas_clave_unica),
                        valor=valor_resumen,
                        motivo=f"Registro duplicado dentro del mismo archivo para {self.tabla_destino}",
                        tipo_regla='DUPLICADO_INTERNO',
                        severidad='MEDIO',
                        fila=row
                    )
                    
                    # Removemos de procesados si estaba allí
                    if id_origen in self.ids_procesados:
                        self.ids_procesados.remove(id_origen)
            else:
                vistos.add(clave)
                lista_limpia.append(row)

        return lista_limpia

    def pre_limpiar_duplicados_batch(self, df: pd.DataFrame, columnas_clave_negocio: list[str]) -> pd.DataFrame:
        """
        Deduplica un DataFrame de Bronce antes de procesarlo, para evitar trabajo en vano.
        Útil para Facts con mucho volumen y duplicados técnicos.
        """
        if df.empty or not columnas_clave_negocio:
            return df
            
        antes = len(df)
        # Aseguramos que las columnas existen en el DF
        cols_finales = [c for c in columnas_clave_negocio if c in df.columns]
        if not cols_finales:
            return df

        # Mantenemos el primero de cada grupo (asumiendo orden cronológico en Bronce si aplica)
        df_limpio = df.drop_duplicates(subset=cols_finales, keep='first')
        despues = len(df_limpio)
        
        if antes != despues:
            _log.info(f"Deduplicación temprana: {antes - despues} filas redundantes filtradas antes de procesar.")
            
        return df_limpio

    # ── metodo principal ───────────────────────────────────────────────────────

    def _ejecutar_insercion_masiva_segura(
        self,
        contexto: ContextoTransaccionalETL,
        lista_dicts: list[dict],
        nombre_temp: str,
    ) -> None:
        """
        Deduplicacion y carga masiva en 6 pasos sin consumir RAM del servidor.

        1. Deduplicación Interna: Limpia duplicados en el mismo batch.
        2. Inferir tipos SQL para cada columna del batch.
        3. Crear #Temp table en la MISMA transaccion activa.
        4. Insertar todo el batch via fast_executemany.
        5. Detectar duplicados con INNER JOIN contra la tabla destino.
        6. Insertar solo los NO-duplicados con WHERE NOT EXISTS.
        """
        if not lista_dicts:
            return

        # 1. Deduplicación interna en memoria (para evitar IntegrityError en el INSERT final)
        lista_dicts_limpia = self._limpiar_duplicados_internos(lista_dicts)
        
        if not lista_dicts_limpia:
             return

        conexion = contexto._conexion_activa()
        todas_cols = list(lista_dicts_limpia[0].keys())

        # 2. Inferir tipos SQL para cada columna
        primera = lista_dicts_limpia[0]
        cols_con_tipos: list[tuple[str, str]] = []
        for col in todas_cols:
            val = primera.get(col)
            cols_con_tipos.append((col, self._tipo_sql_para_valor(val)))

        # 3. Crear #Temp en la sesion activa
        self._crear_tabla_temp_en_sesion(conexion, nombre_temp, cols_con_tipos)

        # 4. Insertar batch via fast_executemany
        self._insertar_en_temp(conexion, nombre_temp, lista_dicts_limpia, todas_cols)

        # 5. Detectar duplicados (Filtrando columnas que existen en el destino para evitar error 42S22)
        columnas_fisicas_key = [c for c in self.columnas_clave_unica if c in todas_cols and c != 'id_origen_rastreo']
        if not columnas_fisicas_key:
            raise ValueError(
                f"[{self.tabla_destino}] columnas_clave_unica no tiene interseccion con el payload. "
                f"Clave definida: {self.columnas_clave_unica} | Columnas en payload: {todas_cols}"
            )
        clausula_on = " AND ".join([f"tmp.[{c}] = dest.[{c}]" for c in columnas_fisicas_key])
        
        sql_duplicados = text(f"""
            SELECT tmp.[id_origen_rastreo]
            FROM {nombre_temp} tmp
            INNER JOIN {self.tabla_destino} dest ON {clausula_on}
        """)
        duplicados = conexion.execute(sql_duplicados).fetchall()
        ids_duplicados = {int(d[0]) for d in duplicados if d[0] is not None}

        for id_dup in ids_duplicados:
            # Marcamos como duplicado externo para que el reporte de CALIDAD lo ignore
            if 'rechazados_ids' not in self.resumen:
                self.resumen['rechazados_ids'] = []
                
            self.resumen['rechazados_ids'].append({
                'id': id_dup,
                'es_duplicado_externo': True
            })
            if id_dup in self.ids_procesados:
                self.ids_procesados.remove(id_dup)

        # 6. INSERT solo los nuevos via WHERE NOT EXISTS
        # Filtramos 'Punto_Virtual' y otras columnas no fisicas del insert final
        columnas_dest = [c for c in todas_cols if c != 'id_origen_rastreo' and not c.endswith('_Virtual')]
        col_select = ", ".join([f"tmp.[{c}]" for c in columnas_dest])
        col_insert = ", ".join([f"[{c}]" for c in columnas_dest])

        sql_insert = text(f"""
            INSERT INTO {self.tabla_destino} ({col_insert})
            SELECT {col_select}
            FROM {nombre_temp} tmp
            WHERE NOT EXISTS (
                SELECT 1 FROM {self.tabla_destino} dest
                WHERE {clausula_on}
            )
        """)
        resultado = conexion.execute(sql_insert)
        self.resumen['insertados'] += resultado.rowcount

        # 7. Limpieza
        conexion.execute(text(f"IF OBJECT_ID('tempdb..{nombre_temp}') IS NOT NULL DROP TABLE {nombre_temp}"))

    # ── finalizacion ───────────────────────────────────────────────────────────

    def finalizar_proceso(self, contexto: ContextoTransaccionalETL) -> dict:
        """Marca estados de carga, reporta cuarentena y evalua el Circuit Breaker."""
        
        if self.ids_procesados:
            contexto.marcar_estado_carga(self.tabla_origen, self.columna_id, self.ids_procesados)
        if self.ids_rechazados:
            contexto.marcar_estado_carga(self.tabla_origen, self.columna_id, self.ids_rechazados, estado='RECHAZADO')
        if self.resumen.get('cuarentena'):
            contexto.enviar_cuarentena(self.tabla_origen, self.resumen['cuarentena'])

        # ── Resumen de Continuidad ─────────────────────────────────────────────
        
        # 3. Calcular rechazo real (ignoring already existing technical duplicates)
        # Solo contamos como rechazo de CALIDAD lo que no es un duplicado externo
        lista_rechazos = self.resumen.get('rechazados_ids', [])
        rechazos_reales = [r for r in lista_rechazos if not r.get('es_duplicado_externo', False)]
        unique_ids_rechazados = len(set(r['id'] for r in rechazos_reales))
        
        total_leidos = self.resumen.get('leidos', 0)
        porcentaje_rechazo = (unique_ids_rechazados / total_leidos * 100) if total_leidos > 0 else 0
        
        # 4. Reporte final
        _log.info(f"-> {total_leidos} leidos | {self.resumen.get('insertados', 0)} insertados | {unique_ids_rechazados} rechazados reales | {int(porcentaje_rechazo)}% rechazo real")

        bloqueo = False
        if porcentaje_rechazo > self.LIMITE_RECHAZO:
            bloqueo = True
            _log.warning(f"AVISO: {porcentaje_rechazo:.1f}% de rechazo real ({unique_ids_rechazados}/{total_leidos} filas afectadas). Limite de calidad ({self.LIMITE_RECHAZO}%) superado, pero se continua por solicitud.")

        return {
            'Tabla_Destino': self.tabla_destino,
            'Filas_Leidas_Bronce': total_leidos,
            'Filas_Insertadas': self.resumen.get('insertados', 0),
            'Nuevos_Casos_Cuarentena': unique_ids_rechazados,
            'Bloqueo_Integridad': bloqueo,
            'Dependencias_Incumplidas': [],
        }
