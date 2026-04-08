# Informe de Avance de Base de Datos y Plataforma DWH - Version Contrastada

Fecha de corte: `2026-04-08`  
Ruta base: `D:\Proyecto2026\ACP_DWH\ACP Proyecciones`  
Servidor SQL de referencia documental: `LCP-PAG-PRACTIC`  
Base SQL de referencia documental: `ACP_DataWarehose_Proyecciones`

## 1. Proposito

Este informe actualiza y corrige el avance consolidado del proyecto contrastandolo no solo con baseline y codigo, sino tambien con la lectura manual de base de datos, dominio, backlog, linaje y mapeo de procesos disponible en `ETL/Avance`.

La intencion de esta version es dejar una lectura mas fiel del estado real del proyecto:

- sin sobredeclarar cierres;
- sin perder de vista el avance real alcanzado;
- distinguiendo mejor madurez de arquitectura, madurez de datos y madurez funcional.

## 2. Fuentes contrastadas

Este informe se apoya en cuatro tipos de evidencia:

1. Baseline y cierres oficiales:
   - `BASELINE_OPERATIVO_ETL_20260406_FINAL.md`
   - `CIERRE_BASELINE_CONTROL_PLANE_ETL_20260407.md`
   - `CIERRE_OPERATIVO_ACTIVOS_FASE21_20260406.md`
2. Documentacion operativa:
   - `ETL/README_OPERATIVO_PIPELINE.md`
   - `backend/RUNBOOK_CONTROL_PLANE_ETL.md`
3. Codigo real de:
   - `ETL/`
   - `backend/`
   - `acp_mdm_portal/`
4. Lectura manual de base de datos, dominio y backlog:
   - `INFORME_EJECUTIVO_BD_Y_DOMINIO_20260406.md`
   - `INFORME_EJECUTIVO_MAPEO_BACKLOG_LINAJE_20260406.md`
   - `INFORME_GENERAL_PROYECTO_PRELIMINAR_20260406.md`

Limitacion metodologica:

- en esta sesion no se pudo abrir conexion SQL live contra `LCP-PAG-PRACTIC`;
- por tanto, el contraste se hace contra analisis manual previo con evidencia SQL documentada, no contra una consulta en vivo ejecutada hoy.

## 3. Que corrige o matiza esta version contrastada

La version anterior del informe era tecnicamente util, pero el contraste manual obliga a ajustar cuatro matices importantes.

### 3.1 La base no debe describirse como madura de forma uniforme

Correccion:

No es correcto decir simplemente que la base esta “madura y operativa” en bloque. La lectura manual muestra algo mas preciso:

- la arquitectura esta madura;
- los frentes activos principales estan operativos;
- pero la madurez por dominio y por fuente no es uniforme.

Lectura correcta:

- `Dim_Geografia`, `Conteo`, `Tasa`, `Clima` y el control-plane estan en un estado mucho mas maduro;
- `Dim_Personal`, `Tareo` y ciertos backlog MDM muestran que la plataforma todavia convive con zonas de madurez desigual.

### 3.2 El backlog original era sistemico, no una suma de bugs

La lectura manual del backlog y del mapeo de procesos deja claro que el proyecto arranco desde una operacion muy dependiente de:

- manualidad;
- criterios no estandarizados;
- llaves inestables;
- dobles verdades entre fuentes;
- poca trazabilidad de corridas;
- alta dependencia de memoria individual.

Por tanto, el avance actual debe leerse no solo como “mejoras ETL”, sino como una reorganizacion del sistema de datos en su conjunto.

### 3.3 El valor del proyecto no esta solo en el ETL

El contraste manual confirma que el progreso real viene de la combinacion de varias piezas:

- DWH por capas;
- ETL con residual explicable;
- MDM con cuarentena y homologacion;
- control-plane;
- backlog formalizado;
- linaje y mapeo de procesos;
- RACI mas visible.

Esto es importante porque cambia la narrativa. El proyecto ya no es solo “un ETL que carga Excel”. Hoy es una plataforma de datos en consolidacion.

### 3.4 Gold debe leerse con mas precision

El contraste con `CIERRE_OPERATIVO_ACTIVOS_FASE21_20260406.md` obliga a matizar la lectura de Gold:

- Gold si forma parte del flujo operativo;
- pero no todos los marts se poblaron con datos en las corridas parciales o de dominios activos;
- por ejemplo, en la recarga limpia de dominios activos hubo marts en `0` y el valor real relevante estuvo concentrado en `Mart_Fenologia`.

Lectura correcta:

- Gold esta operativo como capa y como mecanismo de publicacion;
- pero su utilidad efectiva depende del alcance de la corrida, de los dominios recargados y del estado de las facts que alimentan cada mart.

## 4. Lectura ejecutiva contrastada

La impresion general del proyecto sigue siendo positiva, pero la formulacion correcta es mas fina:

ACP Proyecciones ya no se ve como una coleccion de archivos, reglas sueltas y correcciones manuales. Se ve como una plataforma de datos que ya tiene capas, trazabilidad, gobierno operativo y una ruta clara de mejora. Sin embargo, esa plataforma todavia no tiene una madurez homogenea en todas sus fuentes y dominios.

Dicho de forma simple:

- el proyecto ya construyo estructura;
- ya ordeno gran parte del caos original;
- ya puede explicarse y medirse mucho mejor;
- pero aun no esta igualmente consolidado en todos los frentes.

## 5. Recorrido del proyecto, leido con el contraste manual

### 5.1 El avance principal no fue cargar mas datos, sino volver visible el sistema

La lectura manual de base de datos y backlog coincide con el codigo y el baseline en un punto central: el mayor avance fue volver visible lo que antes estaba mezclado.

Eso se nota en:

- capas bien separadas;
- cuarentena con causas visibles;
- residual medible;
- reglas de geografia mas gobernadas;
- posibilidad de decir que esta cerrado, estable o pendiente sin mezclar categorias.

Este cambio es estructural porque reemplaza memoria tacita por evidencia.

### 5.2 La base de datos ya transmite orden, pero no cierre absoluto

La lectura manual de dominio dice algo correcto: hoy la base transmite una sensacion de plataforma mas explicable y util. Eso se sostiene. Pero tambien deja una advertencia sana: esa sensacion positiva no significa que todo este listo para explotacion sin reservas.

La lectura ejecutiva correcta es:

- la base ya es una fuente mucho mas seria de trabajo;
- pero algunas dimensiones, fuentes y decisiones funcionales siguen abiertas y deben reflejarse en cualquier informe de estado.

### 5.3 El backlog mejoro porque ahora ya se puede priorizar

El contraste con el informe de backlog y linaje muestra que el proyecto avanzo sobre todo porque ya convierte problemas difusos en frentes priorizables.

Ejemplos:

- `Test Block` deja de ser ruido y pasa a ser regla;
- `VIVERO` deja de venderse como bug general y pasa a ser deuda funcional/MDM;
- `Conteo` y `Tasa` ya no se leen como “malos o buenos”, sino como dominios estables con residual explicado;
- la cuarentena deja de ser un agujero negro y pasa a ser backlog operable.

## 6. Estado actual por capa, ajustado con lectura manual

### 6.1 Bronce

Estado: `ESTABLE`

Bronce ya no es un simple deposito raw. La lectura manual del proyecto y el codigo coinciden en que hoy ya actua como barrera de calidad estructural.

Valor real de esta capa:

- evita contaminar Silver con layouts erroneos;
- hace visible el problema de origen mas temprano;
- reduce manualidad correctiva aguas abajo.

### 6.2 Silver

Estado: `OPERATIVA, PERO CON COBERTURA DESIGUAL POR DOMINIO`

Este es uno de los puntos que la version contrastada debe remarcar mas.

Silver esta bien planteada y tiene dominios ya operables, pero no debe describirse como completamente homogénea. Hay una diferencia clara entre:

- dominios ya estabilizados;
- dominios con residual controlado;
- dominios limitados por fuente o por decision funcional pendiente.

### 6.3 Gold

Estado: `OPERATIVA COMO CAPA, NO UNIFORMEMENTE POBLADA EN TODAS LAS CORRIDAS`

El contraste con el cierre operativo de activos obliga a evitar una lectura simplista. Gold existe, refresca y esta integrada al pipeline, pero no todas las corridas llenan todos los marts, y eso depende del alcance real del reproceso o de los dominios incluidos.

Esto no es un defecto; es una condicion operativa que debe declararse bien.

### 6.4 MDM

Estado: `CRITICO PARA EL CIERRE TOTAL`

La lectura manual y tecnica coinciden plenamente aqui:

- MDM ya no es accesorio;
- MDM es una pieza estructural del sistema;
- gran parte del residual actual depende de decisiones MDM o de backlog maestro, no de parser.

### 6.5 Control

Estado: `MUY BUEN NIVEL DE MADUREZ OPERATIVA`

La lectura manual del problema original resalta cuanto pesaba la falta de trazabilidad y de control de corrida. El estado actual del control-plane, comparado con ese punto de partida, representa uno de los mayores avances reales del proyecto.

## 7. Estado actual por dominios, con lenguaje mas preciso

### Dominios mas consolidados

- `Dim_Geografia`
- `Fact_Conteo_Fenologico`
- `Fact_Tasa_Crecimiento_Brotes` como pipeline y residual explicado
- `Fact_Telemetria_Clima`
- `Fact_Ciclo_Poda`

### Dominios funcionales con residual controlado

- `Fact_Evaluacion_Pesos`
- `Fact_Evaluacion_Vegetativa`
- `Fact_Maduracion`
- `Fact_Fisiologia`
- `Fact_Induccion_Floral`

### Dominios todavia condicionados por fuente o backlog

- `Dim_Personal`
- `Fact_Tareo`
- backlog MDM asociado a variedades y geografia especial

### Lectura contrastada del residual

El residual ya no es un indicador de caos general. Hoy es un indicador de madurez diferencial:

- algunas partes del sistema ya se pueden operar con bastante confianza;
- otras ya estan explicadas, pero aun no cerradas;
- otras siguen dependiendo de que el negocio o la fuente mejoren.

## 8. Valor agregado del linaje, scorecard y RACI

El contraste manual agrega una dimension que no conviene perder en el informe final: el proyecto no solo mejoro por codigo, tambien mejoro porque ya tiene una forma mas seria de explicarse y gobernarse.

El linaje aporta:

- claridad de transformaciones;
- visibilidad de dependencias;
- mejor tiempo de induccion para nuevos integrantes;
- capacidad de justificar cambios y residuales.

El scorecard aporta:

- una lectura realista de que la calidad de fuente no era uniforme;
- evidencia de por que algunos frentes avanzan mas rapido que otros.

El RACI aporta:

- responsabilidades visibles;
- mejor distincion entre deuda tecnica, deuda funcional y decisiones de negocio.

Lectura correcta:

Estos documentos no son decorativos. Son parte del avance del proyecto porque bajan ambiguedad y hacen gobernable lo que antes dependia de criterio disperso.

## 9. Evolucion SQL frente al DDL base

El contraste contra `DDL_Geographic_Phenology_v2.sql` agrega una lectura muy importante: el modelo SQL operativo actual ya no coincide exactamente con el modelo original de arranque. El DDL v2 fue la base fundacional correcta, pero el sistema real evoluciono bastante por encima de ese punto.

### 9.1 Lo que el DDL v2 si definio correctamente

`DDL_Geographic_Phenology_v2.sql` deja claramente establecido el esqueleto inicial del proyecto:

- 6 esquemas base:
  - `Bronce`
  - `Silver`
  - `Gold`
  - `MDM`
  - `Config`
  - `Auditoria`
- estructura medallion completa;
- 63 tablas iniciales;
- dimensiones seed;
- marts Gold;
- catalogos maestros;
- reglas base de validacion.

En ese punto el DDL v2 cumple bien su rol: definir el sistema inicial y dejar una base consistente para empezar la operacion.

### 9.2 Lo que el SQL posterior demuestra que cambio de verdad

El problema no es que el DDL v2 estuviera mal. El problema es que ya no alcanza para describir el sistema actual.

Los scripts posteriores muestran tres grandes lineas de evolucion.

#### A. El modelo de dominio Silver cambio

El DDL v2 original todavia reflejaba un modelo anterior en dos puntos clave:

- `Silver.Fact_Conteo_Fenologico` aun tenia `ID_Cinta`;
- `Silver.Fact_Telemetria_Clima` aun dependia de `ID_Geografia`.

Los scripts posteriores muestran que ese modelo ya fue superado:

- [fase18_fact_maduracion_y_cinta.sql](D:\Proyecto2026\ACP_DWH\ACP Proyecciones\ETL\fase18_fact_maduracion_y_cinta.sql)
  - elimina `ID_Cinta` de `Fact_Conteo_Fenologico`;
  - crea `Silver.Fact_Maduracion`;
  - agrega FKs, `CHECK` e indices para el nuevo fact.
- [fase19_ajuste_fact_clima_sector_climatico.sql](D:\Proyecto2026\ACP_DWH\ACP Proyecciones\ETL\fase19_ajuste_fact_clima_sector_climatico.sql)
  - reconstruye `Silver.Fact_Telemetria_Clima`;
  - cambia el grano a `ID_Tiempo + Sector_Climatico + Fecha_Evento`;
  - reconstruye `Gold.Mart_Clima` bajo esa nueva logica.
- [fase20_backfill_dim_tiempo_historico.sql](D:\Proyecto2026\ACP_DWH\ACP Proyecciones\ETL\fase20_backfill_dim_tiempo_historico.sql)
  - amplía `Dim_Tiempo` para historico operativo.

Lectura correcta:

El SQL confirma que el modelo Silver no solo crecio; tambien cambio semanticamente. Eso fortalece la conclusion de que el proyecto no se limito a “parchar ETL”, sino que fue afinando el modelo de datos real.

#### B. La geografia paso de catalogo estatico a regla gobernada

El DDL v2 si incluia catalogos MDM, pero el SQL evolutivo muestra que despues se agrego una capa de reglas mas rica sobre `Modulo_Raw`.

Evidencia visible:

- [fase17_reglas_modulo_raw_11_13_14.sql](D:\Proyecto2026\ACP_DWH\ACP Proyecciones\ETL\fase17_reglas_modulo_raw_11_13_14.sql)
  - hace `MERGE` sobre `MDM.Regla_Modulo_Raw`;
  - formaliza `11.1`, `11.2`, `13.1`, `13.2`, `14.1`, `14.2`.
- [fase21_regla_test_block_20260406.sql](D:\Proyecto2026\ACP_DWH\ACP Proyecciones\ETL\Avance\fase21_regla_test_block_20260406.sql)
  - agrega alias operativo `Test Block` como caso resoluble.

Lectura correcta:

La resolucion geografica ya no vive solo en estructura maestra. Vive en una combinacion de catalogo, regla y procedimiento.

#### C. La BD ya no es solo DWH: tambien es plataforma operativa

Este es el cambio estructural mas fuerte que aparece al comparar SQL.

El DDL v2 solo definia la base DWH. Los scripts posteriores muestran que la base actual ya aloja tambien piezas de aplicacion y control operacional:

- [crear_tablas_control.sql](D:\Proyecto2026\ACP_DWH\ACP Proyecciones\ETL\crear_tablas_control.sql)
  - crea esquema `Control`;
  - crea `Control.Corrida`;
  - crea `Control.Corrida_Evento`;
  - crea `Control.Corrida_Paso`;
  - crea `Control.Bloqueo_Ejecucion`;
  - crea `Control.Comando_Ejecucion`.
- [fase21_endurecimiento_control_plane.sql](D:\Proyecto2026\ACP_DWH\ACP Proyecciones\ETL\fase21_endurecimiento_control_plane.sql)
  - agrega indices operativos;
  - crea vistas:
    - `Control.vw_Corridas_Activas`
    - `Control.vw_Cola_Comandos`
    - `Control.vw_Ultima_Corrida_Por_Tabla`
- [fase22_retencion_control_plane.sql](D:\Proyecto2026\ACP_DWH\ACP Proyecciones\ETL\fase22_retencion_control_plane.sql)
  - crea `Control.sp_Purgar_Historial_Control`.
- [crear_tablas_seguridad.sql](D:\Proyecto2026\ACP_DWH\ACP Proyecciones\ETL\crear_tablas_seguridad.sql)
  - crea esquema `Seguridad`;
  - crea `Seguridad.Usuarios`;
  - crea `Auditoria.Log_Acceso`.
- [crear_auditoria_cambios_portal.sql](D:\Proyecto2026\ACP_DWH\ACP Proyecciones\ETL\crear_auditoria_cambios_portal.sql)
  - crea `Auditoria.Cambios_Portal`.

Conclusión estructural:

El sistema visible por SQL evoluciono desde un DWH de 6 esquemas hacia una plataforma con por lo menos 8 esquemas operativos al agregarse `Control` y `Seguridad`.

### 9.3 Delta estructural visible en el repo

Tomando solo objetos cuya creacion si se puede ver en los scripts actuales, el modelo ya crecio por encima del DDL base en al menos:

- `+2` esquemas:
  - `Control`
  - `Seguridad`
- `+9` tablas visibles en SQL evolutivo:
  - `Silver.Fact_Maduracion`
  - `Control.Corrida`
  - `Control.Corrida_Evento`
  - `Control.Corrida_Paso`
  - `Control.Bloqueo_Ejecucion`
  - `Control.Comando_Ejecucion`
  - `Seguridad.Usuarios`
  - `Auditoria.Log_Acceso`
  - `Auditoria.Cambios_Portal`
- `+3` vistas operativas:
  - `Control.vw_Corridas_Activas`
  - `Control.vw_Cola_Comandos`
  - `Control.vw_Ultima_Corrida_Por_Tabla`
- `+1` procedimiento de retencion:
  - `Control.sp_Purgar_Historial_Control`

Esto significa que, aun sin contar objetos cuya creacion no se encontro en el repo, el modelo ya paso de 63 tablas originales a por lo menos 72 tablas visibles por linaje SQL.

### 9.4 Brechas de linaje SQL dentro del repo

El contraste tambien descubre un punto debil documental: el linaje SQL del repo no esta completo respecto a la base operativa real.

Objetos confirmados por baseline, README o scripts de consulta, pero cuya creacion no aparece visible en los `.sql` actuales del repo:

- `MDM.Regla_Modulo_Raw` como tabla base
- `Silver.Bridge_Geografia_Cama`
- `Silver.sp_Resolver_Geografia_Cama`
- `Silver.sp_Upsert_Cama_Desde_Bronce`
- `Silver.sp_Validar_Calidad_Camas`
- `Silver.Fact_Induccion_Floral`
- `Silver.Fact_Tasa_Crecimiento_Brotes`

Lectura correcta:

La base real parece ir por delante del linaje SQL visible en el repositorio.

Esto no invalida el avance del proyecto. Pero si implica una deuda de trazabilidad tecnica:

- el estado operativo real existe;
- sin embargo, no todo su camino evolutivo esta igualmente representado en scripts versionados y visibles.

### 9.5 Impacto de este contraste sobre el informe de avance

Gracias al contraste SQL, la lectura del proyecto mejora en tres sentidos:

1. Se confirma que el avance no fue solo documental ni solo Python.
   La base fisica realmente cambio y se endurecio.
2. Se confirma que el proyecto ya salio del perimetro de “solo DWH”.
   Hoy la BD tambien sostiene seguridad, auditoria de aplicacion y control-plane.
3. Se detecta una deuda real de gobierno del repositorio:
   la historia SQL visible no cubre por completo toda la plataforma que hoy ya existe.

## 10. Ajuste de dictamen respecto al informe anterior

Donde el informe anterior decia:

- “Base de datos DWH: madura y operativa”

La formulacion contrastada debe ser:

- “Base de datos DWH: madura en arquitectura y operativa en sus frentes principales, con madurez no uniforme por dominio y backlog funcional todavia relevante.”

Donde el informe anterior podia sonar a que Gold estaba genericamente lista, la lectura correcta es:

- “Gold esta operativa como capa de publicacion, pero su poblamiento y utilidad inmediata dependen del alcance real de la corrida y del estado de las facts alimentadoras.”

Donde el informe anterior podia leerse como un cierre tecnico amplio, esta version aclara:

- “el mayor avance del proyecto no es que todo este cerrado, sino que ahora lo abierto ya esta mucho mejor delimitado, explicado y priorizado.”

## 11. Dictamen final contrastado

Estado del proyecto al `2026-04-08`, contrastado con analisis manual de base de datos:

- Arquitectura DWH: `MADURA`
- ETL: `ESTABLE`
- Control-plane: `ESTABLE CON UNA BRECHA PUNTUAL`
- Datos por dominio: `MADUREZ DESIGUAL, PERO MUCHO MAS GOBERNABLE`
- MDM: `DETERMINANTE PARA EL CIERRE DEL BACKLOG`
- Portal actual: `FUNCIONAL COMO TRANSICION, NO CAPA FINAL`
- Gobierno documental: `MUCHO MAS FUERTE QUE AL INICIO`

Opinion tecnica final:

ACP Proyecciones ya supero la etapa donde el principal problema era desorden tecnico general. El proyecto ahora esta en una etapa mas exigente y mas sana: sostener baseline, cerrar backlog funcional, fortalecer fuentes debiles y consolidar la capa operativa para que la plataforma no solo funcione, sino que pueda escalar con menos dependencia de conocimiento tacito.

## 12. Recomendacion de uso

Usar esta version contrastada como informe maestro de avance hasta que se pueda ejecutar una nueva validacion SQL live. En ese momento conviene anexar una seccion adicional con:

- conteos reales por esquema;
- resumen actual de `Control.*`;
- estado actual de `MDM.Cuarentena`;
- fotografia live de Silver y Gold.

Hasta entonces, esta es la version mas fiel y mejor balanceada entre:

- baseline oficial;
- lectura de codigo;
- y analisis manual de base de datos ya documentado.
