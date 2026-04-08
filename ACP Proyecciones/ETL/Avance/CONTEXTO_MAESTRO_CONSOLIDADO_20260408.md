# CONTEXTO MAESTRO CONSOLIDADO - ACP PROYECCIONES

Fecha de consolidacion: 2026-04-08
Ruta base del proyecto: D:\Proyecto2026\ACP_DWH\ACP Proyecciones
Servidor SQL de referencia documental: LCP-PAG-PRACTIC
Base SQL de referencia documental: ACP_DataWarehose_Proyecciones

## 0. Proposito y alcance

Este documento consolida el contexto tecnico-operativo vigente del proyecto ACP Proyecciones usando dos fuentes de evidencia:

1. Documentacion operativa y de baseline en `ETL/Avance`, `ETL/README_OPERATIVO_PIPELINE.md`, `backend/README.backend.md` y `backend/RUNBOOK_CONTROL_PLANE_ETL.md`.
2. Codigo real de `ETL/`, `backend/` y `acp_mdm_portal/`.

Objetivo:
- dejar un punto de continuidad unico para futuras sesiones;
- separar lo historico de lo vigente;
- distinguir deuda tecnica, deuda de fuente y deuda funcional;
- evitar reabrir frentes ya cerrados por leer un `.md` viejo fuera de contexto.

Importante:
- este documento NO reemplaza la evidencia SQL real;
- cuando un `.md` y la corrida real se contradicen, prevalece la evidencia SQL;
- cuando la documentacion y el codigo se contradicen, se debe revisar si la documentacion quedo atras o si el codigo esta incompleto.

## 0.1 Metodologia usada para esta consolidacion

### Documentacion revisada

Se revisaron, entre otros:
- `backend/README.backend.md`
- `backend/RUNBOOK_CONTROL_PLANE_ETL.md`
- `ETL/README_OPERATIVO_PIPELINE.md`
- `ETL/DOCUMENTACION_ACTUALIZACION_MODULO_RAW_20260325.md`
- `ETL/Avance/ACTUALIZACION_OPERATIVA_CLIMA_20260331.md`
- `ETL/Avance/ACTUALIZACION_OPERATIVA_INDUCCION_TASA_20260401.md`
- `ETL/Avance/BASELINE_OPERATIVO_ETL_20260406_FINAL.md`
- `ETL/Avance/CIERRE_BASELINE_CONTROL_PLANE_ETL_20260407.md`
- `ETL/Avance/CIERRE_OPERATIVO_ACTIVOS_FASE21_20260406.md`
- `ETL/Avance/CIERRE_ESTABLE_ETL_20260330.md`
- `ETL/Avance/CHECKLIST_OPERATIVO_5_MINUTOS.md`
- `ETL/Avance/PARCHE_TEMPORAL_GEO_TASA_20260406.md`
- documentos de contexto, baseline, guia y prompts maestros en `ETL/Avance/`.

### Codigo revisado

Se reviso codigo real de estos puntos criticos:
- `ETL/pipeline.py`
- `ETL/utils/ejecucion.py`
- `ETL/bronce/cargador.py`
- `ETL/utils/fechas.py`
- `ETL/utils/texto.py`
- `ETL/mdm/lookup.py`
- `ETL/silver/dims/dim_geografia.py`
- `ETL/silver/facts/fact_tasa_crecimiento_brotes.py`
- `ETL/silver/facts/fact_telemetria_clima.py`
- `backend/main.py`
- `backend/api/rutas_etl.py`
- `backend/api/rutas_health.py`
- `backend/repositorios/repo_control.py`
- `backend/runner/runner.py`
- `backend/runner/ejecutor.py`
- `backend/nucleo/etl_argumentos.py`
- `backend/nucleo/auth.py`
- `backend/servicios/servicio_etl.py`
- `backend/tests/test_etl.py`
- `acp_mdm_portal/app.py`
- `acp_mdm_portal/paginas/*`
- `acp_mdm_portal/utils/auth.py`
- `acp_mdm_portal/utils/api_client.py`

### Limite de esta lectura

En esta consolidacion se analizo documentacion y codigo. No se ejecuto una corrida ETL real ni se corrieron pruebas automatizadas en esta pasada. Por tanto:
- las afirmaciones de runtime vienen de baseline documentado y del comportamiento observable del codigo;
- las afirmaciones de implementacion vienen del codigo fuente inspeccionado.

## 1. Regla maestra de lectura y jerarquia de verdad

Jerarquia recomendada para tomar decisiones:

1. Evidencia SQL y corrida real validada del dia.
2. Baseline final oficial:
   - `ETL/Avance/BASELINE_OPERATIVO_ETL_20260406_FINAL.md`
   - `ETL/Avance/CIERRE_BASELINE_CONTROL_PLANE_ETL_20260407.md`
3. Runbooks y README operativos vigentes:
   - `backend/RUNBOOK_CONTROL_PLANE_ETL.md`
   - `ETL/README_OPERATIVO_PIPELINE.md`
   - `backend/README.backend.md`
4. Codigo real.
5. Checkpoints historicos y prompts maestros en `ETL/Avance/`.

Lectura tecnica:
- esta jerarquia es correcta y madura;
- evita declarar cerrado algo solo porque un documento lo diga;
- obliga a separar narrativa historica de contrato operativo real.

## 2. Resumen ejecutivo actual

Estado general al cierre de esta consolidacion:
- el ETL ya no esta en falla sistemica;
- el backend ya no es un lanzador directo de subprocess; opera con control-plane persistente;
- el runner separado, el lock y la persistencia de eventos estan implementados en codigo;
- `Test Block` geograficamente esta cerrado para el frente validado;
- `VIVERO` sigue siendo deuda funcional/MDM, no bug general del pipeline;
- el frente de cama sigue existiendo, pero su bloqueo depende de si la corrida trae tablas con cama;
- clima ya no depende de `Dim_Geografia` y esta modelado por `Sector_Climatico`;
- `Dim_Personal` sigue siendo una deuda transversal seria;
- `Fact_Tareo` sigue bloqueado por fuente insuficiente;
- el portal Streamlit ya tiene codigo funcional, aunque el frente operativo vigente sigue siendo `backend + ETL + runner`.

Opinion tecnica corta:
- el proyecto ya dejo de ser una coleccion de scripts y paso a ser una plataforma de datos con gobierno tecnico reconocible;
- el principal trabajo pendiente ya no es parser ni infraestructura, sino cerrar correctamente deuda de fuente, catalogo y negocio.

## 3. Linea de tiempo consolidada por fechas y avances

## 3.1 2026-03-25 - Regla de modulo raw, submodulo y cama

### Avance documental
Se formaliza la capa de reglas para `Modulo_Raw` en MDM:
- `VI` como `Test Block`;
- `9.1` y `9.2` como conducciones validas;
- resolucion geografia usando `Modulo/SubModulo/Turno/Valvula`;
- inicio del endurecimiento del frente cama.

### Codigo que hoy confirma ese frente
- `ETL/silver/dims/dim_geografia.py`
  - sincroniza `Silver.Dim_Geografia` desde `MDM.Catalogo_Geografia`;
  - incorpora `SubModulo` y `Tipo_Conduccion` cuando existen en modelo;
  - hace SCD2 real cerrando vigencias cuando cambian atributos no clave;
  - evita contaminar la dimension si el modulo no es resoluble y no es test block.
- `ETL/mdm/lookup.py`
  - primero intenta resolver por `Silver.sp_Resolver_Geografia_Cama`;
  - conserva fallback legacy en Python;
  - no salta el SP cuando hay granularidad operativa real;
  - respeta `Test Block` y casos especiales.

### Opinion tecnica
Esta fecha marca el giro correcto del proyecto:
- antes la geografia especial era ruido;
- desde aqui pasa a ser una regla gobernada.

Sin esta capa, el sistema quedaba condenado a cuarentena repetitiva y decisiones manuales sin acumulacion de conocimiento.

## 3.2 2026-03-30 - Cierre del ETL base estable

### Avance documental
Queda congelado un estado estable del ETL base con estos frentes:
- Bronce con normalizacion robusta de encabezados;
- bloqueo de archivos mal ubicados o incompatibles;
- `Bridge_Geografia_Cama` persistido correctamente;
- normalizacion global de geografia;
- homologacion tipografica segura de variedades;
- estabilizacion de `Fact_Conteo_Fenologico`, `Fact_Evaluacion_Pesos`, `Fact_Evaluacion_Vegetativa`, `Fact_Ciclo_Poda` y `Fact_Maduracion`.

### Codigo que hoy lo confirma
- `ETL/bronce/cargador.py`
  - detecta header real;
  - normaliza columnas con Unicode `NFKD`;
  - consolida columnas duplicadas post-normalizacion;
  - valida `LAYOUT_INCOMPATIBLE` y `RUTA_CONTENIDO_INCOMPATIBLE`;
  - mueve archivos rechazados a `data/rechazados/...`;
  - tiene loaders especiales para clima, induccion floral y tasa.
- `ETL/utils/texto.py`
  - normaliza modulo, componentes geograficos y variedades;
  - hace homologacion tipografica segura sin forzar merge semantico.
- `ETL/pipeline.py`
  - ya implementa gates para `SP_Cama` y para calidad de cama;
  - detiene pipeline si hay inconsistencia de bridge o `RIESGO_CONTAMINACION`.

### Hallazgo importante por codigo
La documentacion historica habla a veces de un pipeline de 17 pasos. El codigo actual en `ETL/pipeline.py` ya opera con 22 pasos en modo completo. Esto confirma que la documentacion antigua sobre numero de pasos quedo desactualizada.

### Opinion tecnica
El gran cierre de esta fecha no fue "cargar mas filas". Fue separar:
- falla estructural;
- residual funcional;
- backlog de catalogo.

Eso es lo que vuelve posible el baseline serio posterior.

## 3.3 2026-03-31 - Cierre del frente Clima

### Avance documental
Se cierra el frente de `Fact_Telemetria_Clima` con estos cambios:
- clima deja de depender de `Dim_Geografia`;
- se desacopla parseo de fecha y validacion de campana;
- se recupera hora desde `Valores_Raw`;
- se controla duplicado logico conflictivo;
- se extiende `Dim_Tiempo` para historico.

### Codigo que hoy lo confirma
- `ETL/silver/facts/fact_telemetria_clima.py`
  - usa `Sector_Climatico` directo;
  - construye `Fecha_Evento` con fecha + hora real;
  - lee `ID_Variables_Met` como surrogate tecnico correcto;
  - separa `Bronce.Reporte_Clima` de `Bronce.Variables_Meteorologicas`;
  - colapsa duplicado exacto y manda duplicado conflictivo a cuarentena.
- `ETL/utils/fechas.py`
  - define politicas por dominio;
  - `clima` y `historico` trabajan con `validar_campana=False`.

### Opinion tecnica
Este frente esta bien resuelto conceptual y tecnicamente.

Puntos acertados:
- no forzar sector meteorologico a geografia agronomica;
- no arbitrar una fila ganadora ante conflicto logico;
- mantener el historico aunque quede fuera de la campana agronomica.

## 3.4 2026-04-01 - Induccion Floral, Tasa y Fisiologia

### Avance documental
Se formalizan dos facts nuevos y se cierra un baseline para fisiologia:
- `Silver.Fact_Induccion_Floral`;
- `Silver.Fact_Tasa_Crecimiento_Brotes`;
- `Fact_Fisiologia` estable con residual acotado.

### Codigo que hoy lo confirma
- `ETL/bronce/cargador.py`
  - `Bronce.Induccion_Floral` ya usa proyeccion especial a columnas fisicas;
  - `Bronce.Tasa_Crecimiento_Brotes` ya usa solo la hoja `BD_General`;
  - `Valores_Raw` deja de ser el payload estructural principal en estos dominios.
- `ETL/silver/facts/fact_tasa_crecimiento_brotes.py`
  - usa `validar_campana=False` via dominio;
  - exige `Codigo_Ensayo`;
  - valida `Medida_Crecimiento >= 0`;
  - calcula `Dias_Desde_Poda`;
  - preserva `Modulo_Raw` cuando el caso es `Test Block`.
- `ETL/utils/fechas.py`
  - `induccion_floral` y `tasa_crecimiento_brotes` ya tienen politica sin validacion global de campana.

### Opinion tecnica
La separacion de `Induccion_Floral` y `Tasa_Crecimiento_Brotes` como facts propios fue correcta.

No se debieron fusionar dentro de `Fact_Evaluacion_Vegetativa` porque:
- tienen grano diferente;
- tienen reglas DQ distintas;
- responden a preguntas biologicas distintas.

Sobre fisiologia:
- la documentacion dice que `Modulo 11` por turno fue regresivo;
- el codigo actual respeta esa cautela indirectamente, al no introducir regla agresiva nueva en el resolvedor Python;
- el residual sano sigue concentrado en `9.`.

## 3.5 2026-04-06 - Baseline formal sobre SQL real

### Avance documental
Se fija el baseline real de Fase 1 con corrida validada sobre SQL:
- `Test Block` geograficamente cerrado;
- `Fact_Conteo_Fenologico` estable con residual controlado;
- `Fact_Tasa_Crecimiento_Brotes` estable con residual controlado;
- `VIVERO` reconocido como deuda de negocio/MDM;
- frente cama abierto pero no bloqueante para esa corrida.

### Codigo que hoy lo respalda
- `ETL/silver/facts/fact_tasa_crecimiento_brotes.py`
  - usa `resolver_geografia()` preservando `Test Block`;
  - transforma residuales de geografia en cuarentena con motivos tipados;
  - no inventa geografia para casos no resueltos.
- `ETL/mdm/lookup.py`
  - respeta el resolvedor oficial por SP;
  - solo usa fallback legacy cuando el SP no aplica o cuando la granularidad operativa no debe saltarse.

### Opinion tecnica
Este baseline es hoy la referencia mas importante para el ETL.

Lo mas valioso no es el numero de filas, sino la lectura madura del residual:
- `VIVERO` ya no se vende como bug del pipeline;
- `Test Block` deja de contaminar la lectura general;
- los casos de variedad y DQ quedan separados del problema geografico.

## 3.6 2026-04-07 - Cierre del control-plane ETL

### Avance documental
Se cierra el baseline tecnico integrado de `ETL + backend + Control.*`.

### Codigo que hoy lo confirma
- `backend/main.py`
  - registra routers, middlewares y validacion de conexion en lifespan.
- `backend/api/rutas_etl.py`
  - expone `POST /api/v1/etl/corridas`, catalogo de facts, estado, pasos, SSE y cancelacion.
- `backend/repositorios/repo_control.py`
  - persiste corrida, pasos, eventos, lock y cola de comandos.
- `backend/runner/runner.py`
  - proceso separado del web server;
  - toma comandos desde SQL;
  - adquiere lock;
  - ejecuta corrida;
  - libera lock.
- `backend/runner/ejecutor.py`
  - lanza `ETL/pipeline.py` como subprocess;
  - persiste stdout como eventos;
  - abre y cierra pasos a partir de los hitos del log;
  - maneja timeout y cancelacion.
- `backend/nucleo/etl_argumentos.py`
  - serializa configuracion ETL en `Comentario` sin tocar esquema `Control.*`.

### Opinion tecnica
Este cierre cambia la naturaleza del sistema.

Antes:
- la operacion dependia de consola, proceso web y memoria.

Ahora:
- la corrida existe como entidad persistente;
- el web server ya no es el ejecutor;
- el SSE ya no depende de memoria del proceso;
- el lock y el heartbeat ya son parte del contrato operativo.

## 4. Arquitectura vigente confirmada por codigo

## 4.1 ETL - modos de ejecucion reales

`ETL/pipeline.py` confirma dos modos:
- `completo`
- `facts`

`ETL/utils/ejecucion.py` confirma ademas:
- manifiesto oficial de facts soportadas;
- dependencias previas por fact;
- marts Gold impactados por cada fact;
- estrategia `rerun` declarada por fact.

Hallazgos claves:
- una fact sin `estrategia_rerun` declarada falla rapido;
- `Fact_Maduracion` tiene una diferencia importante: su estrategia es `rebuild_total_fact` y `releer_bronce_por_estado=False`;
- `Fact_Telemetria_Clima` no depende de `Dim_Geografia`;
- `Fact_Evaluacion_Pesos` y `Fact_Evaluacion_Vegetativa` dependen del frente `SP_Cama`.

Opinion tecnica:
- este manifiesto es una muy buena practica;
- le da contrato al `rerun` y evita reprocesos ambiguos.

## 4.2 Bronce - ya no es solo una zona raw ciega

`ETL/bronce/cargador.py` confirma que Bronce hace mas que guardar Excel como texto:
- detecta header real;
- normaliza encabezados;
- proyecta layouts especiales;
- rechaza layouts incompatibles de forma critica;
- enruta archivos a `rechazados` si la carpeta y el contenido no corresponden.

Opinion tecnica:
- esto esta bien hecho;
- evita que Silver quede contaminada por errores operativos basicos;
- el costo de ser mas estricto en Bronce es correcto si el objetivo es confiabilidad.

## 4.3 Resolver geografico - contrato actual

`ETL/mdm/lookup.py` define un contrato importante:
- SP oficial primero;
- fallback legacy despues;
- no hacer fallback cuando hay granularidad operativa real que no debe saltarse.

`ETL/silver/dims/dim_geografia.py` ademas confirma:
- SCD2 real;
- insercion condicionada;
- conteo de vigentes, test block, operativos y duplicados.

Opinion tecnica:
- el proyecto ya tiene dos capas de defensa: SQL oficial y fallback Python;
- eso es util para contingencia, pero a mediano plazo conviene converger cada vez mas al resolvedor oficial y dejar el fallback solo como red de seguridad bien acotada.

## 4.4 Politica temporal - campana ya no es global para todo

`ETL/utils/fechas.py` confirma que la validacion temporal ya se volvio por dominio.

Politicas relevantes en codigo:
- `clima`: sin validacion de campana
- `historico`: sin validacion de campana
- `induccion_floral`: sin validacion de campana
- `tasa_crecimiento_brotes`: sin validacion de campana
- dominios agronomicos principales: con validacion de campana

Opinion tecnica:
- esta correccion era obligatoria;
- la validacion global hardcodeada era una deuda de modelado, no solo un bug de parseo.

## 4.5 Backend y control-plane

### Lo confirmado por codigo
- autenticacion JWT con RBAC en `backend/nucleo/auth.py`;
- control-plane persistente en `backend/repositorios/repo_control.py`;
- salud HTTP y salud del runner en `backend/api/rutas_health.py`;
- corrida asyncrona y SSE persistente en `backend/servicios/servicio_etl.py`;
- pasos de corrida persistidos desde el ejecutor.

### Roles backend realmente definidos
- `admin`
- `operador_etl`
- `analista_mdm`
- `viewer`

Opinion tecnica:
- el backend ya no es una promesa arquitectonica, ya es parte real del sistema;
- la frontera API/runner/SQL esta mucho mas sana que un modelo `subprocess` dentro del web server.

## 4.6 Portal Streamlit - estado real por codigo

La documentacion historica repetia que el portal estaba pendiente. El codigo actual ya muestra algo mas avanzado:
- `acp_mdm_portal/app.py` existe y enruta paginas reales;
- hay paginas de inicio, cuarentena, homologacion, catalogos y configuracion;
- `acp_mdm_portal/utils/api_client.py` confirma que el portal consume backend via HTTP y JWT, no SQL directo;
- `acp_mdm_portal/utils/auth.py` confirma un esquema hibrido: usuarios hardcodeados locales + login contra backend.

Opinion tecnica:
- el portal YA existe como implementacion funcional parcial;
- sin embargo, no es todavia la fuente de verdad operativa principal del baseline;
- ademas tiene deuda de seguridad clara: usuarios hardcodeados, passwords hash locales y mezcla de autoridad local con token backend.

Conclusion practica:
- no debe describirse como "solo diseno";
- tampoco debe tratarse todavia como frente plenamente endurecido para produccion.

## 5. Estado consolidado por dominio

| Dominio | Estado vigente | Evidencia dominante | Lectura tecnica |
| --- | --- | --- | --- |
| `Dim_Geografia` | CERRADO | docs + `dim_geografia.py` | pieza sana del sistema |
| `Dim_Personal` | PENDIENTE FUNCIONAL | baseline + dependencia masiva en facts | deuda transversal fuerte |
| `Fact_Conteo_Fenologico` | ESTABLE CON RESIDUAL CONTROLADO | baseline + pipeline + manifiesto | no reabrir salvo regresion real |
| `Fact_Evaluacion_Pesos` | ESTABLE CON RESIDUAL CONTROLADO | baseline + gate de cama | residual ya es DQ/catalogo |
| `Fact_Evaluacion_Vegetativa` | ESTABLE CON RESIDUAL CONTROLADO | baseline + cargador + cama | buen candidato a bajar backlog geografico |
| `Fact_Ciclo_Poda` | CERRADO | baseline + cargador | dominio estable |
| `Fact_Maduracion` | ESTABLE CON RESIDUAL CONTROLADO | docs + manifiesto + codigo | modelo por organo bien orientado |
| `Fact_Telemetria_Clima` | ESTABLE CON RESIDUAL CONTROLADO | docs + `fact_telemetria_clima.py` | buen modelado por `Sector_Climatico` |
| `Fact_Induccion_Floral` | FUNCIONAL / ESTABLE | docs + cargador especial | mantener separado |
| `Fact_Tasa_Crecimiento_Brotes` | ESTABLE CON RESIDUAL CONTROLADO | baseline final + codigo | residual ya no es parser, es negocio/MDM/DQ |
| `Fact_Tareo` | BLOQUEADO POR FUENTE | docs + README + ausencia de geografia fuente | no forzar geografia inventada |
| `Fact_Fisiologia` | ESTABLE CON RESIDUAL CONTROLADO | baseline + docs | no reabrir con reglas debiles |

## 6. Discrepancias y complementos descubiertos al contrastar documentos con codigo

## 6.1 El pipeline actual no es el mismo que describen algunos `.md` viejos

Hallazgo:
- el codigo actual `ETL/pipeline.py` trabaja con 22 pasos y 13 facts en modo completo.

Lectura:
- cualquier referencia a pipeline de 17 pasos debe leerse como historica.

## 6.2 El portal Streamlit ya no esta "solo pendiente"

Hallazgo:
- existe `acp_mdm_portal/app.py` con paginas reales y cliente API.

Lectura:
- la documentacion inicial que lo marcaba como pendiente quedo desactualizada;
- el estado correcto hoy es: implementacion parcial real, pero no frente operativo principal del baseline.

## 6.3 El retry automatico del control-plane esta disenado, pero no completamente operativo

Hallazgo de codigo:
- `backend/runner/runner.py` encola reintentos como `Tipo_Comando='REINTENTAR'`.
- `backend/repositorios/repo_control.py` en `tomar_comando_pendiente()` solo toma `Tipo_Comando='INICIAR'`.

Implicancia:
- el reintento automatico esta documentado y parcialmente implementado;
- pero el consumidor actual de cola no recoge comandos `REINTENTAR`.

Opinion tecnica:
- esto debe considerarse una deuda tecnica real y concreta;
- no invalida el baseline general, pero si invalida asumir que el retry ya esta cerrado end-to-end.

## 6.4 Clima realmente ya no depende de `Dim_Geografia`

Hallazgo:
- `fact_telemetria_clima.py` no busca `ID_Geografia`;
- el grano es `Sector_Climatico + Fecha_Evento`.

Lectura:
- cualquier referencia residual a clima como dominio ligado a geografia agronomica debe considerarse historica.

## 6.5 El backend ya endurecio contratos que algunos `.md` mencionan solo como roadmap

Hallazgo:
- `backend/nucleo/auth.py` ya implementa JWT y jerarquia de roles;
- `backend/api/rutas_etl.py` ya expone catalogo de facts y traza persistida de pasos;
- `backend/servicios/servicio_etl.py` ya serializa opciones ETL en comentario y las rehidrata.

Lectura:
- el backend actual ya esta mas avanzado que varias notas antiguas de planeamiento.

## 6.6 El portal consume API, pero su seguridad sigue siendo hibrida

Hallazgo:
- `acp_mdm_portal/utils/api_client.py` usa backend via HTTP + JWT;
- `acp_mdm_portal/utils/auth.py` mantiene usuarios y roles hardcodeados localmente.

Opinion tecnica:
- esto es util para avanzar rapido en fase local;
- no es el modelo final deseable;
- existe riesgo de divergencia entre autorizacion del portal y autorizacion real del backend.

## 7. Opinion tecnica global del estado del proyecto

## 7.1 Lo mejor resuelto hoy

- separacion por capas en ETL;
- contrato explicito de `rerun` por fact;
- gates duros en Bronce para layout/ruta incompatible;
- control-plane persistente con lock, eventos y pasos;
- modelado correcto del frente clima;
- lectura madura del residual de Tasa y Test Block.

## 7.2 Lo que sigue siendo deuda seria

- `Dim_Personal` como cuello de botella transversal;
- `Fact_Tareo` por fuente insuficiente;
- definicion funcional de `VIVERO`;
- formalizacion final de `9.`;
- endurecimiento de seguridad del portal;
- cierre real del retry automatico del runner.

## 7.3 Riesgos de gestion si se trabaja mal este proyecto

- reabrir frentes cerrados por leer checkpoints viejos;
- confundir backlog MDM con bug de parser;
- forzar geografia inventada para reducir cuarentena artificialmente;
- promover a ML o Gold dominios con deuda funcional abierta;
- asumir que el portal ya es arquitectura final cuando aun tiene deuda de auth;
- asumir que `retry` ya esta operativo solo porque esta documentado.

## 8. Reglas de no regresion recomendadas

No hacer:
- no inferir `9.` a `9.1/9.2`;
- no volver a acoplar clima a `Dim_Geografia`;
- no quitar la validacion de `LAYOUT_INCOMPATIBLE` o `RUTA_CONTENIDO_INCOMPATIBLE`;
- no mezclar homologacion tipografica con merge semantico agresivo;
- no usar `Fact_Conteo_Fenologico` como si volviera a depender de `ID_Cinta`;
- no declarar cerrado el frente `retry` sin corregir el consumidor de cola;
- no tratar el portal como capa final endurecida mientras mantenga auth hibrida.

Si hacer:
- usar baseline final y corrida real como referencia operativa;
- leer siempre `Servidor SQL` y `Base SQL` del resumen final;
- validar `SP_Cama` solo cuando entren tablas con cama;
- usar `utils/ejecucion.py` como contrato oficial del `rerun`;
- considerar a `VIVERO` como deuda funcional/MDM hasta que negocio cierre criterio.

## 9. Prioridades recomendadas desde este contexto

### Prioridad 1 - Negocio / MDM
- cerrar politica formal de `VIVERO`;
- cerrar backlog de variedades pendientes en Tasa.

### Prioridad 2 - Datos
- fortalecer `Dim_Personal` con fuente operativa suficiente;
- no seguir aceptando `ID_Personal = -1` masivo como estado permanente.

### Prioridad 3 - Plataforma
- corregir el flujo de `REINTENTAR` en control-plane para que el retry sea real;
- luego recien declarar ese frente cerrado.

### Prioridad 4 - Portal
- unificar auth del portal con backend real;
- retirar usuarios hardcodeados cuando la capa de seguridad este lista.

### Prioridad 5 - Documentacion
- mantener este documento como resumen vivo;
- relegar los prompts y checkpoints viejos a contexto historico, no a contrato operativo.

## 10. Uso recomendado de este contexto maestro

Para futuras sesiones, este documento debe usarse asi:

1. Leer primero este contexto maestro.
2. Si el trabajo toca operacion ETL, contrastar con:
   - `ETL/Avance/BASELINE_OPERATIVO_ETL_20260406_FINAL.md`
   - `ETL/README_OPERATIVO_PIPELINE.md`
3. Si el trabajo toca backend/control-plane, contrastar con:
   - `ETL/Avance/CIERRE_BASELINE_CONTROL_PLANE_ETL_20260407.md`
   - `backend/RUNBOOK_CONTROL_PLANE_ETL.md`
4. Si aparece contradiccion, revisar codigo real antes de proponer cambio.
5. Si aparece contradiccion entre codigo y SQL real, prevalece SQL real.

## 11. Dictamen final de esta consolidacion

Estado del proyecto al 2026-04-08:
- ETL base: ESTABLE
- Control-plane backend: ESTABLE CON UNA BRECHA PUNTUAL EN RETRY
- Clima: ESTABLE
- Conteo: ESTABLE CON RESIDUAL CONTROLADO
- Tasa: ESTABLE CON RESIDUAL CONTROLADO
- Fisiologia: ESTABLE CON RESIDUAL CONTROLADO
- Tareo: BLOQUEADO POR FUENTE
- Dim_Personal: PENDIENTE FUNCIONAL
- Portal MDM: IMPLEMENTADO PARCIALMENTE, NO ENDURECIDO COMO FRENTE FINAL

Opinion tecnica final:
El proyecto ya no necesita rescate estructural. Necesita disciplina de baseline, cierre selectivo de backlog funcional, refuerzo de dimensiones debiles y endurecimiento de los bordes que aun estan a medio camino, especialmente `retry` en control-plane y seguridad del portal.

Checkpoint maestro recomendado: ACTIVO
