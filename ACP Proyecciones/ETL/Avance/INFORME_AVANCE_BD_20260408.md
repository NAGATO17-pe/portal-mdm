# Informe de Avance de Base de Datos y Plataforma DWH

Fecha de corte: `2026-04-08`  
Ruta base: `D:\Proyecto2026\ACP_DWH\ACP Proyecciones`  
Servidor SQL de referencia documental: `LCP-PAG-PRACTIC`  
Base SQL de referencia documental: `ACP_DataWarehose_Proyecciones`

## 1. Proposito

Este informe resume el recorrido tecnico del proyecto desde la construccion del DWH hasta el estado operativo actual de la base de datos, el ETL, el control-plane y el portal de operacion.

El objetivo es dejar una fotografia ejecutiva-tecnica del avance real del sistema:

- que ya esta construido;
- que ya quedo validado;
- que sigue abierto;
- que deuda corresponde a plataforma y cual a negocio/MDM;
- que condiciones deben cumplirse antes de considerar la base lista para una siguiente etapa de explotacion, automatizacion o ML.

## 2. Fuente de evidencia y limite de este informe

Este informe fue consolidado usando tres fuentes:

1. Baselines y cierres oficiales en `ETL/Avance/`.
2. Documentacion operativa vigente en:
   - `ETL/README_OPERATIVO_PIPELINE.md`
   - `backend/RUNBOOK_CONTROL_PLANE_ETL.md`
   - `backend/README.backend.md`
3. Codigo real en:
   - `ETL/`
   - `backend/`
   - `acp_mdm_portal/`

Limitacion importante:

- En esta sesion no fue posible conectarse a SQL Server en vivo.
- La conexion hacia `LCP-PAG-PRACTIC` fallo por autenticacion/conectividad del entorno local de esta sesion.
- Por tanto, este documento refleja el estado consolidado a partir del baseline oficial mas reciente y del codigo actual, pero no reemplaza una validacion SQL live del dia.

Lectura correcta:

- para continuidad tecnica y gestion del proyecto, este informe es valido;
- para decisiones de cierre final del dia, siempre debe contrastarse con evidencia SQL real.

## 3. Resumen ejecutivo

El proyecto ACP Proyecciones ya no esta en fase de rescate estructural. La base de datos y la plataforma DWH alcanzaron un estado operativo serio y con gobierno tecnico reconocible.

Hoy se puede afirmar lo siguiente:

- la arquitectura DWH esta establecida y operativa;
- el ETL ya no presenta falla sistemica general;
- la resolucion geografica dejo de ser un problema difuso y paso a estar gobernada por reglas y SP;
- el control-plane ya existe como capa persistente con lock, cola, pasos, eventos y health checks;
- `Gold` ya se refresca dentro de un flujo operativo formal;
- el portal actual ya funciona como frente parcial, aunque no es todavia la capa final endurecida;
- el principal backlog ya no es de parser ni de infraestructura, sino de fuente, MDM y definicion funcional.

Dictamen ejecutivo:

- plataforma: madura para operacion local controlada;
- datos: estables por dominio principal, con residual controlado;
- negocio/MDM: sigue siendo el frente que mas condiciona el cierre total;
- portal/frontend: util, pero todavia en transicion hacia una arquitectura web mas robusta.

## 4. Linea de avance del proyecto

### 4.1 Marzo 2026 - Ordenamiento de geografia, modulo raw y cama

En esta fase el proyecto corrige el principal foco de ambiguedad del DWH: la geografia agronomica. Se consolida el manejo de `Modulo_Raw`, `SubModulo`, `Turno`, `Valvula` y `Cama`, y se dejan de tratar casos especiales como ruido sin gobierno.

Avances relevantes:

- `VI` pasa a leerse como caso especial de `Test Block`;
- `9.1` y `9.2` dejan de tratarse como error de modulo y se reconocen como conducciones distintas;
- se consolida `MDM.Regla_Modulo_Raw`;
- el resolvedor SQL pasa a ser la referencia formal para geografia;
- `Dim_Geografia` avanza hacia un modelo SCD2 mas consistente.

Impacto:

- la cuarentena deja de ser un deposito indiscriminado;
- la geografia especial empieza a resolverse con trazabilidad;
- el ETL gana una capa real de gobierno de datos.

### 4.2 2026-03-30 - Cierre del ETL base estable

Esta fecha marca el primer cierre operativo serio del ETL.

Avances consolidados:

- normalizacion fuerte de encabezados en Bronce;
- bloqueo de layouts incompatibles y archivos mal enroutados;
- persistencia correcta del `Bridge_Geografia_Cama`;
- normalizacion geografica global previa al resolvedor;
- homologacion tipografica segura de variedades;
- estabilizacion de dominios como:
  - `Fact_Conteo_Fenologico`
  - `Fact_Evaluacion_Pesos`
  - `Fact_Evaluacion_Vegetativa`
  - `Fact_Ciclo_Poda`
  - `Fact_Maduracion`

Opinion tecnica:

El cambio importante aqui no fue aumentar inserciones, sino separar tres cosas que antes estaban mezcladas:

- error estructural;
- residual de calidad de datos;
- backlog funcional/MDM.

Sin esa separacion no era posible tener un baseline serio.

### 4.3 2026-03-31 - Clima deja de depender de geografia agronomica

Se consolida un ajuste conceptual importante: `Fact_Telemetria_Clima` deja de depender de `Dim_Geografia` y pasa a modelarse con `Sector_Climatico`.

Avances:

- clima usa `Sector_Climatico` directo;
- se recupera la hora real del payload;
- la politica temporal deja de ser global y se vuelve sensible al dominio;
- el historico meteorologico ya no se descarta como si fuera error de campana;
- duplicados conflictivos van a cuarentena en lugar de “arreglarse” automaticamente.

Opinion tecnica:

Este fue uno de los mejores cambios del proyecto. Forzar clima a `Dim_Geografia` hubiera degradado el modelo de datos.

### 4.4 2026-04-01 - Separacion formal de Tasa, Induccion y estabilizacion de Fisiologia

Se formalizan `Fact_Induccion_Floral` y `Fact_Tasa_Crecimiento_Brotes` como dominios propios, con reglas temporales y DQ especificas.

Avances:

- cargadores especiales en Bronce;
- facts dedicados en Silver;
- `validar_campana=False` para dominios que lo requieren;
- `Fact_Fisiologia` queda con baseline sano y residual controlado;
- la regla agresiva de `Modulo 11` por turno se reconoce como regresiva y no se vuelve a activar.

Opinion tecnica:

Fue una decision correcta no fusionar estos dominios dentro de un solo fact generico. El modelo gano claridad biologica y calidad operativa.

### 4.5 2026-04-06 - Baseline operativo formal sobre SQL real

Esta es la fecha de referencia principal del ETL.

Queda validado en SQL real:

- `Dim_Geografia` sana;
- `Fact_Conteo_Fenologico` estable con residual controlado;
- `Fact_Tasa_Crecimiento_Brotes` estable con residual controlado;
- `Test Block` geograficamente cerrado;
- `Gold` refrescando dentro de corrida real;
- frente cama abierto, pero no bloqueante para esa corrida puntual.

Evidencia destacada del baseline:

- `Fact_Conteo_Fenologico = 60328`
- `Fact_Tasa_Crecimiento_Brotes = 263388`
- `Gold.Mart_Fenologia = 20808`
- `Duracion total = 154.62s`

Lectura correcta del residual:

- `VIVERO` no es bug general del pipeline;
- `VIVERO` es backlog funcional/MDM;
- `Test Block` deja de ser residual geografico dominante;
- las variedades no homologadas y los casos DQ quedan mejor separados.

### 4.6 2026-04-07 - Cierre del baseline tecnico de plataforma

Esta fecha cierra no solo el ETL, sino el stack `ETL + backend + Control.*`.

Avances:

- runner separado y persistente;
- trazabilidad por corrida y paso;
- eventos persistidos;
- health checks operativos;
- retencion de historial;
- `rerun` dirigido por fact;
- backend validado con suite verde.

Impacto:

- el sistema deja de depender de stdout efimero o memoria del web server;
- la corrida ETL pasa a ser una entidad operativa persistente;
- se vuelve posible monitoreo multiusuario con criterio tecnico serio.

## 5. Estado actual por capa de la base de datos

### 5.1 Capa Bronce

Estado: `ESTABLE`

Lo que ya esta bien resuelto:

- deteccion de header real;
- normalizacion de encabezados;
- loaders especiales cuando el layout lo requiere;
- rechazo de rutas y layouts incompatibles;
- segregacion de archivos rechazados.

Lectura tecnica:

Bronce ya no es una simple zona raw ciega. Hoy es una capa defensiva que evita contaminar Silver con errores operativos basicos.

### 5.2 Capa Silver

Estado: `OPERATIVA CON DOMINIOS MAYORMENTE ESTABLES`

Fortalezas:

- star schema operativo;
- gobernanza geografica madura;
- uso correcto de surrogate keys donde ya existe soporte de dimension;
- cuarentena tipada y razones de rechazo mas claras.

Debilidades abiertas:

- `Dim_Personal` sigue debil;
- `Fact_Tareo` sigue atado a una fuente insuficiente;
- algunos dominios siguen con residual funcional, no estructural.

### 5.3 Capa Gold

Estado: `OPERATIVA CON GATES`

Gold ya no se trata como salida automatica ciega. El pipeline actual incorpora criterios de calidad antes de refrescar marts, especialmente cuando entra en juego el frente cama y el bridge geografico.

Lectura tecnica:

Esto es correcto. Publicar Gold sin gate de calidad hubiera convertido el sistema en una fabrica de dashboards inconsistentes.

### 5.4 MDM

Estado: `ESTRATEGICO Y TODAVIA DETERMINANTE`

El proyecto ya reconoce algo importante: muchos rechazos ya no son fallo de codigo, sino deuda de catalogo y negocio.

MDM hoy concentra:

- homologacion de variedades;
- reglas de `Modulo_Raw`;
- cuarentena operativa;
- backlog de valores no catalogados;
- decisiones que el ETL no debe inventar.

Lectura tecnica:

Este frente es hoy el principal cuello de botella para cerrar residual remanente, especialmente en `Tasa`.

### 5.5 Control

Estado: `ESTABLE CON UNA BRECHA PUNTUAL`

Ya existe:

- `Control.Corrida`
- `Control.Corrida_Paso`
- `Control.Corrida_Evento`
- `Control.Comando_Ejecucion`
- `Control.Bloqueo_Ejecucion`
- vistas operativas
- procedimiento de purga

Brecha conocida:

- el `retry` esta disenado y parcialmente implementado, pero el flujo `REINTENTAR` no estaba completamente cerrado end-to-end en el codigo auditado.

Lectura tecnica:

No invalida el baseline general, pero si impide considerar ese frente absolutamente terminado.

## 6. Estado actual por dominios de negocio

### Dominios mas sanos

- `Dim_Geografia`
- `Fact_Ciclo_Poda`
- `Fact_Telemetria_Clima`
- `Fact_Conteo_Fenologico`
- `Fact_Tasa_Crecimiento_Brotes` como pipeline, no como negocio totalmente cerrado

### Dominios estables con residual controlado

- `Fact_Evaluacion_Pesos`
- `Fact_Evaluacion_Vegetativa`
- `Fact_Maduracion`
- `Fact_Fisiologia`
- `Fact_Induccion_Floral`

### Dominios pendientes o limitados por fuente

- `Dim_Personal`
- `Fact_Tareo`

### Lectura ejecutiva del residual actual

El residual dominante ya no es “el ETL falla”. El residual dominante hoy se distribuye en:

- geografia especial que requiere criterio de negocio;
- variedades no homologadas;
- calidad de dato puntual;
- debilidad de dimensiones fuente;
- fuente insuficiente en dominios especificos.

## 7. Avance del backend y la capa operativa

El backend FastAPI ya es parte del producto y no solo soporte tecnico.

Capacidades reales observadas:

- autenticacion JWT;
- roles (`admin`, `operador_etl`, `analista_mdm`, `viewer`);
- lanzamiento de corridas ETL;
- consulta de historial;
- eventos en tiempo real por SSE;
- consulta de pasos;
- lectura de catalogo oficial de facts para `rerun`;
- cancelacion;
- health checks por capa;
- auditoria y catálogos por API.

Opinion tecnica:

La plataforma ya tiene una frontera saludable entre:

- ETL;
- API;
- runner;
- SQL persistente.

Eso es un salto grande respecto a una operacion basada solo en consola.

## 8. Avance del portal actual

El portal actual ya no debe describirse como un diseno pendiente.

Estado real:

- existe app Streamlit;
- existe login contra backend;
- existe pagina de inicio;
- existe pagina de cuarentena;
- existen catalogos;
- existe una base de cliente API;
- existe estructura util para transicion a un frontend web mas serio.

Pero tambien hay limites:

- seguridad hibrida;
- deuda de permisos locales;
- UX limitada por la naturaleza de Streamlit;
- no es la mejor base para escalar observabilidad, interaccion densa y experiencia operativa multirol.

Conclusion:

El portal actual es un frente util y valido para transicion, no la arquitectura final ideal.

## 9. Riesgos y deudas abiertas

### 9.1 Riesgos tecnicos

- dar por cerrado el retry cuando todavia existe una brecha de implementacion;
- asumir que la BD queda “lista para ML” solo porque el ETL corre;
- reabrir reglas geograficas ya estabilizadas por leer documentos viejos;
- debilitar gates de calidad en Bronce o Gold para “cargar mas rapido”.

### 9.2 Riesgos funcionales

- mapear `VIVERO` a una geografia artificial solo para bajar cuarentena;
- forzar resolucion de `9.` sin criterio funcional cerrado;
- permitir que `Dim_Personal` siga vacia indefinidamente mientras se consume Silver como si estuviera cerrada.

### 9.3 Deuda estructural prioritaria

- politica formal de `VIVERO`;
- fortalecimiento de `Dim_Personal`;
- resolucion de fuente para `Fact_Tareo`;
- cierre real del flujo `retry`;
- endurecimiento final del frente portal/frontend.

## 10. Valor construido hasta hoy

El recorrido del proyecto si muestra avance real y medible.

Valor ya construido:

- arquitectura DWH funcional;
- ETL con capas y contratos mejores definidos;
- control-plane persistente;
- capacidad de reproceso dirigido;
- cuarentena mas trazable;
- mejor separacion entre deuda tecnica y deuda funcional;
- base operativa suficiente para continuar hacia un portal web mas serio;
- entorno mucho mas preparado para una futura capa analitica o predictiva, aunque todavia no listo para saltar directo a ML sin cerrar backlog de datos.

## 11. Dictamen final

Dictamen del estado actual del proyecto al `2026-04-08`:

- Base de datos DWH: `MADURA Y OPERATIVA`
- ETL: `ESTABLE`
- Control-plane: `ESTABLE CON UNA BRECHA PUNTUAL`
- Dominios principales: `MAYORMENTE ESTABLES CON RESIDUAL CONTROLADO`
- MDM: `SIGUE SIENDO EL FRENTE MAS CONDICIONANTE`
- Portal actual: `FUNCIONAL, PERO NO FINAL`

Opinion tecnica final:

El proyecto ya construyo lo mas dificil: una base DWH operable, con criterio tecnico y separacion clara entre ejecucion, control, datos y gobierno. Lo que sigue no es “salvar la base”, sino terminar de cerrar deuda funcional, robustecer dimensiones debiles y consolidar el frente de operacion para que la plataforma pueda sostenerse con menos friccion y mayor trazabilidad.

## 12. Recomendacion inmediata

Usar este informe como avance consolidado del proyecto y ejecutar una segunda pasada cuando haya acceso SQL real para anexar:

- evidencia live de conteos por esquema;
- estado actual de tablas Silver y Gold;
- backlog real en `MDM.Cuarentena`;
- estado vigente de `Control.*`;
- fotografia SQL del dia.

En ese punto el documento pasaria de “avance consolidado” a “avance validado con evidencia live”.
