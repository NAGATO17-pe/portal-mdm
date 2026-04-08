# Documentación de Creación y Avance del DWH ACP

Fecha: 2026-04-08

## 1. Introducción

Este documento cuenta, de forma ordenada y entendible, cómo nació el proyecto de base de datos y cómo fue avanzando hasta convertirse en una plataforma operativa mucho más sólida de lo que era al inicio.

La idea original no era solamente guardar información. El objetivo real era poner orden en un conjunto de archivos de campo, reportes y evaluaciones que llegaban en formatos distintos, con nombres distintos y con muchas formas diferentes de escribir una misma realidad. Dicho en simple: el negocio necesitaba dejar de trabajar con información dispersa y empezar a trabajar con una sola versión confiable de la verdad.

Al arrancar, el proyecto se pensó como un DWH para organizar la campaña, limpiar los datos y preparar una base seria para análisis, seguimiento y proyección. Con el paso de los días, el trabajo dejó de ser solo “crear tablas” y pasó a convertirse en algo más importante: construir reglas para que el sistema entendiera la realidad del campo tal como realmente llega, no como se imaginó en el diseño inicial.

Ese cambio fue clave. El avance real del proyecto no estuvo solamente en crear estructuras nuevas, sino en aprender a distinguir entre tres cosas que antes se mezclaban:

1. errores reales de dato,
2. casos especiales que sí eran válidos pero no estaban bien modelados,
3. decisiones de negocio que no debía inventar el sistema por su cuenta.

Por eso, esta documentación no se limita a decir “qué se mejoró”. Explica cómo empezó todo, qué problemas aparecieron, cómo se detectaron, por qué ocurrían, qué se implementó para resolverlos y cuál fue el impacto real en la operación.

## 2. Línea de tiempo general

La evolución del proyecto puede leerse de manera muy clara en esta secuencia:

| Fecha | Hito | Cambio visible |
| --- | --- | --- |
| Marzo 2026 | Creación inicial del DWH | Se construye la base, los esquemas y la primera versión del modelo de datos |
| 2026-03-25 | Corrección de geografía especial, módulo, submódulo y cama | El sistema deja de tratar como error varios casos que en campo sí eran válidos |
| 2026-03-30 | Cierre del ETL estable | La carga deja de contaminarse con archivos mal ubicados y layouts inconsistentes |
| 2026-03-31 | Cierre del frente clima | Se recupera hora real, se corrige reproceso artificial y se controla el duplicado lógico |
| 2026-04-01 | Incorporación de Inducción Floral y Tasa de Crecimiento | El DWH amplía su alcance hacia dominios biológicos más útiles para análisis futuro |
| 2026-04-06 | Baseline formal | Se valida qué partes están realmente estables y qué pendientes ya no son bug técnico |
| 2026-04-07 | Control-plane operativo | El proyecto deja de ser solo un ETL y pasa a ser una plataforma operable y trazable |

## 3. Cómo empezó el proyecto

### 3.1 El punto de arranque

El proyecto arranca formalmente con la creación de la base principal y su estructura inicial mediante el script `DDL_Geographic_Phenology_v2.sql`.

Ese primer paso fue fundamental porque le dio al proyecto un idioma común. A partir de ese momento ya no se hablaba solo de archivos Excel sueltos, sino de una arquitectura organizada por capas:

- una capa para recibir lo crudo,
- una capa para ordenar y tipar la información,
- una capa para consumo,
- una capa para decisiones maestras,
- una capa para reglas,
- y una capa para auditoría.

En paralelo, el script `seed_dimensiones.sql` dejó cargadas las dimensiones base que el sistema necesitaba para poder empezar a trabajar con contexto. Ahí quedaron listas piezas como el calendario, los estados fenológicos, las cintas de color, los escenarios de proyección, los estados de workflow y un registro sustituto para representar el caso “Sin Evaluador”.

Dicho de forma sencilla, esta etapa fue como construir el edificio y dejar las primeras oficinas habilitadas. Todavía no estaba resuelto el trabajo de cada equipo dentro del edificio, pero sí existía una estructura clara donde cada cosa debía vivir.

### 3.2 El problema que todavía no se veía

En esa primera versión, el diseño estaba bien pensado para un mundo ideal: cada archivo bien ubicado, cada módulo bien escrito, cada fecha en su rango esperado y cada dato entrando por el camino correcto.

Pero la realidad operativa no funciona así.

Cuando el proyecto empezó a enfrentarse con archivos reales, aparecieron rápidamente las diferencias entre el diseño original y el comportamiento real del campo:

- módulos escritos como `9.1`, `9.2` o `VI`,
- turnos y válvulas escritos con prefijos y formatos distintos,
- archivos puestos en carpetas equivocadas,
- columnas que cambiaban de nombre,
- dominios nuevos que no cabían cómodamente en las tablas ya diseñadas,
- y datos históricos que no debían rechazarse solo porque quedaban fuera del rango agrícola de la campaña.

Ese fue el verdadero inicio del proyecto en términos prácticos: cuando la base dejó de ser solo una estructura creada y empezó a aprender a convivir con la realidad operativa.

## 4. Primer gran frente: geografía especial, módulo, submódulo y cama

### Fecha de consolidación: 2026-03-25

### Problema detectado

Uno de los primeros problemas fuertes fue que el sistema estaba tratando como error varias formas de geografía que en la operación sí tenían sentido.

Casos como `9.1`, `9.2`, `11.1`, `11.2`, `13.1`, `13.2`, `14.1`, `14.2` y `VI` estaban elevando cuarentena y rompiendo la resolución geográfica, cuando en realidad muchos de ellos no eran basura ni digitación incorrecta. Eran formas reales de describir el campo.

El caso más sensible fue `VI`, porque no representaba un número mal escrito, sino un caso especial asociado a `Test Block`. En otras palabras: el sistema estaba leyendo como ruido algo que en la operación tenía un significado concreto.

### Cómo se descubrió

Se descubrió porque empezaron a aparecer rechazos repetitivos en los facts que dependían de la geografía, especialmente cuando se intentaba traducir módulo, turno, válvula y cama a una ubicación válida dentro del modelo.

En lugar de ver una caída ocasional, se observó un patrón: muchos registros rechazados no venían por mala data pura, sino por una interpretación insuficiente del sistema.

También se hizo evidente al revisar que ciertos formatos sí se repetían con lógica de negocio y no como excepciones aisladas. Cuando un caso “raro” aparece una vez, puede ser ruido. Cuando aparece una y otra vez con el mismo comportamiento, ya no es ruido: es una regla que el sistema todavía no aprendió.

### Por qué pasaba

Pasaba porque la versión inicial del modelo todavía no tenía una capa suficientemente madura para traducir las variantes reales de `Modulo_Raw` a una geografía operativa.

El sistema esperaba direcciones demasiado “limpias”, mientras el campo reportaba direcciones con submódulos, conducciones distintas y formas de escritura heterogéneas.

Además, la lógica de cama todavía no estaba completamente gobernada de forma operativa. Eso hacía que incluso cuando parte de la geografía sí podía resolverse, la relación entre geografía y cama no quedara siempre persistida como debía.

### Qué se cambió y qué se implementó

En esta etapa se hizo un trabajo combinado de reglas, procedimientos y saneamiento previo.

Del lado SQL se consolidó la tabla `MDM.Regla_Modulo_Raw` como punto central para enseñar al sistema qué significaba cada forma especial de módulo. Para eso se utilizaron principalmente los scripts `fase12_regla_modulo_raw_y_sp.sql` y `fase17_reglas_modulo_raw_11_13_14.sql`.

En esos scripts se formalizaron reglas como estas:

- `9.1` y `9.2` dejaron de tratarse como error y pasaron a resolverse como submódulos válidos,
- `11.1/11.2`, `13.1/13.2` y `14.1/14.2` quedaron definidos como combinaciones reales y no como ruido,
- `VI` se incorporó como caso especial asociado a `Test Block`.

La pieza más importante de esa etapa fue el procedimiento `Silver.sp_Resolver_Geografia_Cama`, porque se convirtió en el traductor central entre la forma en que llega la geografía en el Excel y la forma en que esa geografía debe existir dentro del DWH. Ese procedimiento no solo “busca un dato”; realmente decide cómo interpretar una dirección operativa.

Alrededor de esa lógica se reforzaron también `Silver.sp_Upsert_Cama_Desde_Bronce` y `Silver.sp_Validar_Calidad_Camas`, para que la parte de camas no quedara librada a una coincidencia parcial sino a una validación formal.

Del lado Python, el cambio importante estuvo en `utils/texto.py` y `mdm/lookup.py`.

`utils/texto.py` se convirtió en la capa que limpia la forma de escribir componentes geográficos antes de intentar resolverlos. Su trabajo fue muy concreto: convertir cosas como `MODULO 2`, `TURNO 04`, `NROVALVULA 15` o `Valvula=57` a una forma normalizada que el resolvedor pudiera entender.

`mdm/lookup.py`, por su parte, dejó de depender de coincidencias demasiado frágiles y pasó a apoyarse en esa limpieza previa para resolver mejor la geografía de forma reutilizable desde distintos facts.

También se formalizó una regla muy importante: la cama operativa válida se gobernaría dentro del rango `1..100`. Eso permitió separar mejor tres casos distintos:

- cuando hay cama válida,
- cuando no hay cama explícita,
- y cuando la cama reportada no es confiable.

### Resultado antes/después

Antes de este trabajo, muchos registros terminaban en cuarentena por no poder traducir su geografía, aunque el problema real no fuera la información del campo sino la falta de reglas en el modelo.

Después de este trabajo:

- `VI` pasó a resolverse como `Test Block`,
- `9.1`, `9.2` y otros submódulos especiales dejaron de inflar cuarentena por simple desconocimiento del sistema,
- la resolución geográfica dejó de depender tanto de cómo estaba escrito el texto en el Excel,
- y el gobierno de cama empezó a validarse con reglas operativas mucho más claras.

En las corridas de referencia, esto se volvió visible cuando el puente de relación entre geografía y cama dejó de quedarse vacío y pasó a poblarse correctamente. Un caso ya validado mostró `3866` combinaciones aptas y `3866` registros insertados en el bridge, con estado final `OK_OPERATIVO`.

### Impacto real

El impacto real fue enorme porque esta etapa corrigió una injusticia del sistema: dejó de llamar “error” a cosas que en realidad sí eran válidas.

Eso cambió completamente la calidad del ETL. Ya no se trataba de perseguir falsos problemas uno por uno, sino de enseñarle al sistema cómo leer mejor el lenguaje operativo del campo.

En términos prácticos, desde aquí el proyecto dejó de depender solo de limpieza manual y empezó a construir memoria operativa.

## 5. Segundo gran frente: estabilización del ETL y orden de la carga

### Fecha de consolidación: 2026-03-30

### Problema detectado

Una vez que la geografía empezó a estabilizarse, apareció otro problema igual de importante: el ETL seguía siendo vulnerable cuando el archivo correcto llegaba mal ubicado, cuando los encabezados cambiaban de forma o cuando una tabla esperaba una estructura distinta a la que realmente venía en el Excel.

En otras palabras, el sistema ya empezaba a entender mejor la geografía, pero todavía podía contaminarse desde la puerta de entrada.

Eso afectaba especialmente a dominios como evaluación de pesos, evaluación vegetativa, ciclo de poda y maduración.

### Cómo se descubrió

Se descubrió de una forma muy concreta: columnas que debían poblarse terminaban en `NULL`, layouts reales quedaban parcialmente atrapados dentro de `Valores_Raw`, archivos correctos se estaban procesando en la carpeta equivocada y algunos rechazos masivos no se explicaban por negocio, sino por un mal entendimiento del layout.

Un ejemplo muy claro fue cuando un archivo de vegetativa, por estar en una ruta equivocada, podía contaminar la carga de otro dominio. Eso mostró que el problema ya no era solo “leer archivos”, sino saber defender al sistema de entradas que parecían válidas pero estaban mal ubicadas.

### Por qué pasaba

Pasaba porque el mundo real no conserva siempre el mismo nombre exacto de columna, la misma jerarquía de encabezado ni la misma disciplina de ubicación de archivos.

La versión inicial del ETL estaba preparada para leer, pero todavía no estaba lo suficientemente endurecida para resistir inconsistencias operativas del día a día.

En esa etapa también se hizo evidente que había una diferencia importante entre dos cosas:

- un archivo malo,
- y un archivo bueno puesto en el lugar equivocado.

Sin esa distinción, el sistema podía convertir un problema operativo simple en un problema de calidad de datos mucho más grande.

### Qué se cambió y qué se implementó

Aquí el cambio fuerte ocurrió en el cargador de Bronce y en varios facts.

El archivo `bronce/cargador.py` se fortaleció para detectar mejor el encabezado real, normalizar nombres de columnas, resolver duplicados post-normalización y bloquear archivos incompatibles antes de que contaminaran las capas siguientes.

Se incorporaron controles para generar incidentes como `LAYOUT_INCOMPATIBLE` o `RUTA_CONTENIDO_INCOMPATIBLE` cuando el archivo no correspondía al dominio en el que estaba entrando. Además, esos archivos pasaron a moverse a `data/rechazados/...`, lo que permitió separar con claridad un problema de origen de un problema del pipeline.

En los facts se hicieron ajustes importantes:

- `fact_evaluacion_pesos.py`
- `fact_evaluacion_vegetativa.py`
- `fact_ciclo_poda.py`
- `fact_maduracion.py`

En paralelo, el script `fase18_fact_maduracion_y_cinta.sql` cumplió un rol clave porque marcó un cambio de diseño importante:

- `Fact_Conteo_Fenologico` dejó de depender de `ID_Cinta`,
- y se creó formalmente `Silver.Fact_Maduracion` para tratar la maduración como un dominio propio, con su propia lógica y no como un apéndice mal acomodado de otra tabla.

Ese cambio fue mucho más profundo de lo que parece. No fue solo crear una tabla nueva; fue aceptar que el seguimiento de maduración merecía su propio modelo y que mezclarlo con otra lógica solo generaba ruido.

### Resultado antes/después

Antes de este punto, el ETL todavía podía dar una falsa sensación de avance: procesaba, pero parte del esfuerzo se perdía porque la estructura de entrada no estaba suficientemente blindada.

Después de esta etapa, el comportamiento cambió de forma visible:

- la carga dejó de aceptar silenciosamente archivos mal ubicados,
- las columnas de Bronce empezaron a poblarse de forma mucho más consistente,
- `Fact_Ciclo_Poda` dejó de ser un frente abierto,
- `Fact_Maduracion` dejó de estar indefinida y pasó a ser una pieza funcional,
- y el puente de camas dejó de quedarse sin persistencia cuando sí había combinaciones válidas.

Las corridas de referencia mostraron números mucho más sanos:

- `Fact_Conteo_Fenologico`: `67860` insertados y `67` rechazados,
- `Fact_Evaluacion_Pesos`: `5658` insertados y `14` rechazados,
- `Fact_Evaluacion_Vegetativa`: `22872` insertados y `545` rechazados,
- `Fact_Ciclo_Poda`: `5205` insertados y `0` rechazados.

### Impacto real

El proyecto dejó de ser frágil en la entrada.

Eso es clave porque una base de datos empresarial no se rompe solo cuando el dato viene mal; también se rompe cuando el proceso acepta cosas que no debió aceptar.

Desde esta etapa, el ETL ya no solo carga: también protege el sistema.

## 6. Tercer gran frente: cierre del dominio clima

### Fecha de consolidación: 2026-03-31

### Problema detectado

El frente de clima parecía estar cargando, pero en realidad escondía varios problemas distintos al mismo tiempo.

No era un único error. Era una cadena:

- fechas históricas que se rechazaban por una regla demasiado rígida,
- conflicto con el calendario de tiempo,
- reproceso artificial porque las filas no quedaban marcadas correctamente,
- pérdida de la hora real de medición,
- y aparición de duplicados lógicos conflictivos cuando la hora finalmente se recuperó.

### Cómo se descubrió

Se descubrió cuando se auditó el dominio clima en detalle y se vio que el problema no era solamente “hay muchos rechazados”.

El equipo fue encontrando capas del problema:

1. primero se vio que el clima histórico estaba siendo rechazado por campaña;
2. luego apareció el conflicto con `Dim_Tiempo`;
3. después se detectó que las filas quedaban otra vez en estado de carga, como si nunca hubieran sido procesadas;
4. finalmente, al recuperar la hora real, aparecieron duplicados lógicos que antes no se veían porque todo estaba colapsado al mismo día.

Ese orden es importante porque muestra algo muy propio del trabajo real con datos: a veces un error tapa al siguiente. Cuando corriges el primero, recién aparece el segundo.

### Por qué pasaba

Pasaba porque el modelo inicial trataba el clima con una lógica demasiado parecida a la lógica agronómica de campaña.

Eso no era correcto.

El clima necesita conservar histórico. Un dato meteorológico antiguo puede ser totalmente válido aunque no pertenezca al rango operativo de una campaña específica.

Además, en `Bronce.Variables_Meteorologicas` la hora no venía como una columna limpia, sino serializada dentro de `Valores_Raw`. Mientras eso no se leyera correctamente, el sistema estaba perdiendo precisión temporal.

Y como si eso fuera poco, el proceso estaba actualizando el estado de carga usando una columna equivocada. Entonces las filas ya procesadas volvían a parecer pendientes y se reprocesaban.

### Qué se cambió y qué se implementó

Aquí se hizo un trabajo fino y muy importante en tres piezas principales:

- `utils/fechas.py`
- `silver/facts/fact_telemetria_clima.py`
- `fase19_ajuste_fact_clima_sector_climatico.sql`

En `utils/fechas.py` se desacopló el parseo de la fecha respecto de la validación de campaña. Eso permitió que el clima pudiera procesar histórico sin ser castigado por una regla pensada para otros dominios.

En `fact_telemetria_clima.py` se corrigieron varios puntos de fondo:

- se aceptó clima histórico sin validación de campaña agronómica,
- se dejó de romper por fechas válidas que todavía no existían en el calendario,
- se empezó a usar el identificador correcto para marcar estado en Bronce,
- se extrajo `Hora_Raw` desde `Valores_Raw`,
- se reconstruyó `Fecha_Evento` con granularidad real,
- y se diferenció entre duplicado exacto y duplicado conflictivo.

El criterio nuevo fue muy sensato: si dos filas son exactamente la misma medición, una sobra y se puede conservar solo una. Pero si dos filas distintas dicen cosas distintas para el mismo sector y la misma hora, el sistema no debe elegir por intuición. En ese caso las envía a cuarentena para revisión.

Del lado SQL, `fase19_ajuste_fact_clima_sector_climatico.sql` formalizó otro cambio importante: `Fact_Telemetria_Clima` dejó de depender de una geografía agronómica y pasó a trabajar con `Sector_Climatico`. Esa decisión fue correcta porque evitó forzar a clima dentro de una estructura que no era la suya.

También se extendió `Dim_Tiempo` con histórico, apoyándose en `seed_dimensiones.sql`, para que el calendario no se quedara corto frente a los datos reales.

### Resultado antes/después

Antes, el frente clima parecía un problema confuso y repetitivo:

- había reproceso artificial,
- se perdía la hora real,
- el histórico se rechazaba indebidamente,
- y la tabla podía contaminarse con conflictos lógicos.

Después del trabajo:

- el histórico quedó habilitado,
- la hora subdiaria se preservó,
- desapareció el bucle de reproceso artificial,
- y los duplicados conflictivos dejaron de entrar a Silver.

El cierre fue claro: la validación final de duplicados en Silver quedó en `0` filas para la consulta de control usada como canon del dominio.

### Impacto real

El impacto real fue que el clima dejó de ser un frente inseguro.

Esto no solo mejora un fact. También mejora cualquier análisis futuro que dependa de clima, porque ahora la base conserva mejor la historia, respeta la hora real de medición y evita mezclar observaciones incompatibles.

En términos de negocio, el sistema dejó de deformar el clima.

## 7. Cuarto gran frente: incorporación de nuevos dominios biológicos

### Fecha de consolidación: 2026-04-01

### Problema detectado

Con el sistema más estable, apareció una necesidad natural del proyecto: incorporar dominios que eran importantes para el seguimiento biológico, pero que todavía no tenían un tratamiento maduro dentro del modelo.

Los dos frentes más claros fueron:

- `Induccion_Floral`
- `Tasa_Crecimiento_Brotes`

El riesgo aquí era resolverlos mal, por ejemplo metiéndolos a la fuerza dentro de estructuras que no representaban su verdadera naturaleza.

### Cómo se descubrió

Se descubrió cuando se revisaron archivos reales y se vio que ambos dominios tenían un grano y una lógica propios.

No eran simplemente “más columnas” de vegetativa. Tenían sus propios campos, su propia manera de medir y su propio valor analítico.

Particularmente en `Tasa_Crecimiento_Brotes`, la hoja correcta era `BD_General`, no las hojas resumen. Eso mostró con claridad que el sistema necesitaba una lectura más específica y no un tratamiento genérico.

### Por qué pasaba

Pasaba porque el diseño inicial no podía anticipar completamente cómo iban a entrar estos dominios ni qué importancia operativa iban a tener dentro del seguimiento de campaña.

Además, intentar reutilizar loaders genéricos o meter ambos dominios dentro de otro fact habría sido una simplificación peligrosa. Hubiera hecho parecer el modelo más simple, pero lo habría vuelto menos fiel a la realidad del negocio.

### Qué se cambió y qué se implementó

Aquí se amplió la capa Bronce para ambos dominios, dándoles estructura física propia y no dejándolos escondidos dentro de una carga genérica.

Se consolidaron:

- `Bronce.Induccion_Floral`
- `Bronce.Tasa_Crecimiento_Brotes`

Y en Silver se formalizaron:

- `Silver.Fact_Induccion_Floral`
- `Silver.Fact_Tasa_Crecimiento_Brotes`

Los archivos Python que materializaron este avance fueron principalmente:

- `silver/facts/fact_induccion_floral.py`
- `silver/facts/fact_tasa_crecimiento_brotes.py`

La lógica aplicada fue muy importante:

- ambos dominios quedaron separados, cada uno con su propio fact,
- se evitó mezclarlos artificialmente con `Fact_Evaluacion_Vegetativa`,
- se respetó su grano real,
- y se mantuvo la validación temporal adecuada para cada dominio.

También se aceptó algo muy importante a nivel de criterio: mientras `Dim_Personal` no estuviera suficientemente poblada, la presencia de `ID_Personal = -1` no debía leerse como bug del fact, sino como una limitación conocida de la dimensión.

### Resultado antes/después

Antes de este trabajo, estos dominios estaban fuera de la parte más madura del DWH o corrían el riesgo de entrar de manera forzada y poco confiable.

Después:

- quedaron con Bronce estructurado,
- tuvieron facts propios en Silver,
- y pasaron a formar parte del DWH de forma gobernada.

Más adelante, esto se vería reflejado en una corrida real muy importante donde `Fact_Tasa_Crecimiento_Brotes` alcanzó `263388` insertados, mostrando que el dominio no solo se incorporó, sino que se volvió masivo y útil.

### Impacto real

Este avance fue importante porque amplió el alcance biológico del DWH.

En términos simples, la base dejó de servir solo para revisar lo ya conocido y empezó a prepararse mejor para entender procesos más finos de crecimiento y desarrollo.

Ese paso también fue estratégico para el futuro analítico y predictivo del proyecto.

## 8. Quinto gran frente: baseline formal y lectura correcta del residual

### Fecha de consolidación: 2026-04-06

### Problema detectado

Cuando un proyecto empieza a madurar, aparece una necesidad distinta: dejar de discutir por sensaciones y empezar a separar con claridad qué está realmente fallando y qué ya es un pendiente de negocio o de catalogación.

Ese fue el gran trabajo de esta etapa.

Había que responder una pregunta muy concreta: ¿el ETL todavía tiene una falla estructural o lo que queda abierto ya es residual controlado?

### Cómo se descubrió

Se descubrió al validar una corrida formal y revisar sus resultados ya no como anécdotas, sino como evidencia del estado real del sistema.

Ahí se hizo visible algo muy importante: algunos casos que antes se leían como “el pipeline está mal” en realidad ya no eran problema técnico.

El ejemplo más claro fue `Test Block`, y el segundo ejemplo más importante fue `VIVERO`.

### Por qué pasaba

Pasaba porque hasta ese momento todavía era fácil mezclar tres cosas:

- geografía no resuelta por falta de regla,
- caso especial ya corregido pero con residual por otra causa,
- y pendiente de negocio que el sistema no debía inventar.

Sin separar esas tres capas, era muy fácil pedir cambios de código para problemas que ya no eran de código.

### Qué se cambió y qué se implementó

En esta etapa se cerró especialmente el caso de `Test Block`.

Se agregó un alias activo en `MDM.Regla_Modulo_Raw` y se ajustó `fact_tasa_crecimiento_brotes.py` para conservar correctamente `Modulo_Raw` cuando se trataba de ese caso especial.

La validación concreta se apoyó en `Silver.sp_Resolver_Geografia_Cama`, que ya respondió el caso `Test Block` con `Estado_Resolucion = RESUELTA_TEST_BLOCK`.

Más allá del cambio puntual, lo más importante de esta etapa fue la forma de leer el resultado:

- ya no todo rechazo se interpretó como bug,
- ya no toda cuarentena se interpretó como fracaso,
- y ya no todo residual se quiso resolver con parche técnico.

### Resultado antes/después

La corrida formal dejó una fotografía muy clara del avance:

- `Dim_Geografia`: `1129` vigentes, `0` duplicados y `1` test block vigente,
- `Fact_Conteo_Fenologico`: `60328` insertados y `66` rechazados,
- `Fact_Tasa_Crecimiento_Brotes`: `263388` insertados y `5086` rechazados,
- `Gold.Mart_Fenologia`: `20808` filas,
- duración total de la corrida: `154.62s`.

Lo más importante fue la lectura del residual de `Tasa`:

- `3871` casos correspondían a geografía especial pendiente bajo `VIVERO`,
- `1115` casos eran variedades no reconocidas,
- `100` casos correspondían a fecha de poda auxiliar posterior a la evaluación.

Eso cambió completamente la conversación del proyecto, porque permitió afirmar con fundamento que:

- `Test Block` ya no era el problema,
- `11.1` y `9.1` ya no representaban el mismo problema que al inicio,
- y `VIVERO` no debía seguir tratándose como bug del pipeline, sino como una decisión de negocio y MDM pendiente de cierre.

### Impacto real

Esta etapa fue fundamental porque ordenó la conversación.

Un proyecto madura de verdad cuando deja de arreglar a ciegas y empieza a saber exactamente qué le toca al código, qué le toca a la catalogación y qué le toca a negocio.

Desde aquí, el DWH dejó de estar en “modo rescate” y pasó a estar en “modo gobierno”.

## 9. Sexto gran frente: del ETL a una plataforma operativa

### Fecha de consolidación: 2026-04-07

### Problema detectado

Hasta este punto, el ETL ya estaba mucho mejor, pero operar el sistema todavía dependía demasiado de una ejecución técnica aislada.

Eso podía servir al principio, pero no era suficiente para una operación diaria ordenada. Hacía falta saber:

- quién lanzó una corrida,
- cuándo empezó,
- qué paso estaba ejecutando,
- cómo ver su progreso,
- cómo cancelarla,
- cómo reintentarse de forma dirigida,
- y cómo conservar historial sin depender solo de lo que mostrara la consola.

### Cómo se descubrió

Se descubrió por una necesidad natural del crecimiento del proyecto. Cuando el sistema dejó de ser una prueba aislada y empezó a consolidarse como operación real, ya no alcanzaba con “correr el pipeline y mirar la salida”.

Había que convertir la ejecución en algo auditable, visible y repetible.

### Por qué pasaba

Pasaba porque el proyecto había crecido. Ya no era razonable que un proceso importante dependiera solo de memoria temporal o de revisar salidas sueltas.

El DWH necesitaba una capa de control que le diera orden a la operación, del mismo modo en que las capas anteriores le dieron orden a los datos.

### Qué se cambió y qué se implementó

Aquí el proyecto dio un salto de madurez muy claro.

Del lado SQL se creó el esquema `Control` mediante `crear_tablas_control.sql`. Ese script creó la base del control operativo:

- `Control.Corrida`
- `Control.Corrida_Evento`
- `Control.Corrida_Paso`
- `Control.Bloqueo_Ejecucion`
- `Control.Comando_Ejecucion`

En palabras simples, estas tablas hicieron posible que el sistema tuviera memoria operativa. Ya no solo recuerda los datos de campo: también recuerda cómo se ejecutó el proceso que los cargó.

Luego, `fase21_endurecimiento_control_plane.sql` y `fase22_retencion_control_plane.sql` fortalecieron esa capa con vistas operativas y con el procedimiento `Control.sp_Purgar_Historial_Control`, para que el historial pudiera mantenerse sano sin convertirse en acumulación desordenada.

Del lado Python y backend, el cambio se apoyó en varios componentes:

- `pipeline.py` como orquestador central,
- `backend/runner/runner.py` como ejecutor separado del proceso web,
- `backend/runner/ejecutor.py` para materializar la corrida,
- `backend/api/rutas_etl.py` para exponer la operación,
- y el resto del backend para mostrar corridas, pasos, eventos y estado.

Paralelamente, la base también se preparó mejor para gobierno de acceso y trazabilidad con scripts como `crear_tablas_seguridad.sql` y `crear_auditoria_cambios_portal.sql`.

### Resultado antes/después

Antes de esta etapa, el proyecto tenía ETL. Después de esta etapa, el proyecto ya tenía una plataforma operativa.

Las validaciones dejaron evidencia clara:

- corridas completas funcionando,
- `rerun` dirigido por fact,
- salud del runner y del control operativo,
- historial persistido,
- y pruebas del backend en verde.

Una corrida amplia validó `491174` filas en Bronce y una duración total de `419.86s`. Luego, una corrida de no regresión confirmó nuevamente la estabilidad con `332941` filas en Bronce y resultados consistentes en facts clave como fisiología y tasa.

### Impacto real

Este fue uno de los avances más importantes de todo el proyecto.

Porque a partir de aquí el DWH ya no depende solo de que “alguien experto lo sepa correr”. Ahora el sistema tiene orden, trazabilidad y capacidad de operación diaria mucho más seria.

Eso cambia el valor del proyecto ante la organización: deja de ser un desarrollo técnico valioso pero artesanal y empieza a comportarse como una plataforma que puede sostenerse y gobernarse mejor.

## 10. Qué ha cambiado realmente desde que el proyecto arrancó

Si se mira el recorrido completo, el cambio real del proyecto puede resumirse así:

Al principio se construyó una base para ordenar información.

Después, esa base tuvo que aprender a leer mejor la realidad del campo:

- entendiendo geografía especial,
- soportando submódulos y casos de Test Block,
- diferenciando lo que era error de lo que era solo una forma distinta de escribir,
- aceptando histórico donde correspondía,
- separando dominios que necesitaban su propio tratamiento,
- y construyendo control operativo para que el sistema no solo cargue, sino que también pueda gobernarse.

Dicho de una manera muy simple:

al inicio se construyó la estructura;
luego se corrigió la interpretación;
después se estabilizó la carga;
más adelante se amplió el alcance;
y finalmente se ordenó la operación.

## 11. Estado actual contado en lenguaje simple

Hoy el proyecto ya no está en una etapa de caos inicial.

Lo que ya se ve y se nota como avance real es esto:

- la base ya no confunde varios casos válidos con errores,
- la carga es mucho más resistente a archivos desordenados,
- la geografía se resuelve con reglas mucho más maduras,
- clima ya no pierde hora ni reprocesa artificialmente,
- dominios nuevos ya entraron al modelo con estructura propia,
- existe una línea clara entre lo que es bug técnico y lo que es pendiente funcional,
- y la operación ya cuenta con control, historial y trazabilidad.

Eso no significa que todo esté cerrado. Significa algo más importante: que el proyecto ya tiene forma.

## 12. Pendientes que todavía requieren trabajo

Los pendientes que quedan ya no tienen la misma naturaleza de los problemas iniciales.

Lo principal que sigue abierto es:

- la decisión formal sobre `VIVERO`,
- el fortalecimiento de `Dim_Personal`,
- la resolución de ciertos casos residuales de geografía no catalogada,
- y la definición de algunos dominios que siguen dependiendo de fuente o decisión de negocio.

La diferencia es que hoy esos pendientes ya no están mezclados con fallas estructurales del sistema.

Eso, por sí solo, ya representa un avance muy importante.

## 13. Cierre

La historia de este proyecto no es la de una base que simplemente “se creó”.

Es la historia de un sistema que comenzó con una estructura técnica inicial y fue aprendiendo, paso a paso, a representar mejor la realidad del negocio.

El avance más valioso no fue solamente agregar tablas, scripts o procedimientos. Fue construir criterio dentro del sistema:

- cuándo aceptar,
- cuándo rechazar,
- cuándo mandar a revisión,
- cuándo resolver automáticamente,
- y cuándo no inventar una respuesta.

Ese es el cambio más importante entre el inicio y el estado actual.

Hoy ya no se está trabajando sobre una idea de DWH. Hoy ya existe un DWH con recorrido, con decisiones tomadas, con problemas reales enfrentados y con resultados visibles entre una fecha y otra.
