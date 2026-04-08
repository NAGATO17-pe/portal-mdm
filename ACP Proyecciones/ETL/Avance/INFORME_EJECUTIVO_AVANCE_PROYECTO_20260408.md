# Informe Ejecutivo de Avance del Proyecto ACP Proyecciones

Fecha de corte: 08 de abril de 2026

## Resumen ejecutivo

ACP Proyecciones ha dado un paso importante: dejo de depender principalmente de archivos sueltos, revisiones manuales y criterio individual, y empezo a convertirse en una plataforma de datos mucho mas ordenada, trazable y util para la operacion.

El avance mas valioso no es solo que hoy carguemos mas informacion. Lo mas importante es que ahora el sistema explica mucho mejor que paso con cada dato: que ingreso bien, que quedo pendiente, que necesita una decision de negocio y que aun depende de mejorar la fuente original. Eso le da al proyecto una base mucho mas seria para seguir creciendo.

Hoy ya no estamos frente a un proyecto disperso. Estamos frente a una plataforma de trabajo que ya tiene estructura, control, reglas mas claras y una forma bastante mas confiable de sostener el dia a dia.

## Como estaba el escenario al inicio

Al comienzo, el problema no era solo tecnico. El problema era operativo. Habia demasiada dependencia de archivos manuales, criterios que cambiaban entre una fuente y otra, nombres poco consistentes y poca claridad para explicar por que una carga salia bien o salia mal.

En una situacion asi, el equipo puede avanzar, pero avanza con mucho desgaste. Cada cierre depende demasiado de memoria individual. Cada diferencia obliga a revisar manualmente. Cada excepcion se vuelve una discusion larga. Y cuando eso pasa de manera repetida, no solo se pierde tiempo: tambien se debilita la confianza en los datos.

Lo que se ha venido haciendo en estos meses ha sido, en el fondo, ordenar esa realidad.

## Los pasos fundamentales que se han dado

### 1. Se ordeno la casa

El primer gran paso fue organizar la base de datos por zonas de trabajo. En lugar de mezclarlo todo en un solo lugar, se separo el dato crudo, el dato ya trabajado, el dato listo para analisis, los catalogos maestros, la configuracion y la auditoria.

Eso puede sonar tecnico, pero en la practica significa algo muy simple: cada cosa empezo a vivir en el lugar que le corresponde. Cuando un proyecto logra eso, deja de reaccionar a los problemas y empieza a entenderlos mejor.

### 2. Se pusieron reglas donde antes habia interpretacion

Otro avance clave fue la formalizacion de reglas de negocio que antes generaban mucho ruido. Casos que antes se mezclaban con errores generales ahora ya pueden reconocerse como situaciones particulares.

Un ejemplo muy claro es `Test Block`. Antes podia terminar contaminando la lectura general de una carga. Hoy ya se trata como un caso conocido y controlado. En otras palabras: dejo de parecer un error general y paso a leerse como lo que realmente es.

Eso mismo ocurrio con otros componentes de geografia, con la lectura de modulo y con ciertas excepciones operativas que antes obligaban a revisar a mano una y otra vez.

### 3. Se separo mejor lo que es error tecnico de lo que es decision del negocio

Este cambio ha sido fundamental. Antes muchas veces todo quedaba mezclado: si algo no cargaba, parecia que todo era culpa del sistema. Hoy ya se puede distinguir mejor.

Por ejemplo:

- si una variedad no esta reconocida, eso ya no se ve igual que un error de parser;
- si aparece un caso como `VIVERO`, ya no se interpreta automaticamente como un defecto tecnico general;
- si un dato llega fuera de las reglas de calidad, ya no desaparece ni queda invisible: queda identificado y pendiente.

Esto vuelve mucho mas madura la conversacion con el negocio, porque ya no se discute sobre intuiciones, sino sobre causas concretas.

### 4. El ETL dejo de ser una caja cerrada

Uno de los avances mas importantes es que el proceso de carga ya no se comporta como una caja negra.

Hoy la corrida deja mas evidencia, mas trazabilidad y mas control. Eso significa que se puede saber mejor:

- quien lanzo una ejecucion,
- cuando empezo,
- que etapas recorrio,
- si termino bien o mal,
- donde se produjo un problema,
- y que quedo pendiente despues de la corrida.

Dicho de manera sencilla: ya no solo corremos procesos; ahora tambien podemos explicarlos.

### 5. La base de datos crecio hacia una plataforma, no solo hacia un deposito

La comparacion entre el diseño original y los cambios SQL posteriores muestra algo muy importante: la base de datos ya no es solo un lugar donde se guardan tablas de analisis.

En el diseño inicial se construyo la estructura principal del DWH. Pero despues se agregaron piezas nuevas que hacen que la base hoy tambien funcione como centro de control del proceso:

- control de corridas,
- bitacora de eventos,
- seguimiento por pasos,
- control de bloqueo para que no se crucen ejecuciones,
- seguridad de usuarios,
- auditoria de cambios hechos desde el portal.

En terminos simples, al principio se construyo la casa. Despues se agregaron la sala de control, la recepcion de accesos y la bitacora de todo lo que ocurre adentro.

Ese cambio es una muestra de madurez real.

## Que ya se puede afirmar con seguridad

Hoy ya se puede decir que:

- la plataforma esta mucho mas ordenada que al inicio;
- el proceso de carga principal ya no esta en una falla sistemica;
- los casos especiales mas ruidosos ya no dominan la lectura general del sistema;
- la geografia quedo mucho mas controlada;
- el clima paso a leerse con un criterio mas sano y propio;
- el sistema ya registra mejor sus corridas y sus eventos;
- la cuarentena ya no es un lugar ciego, sino un espacio de trabajo mas gobernado.

Tambien hay evidencia concreta de escala. En una corrida validada se procesaron cerca de 295 mil filas. Dentro de ese volumen, solo el frente de Tasa aporto mas de 263 mil registros y Conteo supero los 60 mil. Ademas, la geografia vigente quedo sin duplicados, lo cual es una muy buena señal de orden estructural.

Estas cifras no se deben leer solo como volumen. Deben leerse como una señal de que el sistema ya puede trabajar sobre una base bastante mas estable.

## Ejemplos sencillos para entender la mejora

### Ejemplo 1. Antes: una excepcion ensuciaba todo

Antes, cuando aparecia algo especial como `Test Block`, ese caso podia mezclarse con otros errores y terminar dando la sensacion de que toda la carga geografica estaba mal.

Hoy, ese mismo caso ya se reconoce como una situacion particular. Eso permite que el equipo no pierda tiempo discutiendo algo que ya esta entendido y controlado.

### Ejemplo 2. Antes: lo pendiente se perdia en el ruido

Cuando una variedad no estaba homologada o una geografia no cuadraba, el problema podia quedar repartido entre archivos, notas y memoria del equipo.

Hoy ese caso queda identificado, separado y visible. No significa que ya este resuelto, pero si significa que ya no se pierde.

### Ejemplo 3. Antes: correr el proceso era una accion; hoy es una operacion trazable

Antes importaba sobre todo ejecutar. Hoy importa tambien saber que paso durante la ejecucion. Esa diferencia puede parecer pequena, pero para una operacion diaria cambia mucho la capacidad de respuesta y de control.

## Que sigue pendiente

Seria incorrecto decir que todo ya esta cerrado. El proyecto avanzo mucho, pero todavia tiene temas importantes por resolver.

Los principales son:

- definir de manera formal como se tratara `VIVERO`;
- fortalecer la informacion de personal, que sigue siendo una parte debil;
- resolver el frente de `Tareo`, que depende de una fuente todavia insuficiente;
- terminar de consolidar algunos backlog de catalogos y homologaciones;
- seguir robusteciendo el frente web para que la operacion diaria sea todavia mas sencilla y visible.

La buena noticia es que estos pendientes ya no son un caos desordenado. Hoy ya estan mucho mejor delimitados.

## Que significa esto para la directiva

Visto desde negocio, el proyecto ya entrego varias mejoras concretas:

- reduce dependencia de criterio individual;
- mejora la confianza sobre que dato paso, cual no paso y por que;
- ordena mejor las excepciones;
- da una base mas seria para seguimiento y proyecciones;
- prepara mejor el camino para una operacion diaria menos manual.

Dicho en una frase: el proyecto ya no solo procesa informacion; ahora tambien la gobierna mucho mejor.

## Lo mas valioso del avance

Lo mas valioso no es una tabla puntual ni una corrida puntual. Lo mas valioso es que el proyecto ya paso de una etapa de correccion constante a una etapa de mayor gobierno.

Eso se nota en que hoy:

- hay mas orden,
- hay mas trazabilidad,
- hay mas claridad sobre lo pendiente,
- y hay una mejor separacion entre lo tecnico y lo funcional.

Ese es el tipo de avance que permite seguir construyendo con mas seguridad.

## Recomendacion ejecutiva

La recomendacion no es rehacer ni cambiar de rumbo. La recomendacion es sostener el camino actual con tres prioridades muy claras:

1. Mantener la disciplina del baseline y de la evidencia para no volver a trabajar sobre percepciones.
2. Cerrar los pendientes funcionales y de catalogo que hoy explican buena parte del residual.
3. Consolidar la capa de operacion y visualizacion para que el equipo trabaje cada vez menos a mano y con mas trazabilidad.

## Cierre

ACP Proyecciones ya construyo una parte muy importante de lo mas dificil: pasar de una operacion muy dependiente de correcciones manuales a una plataforma que empieza a sostenerse con reglas, evidencias y control.

Todavia hay trabajo por delante, pero la lectura general del avance es favorable. El proyecto ya tiene forma, ya tiene direccion y ya tiene una base mucho mas seria para convertirse en una fuente confiable de trabajo para proyecciones y toma de decisiones.
