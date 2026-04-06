# Informe Ejecutivo de Mapeo de Procesos, Backlog y Linaje

**Proyecto:** ACP Proyecciones  
**Fecha:** 06 de abril de 2026  
**Tipo de lectura:** Tecnico-ejecutiva preliminar

## 1. Introduccion ejecutiva

La revision del mapeo de procesos, del backlog de mejoras y del material de linaje deja una conclusion bastante clara: el proyecto no partio de un problema pequeño ni puntual. Lo que habia era una mezcla de procesos manuales, fuentes con criterios distintos, nombres poco estables, dependencias cruzadas y ausencia de una jerarquia tecnica suficientemente clara para sostener proyecciones de manera confiable.

Lo valioso es que ese problema ya esta mucho mejor ordenado. No porque todas las brechas hayan desaparecido, sino porque ahora la mayoria de ellas ya puede describirse, priorizarse y conectarse con acciones concretas. Ese es probablemente el avance mas importante de toda la etapa actual.

## 2. Que aporta el mapeo de procesos

La ruta de Mapeo de Procesos ayuda a entender que el proyecto no se limito a construir un ETL aislado. Tambien hubo un esfuerzo por mirar como fluye la informacion a traves del negocio. Los flujogramas, los insumos y salidas entre bases, y la forma en que cada dataset alimenta a otro, dejan ver que el problema era sistemico: no se trataba solo de una tabla mala, sino de una cadena completa con puntos de quiebre.

Cuando se revisan los materiales de esta ruta, aparece una red de relaciones entre seguimiento de productividad, seguimiento de cosecha, conteo de fruta, induccion floral, fenologia, evaluacion vegetativa, tasa de crecimiento, poda, fisiologia, maduracion, meteorologia, proyeccion de pesos, calibres y peladas. En otras palabras, aparece el negocio trabajando como un sistema, no como archivos independientes.

Eso cambia la lectura del proyecto. El ETL no esta solo para mover archivos. Esta para darle coherencia a un conjunto de dependencias que, si no se ordenan, terminan rompiendo la proyeccion, la comparabilidad y hasta la confianza del equipo en sus propias cifras.

## 3. Que revela realmente el backlog de mejoras

El backlog muestra algo que ya se intuía en la operacion: habia demasiada manualidad. No solo manualidad de carga, tambien manualidad para interpretar, reconciliar, corregir y hasta completar datos que venian con llaves debiles o definiciones poco estables. El problema no era solamente de velocidad; era de repetibilidad y de control.

Entre las brechas mas visibles aparecen la falta de integracion end-to-end del ETL, la dependencia de imagenes o cierres manuales, la desactualizacion de dashboards por ausencia de automatizacion, la falta de una base unificada que funcione como referencia mas estable y la persistencia de nombres diferentes para las mismas entidades. Tambien se observa la ausencia de un estandar mas fuerte para nomenclatura, consultas automatizadas y jerarquia entre fuentes.

Leyendolo en conjunto, el backlog no se siente como una lista de molestias menores. Se siente como el registro de un modelo operativo que funcionaba, pero que dependia demasiado de experiencia tacita y de trabajo correctivo. Por eso es tan importante que hoy ya exista una arquitectura mas clara para Bronce, Silver, Gold, MDM, configuracion y auditoria.

- La manualidad excesiva era un riesgo operativo, no solo una incomodidad.
- La falta de golden sources generaba dobles verdades y cierres discutibles.
- Las inconsistencias de nombre y llave debilitaban la trazabilidad.
- La ausencia de control de corrida hacia dificil explicar porque algo salio bien o salio mal.

## 4. Como dialoga el backlog con lo que ya mejoro

Aqui es donde el proyecto muestra avance real. Varias de las brechas del backlog hoy ya tienen una respuesta visible en el ETL y en la operacion. La primera es la formalizacion del baseline con evidencia SQL. Eso puede sonar administrativo, pero en realidad es una mejora estructural: obliga a separar percepcion de evidencia y evita declarar estable algo que no se verifico.

La segunda mejora fuerte es la separacion entre frentes cerrados, frentes estables con residual controlado y frentes pendientes por negocio o MDM. Antes era mas facil mezclar todo y decir que el sistema estaba mal sin distinguir la causa. Hoy ya se puede decir, por ejemplo, que Test Block quedo resuelto a nivel geografico, que Conteo y Tasa estan estables con residual controlado y que VIVERO no es un bug general del pipeline, sino una decision de negocio/MDM todavia abierta.

La tercera mejora es el control explicito de cuarentena. Esto responde de forma directa a varios problemas del backlog. Cuando un valor no encuentra geografia, no cae en un limbo. Cuando una variedad no esta homologada, no se pierde. Cuando hay una regla de calidad de datos que no se cumple, el motivo queda visible. Ese cambio parece operativo, pero en realidad fortalece el gobierno de datos.

Tambien mejoro la gestion de reglas para modulo y geografia. La resolucion de Test Block es el caso mas claro. Antes generaba ruido tecnico y quedaba mezclado con otros errores. Ahora ya existe una regla mas formal, una resolucion verificable y un residual que puede explicarse por otra causa distinta. Ese tipo de correccion reduce ruido y hace mas precisa la conversacion con negocio.

## 5. Lectura ejecutiva del scorecard

El scorecard muestra que la calidad y madurez de las bases no era uniforme. Algunas fuentes tenian mejor posicion relativa y otras venian claramente mas debiles en completitud, puntualidad, consistencia y exactitud. Eso es relevante porque explica por que no todos los frentes podian cerrarse al mismo ritmo.

Mirado con calma, el scorecard no contradice el estado actual del ETL. Lo complementa. El ETL puede ordenar el flujo, estandarizar, rechazar, cuarentenar y publicar, pero no puede convertir por si solo una fuente irregular en una fuente impecable. Lo que si hace, y eso ya es bastante, es transformar una realidad difusa en una realidad medible.

Ese matiz importa mucho. Hoy no seria correcto decir que el proyecto elimino todos los problemas de calidad. Lo correcto es decir que ya tiene una forma mucho mas madura de hacerlos visibles, delimitarlos y trabajarlos sin confundir deuda tecnica con deuda de origen o deuda funcional.

## 6. Lectura ejecutiva del RACI

El RACI simplificado aporta algo que normalmente se subestima: responsabilidad visible. Cuando una base de datos o un frente operativo no tiene responsable, el backlog se vuelve un espacio de nadie. En cambio, cuando aparecen responsable, autoridad, consultor e informado, el problema deja de ser abstracto y empieza a tener ruta de accion.

Esa claridad no resuelve sola las brechas, pero si ordena la conversacion. Tambien ayuda a que el proyecto no cargue todo el peso sobre tecnologia. Hay temas que dependen del ETL, si. Pero otros dependen de definiciones funcionales, disciplina operativa y decision sobre catalogos. El RACI hace visible justamente ese reparto.

## 7. Lo que agrega el linaje

El linaje detallado agrega una capa muy valiosa porque muestra que el trabajo no se quedo solo en identificar datasets. Tambien entra a describir columnas, definiciones, precedentes, transformaciones, llaves de union, filtros, manejo de nulos, criterios de calidad y errores comunes. Eso eleva bastante el nivel de madurez documental del proyecto.

La hoja de bases deja ver capas, dominio, frecuencia, responsables y dependencias. La hoja de linaje, por su parte, baja a una lectura mucho mas operativa y muestra donde se transforma, donde se une, donde se filtra y donde suelen aparecer errores. Ese tipo de documento no cierra por si solo la operacion, pero acorta mucho el tiempo para explicar el sistema, entrenar a alguien nuevo o justificar un cambio.

Tambien deja una señal importante: aun hay transformaciones descritas como manuales o de limpieza basica. Eso confirma que el backlog no estaba sobredimensionado. Habia trabajo real por automatizar y por formalizar. La diferencia es que ahora esa manualidad ya esta mas localizada y menos normalizada como costumbre.

## 8. Comparacion backlog versus mejora real

### 8.1 Lo que si mejoro de manera visible

- La integracion end-to-end del ETL mejoro de forma clara. Ya existe una corrida mas controlada, con logs, estados y evidencia SQL.
- La trazabilidad mejoro bastante. El residual ya no queda escondido; queda explicado por motivo, por modulo y por tabla origen.
- Las reglas MDM para modulo y geografia maduraron. Test Block es el mejor ejemplo de una excepcion que dejo de ser ruido y paso a ser una regla controlada.
- El gobierno operativo mejoro. Hoy se puede distinguir lo que esta cerrado, lo que esta estable y lo que sigue abierto por negocio.
- La separacion entre deuda tecnica, deuda de datos y deuda funcional ya se ve en la practica, no solo en el discurso.

### 8.2 Lo que mejoro pero no esta cerrado

- La estandarizacion avanzo, pero siguen existiendo variedades y geografias pendientes de catalogacion.
- La automatizacion subio bastante, pero todavia hay frentes con manualidad residual y fuentes menos maduras.
- El linaje ya esta mucho mejor dibujado, aunque algunas transformaciones siguen dependiendo de limpieza o criterio operativo.
- El proyecto ya tiene mejores llaves y mas control, pero aun no todas las fuentes llegan con la misma calidad.

### 8.3 Lo que sigue abierto

- VIVERO sigue siendo una decision de negocio/MDM.
- Las variedades pendientes siguen siendo backlog maestro, no un tema cerrado de codigo.
- El frente de cama no quedo revalidado en la corrida actual y sigue abierto, aunque no bloqueante.
- Algunos dominios todavia no tienen el mismo nivel de validacion operativa que Conteo y Tasa.

## 9. Conexion con las mejoras ya implementadas en el ETL

El avance del backlog no debe leerse como algo aparte del ETL. En realidad, muchas de las mejoras ya implementadas son la traduccion operativa de ese backlog. El baseline formal, las reglas MDM, la cuarentena, la validacion mas honesta del residual, la resolucion de Test Block y la posibilidad de decir que VIVERO es un caso funcional pendiente son ejemplos concretos de backlog convertido en accion.

Esto tambien mejora el linaje de manera practica. No solo porque existan mejores documentos, sino porque la ejecucion del pipeline ya deja una traza mas interpretable: que entro, que salio, que no paso y por que no paso. Esa es una mejora de linaje vivida, no solamente documentada.

## 10. Diagnostico ejecutivo y siguientes pasos

La lectura final es que el proyecto ya logro algo importante: pasar de problemas dispersos a una agenda priorizada, trazable y ejecutable. Eso no cierra el backlog, pero si cambia la velocidad y la calidad con la que puede resolverse.

La recomendacion es mantener esta misma logica. Primero, seguir cerrando lo que ya esta bastante acotado, como las decisiones de VIVERO y las variedades pendientes. Segundo, no perder la disciplina del baseline con evidencia real. Y tercero, seguir usando backlog, RACI y linaje como herramientas de gobierno, no como documentos decorativos.

En resumen: el mayor avance no es solo tecnico. Es haber convertido una operacion muy dependiente de memoria, correccion manual y criterio disperso en un sistema que ya puede explicarse, medirse y mejorarse con bastante mas orden.
