# Informe General Preliminar del Proyecto ACP Proyecciones

**Fecha:** 06 de abril de 2026  
**Tipo de lectura:** Descriptiva integral

## 1. Proposito de este informe

Este informe busca dejar una imagen general del proyecto, en un lenguaje claro y mas humano, sin entrar a codigo ni a detalle tecnico fino. La idea es explicar que hace hoy el proyecto, como estan conectadas sus partes principales y que sensacion deja el avance alcanzado hasta esta fecha.

La lectura incorpora el analisis del repositorio completo, los documentos operativos del ETL, el baseline validado con evidencia SQL, el documento de dominio de datos, la ruta de Mapeo de Procesos, el backlog, el scorecard, el RACI y el material de linaje disponible.

## 2. Vision general del proyecto

La primera impresion es que ACP Proyecciones ya no se ve como una suma de archivos de trabajo ni como un conjunto de scripts sueltos. Hoy se ve como una plataforma de datos en consolidacion. No esta terminada, pero ya tiene forma, direccion y una arquitectura que empieza a sostener la operacion con bastante mas orden.

El proyecto combina una base de datos analitica, un ETL por capas, un backend para exponer servicios y control, y un portal MDM pensado para que el equipo funcional pueda intervenir catalogos, cuarentenas y reglas sin depender siempre de cambios de codigo. Esa combinacion es importante porque convierte el proyecto en algo mas cercano a un producto interno que a una herramienta aislada.

## 3. Que hace cada bloque principal

### 3.1 ETL

El ETL recibe archivos operativos, los carga en una capa inicial, valida estructura y contenido, resuelve catalogos y reglas, separa lo que debe pasar de lo que debe quedar pendiente y despues publica informacion mas estable para analisis. En terminos simples, hace el trabajo duro de transformar una entrada muy variable en una salida mucho mas confiable.

Lo que mas destaca no es solo que cargue datos. Lo importante es que ya sabe dejar evidencia de que paso con cada frente: que se proceso, que se rechazo, que quedo en cuarentena y por que. Esa capacidad de explicar el resultado cambia mucho la madurez del sistema.

### 3.2 Base de datos

La base de datos esta organizada con un criterio de capas y de gobierno. El dato crudo, el transformado, el publicable, los catalogos maestros, la configuracion y la auditoria ya no viven mezclados. Eso le da mas claridad al proyecto y ayuda a que el equipo entienda mejor donde nace cada problema y donde deberia resolverse.

### 3.3 Backend

El backend le da al proyecto una capa de servicio y control. Ya no todo depende de abrir SQL o de correr scripts manualmente. Aparecen rutas, servicios, autenticacion, repositorios, control de ejecucion y una forma mas ordenada de interactuar con la operacion del ETL, con catalogos, con auditoria y con salud del sistema.

### 3.4 Portal MDM

El portal MDM cumple una funcion muy importante: traduce parte del problema de datos a un espacio donde el negocio puede intervenir sin tocar codigo. Eso se nota en la gestion de cuarentena, homologacion, catalogos y parametros. En la practica, este portal es el puente entre la excepcion operativa y la correccion controlada.

## 4. Lo que deja el dominio de datos

La lectura del documento de dominio de datos deja una sensacion positiva. Se nota un esfuerzo por definir el lenguaje central del negocio y por ordenar entidades que son clave para proyecciones y seguimiento: tiempo, campaña, geografia, personal, variedades, pesos, cosecha, plantas, fenologia, poda, proyecciones y meteorologia.

Eso influye bastante en lo que el proyecto esta haciendo, porque ayuda a bajar ambigüedad. Cuando el dominio esta mejor definido, tambien mejora la forma de construir catalogos, de resolver excepciones, de documentar el linaje y de automatizar sin perder el significado del dato.

## 5. Lo que aporta la ruta de Mapeo de Procesos

El mapeo de procesos muestra el proyecto desde otra perspectiva: la de las dependencias reales entre bases y procesos. Los flujogramas, la matriz de inputs y outputs, el backlog, el scorecard, el RACI y el linaje detallado ayudan a ver que la complejidad no estaba solo dentro del ETL. Tambien estaba en la forma en que una base alimentaba a otra y en como ciertas decisiones se sostenian todavia sobre trabajo manual.

Esta ruta aporta mucho porque aterriza la complejidad en algo visible. Muestra precedencias, responsables, frecuencia de actualizacion, transformaciones, errores comunes y relaciones entre datasets. No es solo documentacion bonita. Es una pieza que ayuda a ordenar la operacion y a darle contexto al backlog.

## 6. Que estaba roto o muy debil al inicio

La lectura conjunta de backlog, scorecard y linaje deja claro que el problema original no era simplemente cargar Excel. Habia demasiada manualidad, falta de integracion completa, dashboards que dependian de actualizaciones no estables, diferencias de nombres, llaves poco solidas, poca jerarquia entre fuentes y una dependencia fuerte del criterio humano para cerrar o reconciliar informacion.

Visto asi, el proyecto no estaba peleando solo con calidad de datos. Tambien estaba peleando con falta de gobierno, con ambigüedad operativa y con una trazabilidad insuficiente para sostener decisiones de proyeccion con confianza.

## 7. Que mejoro de verdad

El avance mas fuerte es que el proyecto ya salio de la etapa mas difusa. Hoy existe un baseline operativo formal, validado con evidencia SQL. Ya no se marca un dominio como estable solo por documento o por intuicion. Tambien existe una separacion mas clara entre dominios cerrados, estables con residual controlado y pendientes por negocio o por MDM.

Otro avance importante es la visibilidad del residual. Casos como Test Block dejaron de contaminar la lectura general del problema y pasaron a resolverse de manera mas controlada. Conteo y Tasa ya pueden leerse con mas honestidad: no estan perfectos, pero si estables y con residual explicable. Eso cambia mucho la conversacion con el equipo.

Ademas, el proyecto mejoro su capacidad de trazabilidad. La cuarentena ya no es un saco ciego; hoy funciona como un espacio donde el problema queda identificado por causa. Eso ayuda a no mezclar deuda tecnica con deuda de catalogacion o con decisiones pendientes del negocio.

## 8. Estado preliminar actual del proyecto

- El ETL ya no esta en falla sistemica.
- Test Block quedo cerrado a nivel geografico.
- Conteo y Tasa estan estables con residual controlado.
- VIVERO sigue abierto como decision de negocio/MDM.
- El frente de cama sigue abierto, pero no bloquea la corrida validada actual.
- El proyecto ya puede separar mejor lo tecnico, lo funcional y lo que depende de calidad de fuente.

## 9. Impresion general del avance

La impresion general es buena. No porque todo este terminado, sino porque ahora el proyecto se entiende mejor. Ya no parece una operacion sostenida principalmente por memoria individual y por arreglos repetidos. Se siente mas cerca de una plataforma que aprende, registra, clasifica y deja evidencia.

Eso tambien ayuda a imaginar el siguiente paso con mas orden. El backlog ya no luce inabarcable. Sigue siendo amplio, pero ahora esta mejor organizado. El dominio de datos da lenguaje. El linaje da contexto. El baseline da evidencia. Y el ETL da una base operativa sobre la cual realmente se puede seguir construyendo.

## 10. Cierre

En resumen, ACP Proyecciones ya no esta solo ordenando archivos. Esta ordenando su sistema de datos. Y aunque todavia hay trabajo por delante, lo que se ve hoy ya es un avance estructural, no cosmetico. Eso es probablemente lo mas rescatable de esta etapa.
