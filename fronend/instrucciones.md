Actuar como arquitecto de software frontend senior especializado en aplicaciones corporativas orientadas a datos. El backend ya se encuentra implementado en FastAPI y expone endpoints REST, SSE y WebSockets. Además, Apache Spark ya está definido en el backend como motor y orquestador de los procesos ETL. La tarea consiste en diseñar y desarrollar un portal de control ETL con Next.js, TypeScript, Tailwind CSS y, opcionalmente, shadcn/ui, bajo criterios estrictos de mantenibilidad, modularidad, reutilización, accesibilidad, escalabilidad, rendimiento, observabilidad y consistencia visual.

Contexto operacional

El sistema debe permitir la ejecución, supervisión y administración de procesos ETL en un entorno corporativo. La capa frontend no debe reimplementar lógica de orquestación, ya que dicha responsabilidad pertenece al backend y a Spark. La función del frontend consiste en representar de forma clara, segura y eficiente el estado operacional del sistema, facilitar la interacción del usuario con los flujos disponibles y exponer información relevante sobre ejecuciones, etapas, métricas, logs, errores y estados del procesamiento distribuido.

La solución frontend debe integrarse con los servicios existentes del backend sin alterar sus contratos, salvo que se detecten inconsistencias técnicas que deban documentarse explícitamente. La interfaz debe transmitir una estética profesional, limpia y moderna, con soporte para modo claro y oscuro, y con una línea visual sobria inspirada en interfaces tecnológicas orientadas a datos, usando gradientes, transparencias y recursos visuales discretos sin comprometer legibilidad, rendimiento ni usabilidad.

Objetivo general

Construir un portal frontend robusto para:

Lanzar ejecuciones ETL a través del backend.
Visualizar progreso en tiempo real.
Consultar logs, métricas y estados históricos.
Administrar datos maestros y configuraciones operativas.
Gestionar autenticación y sesión de usuario.
Representar correctamente la ejecución de pipelines orquestados por Spark.
Mantener una arquitectura modular, reutilizable, escalable y fácilmente testeable desde el inicio.

Alcance funcional respecto de Spark

Apache Spark ya está definido y operativo en el backend como componente responsable de la orquestación y ejecución de cargas ETL. El frontend debe asumir este hecho como parte del contexto técnico fijo del sistema. En consecuencia, la solución debe:

Mostrar el estado de ejecuciones disparadas sobre Spark.
Reflejar etapas, progreso, transiciones de estado y resultados reportados por el backend.
Exponer métricas, eventos, logs o trazas vinculadas con jobs, batches, stages o pipelines, siempre que dicha información sea entregada por FastAPI.
Permitir al usuario identificar ejecuciones activas, completadas, fallidas, canceladas o en espera.
Modelar claramente la relación entre una ejecución funcional de negocio y su ejecución técnica en Spark, si el backend expone ambos niveles.
Mantener una separación estricta entre visualización y orquestación, sin asumir lógica interna de Spark no expuesta por la API.

Restricciones técnicas obligatorias

Usar Next.js con App Router y TypeScript.
Usar Tailwind CSS como base de estilos.
Permitir el uso de shadcn/ui para componentes base y patrones accesibles.
Mantener separación estricta entre presentación, lógica de negocio, acceso a datos, estado y utilidades.
No exponer secretos, tokens ni credenciales en el cliente.
Priorizar componentes reutilizables, composición sobre duplicación y bajo acoplamiento entre módulos.
Garantizar compatibilidad responsive para móvil, tablet y escritorio.
Cumplir criterios de accesibilidad WCAG 2.1 como mínimo.
Diseñar la solución para evolución incremental, pruebas automatizadas y despliegue continuo.
Tratar la integración con Spark exclusivamente a través del backend FastAPI y de los contratos definidos por este.

Principios de arquitectura

Definir una arquitectura clara, extensible y orientada a dominio, evitando estructuras monolíticas centradas únicamente en tipos de archivo. La organización debe facilitar mantenimiento, evolución funcional, escalado del producto y reutilización transversal. Cada módulo debe poder evolucionar de forma independiente con impacto mínimo sobre el resto del sistema.

La estructura base debe contemplar, como mínimo:
app/ para rutas, layouts y páginas.
components/ para componentes visuales reutilizables y composables.
features/ para encapsular funcionalidades por dominio, por ejemplo dashboard, ejecuciones, observabilidad, mdm, autenticación y configuración.
services/ para clientes HTTP, consumo de SSE o WebSockets y adaptadores de backend.
hooks/ para lógica reutilizable de comportamiento y estado.
lib/ para utilidades compartidas, helpers, validaciones, constantes y configuraciones.
types/ o schemas/ para contratos tipados, DTOs y validaciones con Zod o solución equivalente.
store/ si se requiere estado global.
styles/ únicamente para capas globales mínimas, fuentes y variables.

Requisito transversal de modularidad y reutilización

Toda la solución debe diseñarse con enfoque modular, reutilizable, componible y desacoplado. Cada componente, hook, servicio, esquema, utilidad, adaptador o módulo debe tener una única responsabilidad claramente definida y una interfaz estable. Deben evitarse implementaciones duplicadas, dependencias implícitas, lógica embebida en vistas concretas y estructuras difíciles de extender o mantener.

Se debe exigir que:

Los componentes visuales sean independientes de la lógica de negocio.
Las vistas ensamblen componentes reutilizables en lugar de contener lógica extensa.
Los hooks encapsulen comportamiento compartido, sincronización con eventos y manejo de estado.
Los servicios abstraigan completamente la comunicación con el backend.
Los tipos, esquemas, constantes y utilidades sean centralizados y reutilizables.
Las variantes visuales se resuelvan mediante props, configuración o composición, no mediante duplicación de componentes.
Las funciones y módulos sean fácilmente testeables de forma unitaria.
La arquitectura permita reutilizar piezas en múltiples pantallas sin refactorización significativa.
La incorporación de nuevas entidades, nuevos flujos o nuevas vistas no obligue a reescribir módulos existentes.
Se prioricen composición, cohesión alta, acoplamiento bajo e inversión de dependencias.

Modelo funcional sugerido

La solución debe distinguir al menos las siguientes áreas:

Dashboard operativo.
Gestión de ejecuciones ETL.
Observabilidad y logs.
Gestión de datos maestros.
Configuración.
Autenticación y autorización.

Cada área debe definirse como módulo funcional independiente, con sus propios componentes, hooks, servicios, tipos y pruebas, reutilizando una base común compartida cuando corresponda.

Diseño de interfaz y sistema visual

Definir un sistema de diseño consistente mediante tokens de color, tipografía, espaciado, radios, sombras, transiciones, estados e iconografía. Centralizar dichos tokens en Tailwind y, cuando resulte conveniente, en variables CSS. El diseño debe soportar modo claro y oscuro de forma nativa y consistente.

La interfaz debe incluir:

Layout principal con barra lateral, cabecera y área de contenido.
Navegación clara entre Dashboard, Control de Ejecuciones, Observabilidad, MDM y Configuración.
Uso consistente de patrones visuales para formularios, tablas, tarjetas, diálogos, filtros, indicadores de estado, alertas, métricas y barras de progreso.
Fondos sutiles con gradientes y transparencias controladas para aportar profundidad visual sin degradar rendimiento ni legibilidad.
Estados visuales completos: loading, empty, success, error, disabled, updating y reconnecting.
Jerarquía visual clara para lectura operativa rápida.
Componentes preparados para reutilización intermodular.

Accesibilidad

Todos los componentes deben ser navegables por teclado y compatibles con lectores de pantalla. Deben emplearse roles ARIA apropiados, etiquetas descriptivas, foco visible, contraste suficiente y mensajes de error comprensibles. Los componentes personalizados deben mantener el mismo nivel de accesibilidad que los componentes base de librerías como Radix UI o shadcn/ui.

Requisitos funcionales mínimos

Autenticación
Implementar login y logout con gestión de sesión segura. Si se usa NextAuth, integrarlo correctamente con el backend y almacenar tokens solo mediante mecanismos seguros, preferiblemente cookies HttpOnly. Definir expiración, renovación de sesión, protección de rutas y tratamiento de permisos.
Dashboard
Mostrar resumen operativo con métricas clave, estado general del sistema, últimas ejecuciones, alertas, incidencias recientes y accesos directos a acciones frecuentes.
Control de ejecuciones ETL
Permitir iniciar procesos ETL mediante formularios validados, mostrar estados de ejecución, progreso en tiempo real, tiempos estimados, errores y acciones disponibles según el ciclo de vida reportado por el backend.
Observabilidad y Spark
Implementar vistas que permitan visualizar la ejecución técnica asociada a Spark cuando el backend provea dicha información. Esto puede incluir identificadores de job, estado de stages, métricas de procesamiento, eventos de ejecución, duración, throughput, errores y logs asociados. El frontend debe representar estos datos de manera clara y desacoplada, sin inferir comportamiento no expuesto por la API.
Logs
Implementar visualización de logs con actualización incremental, scroll eficiente, filtrado, búsqueda y segmentación por ejecución, fecha, severidad, etapa o componente, según disponibilidad del backend.
Gestión de datos maestros
Incluir pantallas para listar, consultar, filtrar, editar o registrar entidades maestras relevantes para la operación, con validación de datos y manejo de permisos.
Configuración
Permitir administración de parámetros operativos, preferencias visuales y elementos de configuración requeridos por el negocio, bajo controles de acceso definidos.

Integración con backend

Toda la interacción con FastAPI debe quedar encapsulada en servicios o adaptadores específicos. No se debe dispersar lógica de acceso a datos dentro de componentes de presentación. Cada integración debe definirse con contratos tipados y manejo consistente de errores, reintentos, estados de carga, reconexión y cancelación de solicitudes cuando corresponda.

Se deben contemplar como mínimo:

Cliente HTTP tipado.
Hook o adaptador para SSE o WebSockets.
Funciones como startRun(params), fetchRuns(filters), fetchRunDetails(runId), fetchLogs(runId), fetchDashboardMetrics(), fetchSparkExecution(runId), fetchSparkMetrics(runId) y equivalentes para MDM y configuración, ajustadas a los endpoints reales disponibles.
Modelado explícito de DTOs, responses, errores y estados intermedios.
Validación de payloads de entrada y salida cuando resulte viable.
Adaptadores desacoplados para mapear contratos del backend a modelos de UI reutilizables.

Estado y manejo de datos

La estrategia de estado debe distinguir claramente entre:

Estado del servidor.
Estado local de UI.
Estado global compartido.
Estado derivado.

Usar SWR o React Query para datos remotos, caché, revalidación e invalidación controlada. Reservar contexto global o una librería de estado únicamente para necesidades transversales reales. Evitar componentes sobrecargados con lógica de coordinación de datos.

Tiempo real

La solución debe considerar que el backend puede emitir eventos mediante SSE o WebSockets. El frontend debe contar con módulos reutilizables para:

Suscripción a eventos.
Reconexión controlada.
Normalización de eventos.
Manejo de estados transitorios.
Actualización eficiente de UI.
Aislamiento de la lógica de streaming respecto de la capa visual.

Esta capacidad debe diseñarse como infraestructura reusable para múltiples módulos y no como implementación específica de una única pantalla.

Rendimiento

Aplicar optimizaciones desde el diseño:

Usar SSR, SSG o ISR según el tipo de contenido.
Cargar componentes pesados de forma diferida con dynamic().
Minimizar renders innecesarios y propagación excesiva de props.
Diseñar tablas, listas, logs y paneles de ejecución con estrategias eficientes para grandes volúmenes de datos.
Limitar CSS global y aprovechar tokens y utilidades de Tailwind.
Optimizar imágenes, iconos y recursos estáticos.
Prever virtualización cuando existan listados extensos.
Garantizar tiempos de interacción fluidos en dispositivos de gama media.
Diseñar componentes de observabilidad escalables para ejecuciones concurrentes y flujos de eventos continuos.

Internacionalización

Si existe posibilidad de multilenguaje, preparar la solución desde el inicio mediante next-intl o alternativa equivalente. Centralizar textos, formatos de fecha, hora, números y mensajes de interfaz para evitar cadenas embebidas en componentes.

Calidad, pruebas y validación

La solución debe incorporar controles de calidad desde la base:

ESLint y Prettier con reglas consistentes.
TypeScript en modo estricto.
Testing Library y Jest o Vitest para pruebas unitarias y de integración.
Playwright para flujos críticos end to end.
eslint-plugin-jsx-a11y y jest-axe para accesibilidad.
Validación de componentes, hooks, servicios, adaptadores y flujos principales.
Cobertura suficiente en módulos críticos del negocio.
Pruebas específicas para componentes reutilizables y módulos de integración con eventos en tiempo real.

CI/CD y despliegue

Definir pipeline de integración y despliegue continuo con GitHub Actions u otra solución equivalente. El pipeline debe ejecutar instalación, verificación de tipos, lint, pruebas unitarias, pruebas de accesibilidad, build y validaciones necesarias para asegurar estabilidad del frontend antes del despliegue. El despliegue debe ser compatible con Vercel, contenedores Docker o la infraestructura objetivo del proyecto.

Seguridad

Aplicar prácticas mínimas obligatorias:

No exponer secretos en el cliente.
Gestionar autenticación con cookies seguras y políticas adecuadas.
Aplicar HTTPS.
Definir Content Security Policy.
Validar entradas del usuario.
Sanitizar contenido renderizado cuando sea necesario.
Proteger rutas y acciones sensibles según roles o permisos.
Evitar almacenamiento inseguro de credenciales o tokens.
No exponer detalles internos de Spark que no deban ser visibles al usuario final sin una política de autorización explícita.

Entregables esperados

La respuesta debe producir:

Propuesta de arquitectura frontend detallada y justificada.
Estructura de carpetas recomendada.
Definición de módulos, responsabilidades e interfaces entre capas.
Estrategia de reutilización de componentes, hooks, servicios, adaptadores y utilidades.
Propuesta de sistema de diseño y tematización.
Lista de componentes base y componentes de dominio.
Estrategia de integración con FastAPI y con los flujos observables derivados de Spark a través del backend.
Estrategia de estado, caché, streaming y tiempo real.
Criterios de accesibilidad, rendimiento, seguridad y pruebas.
Ejemplos de implementación cuando aporten claridad.
Recomendaciones para escalabilidad futura.
Convenciones de desarrollo para mantener uniformidad técnica.

Formato de salida requerido

La respuesta debe entregarse en secciones claramente tituladas y en lenguaje técnico formal. Debe incluir decisiones arquitectónicas, justificación breve de cada decisión y propuestas accionables. Cuando corresponda, incluir ejemplos de estructura de carpetas, contratos tipados, pseudocódigo o fragmentos de código mínimos y precisos. No incluir explicaciones genéricas sin aplicación directa al caso.

Criterios de decisión

Ante varias alternativas técnicas, seleccionar la opción que ofrezca mejor equilibrio entre mantenibilidad, reutilización, claridad, accesibilidad, rendimiento, observabilidad y escalabilidad. Si se detectan supuestos no definidos, explicitar el supuesto adoptado y continuar sin bloquear la propuesta.

Límite de procesamiento

No simplificar la arquitectura sacrificando modularidad o reutilización. No generar una solución genérica, superficial o centrada solo en componentes visuales. Toda recomendación debe poder traducirse a implementación real en un proyecto corporativo de mediana o alta complejidad, considerando que la orquestación ETL ya depende de Spark en backend y que el frontend debe integrarse con ese contexto de forma precisa, desacoplada y reutilizable.
