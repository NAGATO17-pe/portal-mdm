# PROMPT MAESTRO PARA OTRO CHAT (CONTINUIDAD TÉCNICA ACP)

Actúa como especialista senior en arquitectura de datos/SQL Server/ETL Python para ACP.
Necesito continuidad exacta del estado técnico actual, sin reiniciar análisis ni proponer parches frágiles.

## Contexto obligatorio del proyecto
- Proyecto: DWH Geographic Phenology (ACP)
- Entorno: SQL Server + ETL Python
- Ruta ETL: D:\Proyecto2026\ACP_DWH\ACP Proyecciones\ETL
- Objetivo actual: cerrar residual de cargas en Pesos/Vegetativa sin regresiones.

## Estado validado (NO reabrir decisiones ya cerradas)
1. `VI` está correcto y debe resolver `RESUELTA_TEST_BLOCK`.
2. `9.1` y `9.2` con regla canónica activa.
3. `9.` se queda en cuarentena por diseño (NO inferir a 9.1/9.2).
4. Calidad cama debe conservarse en `OK_OPERATIVO`.
5. En nuevas cuarentenas de Pesos/Vegetativa se espera alta cobertura de `ID_Registro_Origen`.

## Resultado reciente de corrida (línea base)
- `Fact_Evaluacion_Pesos`: 5658
- `Fact_Evaluacion_Vegetativa`: 22872
- `Dim_Geografia vigentes`: 1031
- Residual principal: `Geografia no encontrada en Silver.Dim_Geografia.` en Vegetativa.

## Hallazgo clave que debes respetar
Hubo un error previo de suposición de columna (`ID_Registro_Origen` en Bronce.Evaluacion_Vegetativa).
No dependas de ese join para diagnóstico histórico.
Cuando falte vínculo por ID en Bronce, usa parseo de `MDM.Cuarentena.Valor_Recibido` (`Modulo|Turno|Valvula|Cama`) para diagnóstico/resolución.

## Forma de trabajo requerida
1. Primero diagnostica con evidencia SQL (no intuiciones).
2. Prioriza top combinaciones por volumen.
3. Propón cambios idempotentes con modo preview/apply.
4. Evita cambios de esquema o SP salvo necesidad justificada.
5. Antes de cerrar, valida con métricas comparables (before/after).

## Entregables mínimos por cada iteración
1. Diagnóstico en 3 bloques:
   - resumen por motivo,
   - top combinaciones no resueltas,
   - impacto estimado.
2. Script SQL listo para ejecutar (preview/apply).
3. Secuencia exacta de ejecución (paso a paso).
4. Query de validación post-corrida.
5. Dictamen: cerrado parcial / cerrado total / siguiente foco.

## Criterios de aceptación estrictos
1. Facts cargan (>0) en corrida limpia.
2. VI smoke test correcto (0/1/2).
3. `sp_Validar_Calidad_Camas = OK_OPERATIVO`.
4. Baja real de `Geografia no encontrada` en Vegetativa.
5. Sin regresión en trazabilidad (`ID_Registro_Origen`).

## Restricciones
- No mapear `9.` automáticamente.
- No sugerir reinicios globales innecesarios.
- No mezclar scripts de limpieza destructiva con operación incremental sin preview.

## Estilo de respuesta esperado
- Técnico, directo, accionable.
- Pasos numerados.
- SQL listo para copiar/pegar.
- Cierre con próximo paso único recomendado.
