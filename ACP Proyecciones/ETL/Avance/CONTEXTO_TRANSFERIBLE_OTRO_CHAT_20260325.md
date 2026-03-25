# CONTEXTO TRANSFERIBLE PARA OTRO CHAT (ACP DWH)

Fecha de corte: 2026-03-25
Proyecto: DWH Geographic Phenology (ACP)
Ruta operativa ETL: D:\Proyecto2026\ACP_DWH\ACP Proyecciones\ETL

## 1) Objetivo operativo actual
Cerrar brechas de carga en:
1. `Silver.Fact_Evaluacion_Pesos`
2. `Silver.Fact_Evaluacion_Vegetativa`

sin romper:
- reglas MDM ya estabilizadas,
- calidad de cama,
- trazabilidad en cuarentena.

## 2) Estado funcional confirmado
1. `VI` resuelve correctamente como `RESUELTA_TEST_BLOCK`.
2. `9.1` y `9.2` tienen regla canónica activa.
3. `9.` (sin submódulo) se mantiene en cuarentena por diseño (NO inferir automáticamente).
4. `Silver.sp_Validar_Calidad_Camas` permanece en `OK_OPERATIVO`.
5. Nuevas cuarentenas de Pesos/Vegetativa registran `ID_Registro_Origen` con cobertura del 100% en las últimas corridas limpias observadas.

## 3) Resultado de corrida limpia más reciente
Resumen relevante:
1. `Fact_Evaluacion_Pesos`: 5658 insertados, 14 rechazados.
2. `Fact_Evaluacion_Vegetativa`: 22872 insertados, 545 rechazados.
3. `Dim_Geografia vigentes`: 1031.
4. Calidad cama: `OK_OPERATIVO`.

Interpretación:
- Pesos quedó prácticamente estable.
- Vegetativa aún concentra residual relevante en geografía no encontrada.

## 4) Residual actual (foco real)
### Pesos
- Residual pequeño.
- Geografía pendiente asociada principalmente a `Modulo=9.` (esperado por diseño).

### Vegetativa
- Residual principal: `Geografia no encontrada en Silver.Dim_Geografia.`
- Top combinaciones pendientes observadas en diagnóstico:
  - Mayormente `9.1` en turnos 03/04/05/06 con válvulas y camas específicas.
  - También algunas `9.2` en turnos 01/03/09.

Conclusión técnica:
- El problema restante ya no es de regla VI ni de parser de módulo.
- Es cobertura de catálogo geográfico operativo (faltan firmas reales en catálogo/dim o no vigentes).

## 5) Hallazgo crítico de diagnóstico (importante para no repetir error)
Se detectó y corrigió un error de suposición:
- Se asumió que `Bronce.Evaluacion_Vegetativa` tenía `ID_Registro_Origen` con ese nombre.
- En el esquema real no existe esa columna con ese nombre.

Corrección metodológica:
- Diagnóstico sin depender de ID de Bronce.
- Se parseó `MDM.Cuarentena.Valor_Recibido` con formato:
  - `Modulo=... | Turno=... | Valvula=... | Cama=...`
- Con esos tokens se evaluó resolución por `Silver.sp_Resolver_Geografia_Cama`.

Lección:
- Para backlog histórico, `Valor_Recibido` es una fuente robusta cuando no hay vínculo por ID directo en Bronce.

## 6) Scripts clave ya creados y vigentes
En `D:\Proyecto2026\ACP_DWH\ACP Proyecciones\ETL`:
1. `fase14_patch_sp_resolver_vi.sql`
2. `fase14_seed_vi_test_block.sql`
3. `fase15_cierre_geografia_pesos_vegetativa.sql`
4. `fase15_reapertura_backlog_geografia.sql` (ajustado para ambos motivos geográficos)
5. `fase15_reproceso_pesos_vegetativa.py`
6. `fase15_validacion_post_reproceso.sql`
7. `fase16_limpieza_hechos_hoy.sql`
8. `fase16_snapshot_baseline.sql`
9. `fase17_catalogar_geografia_9_2_turnos_10_11.sql`

## 7) Flujo operativo recomendado (sin improvisar)
1. Ejecutar snapshot diagnóstico.
2. Identificar top firmas no resueltas (`Geografia no encontrada`).
3. Catalogar faltantes (preview/apply).
4. Ejecutar `sp_Upsert_Cama_Desde_Bronce`.
5. Ejecutar `sp_Validar_Calidad_Camas`.
6. Reapertura dirigida de backlog geográfico.
7. Reproceso dirigido (`fase15_reproceso_pesos_vegetativa.py`) o pipeline completo.
8. Validación post-corrida con KPIs + smoke VI + trazabilidad ID.

## 8) Criterios de aceptación operativa
Se considera avance real solo si se cumplen simultáneamente:
1. `Fact_Evaluacion_Pesos > 0` y `Fact_Evaluacion_Vegetativa > 0` en corrida limpia.
2. VI smoke (`cama 0/1/2`) = `RESUELTA_TEST_BLOCK`.
3. `sp_Validar_Calidad_Camas = OK_OPERATIVO`.
4. `% con ID_Registro_Origen` en nuevas cuarentenas >= 98%.
5. Tendencia descendente de `Geografia no encontrada` en Vegetativa.

## 9) Reglas de no regresión
NO hacer:
1. No mapear `9.` automáticamente a `9.1` o `9.2`.
2. No alterar SP de VI si ya está estable.
3. No usar scripts de limpieza masiva sin preview.

SI hacer:
1. Mantener enfoque incremental por evidencia.
2. Priorizar catálogo geográfico faltante sobre parches de parser.
3. Validar siempre con snapshot post-corrida.

## 10) Analogía operativa (equipo no técnico)
- `VI`: carril exclusivo ya señalizado (funciona).
- `9.`: dirección incompleta (no se adivina).
- `Geografia no encontrada`: dirección válida pero no registrada en el GPS.
- `ID_Registro_Origen`: número de ticket para rastrear exactamente qué registro falló.
