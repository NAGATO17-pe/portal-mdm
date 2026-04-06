# Parche Temporal de GeografÃ­a para Tasa (2026-04-06)

## Objetivo
Documentar un **parche temporal propuesto** para cerrar el residual de
geografia en `Fact_Tasa_Crecimiento_Brotes` usando un Fundo/Sector genÃ©rico
mientras se define el mapeo real.

## Regla temporal propuesta (no ejecutada)
- Fundo: `Arandano Acp`
- Sector: `Sin_Sector_Mapa`
- Aplica solo a combinaciones faltantes de:
  - `Modulo/SubModulo`
  - `Turno`
  - `Valvula`
- No aplica a `Test Block`.

## Alcance
- Dominio afectado: `Fact_Tasa_Crecimiento_Brotes`.
- Fuente de combinaciones: `Bronce.Tasa_Crecimiento_Brotes` (Estado_Carga = `RECHAZADO`).
- InserciÃ³n en `MDM.Catalogo_Geografia` pendiente de ejecuciÃ³n.

## Riesgo conocido
Este parche permite carga, pero **mezcla geografia real** dentro de un
sector ficticio. No debe usarse para anÃ¡lisis por sector ni decisiones de
operaciÃ³n. Es exclusivamente un puente operativo.

## Estado de ejecuciÃ³n
- **NO ejecutado**
- **SIN evidencia en SQL**

## Criterio de salida del parche
Se elimina o desactiva cuando el equipo entregue el mapeo real
por `Modulo/Turno/Valvula` (o por archivo oficial).

## Script asociado
`Avance/parche_temporal_geografia_tasa_20260406.sql`

## Seguimiento
- Requiere documentar cuÃ¡ntas filas se insertaron.
- Requiere re-ejecutar `Dim_Geografia` y reprocesar `Fact_Tasa_Crecimiento_Brotes`.
