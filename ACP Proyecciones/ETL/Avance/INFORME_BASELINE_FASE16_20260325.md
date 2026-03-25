# INFORME BASELINE FASE 16 - 2026-03-25

## 1) Resumen ejecutivo
El baseline de hoy confirma que la plataforma quedo estable para los dos facts criticos:
- `Silver.Fact_Evaluacion_Pesos`
- `Silver.Fact_Evaluacion_Vegetativa`

Resultado principal:
- La regla `VI => Test Block` funciona y no se rompio.
- La calidad de cama sigue en `OK_OPERATIVO`.
- La trazabilidad nueva de cuarentena ya registra `ID_Registro_Origen` al 100% en esta corrida limpia.

## 2) Resultados finales de la corrida limpia
### Carga
- Fact_Evaluacion_Pesos: `5658` insertados, `14` rechazados, `14` cuarentena.
- Fact_Evaluacion_Vegetativa: `22833` insertados, `584` rechazados, `584` cuarentena.

### Calidad y resolucion
- Smoke VI (cama 0/1/2): `RESUELTA_TEST_BLOCK` en los 3 casos.
- `Silver.sp_Validar_Calidad_Camas`: `OK_OPERATIVO`.

### Check estructural de cuarentena (corrida del dia)
- Bronce.Evaluacion_Pesos: `14` nuevas cuarentenas, `14` con ID origen (100.00%).
- Bronce.Evaluacion_Vegetativa: `584` nuevas cuarentenas, `584` con ID origen (100.00%).

## 3) Criterios de aceptacion (estado)
1. Fact_Evaluacion_Pesos cargo > 0: `CUMPLIDO` (5658).
2. Fact_Evaluacion_Vegetativa cargo > 0: `CUMPLIDO` (22833).
3. VI resuelve Test Block en 0/1/2: `CUMPLIDO`.
4. Calidad cama = OK_OPERATIVO: `CUMPLIDO`.
5. Con_ID_Registro_Origen alto y coherente: `CUMPLIDO` (100%).

## 4) Residual actual (foco real)
### Pesos (14 total)
- Geografia especial requiere catalogacion o regla en MDM_Geografia: `8`.
- No se pudo calcular peso promedio de baya: `3`.
- Peso fuera de rango (12.46 / 19.7263 / 25.0821): `3`.

### Vegetativa (584 total)
- Geografia no encontrada en Silver.Dim_Geografia: `497`.
- Plantas en floracion invalida o mayor al total evaluado: `72`.
- Cantidad de plantas evaluadas invalida: `13`.
- Fecha invalida o fuera de campana: `2`.

## 5) Interpretacion tecnica
- El problema global de geografia por `VI` esta cerrado.
- El residual dominante ahora es puntual:
  - `Modulo=9.` (sin submodulo) en Pesos.
  - combinaciones de geografia no mapeadas en Vegetativa.

## 6) Riesgos y recomendaciones
1. No inferir automatico `9.` hacia `9.1/9.2` (riesgo de datos mal clasificados).
2. Priorizar backlog operativo de Vegetativa (497) por impacto.
3. Mantener corrida diaria con snapshot para medir tendencia real y evitar percepcion por acumulado historico.

## 7) Estado de congelado
Baseline Fase 16 del `2026-03-25`: `CONGELADO`.
