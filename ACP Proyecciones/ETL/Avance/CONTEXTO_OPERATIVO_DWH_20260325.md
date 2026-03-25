# CONTEXTO OPERATIVO DWH - 2026-03-25

## 1) Estado actual del sistema
El sistema ya paso de un problema estructural a un problema puntual de datos.

Antes:
- Fallaba resolucion de geografia para casos especiales.
- Habia reprocesos con ruido por falta de trazabilidad por ID en cuarentena.

Ahora:
- `VI` se resuelve como Test Block correctamente.
- En nuevas cuarentenas se registra `ID_Registro_Origen`.
- La calidad de cama se mantiene en `OK_OPERATIVO`.

## 2) Reglas vigentes clave
1. `VI` => Test Block (por Turno/Valvula).
2. `9.1` y `9.2` son validos por regla MDM.
3. `9.` (sin submodulo) queda en cuarentena por diseno.
4. Cama operativa esperada: `1..100`.

## 3) Que significa esto en lenguaje simple (ejemplos cotidianos)
- Ejemplo 1 (direccion incompleta):
  - `9.` es como decir "vivo en Calle 9" sin numero de casa.
  - El repartidor no inventa el numero: lo deja pendiente.
- Ejemplo 2 (zona especial):
  - `VI` es como una puerta exclusiva de personal autorizado.
  - No entra por la puerta principal de clientes (geografia operativa normal).
- Ejemplo 3 (ticket con ID):
  - Guardar `ID_Registro_Origen` es como tener numero de ticket en soporte.
  - Permite ubicar exactamente el caso y reabrirlo sin adivinar por texto.

## 4) Flujo diario recomendado
1. Ejecutar `py pipeline.py`.
2. Ejecutar `fase16_snapshot_baseline.sql`.
3. Revisar 5 semaforos:
   - Carga de Pesos > 0.
   - Carga de Vegetativa > 0.
   - VI smoke = RESUELTA_TEST_BLOCK.
   - Calidad cama = OK_OPERATIVO.
   - % cuarentena con ID origen alto.

## 5) Troubleshooting rapido
### Si insertados=0 en Pesos/Vegetativa
- Revisar `Estado_Carga` en Bronce.
- Revisar que no se aplico limpieza fuera de secuencia.

### Si sube "Geografia no encontrada"
- Es backlog de catalogo/regla pendiente.
- No es necesariamente falla de codigo.

### Si falla VI smoke
- Revisar `MDM.Regla_Modulo_Raw` y patch de SP.

## 6) Prioridad operativa actual
1. Reducir `Geografia no encontrada` en Vegetativa (`497`).
2. Mantener control de `9.` en cuarentena sin forzar inferencias.
3. Sostener corrida limpia y snapshot diario para tendencia real.

## 7) Bitacora tecnica cronologica (actualizado 2026-03-25)

### 7.1 Error clave detectado y corregido en diagnostico
Se detecto un error de suposicion durante el analisis:
- Se asumio que en `Bronce.Evaluacion_Vegetativa` existia una columna `ID_Registro_Origen`.
- En tu esquema real esa columna NO existe con ese nombre en Bronce.
- Resultado inmediato: query de diagnostico fallo con `Invalid column name 'ID_Registro_Origen'`.

Correccion aplicada:
1. Se descarto el join por ID contra Bronce para el diagnostico.
2. Se paso a un diagnostico robusto sin depender de IDs de Bronce.
3. Se parseo `MDM.Cuarentena.Valor_Recibido` (formato `Modulo|Turno|Valvula|Cama`).
4. Con esos tokens se ejecuto `Silver.sp_Resolver_Geografia_Cama` por combinacion.

Leccion operativa:
- Para soporte de cuarentena historica, la fuente mas estable para diagnostico rapido es `MDM.Cuarentena.Valor_Recibido`.
- El ID de Bronce es util cuando existe trazabilidad directa, pero no debe ser prerequisito para diagnosticar.

### 7.2 Hallazgo funcional real despues de corregir el enfoque
Tras ejecutar el diagnostico correcto:
1. `VI` se mantuvo estable en `RESUELTA_TEST_BLOCK`.
2. En `Pesos`, lo pendiente geografico quedo concentrado en token `9.`.
3. En `Vegetativa`, el foco mayor fue `GEOGRAFIA_NO_ENCONTRADA` en combinaciones operativas reales.

Interpretacion:
- No era un fallo de la regla VI.
- No era un fallo global del pipeline.
- Era brecha de cobertura de catalogo geografico + token ambiguo `9.` (esperado en cuarentena).

### 7.3 Ajustes aplicados despues del diagnostico
Se aplicaron dos acciones tecnicas:
1. Script de catalogacion focalizada para `9.2` (turnos 10/11, valvulas 1/2/3):
   - `fase17_catalogar_geografia_9_2_turnos_10_11.sql`
2. Ajuste de reapertura para considerar ambos motivos geograficos:
   - `Geografia especial requiere catalogacion o regla en MDM_Geografia.`
   - `Geografia no encontrada en Silver.Dim_Geografia.`

Resultado observado:
- El catalogo ya contenia esas combinaciones (preview mostro `Combos_Faltantes_Para_Insert = 0`).
- La reapertura dirigida devolvio `Pesos_Reabiertos=0` y `Vegetativa_Reabiertos=0` en ese corte, consistente con ausencia de backlog reabrible para esas firmas.

### 7.4 Verificacion del pipeline correcto (riesgo de doble archivo)
Se valido explicitamente que Python use el pipeline de produccion ETL:
- Correcto: `D:\Proyecto2026\ACP_DWH\ACP Proyecciones\ETL\pipeline.py`
- Existe otro `pipeline.py` en `Playground`, pero no tiene contexto completo de modulos (`config`), por eso no es el ejecutable real del ETL.

Recomendacion fija:
- Ejecutar siempre con ruta absoluta:
  - `py "D:\Proyecto2026\ACP_DWH\ACP Proyecciones\ETL\pipeline.py"`

### 7.5 Estado consolidado al cierre de esta iteracion
Corrida validada:
- `Fact_Evaluacion_Pesos`: 5658
- `Fact_Evaluacion_Vegetativa`: 22872
- `Dim_Geografia vigentes`: 1031
- `SP_Cama estado calidad`: `OK_OPERATIVO`
- `VI` smoke (cama 0/1/2): `RESUELTA_TEST_BLOCK`
- Trazabilidad en nuevas cuarentenas: `100%` con `ID_Registro_Origen`

Residual principal actual:
1. Pesos: residual pequeno (14), incluyendo `9.` (controlado por diseno).
2. Vegetativa: residual dominante en `Geografia no encontrada` (455 en el corte mostrado).
3. Reglas biologicas y fecha en Vegetativa: residual secundario.

### 7.6 Explicacion cotidiana para equipo no tecnico
- El caso VI ya es un carril exclusivo: ya no se confunde con trafico general.
- El token `9.` es direccion incompleta: no se adivina, se deja pendiente.
- `Geografia no encontrada` es como tener direccion valida pero no registrada en el GPS: hay que dar de alta la ruta en el catalogo.
- `ID_Registro_Origen` en cuarentena es el numero de ticket: permite volver exacto al registro sin buscar manualmente.

### 7.7 Criterio de verdad para las siguientes decisiones
A partir de esta fecha, se considera evidencia valida de avance cuando simultaneamente se cumpla:
1. `Fact_Evaluacion_Pesos > 0` y `Fact_Evaluacion_Vegetativa > 0` en corrida limpia.
2. `VI` = `RESUELTA_TEST_BLOCK` en smoke 0/1/2.
3. `sp_Validar_Calidad_Camas` = `OK_OPERATIVO`.
4. `% con ID_Registro_Origen` en nuevas cuarentenas >= 98%.
5. Tendencia descendente de `Geografia no encontrada` en Vegetativa.

## 8) Incidente cerrado - Fact_Ciclo_Poda (2026-03-25)

### 8.1 Sintoma observado
En corrida de pipeline se observó:
- `Fact_Ciclo_Poda -> 0 insertados | 5205 rechazados | 0 cuarentena`.

### 8.2 Diagnostico tecnico
Se valido con evidencia que:
1. Había 5205 filas `Estado_Carga='CARGADO'` en `Bronce.Evaluacion_Calidad_Poda`.
2. No era falla de variedad ni de fecha.
3. El rechazo masivo venía por `id_geo = NULL` en todas las filas.
4. Causa principal: el loader resolvía geografía solo por módulo, pero para este flujo se requiere granularidad operativa (`Modulo + Turno + Valvula`).

Adicionalmente se detectó:
- `ID_Evaluacion_Calidad_Poda` estaba nulo en esas filas.
- El ID operativo válido era `ID_Evaluacion_Poda`.

### 8.3 Correcciones aplicadas
Archivos ajustados:
1. `mdm/lookup.py`
   - Se reforzó fallback de resolución geográfica cuando el SP no devuelve ID y no hay granularidad operativa.
2. `silver/facts/fact_ciclo_poda.py`
   - Se incorporó lectura de `Turno_Raw` y `Valvula_Raw`.
   - Resolución geográfica actualizada a `Modulo + Turno + Valvula`.
   - Marcado de estado corregido para usar `ID_Evaluacion_Poda`.
   - Homologación de variedad enlazada con `columna_id_origen='ID_Evaluacion_Poda'`.

### 8.4 Validacion posterior al fix
Prueba directa del loader:
- Resultado: `{'insertados': 5205, 'rechazados': 0, 'cuarentena': []}`.

Validación posterior en tabla fact:
- `SELECT COUNT(*) AS CicloPoda_Hoy ...` devolvió `5825` filas del día.

### 8.5 Estado
- Incidente: **CERRADO**.
- Riesgo residual: bajo.
- Recomendación operativa: mantener check diario de `Fact_Ciclo_Poda` para detectar regresión temprana.

## 9) Incidente cerrado - Fact_Conteo_Fenologico (2026-03-25)

### 9.1 Sintoma observado
En corrida de pipeline se observó:
- `Fact_Conteo_Fenologico -> 0 insertados | 1565 rechazados | 1565 cuarentena`.

### 9.2 Diagnostico tecnico
Se identificó que el lote `Conteo frutos.xlsx` llegó en layout ancho:
1. `Color_Cinta_Raw`, `Estado_Raw`, `Cantidad_Organos_Raw` venían nulos.
2. Los valores operativos estaban dentro de `Valores_Raw` (pares `clave=valor`).
3. El loader original esperaba layout largo (estado/cantidad por fila) y geografía sin granularidad operativa.

Causa raíz:
- Desalineación entre layout de entrada y lógica del fact.

### 9.3 Correcciones aplicadas
Archivo ajustado:
- `silver/facts/fact_conteo_fenologico.py`

Cambios funcionales:
1. Soporte dual de layout (largo y ancho).
2. Parseo de `Valores_Raw` para extraer estados fenológicos y cantidades.
3. Resolución geográfica con `Modulo + Turno + Valvula`.
4. Resolución de cinta por fallback desde `Punto_Raw`.

Mapa aplicado de `Punto_Raw -> Color_Cinta`:
- 1=Roja, 2=Azul, 3=Verde, 4=Amarilla, 5=Blanca, 6=Naranja.

### 9.4 Validacion posterior al fix
Prueba directa del loader:
- Resultado: `{'insertados': 14085, 'rechazados': 0, 'cuarentena': []}`.

Estado:
- Incidente: **CERRADO**.
- Riesgo residual: bajo (mantener monitoreo por cambios de layout en origen).

### 9.5 Control recomendado
Validar en cada corrida:
1. `Fact_Conteo_Fenologico` > 0.
2. Si reaparece rechazo masivo, revisar primero estructura de `Valores_Raw`.
