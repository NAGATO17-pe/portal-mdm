# CHECKLIST OPERATIVO 5 MINUTOS — DWH Geographic Phenology

Fecha de referencia: 2026-03-25
Alcance: Operación diaria para validar salud de cargas críticas (`Fact_Evaluacion_Pesos`, `Fact_Evaluacion_Vegetativa`) antes y después de corrida.

---

## 1) Objetivo del checklist
Este checklist sirve para responder en menos de 5 minutos:
- ¿El pipeline está cargando?
- ¿La geografía VI sigue resolviendo como Test Block?
- ¿La cuarentena está bajo control o hay desvío?
- ¿La trazabilidad (`ID_Registro_Origen`) sigue íntegra?

Analogía rápida: como revisar tablero, aceite y presión de llantas antes de salir a carretera.

---

## 2) Frecuencia y momento de uso
1. Antes de corrida completa (pre-check).
2. Inmediatamente después de corrida (post-check).
3. Al cierre del día (corte operativo).

---

## 3) Pre-check (60–90 segundos)

### 3.1 Resolver VI smoke test
Esperado: 3 ejecuciones con `RESUELTA_TEST_BLOCK`.

```sql
EXEC Silver.sp_Resolver_Geografia_Cama @Modulo_Raw='VI', @Turno_Raw='01', @Valvula_Raw='1', @Cama_Raw='0';
EXEC Silver.sp_Resolver_Geografia_Cama @Modulo_Raw='VI', @Turno_Raw='01', @Valvula_Raw='1', @Cama_Raw='1';
EXEC Silver.sp_Resolver_Geografia_Cama @Modulo_Raw='VI', @Turno_Raw='01', @Valvula_Raw='1', @Cama_Raw='2';
```

Acción si falla:
1. Revisar regla activa en `MDM.Regla_Modulo_Raw` para `VI`.
2. Revisar catálogo geográfico test block (`Es_Test_Block=1`).

### 3.2 Calidad de camas
Esperado: `OK_OPERATIVO`.

```sql
EXEC Silver.sp_Validar_Calidad_Camas;
```

Acción si falla:
1. No correr carga masiva todavía.
2. Corregir calidad cama y reintentar pre-check.

---

## 4) Corrida
Ejecutar pipeline completo:

```powershell
py pipeline.py
```

---

## 5) Post-check (2–3 minutos)

### 5.1 Contexto SQL y bridge
Esperado: el resumen final y la base auditada corresponden a la misma instancia.

```sql
SELECT
    CAST(SERVERPROPERTY('ServerName') AS NVARCHAR(128)) AS Servidor_SQL,
    DB_NAME() AS Base_SQL;

SELECT COUNT(*) AS Bridge_Geografia_Cama
FROM Silver.Bridge_Geografia_Cama;
```

Interpretación recomendada:
1. Si `SP_Cama aptas > 0` y `Bridge_Geografia_Cama = 0`, abrir incidente del paso 6.
2. No confiar en un copy/paste de consola si contradice `Auditoria.Log_Carga`.

### 5.2 Carga de facts hoy
Esperado: ambos > 0.

```sql
DECLARE @hoy DATE = CAST(SYSDATETIME() AS DATE);

SELECT 'Silver.Fact_Evaluacion_Pesos' AS Tabla,
       COUNT(*) AS Filas_Hoy,
       MAX(Fecha_Sistema) AS Ultimo_Registro
FROM Silver.Fact_Evaluacion_Pesos
WHERE CAST(Fecha_Sistema AS DATE) = @hoy
UNION ALL
SELECT 'Silver.Fact_Evaluacion_Vegetativa',
       COUNT(*),
       MAX(Fecha_Sistema)
FROM Silver.Fact_Evaluacion_Vegetativa
WHERE CAST(Fecha_Sistema AS DATE) = @hoy;
```

Umbral guía:
1. Pesos: cercano a entrada esperada del día.
2. Vegetativa: cercano a entrada esperada del día.
3. Si alguno cae abruptamente contra su histórico inmediato, abrir incidente.

### 5.3 Cuarentena pendiente por motivo
Objetivo: identificar foco real, no “ruido”.

```sql
SELECT Tabla_Origen, Motivo, COUNT(*) AS Filas_Pendientes
FROM MDM.Cuarentena
WHERE Estado='PENDIENTE'
  AND Tabla_Origen IN ('Bronce.Evaluacion_Pesos','Bronce.Evaluacion_Vegetativa')
GROUP BY Tabla_Origen, Motivo
ORDER BY Tabla_Origen, Filas_Pendientes DESC;
```

Interpretación recomendada:
1. `Geografia especial...` en Pesos debe mantenerse muy bajo (actual: 8).
2. `Geografia no encontrada...` en Vegetativa es el foco principal (actual: 497).
3. `9.` sin submódulo en cuarentena: esperado por diseño.

### 5.4 Trazabilidad de nuevas cuarentenas
Esperado: `Con_ID_Registro_Origen` alto, idealmente 100%.

```sql
DECLARE @hoy DATE = CAST(SYSDATETIME() AS DATE);

SELECT Tabla_Origen,
       COUNT(*) AS Nuevas_Cuarentenas_Hoy,
       SUM(CASE WHEN ID_Registro_Origen IS NOT NULL THEN 1 ELSE 0 END) AS Con_ID_Registro_Origen,
       CAST(100.0 * SUM(CASE WHEN ID_Registro_Origen IS NOT NULL THEN 1 ELSE 0 END) / NULLIF(COUNT(*),0) AS DECIMAL(5,2)) AS Pct_Con_ID
FROM MDM.Cuarentena
WHERE CAST(Fecha_Ingreso AS DATE)=@hoy
  AND Tabla_Origen IN ('Bronce.Evaluacion_Pesos','Bronce.Evaluacion_Vegetativa')
GROUP BY Tabla_Origen;
```

Semáforo:
1. Verde: >= 98%
2. Amarillo: 90–97.99%
3. Rojo: < 90%

---

## 6) Criterios de aceptación diarios
Marcar “cumple” solo si:
1. `Fact_Evaluacion_Pesos` > 0
2. `Fact_Evaluacion_Vegetativa` > 0
3. `VI` resuelve `RESUELTA_TEST_BLOCK` (cama 0/1/2)
4. `sp_Validar_Calidad_Camas = OK_OPERATIVO`
5. Si `SP_Cama aptas > 0`, entonces `Bridge_Geografia_Cama > 0`
6. `Pct_Con_ID` de nuevas cuarentenas >= 98%

---

## 7) Acciones rápidas ante desvío

### Caso A: sube `Geografia especial...` en Pesos
1. Revisar tokens de módulo crudo (`VI`, `9.`, variantes con espacios/punto).
2. Confirmar regla activa MDM (`VI`, `9.1`, `9.2`).

### Caso B: sube `Geografia no encontrada...` en Vegetativa
1. Levantar top combinaciones `Modulo|Turno|Valvula|Cama` faltantes.
2. Decidir si son altas de catálogo válidas o data errónea.

### Caso C: baja `Pct_Con_ID`
1. Revisar rechazo en loaders y homologador.
2. Confirmar envío a cuarentena con `id_registro_origen` en todos los caminos de error.

### Caso D: aparece `LAYOUT_INCOMPATIBLE`
1. Revisar archivo movido a `data/rechazados/<carpeta>/`.
2. Validar si estaba en la carpeta correcta o si es un layout no soportado por el ETL actual.
3. No reprocesar manualmente en otra carpeta salvo evidencia clara.

### Caso E: consola no coincide con auditoría
1. Tomar `Auditoria.Log_Carga` como fuente de verdad.
2. Revisar `Servidor SQL` y `Base SQL` del resumen final.

---

## 8) Registro operativo sugerido (copiar/pegar)

```text
Fecha/Hora:
Operador:
Pipeline OK: SI/NO
Pesos Hoy:
Vegetativa Hoy:
VI Smoke (0/1/2): OK/NO
Estado Calidad Camas:
Bridge camas:
Top 3 Motivos Cuarentena:
Pct Con_ID (Pesos):
Pct Con_ID (Vegetativa):
Acción tomada:
```

---

## 9) Ejemplo cotidiano para el equipo
- Pre-check = revisar semáforo antes de arrancar.
- Post-check = validar que llegaste a destino sin alertas del tablero.
- Cuarentena = bandeja de paquetes con dirección incompleta.
- `ID_Registro_Origen` = número de guía del paquete (sin guía, no hay trazabilidad).

Sin guía, se pierde tiempo buscando. Con guía, el ajuste es directo.
