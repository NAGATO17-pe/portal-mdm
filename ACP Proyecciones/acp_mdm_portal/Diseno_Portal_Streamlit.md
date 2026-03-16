# Diseño Portal Streamlit MDM
## ACP Geographic Phenology | Equipo de Proyecciones | Uso diario

---

## Estructura de navegación

```
app.py
└── sidebar
    ├── 🏠 Inicio
    ├── 🔴 Cuarentena
    ├── 🔗 Homologación
    ├── 📚 Catálogos
    │   ├── Variedades
    │   ├── Geografía
    │   └── Personal
    └── ⚙️ Configuración
        ├── Reglas de Validación
        └── Parámetros Pipeline
```

---

## Página 1 — Inicio

**Propósito:** Ver el estado del pipeline de hoy de un vistazo.
**Lo primero que ve el equipo cada mañana.**

### Sección 1 — Métricas del día
```
┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐
│ Última carga │ │ Filas OK     │ │ En cuarentena│ │ Pendientes   │
│ Hace 2 horas │ │ 5,212        │ │ 460          │ │ MDM: 23      │
└──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘
```

### Sección 2 — Estado por tabla
Tabla con el estado de la última carga de cada tabla Bronce:
- Nombre tabla
- Última carga (fecha/hora)
- Filas insertadas
- Filas rechazadas
- Estado: ✅ OK | ⚠️ Con errores | ❌ Falló

### Sección 3 — Alertas activas
Banner rojo si hay críticos pendientes en cuarentena.
Banner amarillo si hay variedades sin homologar.

### Sección 4 — Log de últimas 10 cargas
Tabla de Auditoria.Log_Carga — solo lectura.

---

## Página 2 — Cuarentena

**Propósito:** Revisar filas rechazadas y decidir qué hacer con cada una.
**Esta es la página más importante del portal.**

### Sección 1 — Filtros
```
[Tabla origen ▼]  [Columna origen ▼]  [Severidad ▼]  [Estado ▼]  [Buscar valor...]
```

### Sección 2 — Tabla de cuarentena
Columnas visibles:
- Fila | Tabla Origen | Columna Origen | Valor Raw | Motivo | Severidad | Fecha ingreso

### Sección 3 — Panel de decisión (al seleccionar una fila)
```
┌─────────────────────────────────────────────────────────────────┐
│  Valor: "FCM14-057"   Columna: Variedad   Filas afectadas: 29  │
│                                                                  │
│  Valores similares en catálogo:                                  │
│    → "Megacrisp"  (score 0.71)                                  │
│    → "Kms1530"    (score 0.62)                                  │
│                                                                  │
│  ¿Qué es este valor?                                             │
│  ○ Variedad nueva    → Nombre canónico: [____________] [Agregar] │
│  ○ Mal escrita       → Homologar a:    [____________] [Aprobar]  │
│  ○ Es Test Block     → Marcar módulo como Test Block  [Marcar]   │
│  ○ Descartar         → Ignorar este registro          [Descartar]│
└─────────────────────────────────────────────────────────────────┘
```

### Acciones masivas
- Seleccionar múltiples filas del mismo tipo → aplicar misma decisión
- Exportar cuarentena a Excel para revisión offline

---

## Página 3 — Homologación

**Propósito:** Revisar sugerencias automáticas del Levenshtein pendientes de aprobación.
**Estas son las homologaciones que el ETL detectó pero no se atrevió a aplicar solo.**

### Sección 1 — Pendientes de aprobación
Tabla con:
- Texto crudo | Valor canónico sugerido | Score | Tabla origen | Veces visto | Fecha

### Sección 2 — Acciones por fila
- ✅ **Aprobar** → UPDATE Aprobado = 1 → próxima ejecución ETL lo aplica solo
- ✏️ **Corregir** → cambiar el valor canónico sugerido antes de aprobar
- ❌ **Rechazar** → va a cuarentena para decisión manual

### Sección 3 — Historial
Tabla de homologaciones aprobadas — para auditoría.
Filtros por tabla, columna, fecha.

---

## Página 4 — Catálogos

### 4a — Variedades
**Propósito:** Agregar, editar o desactivar variedades del catálogo oficial.

Tabla editable con:
- Nombre canónico | Breeder | Activa (toggle)

Acciones:
- **Agregar variedad** → formulario: Nombre canónico + Breeder
- **Desactivar** → Es_Activa = 0 (nunca se elimina)

### 4b — Geografía
**Propósito:** Mantener el catálogo de fundos, sectores y módulos.
**Los cambios aquí activan SCD2 en la próxima ejecución del ETL.**

Tabla con:
- Fundo | Sector | Módulo | Turno | Es_Test_Block (toggle) | Activa

Acciones:
- **Agregar** → formulario completo
- **Marcar Test Block** → toggle directo en tabla
- **Desactivar** → Es_Activa = 0

⚠️ Banner de aviso: "Los cambios en geografía activan SCD2 en la próxima ejecución del ETL."

### 4c — Personal
**Propósito:** Ver y corregir el catálogo de personal.

Tabla con:
- DNI | Nombre completo | Rol | Sexo | Activo

Acciones:
- **Editar nombre** → corregir errores de tipeo
- **Cambiar rol** → Operario / Evaluador / Supervisor
- **Desactivar** → Es_Activo = 0

---

## Página 5 — Configuración

### 5a — Reglas de Validación
**Propósito:** Ajustar rangos biológicos y reglas de DQ sin tocar código.

Tabla editable con:
- Tabla destino | Columna | Tipo validación | Valor min | Valor max | Acción | Activa

Acciones:
- **Agregar regla** → formulario
- **Activar/desactivar** → toggle por regla
- **Editar rango** → cambiar min/max directamente en tabla

⚠️ Banner: "Los cambios aplican en la próxima ejecución del ETL."

### 5b — Parámetros Pipeline
**Propósito:** Cambiar parámetros operativos del ETL.

Tabla con:
- Parámetro | Valor actual | Descripción | Última modificación

Parámetros visibles:
- CAMPANA_ACTIVA
- PESO_BAYA_MIN / MAX
- LEVENSHTEIN_UMBRAL
- CHUNK_SIZE_INSERT
- etc.

Acción: editar valor directamente con confirmación.

---

## Diseño visual

| Elemento | Decisión |
|---|---|
| Color primario | Verde oscuro ACP (#1E6B35) |
| Color secundario | Bronce (#CD7F32) |
| Fondo | Blanco / gris muy claro |
| Tablas | st.dataframe para lectura, st.data_editor para edición |
| Alertas | st.error (rojo), st.warning (amarillo), st.success (verde) |
| Layout | wide — sidebar izquierda + contenido principal |
| Métricas | st.metric con delta |

---

## Flujo diario del equipo

```
08:00  ETL corre automático (o manual)
08:30  Analista abre portal → Inicio
       → ¿Hay alertas? → Cuarentena
       → Revisar filas rechazadas → tomar decisiones
       → Ir a Homologación → aprobar sugerencias pendientes
       → ETL próxima ejecución ya incorpora las decisiones
```

---

## Archivos del portal

```
acp_mdm_portal/
├── app.py                        ← streamlit run app.py
├── config/
│   └── conexion.py
├── paginas/
│   ├── inicio.py
│   ├── cuarentena.py             ← más compleja — panel de decisión
│   ├── homologacion.py
│   ├── catalogos/
│   │   ├── variedades.py
│   │   ├── geografia.py
│   │   └── personal.py
│   └── configuracion/
│       ├── reglas_validacion.py
│       └── parametros_pipeline.py
├── componentes/
│   ├── tabla_editable.py
│   ├── panel_decision.py         ← widget central de cuarentena
│   └── estado_pipeline.py
└── utils/
    ├── queries.py
    └── formato.py
```
