# Backend ACP Platform — Guía de Arranque

## Prerequisitos

| Componente | Versión mínima |
|-----------|----------------|
| Python | 3.11+ |
| ODBC Driver for SQL Server | 17 o 18 |
| SQL Server | 2017+ |
| Acceso de red al servidor BD | Requerido en dev y prod |

Verificar ODBC instalado:
```powershell
# Lista los drivers ODBC instalados
Get-OdbcDriver | Where-Object { $_.Name -like "*SQL Server*" } | Select-Object Name
```

---

## Configuración del entorno

### 1. Crear entorno virtual

```powershell
# Desde la raíz del proyecto (ACP Proyecciones/)
python -m venv .venv
.venv\Scripts\Activate.ps1
```

### 2. Instalar dependencias

```powershell
# Desde el directorio backend/
cd "ACP Proyecciones\backend"
pip install -r requirements.txt
```

### 3. Configurar variables de entorno

```powershell
# Copiar la plantilla
Copy-Item .env.example .env

# Editar .env con los valores reales
notepad .env
```

#### Variables obligatorias

| Variable | Descripción | Ejemplo |
|----------|-------------|---------|
| `DB_SERVIDOR` | Nombre o IP del servidor SQL | `LCP-PAG-PRACTIC` |
| `DB_NOMBRE` | Nombre de la base de datos | `ACP_DataWarehose_Proyecciones` |

#### Variables opcionales (tienen valor por defecto)

| Variable | Default | Descripción |
|----------|---------|-------------|
| `ACP_ENTORNO` | `dev` | Perfil: `dev`, `test`, `prod` |
| `DB_USUARIO` | *(vacío)* | Vacío = Windows Auth |
| `DB_CLAVE` | *(vacío)* | Contraseña SQL Server |
| `ACP_HOST` | `0.0.0.0` | IP de escucha |
| `ACP_PUERTO` | `8000` | Puerto HTTP |
| `ACP_LOG_NIVEL` | `INFO` | `DEBUG`, `INFO`, `WARNING`, `ERROR` |
| `ACP_LOG_FORMATO` | `json` | `json` (prod) o `texto` (dev) |

---

## Arranque del servidor

### Desarrollo (con recarga automática)

```powershell
# Opción 1: por módulo (usa settings para host/puerto)
cd "ACP Proyecciones\backend"
..\.venv\Scripts\python.exe main.py

# Opción 2: uvicorn directo
..\.venv\Scripts\uvicorn.exe main:aplicacion --reload --port 8000
```

### Producción

```powershell
# Sin recarga, múltiples workers
$env:ACP_ENTORNO="prod"
$env:ACP_LOG_FORMATO="json"
..\.venv\Scripts\uvicorn.exe main:aplicacion --host 0.0.0.0 --port 8000 --workers 2
```

---

## Verificación rápida

Una vez arrancado, verificar que el backend responde:

```powershell
# Liveness (no requiere BD)
Invoke-RestMethod http://localhost:8000/health/live

# Readiness (requiere BD)
Invoke-RestMethod http://localhost:8000/health/ready

# Control-plane y lock
Invoke-RestMethod http://localhost:8000/health/ready/control
Invoke-RestMethod http://localhost:8000/health/ready/runner
Invoke-RestMethod http://localhost:8000/health/lock
```

### Endpoints ETL operativos

El backend ya opera con control-plane persistente. Los endpoints clave son:

- `POST /api/v1/etl/corridas`
- `GET /api/v1/etl/corridas`
- `GET /api/v1/etl/corridas/activas`
- `GET /api/v1/etl/corridas/{id_corrida}`
- `GET /api/v1/etl/corridas/{id_corrida}/pasos`
- `GET /api/v1/etl/corridas/{id_corrida}/eventos`
- `GET /api/v1/etl/facts`
- `DELETE /api/v1/etl/corridas/{id_corrida}`

Contrato operativo actual:
- `POST /corridas` mantiene el payload vigente.
- `GET /corridas/{id_corrida}` devuelve parámetros ETL decodificados y la traza de pasos persistida.
- `GET /corridas/{id_corrida}/pasos` devuelve `Control.Corrida_Paso` ya ordenado.
- `GET /facts` expone el catálogo oficial de facts soportadas por `rerun`.

### OpenAPI y documentación

| URL | Descripción |
|-----|-------------|
| http://localhost:8000/docs | Swagger UI interactivo |
| http://localhost:8000/redoc | ReDoc (estático) |
| http://localhost:8000/scalar | Scalar UI (si instalado) |
| http://localhost:8000/openapi.json | Schema JSON crudo |

---

## Ejecución de pruebas

```powershell
cd "ACP Proyecciones\backend"

# Todas las pruebas
..\.venv\Scripts\pytest.exe tests/ -v

# Solo health
..\.venv\Scripts\pytest.exe tests/test_health.py -v

# Con cobertura (requiere pytest-cov)
..\.venv\Scripts\pytest.exe tests/ --cov=. --cov-report=term-missing
```

> **Nota:** Las pruebas no requieren conexión a SQL Server. Usan mocks internos.

---

## Estructura del backend

```
backend/
├── main.py                    ← Punto de entrada
├── requirements.txt
├── .env.example               ← Plantilla de variables
├── .env                       ← (no commitear) Variables locales
│
├── nucleo/                    ← Infraestructura transversal
│   ├── settings.py            ← Fuente única de configuración
│   ├── conexion.py            ← Engine SQLAlchemy
│   ├── auditoria.py           ← Wrapper de auditoría
│   ├── cache.py               ← Cache SQLite WAL
│   ├── excepciones.py         ← Excepciones HTTP tipadas
│   ├── logging.py             ← Logging estructurado JSON
│   └── middleware.py          ← RequestIdMiddleware
│
├── api/                       ← Routers FastAPI
│   ├── rutas_health.py        ← /health + ready/control + ready/runner + lock
│   ├── rutas_etl.py           ← /api/v1/etl/corridas, pasos, eventos, facts
│   ├── rutas_cuarentena.py    ← /api/cuarentena
│   ├── rutas_catalogos.py     ← /api/catalogos
│   └── rutas_auditoria.py     ← /api/auditoria
│
├── servicios/                 ← Lógica de negocio
│   ├── servicio_etl.py
│   ├── servicio_cuarentena.py
│   ├── servicio_catalogos.py
│   ├── servicio_auditoria.py
│   └── servicio_auth.py
│
├── repositorios/              ← Acceso a datos (único lugar con SQL)
│   ├── repo_control.py
│   ├── repo_auditoria.py
│   ├── repo_cuarentena.py
│   └── repo_catalogos.py
│
├── schemas/                   ← Modelos Pydantic por dominio
│   ├── etl/
│   ├── cuarentena/
│   ├── catalogos/
│   └── auditoria/
│
├── runner/                    ← Runner separado del proceso web
│   ├── runner.py              ← Loop consumidor de Control.Comando_Ejecucion
│   └── ejecutor.py            ← Subprocess + eventos + pasos + heartbeat
│
└── tests/                     ← Pruebas unitarias y de contrato
    ├── conftest.py
    ├── test_health.py
    ├── test_etl.py
    ├── test_runner_ejecutor.py
    ├── test_cuarentena.py
    └── test_auditoria.py
```

## Baseline operativa local

Estado validado en la baseline actual:

- Backend con `pytest` verde (`92 passed`).
- Endpoints de salud y lock operativos.
- ETL controlado por runner separado; el proceso web ya no ejecuta `pipeline.py`.
- Trazabilidad por corrida persistida en `Control.Corrida`, `Control.Corrida_Evento` y `Control.Corrida_Paso`.
- Catálogo oficial de facts y `rerun` dirigido expuestos por API.

---

## Solución de problemas comunes

### El servidor arranca pero /health/ready retorna 503

1. Verificar que SQL Server está accesible desde la máquina
2. Verificar `DB_SERVIDOR` y `DB_NOMBRE` en `.env`
3. Si usa Windows Auth, asegurar que el usuario del proceso tiene acceso a la BD
4. Probar conexión ODBC directa:
   ```powershell
   ..\.venv\Scripts\python.exe -c "
   from nucleo.conexion import verificar_conexion
   import json; print(json.dumps(verificar_conexion(), indent=2))
   "
   ```

### Error al importar `pydantic_settings`

```powershell
pip install pydantic-settings
```

### Puerto 8000 ya en uso

```powershell
# Ver qué proceso usa el puerto
netstat -ano | findstr :8000
# Cambiar puerto en .env: ACP_PUERTO=8001
```
