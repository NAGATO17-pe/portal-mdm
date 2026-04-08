# Herramientas Auxiliares

Esta carpeta concentra scripts auxiliares que apoyan tareas puntuales del proyecto, pero que no forman parte del runtime principal de `ETL`, `backend` ni `acp_mdm_portal`.

## Scripts movidos

- `extraer_geografia_desde_plano.py`
  - utilidad puntual para extraer geografia desde planos Excel y generar salidas auxiliares;
  - no participa en la corrida diaria del ETL.

- `poblar_usuarios.py`
  - bootstrap manual de usuarios para la capa de seguridad;
  - no es parte del backend operativo normal.

- `generar_baseline_operativo_20260406.py`
  - generador historico de baseline/documentacion;
  - mantiene salida en `ETL/Avance`.

- `generar_pdf_directiva_20260408.py`
  - generador del PDF ejecutivo para directiva;
  - no forma parte de la logica del producto.

## Criterio de esta carpeta

Aqui deben vivir scripts que sean:

- auxiliares;
- de soporte manual;
- de generacion documental;
- o de uso puntual fuera del flujo principal.

No deben moverse aqui:

- modulos del ETL operativo;
- backend FastAPI y runner;
- portal MDM;
- tests automatizados;
- utilidades que formen parte del flujo normal del proyecto.
