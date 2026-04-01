"""
servicios/servicio_catalogos.py
===============================
Lógica de consulta de catálogos MDM y Silver.
Todos los métodos son de solo lectura.
Delega todo el acceso a datos a repositorios.repo_catalogos.
"""

from __future__ import annotations

from nucleo.cache import cache
from nucleo.logging import obtener_logger
import repositorios.repo_catalogos as repo

log = obtener_logger(__name__)

_TTL_CATALOGOS = 3600   # 1 hora — datos estáticos


def _con_cache(clave: str, ttl: int, fn, *args, **kwargs):
    """Helper: intenta caché antes de llamar al repositorio."""
    cached = cache.obtener(clave)
    if cached:
        log.debug("Cache hit catálogos", extra={"clave": clave})
        return cached
    resultado = fn(*args, **kwargs)
    cache.guardar(clave, resultado, ttl_segundos=ttl)
    return resultado


def listar_variedades(pagina: int = 1, tamano: int = 20) -> dict:
    """Lee MDM.Catalogo_Variedades activas con paginación server-side."""
    return _con_cache(
        f"cat:variedades:p{pagina}:s{tamano}",
        _TTL_CATALOGOS,
        repo.listar_variedades,
        pagina=pagina,
        tamano=tamano,
    )


def listar_geografia(pagina: int = 1, tamano: int = 20) -> dict:
    """Lee Silver.Dim_Geografia vigente con paginación server-side."""
    return _con_cache(
        f"cat:geografia:p{pagina}:s{tamano}",
        _TTL_CATALOGOS,
        repo.listar_geografia,
        pagina=pagina,
        tamano=tamano,
    )


def listar_personal(pagina: int = 1, tamano: int = 20) -> dict:
    """Lee Silver.Dim_Personal con paginación server-side."""
    return _con_cache(
        f"cat:personal:p{pagina}:s{tamano}",
        _TTL_CATALOGOS,
        repo.listar_personal,
        pagina=pagina,
        tamano=tamano,
    )
