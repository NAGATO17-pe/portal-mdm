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

_REPOSITORIOS_CATALOGO = {
    "variedades": repo.listar_variedades,
    "geografia": repo.listar_geografia,
    "personal": repo.listar_personal,
}

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


def _listar_catalogo(nombre_catalogo: str, pagina: int = 1, tamano: int = 20) -> dict:
    return _con_cache(
        f"cat:{nombre_catalogo}:p{pagina}:s{tamano}",
        _TTL_CATALOGOS,
        _REPOSITORIOS_CATALOGO[nombre_catalogo],
        pagina=pagina,
        tamano=tamano,
    )


def listar_variedades(pagina: int = 1, tamano: int = 20) -> dict:
    """Lee MDM.Catalogo_Variedades activas con paginación server-side."""
    return _listar_catalogo(
        "variedades",
        pagina=pagina,
        tamano=tamano,
    )


def listar_geografia(pagina: int = 1, tamano: int = 20) -> dict:
    """Lee Silver.Dim_Geografia vigente con paginación server-side."""
    return _listar_catalogo(
        "geografia",
        pagina=pagina,
        tamano=tamano,
    )


def listar_personal(pagina: int = 1, tamano: int = 20) -> dict:
    """Lee Silver.Dim_Personal con paginación server-side."""
    return _listar_catalogo(
        "personal",
        pagina=pagina,
        tamano=tamano,
    )
