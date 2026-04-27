"""
nucleo/rate_limit.py
====================
Rate limiting en memoria para endpoints sensibles (sin dependencias externas).

Política: ventana deslizante por IP.
  - Máximo N intentos en los últimos W segundos.
  - Al superar el límite retorna HTTP 429 con Retry-After.
  - Las entradas antiguas se purgan automáticamente al consultar.
"""

from __future__ import annotations

import time
from collections import defaultdict, deque
from threading import Lock

from fastapi import HTTPException, Request, status

_lock_global = Lock()

# { ip: deque([timestamp, ...]) }
_registro: dict[str, deque[float]] = defaultdict(deque)


def _purgar_ventana(intentos: deque[float], ventana: int, ahora: float) -> None:
    while intentos and ahora - intentos[0] > ventana:
        intentos.popleft()


def verificar_rate_limit(
    request: Request,
    *,
    max_intentos: int = 5,
    ventana_segundos: int = 60,
) -> None:
    """
    Dependencia FastAPI. Lanza HTTP 429 si la IP supera el límite.

    Uso:
        @router.post("/login")
        async def login(request: Request, _: None = Depends(rate_limit_login)):
            ...

    donde: rate_limit_login = lambda r: verificar_rate_limit(r, max_intentos=5, ventana_segundos=60)
    """
    ip = request.client.host if request.client else "desconocido"
    ahora = time.monotonic()

    with _lock_global:
        intentos = _registro[ip]
        _purgar_ventana(intentos, ventana_segundos, ahora)

        if len(intentos) >= max_intentos:
            tiempo_restante = int(ventana_segundos - (ahora - intentos[0])) + 1
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=(
                    f"Demasiados intentos de inicio de sesión. "
                    f"Intente nuevamente en {tiempo_restante} segundos."
                ),
                headers={"Retry-After": str(tiempo_restante)},
            )

        intentos.append(ahora)
