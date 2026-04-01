"""
nucleo/logging.py
=================
Configuración centralizada de logging para el backend ACP Platform.

- Formato JSON en prod (parseable por Loki, Datadog, etc.)
- Formato texto con colores en dev (legible en consola)
- Campos estándar: timestamp, level, service, request_id, message

Uso:
    from nucleo.logging import obtener_logger
    log = obtener_logger(__name__)
    log.info("Mensaje", extra={"request_id": req_id, "corrida_id": corrida})
"""

from __future__ import annotations
import json
import logging
import sys
from datetime import datetime, timezone
from typing import Any
from nucleo.settings import settings

# ── Constantes ─────────────────────────────────────────────────────────────────
_NOMBRE_SERVICIO = "acp-backend"
_LEVEL_COLORES = {
    "DEBUG":    "\033[36m",   # cian
    "INFO":     "\033[32m",   # verde
    "WARNING":  "\033[33m",   # amarillo
    "ERROR":    "\033[31m",   # rojo
    "CRITICAL": "\033[35m",   # magenta
    "RESET":    "\033[0m",
}


class _JsonFormatter(logging.Formatter):
    """
    Emite cada línea de log como un objeto JSON serializado.
    Los campos extra del LogRecord se incluyen automáticamente.
    """

    _CAMPOS_ESTANDAR = frozenset({
        "name", "msg", "args", "levelname", "levelno", "pathname",
        "filename", "module", "exc_info", "exc_text", "stack_info",
        "lineno", "funcName", "created", "msecs", "relativeCreated",
        "thread", "threadName", "processName", "process", "message",
        "taskName",
    })

    def format(self, record: logging.LogRecord) -> str:
        record.message = record.getMessage()
        extra: dict[str, Any] = {
            k: v
            for k, v in record.__dict__.items()
            if k not in self._CAMPOS_ESTANDAR
        }

        entrada = {
            "timestamp": datetime.now(tz=timezone.utc).isoformat(),
            "level":     record.levelname,
            "service":   _NOMBRE_SERVICIO,
            "logger":    record.name,
            "message":   record.message,
        }
        entrada.update(extra)

        if record.exc_info:
            entrada["exception"] = self.formatException(record.exc_info)

        return json.dumps(entrada, ensure_ascii=False, default=str)


class _TextoFormatter(logging.Formatter):
    """
    Formato legible para consola en desarrollo.
    Incluye colores ANSI y muestra request_id si está disponible.
    """

    FMT = "{color}[{level:<8}]{reset} {ts} | {logger} | {msg}{extra}"

    def format(self, record: logging.LogRecord) -> str:
        record.message = record.getMessage()
        color  = _LEVEL_COLORES.get(record.levelname, "")
        reset  = _LEVEL_COLORES["RESET"]
        ts     = datetime.now(tz=timezone.utc).strftime("%H:%M:%S")

        campos_extra = ["request_id", "corrida_id", "usuario"]
        extra_parts  = [
            f"{k}={record.__dict__[k]}"
            for k in campos_extra
            if k in record.__dict__ and record.__dict__[k]
        ]
        extra_str = "  [" + " ".join(extra_parts) + "]" if extra_parts else ""

        linea = self.FMT.format(
            color=color,
            reset=reset,
            level=record.levelname,
            ts=ts,
            logger=record.name,
            msg=record.message,
            extra=extra_str,
        )

        if record.exc_info:
            linea += "\n" + self.formatException(record.exc_info)
        return linea


def configurar_logging() -> None:
    """
    Configura el logging global del proceso.
    Debe llamarse UNA SOLA VEZ al iniciar el servidor (en lifespan).
    """
    nivel  = getattr(logging, settings.log_nivel.upper(), logging.INFO)
    handler = logging.StreamHandler(sys.stdout)

    if settings.log_formato == "json":
        handler.setFormatter(_JsonFormatter())
    else:
        handler.setFormatter(_TextoFormatter())

    # Configura el root logger sin duplicar handlers
    root = logging.getLogger()
    root.setLevel(nivel)
    if not any(isinstance(h, logging.StreamHandler) for h in root.handlers):
        root.addHandler(handler)

    # Silencia loggers ruidosos de terceros
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)
    logging.getLogger("sqlalchemy.engine").setLevel(logging.WARNING)


def obtener_logger(nombre: str) -> logging.Logger:
    """Alias conveniente: from nucleo.logging import obtener_logger."""
    return logging.getLogger(nombre)
