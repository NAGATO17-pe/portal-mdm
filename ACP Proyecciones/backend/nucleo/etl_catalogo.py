"""
nucleo/etl_catalogo.py
======================
Carga el manifiesto de facts del ETL sin duplicar configuración dentro del backend.
"""

from __future__ import annotations

import importlib.util
from functools import lru_cache
from pathlib import Path


_RUTA_ETL_EJECUCION = Path(__file__).resolve().parents[2] / "ETL" / "utils" / "ejecucion.py"


@lru_cache(maxsize=1)
def _cargar_modulo_ejecucion():
    spec = importlib.util.spec_from_file_location("acp_etl_utils_ejecucion", _RUTA_ETL_EJECUCION)
    if spec is None or spec.loader is None:
        raise RuntimeError(f"No se pudo cargar el manifiesto ETL desde {_RUTA_ETL_EJECUCION}")
    modulo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(modulo)
    return modulo


def listar_facts_disponibles() -> list[dict]:
    modulo = _cargar_modulo_ejecucion()
    catalogo = []
    for nombre, meta in modulo.CONFIG_FACTS.items():
        catalogo.append({
            "nombre_fact": nombre,
            "orden": int(meta["orden"]),
            "tabla_destino": str(meta["tabla_destino"]),
            "fuentes_bronce": list(meta.get("fuentes_bronce", ())),
            "dependencias": list(meta.get("dependencias", ())),
            "marts": list(meta.get("marts", ())),
            "releer_bronce_por_estado": bool(meta.get("releer_bronce_por_estado", True)),
            "estrategia_rerun": str(meta.get("estrategia_rerun", "NO_DECLARADA")),
        })
    return catalogo
