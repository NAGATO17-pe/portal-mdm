"""
migration_runner.py
===================
Runner para aplicar migraciones SQL con trazabilidad.

Lee los archivos .sql de sql_migrations/, calcula su hash SHA-256,
verifica contra Admin.Migraciones_Aplicadas y solo aplica los pendientes.

Uso:
    py tools/migration_runner.py                     # Aplica todas las pendientes
    py tools/migration_runner.py --dry-run           # Solo lista pendientes
    py tools/migration_runner.py --only fase28       # Aplica solo las que contengan 'fase28'
"""

from __future__ import annotations

import argparse
import hashlib
import sys
import time
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.engine import Engine

# Agregar el directorio padre al path para poder importar config
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from config.conexion import obtener_engine


MIGRATIONS_DIR = Path(__file__).resolve().parent.parent / "sql_migrations"

# Archivos que NO deben ejecutarse por el runner (son seed data o consultas de validación)
_EXCLUIDOS = {
    "consultas_dq_operativas.sql",
    "validacion_post_corrida_pilotos.sql",
}


def _calcular_hash(ruta: Path) -> str:
    """SHA-256 del contenido del archivo."""
    contenido = ruta.read_bytes()
    return hashlib.sha256(contenido).hexdigest()


def _tabla_migraciones_existe(engine: Engine) -> bool:
    """Verifica si Admin.Migraciones_Aplicadas ya fue creada."""
    with engine.connect() as conn:
        resultado = conn.execute(text("""
            SELECT CASE 
                WHEN OBJECT_ID('Admin.Migraciones_Aplicadas', 'U') IS NULL 
                THEN 0 ELSE 1 
            END
        """)).scalar_one()
    return bool(resultado)


def _obtener_aplicadas(engine: Engine) -> set[str]:
    """Retorna set de nombres de archivo ya aplicados."""
    with engine.connect() as conn:
        filas = conn.execute(text("""
            SELECT Nombre_Archivo 
            FROM Admin.Migraciones_Aplicadas 
            WHERE Estado = 'OK'
        """)).fetchall()
    return {fila[0] for fila in filas}


def _listar_migraciones_disponibles() -> list[Path]:
    """Lista archivos .sql ordenados por nombre (orden de fases)."""
    archivos = sorted(MIGRATIONS_DIR.glob("*.sql"))
    return [a for a in archivos if a.name not in _EXCLUIDOS]


def _aplicar_migracion(engine: Engine, ruta: Path, hash_archivo: str) -> dict:
    """
    Ejecuta un archivo SQL contra la DB y registra el resultado.
    
    Nota: Los archivos con sentencias GO se separan en bloques
    ya que pyodbc no soporta GO nativamente.
    """
    contenido = ruta.read_text(encoding="utf-8")
    
    # Separar por GO (solo como separador de batch, en línea propia)
    bloques = [
        bloque.strip() 
        for bloque in contenido.split("\nGO\n")
        if bloque.strip() and not bloque.strip().startswith("--")
    ]
    # También manejar GO al final del archivo
    if bloques and bloques[-1].endswith("\nGO"):
        bloques[-1] = bloques[-1][:-3].strip()

    inicio = time.time()
    error_msg = None
    
    try:
        with engine.begin() as conn:
            for bloque in bloques:
                if bloque.strip():
                    conn.execute(text(bloque))
        estado = "OK"
    except Exception as e:
        estado = "ERROR"
        error_msg = str(e)[:4000]
    
    duracion = round(time.time() - inicio, 2)
    
    # Registrar resultado
    try:
        with engine.begin() as conn:
            conn.execute(text("""
                IF NOT EXISTS (
                    SELECT 1 FROM Admin.Migraciones_Aplicadas 
                    WHERE Nombre_Archivo = :nombre
                )
                INSERT INTO Admin.Migraciones_Aplicadas 
                    (Nombre_Archivo, Hash_SHA256, Duracion_Segundos, Estado, Mensaje_Error)
                VALUES (:nombre, :hash, :duracion, :estado, :error)
                ELSE
                UPDATE Admin.Migraciones_Aplicadas 
                SET Hash_SHA256 = :hash, 
                    Duracion_Segundos = :duracion, 
                    Estado = :estado, 
                    Mensaje_Error = :error,
                    Fecha_Aplicacion = GETDATE()
                WHERE Nombre_Archivo = :nombre
            """), {
                "nombre": ruta.name,
                "hash": hash_archivo,
                "duracion": duracion,
                "estado": estado,
                "error": error_msg,
            })
    except Exception:
        pass  # Si la tabla de migraciones aún no existe (bootstrap)
    
    return {
        "archivo": ruta.name,
        "estado": estado,
        "duracion": duracion,
        "error": error_msg,
    }


def ejecutar(
    dry_run: bool = False,
    filtro: str | None = None,
) -> list[dict]:
    """
    Punto de entrada principal del runner.
    
    Args:
        dry_run: Si True, solo lista pendientes sin aplicar.
        filtro: Substring para filtrar migraciones (ej. 'fase28').
    
    Returns:
        Lista de resultados por migración.
    """
    engine = obtener_engine()
    
    # Verificar si la tabla de registro existe
    tabla_existe = _tabla_migraciones_existe(engine)
    
    if not tabla_existe:
        print("⚠  Admin.Migraciones_Aplicadas no existe.")
        print("   Aplique primero: fase30_registro_migraciones.sql")
        if not dry_run:
            print("   Intentando aplicar fase30 automáticamente...")
            bootstrap = MIGRATIONS_DIR / "fase30_registro_migraciones.sql"
            if bootstrap.exists():
                resultado = _aplicar_migracion(engine, bootstrap, _calcular_hash(bootstrap))
                print(f"   Bootstrap: {resultado['estado']}")
                if resultado["estado"] != "OK":
                    print(f"   Error: {resultado['error']}")
                    return [resultado]
            else:
                print("   No se encontró fase30_registro_migraciones.sql")
                return []
    
    # Obtener estado actual
    aplicadas = _obtener_aplicadas(engine) if _tabla_migraciones_existe(engine) else set()
    disponibles = _listar_migraciones_disponibles()
    
    # Filtrar si se especificó
    if filtro:
        disponibles = [d for d in disponibles if filtro.lower() in d.name.lower()]
    
    # Determinar pendientes
    pendientes = [d for d in disponibles if d.name not in aplicadas]
    
    if not pendientes:
        print("✅ Todas las migraciones están aplicadas.")
        return []
    
    print(f"\n{'=' * 60}")
    print(f"  Migraciones pendientes: {len(pendientes)}")
    print(f"{'=' * 60}")
    for p in pendientes:
        h = _calcular_hash(p)
        print(f"  📄 {p.name}  (SHA256: {h[:12]}...)")
    
    if dry_run:
        print(f"\n  [DRY RUN] No se aplicó ninguna migración.\n")
        return [{"archivo": p.name, "estado": "PENDIENTE"} for p in pendientes]
    
    print(f"\n  Aplicando migraciones...\n")
    resultados = []
    for ruta in pendientes:
        h = _calcular_hash(ruta)
        print(f"  ⏳ {ruta.name}...", end=" ", flush=True)
        resultado = _aplicar_migracion(engine, ruta, h)
        icono = "✅" if resultado["estado"] == "OK" else "❌"
        print(f"{icono} ({resultado['duracion']}s)")
        if resultado["error"]:
            print(f"     Error: {resultado['error'][:200]}")
        resultados.append(resultado)
    
    ok = sum(1 for r in resultados if r["estado"] == "OK")
    err = sum(1 for r in resultados if r["estado"] == "ERROR")
    print(f"\n  Resultado: {ok} OK, {err} ERROR\n")
    
    return resultados


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Runner de migraciones SQL con trazabilidad")
    parser.add_argument("--dry-run", action="store_true", help="Solo listar pendientes, no aplicar")
    parser.add_argument("--only", type=str, default=None, help="Filtrar por substring (ej. 'fase28')")
    args = parser.parse_args()
    
    resultados = ejecutar(dry_run=args.dry_run, filtro=args.only)
    
    errores = [r for r in resultados if r.get("estado") == "ERROR"]
    if errores:
        sys.exit(1)
