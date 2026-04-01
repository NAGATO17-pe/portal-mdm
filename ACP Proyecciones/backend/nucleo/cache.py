import sqlite3
import json
import time
import os
from typing import Any, Optional

# Ruta de la base de datos de caché (Local, no requiere servidor)
DB_PATH = os.path.join(os.path.dirname(__file__), "..", "cache_portal.db")

class CacheAlpha:
    """
    Sistema de Caché de Alta Velocidad basado en SQLite (WAL Mode).
    Alternativa ligera a Redis para entornos corporativos sin dependencias externas.
    """
    
    def __init__(self):
        self._inicializar_db()

    def _get_connection(self):
        conn = sqlite3.connect(DB_PATH)
        # Modo WAL: Permite lecturas y escrituras simultáneas ultra rápidas
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA synchronous=NORMAL")
        return conn

    def _inicializar_db(self):
        """Crea la tabla de caché si no existe."""
        with self._get_connection() as conn:
            conn.execute("""
                CREATE TABLE IF NOT EXISTS cache_sistema (
                    clave TEXT PRIMARY KEY,
                    valor TEXT,
                    expiracion INTEGER
                )
            """)
            # Limpieza de registros expirados al iniciar
            conn.execute("DELETE FROM cache_sistema WHERE expiracion < ?", (int(time.time()),))

    def guardar(self, clave: str, valor: Any, ttl_segundos: int = 3600):
        """Guarda un objeto serializado en la caché."""
        expiracion = int(time.time()) + ttl_segundos
        valor_json = json.dumps(valor)
        
        with self._get_connection() as conn:
            conn.execute("""
                INSERT OR REPLACE INTO cache_sistema (clave, valor, expiracion)
                VALUES (?, ?, ?)
            """, (clave, valor_json, expiracion))

    def obtener(self, clave: str) -> Optional[Any]:
        """Recupera un objeto de la caché si no ha expirado."""
        ahora = int(time.time())
        
        with self._get_connection() as conn:
            cursor = conn.execute("""
                SELECT valor FROM cache_sistema 
                WHERE clave = ? AND expiracion > ?
            """, (clave, ahora))
            
            fila = cursor.fetchone()
            if fila:
                return json.loads(fila[0])
        return None

    def eliminar(self, clave: str):
        """Elimina una clave específica."""
        with self._get_connection() as conn:
            conn.execute("DELETE FROM cache_sistema WHERE clave = ?", (clave,))

    def limpiar_todo(self):
        """Vacía la caché por completo."""
        with self._get_connection() as conn:
            conn.execute("DELETE FROM cache_sistema")

# Instancia única (Singleton) para el backend
cache = CacheAlpha()
