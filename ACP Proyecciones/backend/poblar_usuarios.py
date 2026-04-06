import sys
import os
sys.path.insert(0, os.path.abspath('.'))

from nucleo.auth import hash_clave
from repositorios.repo_usuarios import insertar_usuario, buscar_por_nombre

usuarios = [
    ("admin", "Administrador General", "admin123", "admin"),
    ("chernandez", "Carlos Hernandez", "acp2026", "admin")
]

creados = 0
for nick, display, pwd, rol in usuarios:
    try:
        if not buscar_por_nombre(nick):
            insertar_usuario(
                nombre_usuario=nick,
                nombre_display=display,
                hash_clave=hash_clave(pwd),
                rol=rol
            )
            print(f"Usuario BD insertado: {nick}")
            creados += 1
        else:
            print(f"ℹEl usuario '{nick}' ya existe en BD.")
    except Exception as e:
        print(f"Error con {nick}: {e}")

if creados > 0:
    print("\nLa tabla Seguridad.Usuarios ahora tiene credenciales válidas.")
