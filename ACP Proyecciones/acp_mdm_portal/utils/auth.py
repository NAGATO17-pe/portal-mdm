"""
utils/auth.py — Autenticación y control de acceso del Portal MDM ACP
=====================================================================
Sistema de login con usuarios hardcodeados (Fase 1).
Cuando exista la tabla Seguridad.Usuarios, solo hay que cambiar
`_USUARIOS_HARDCODED` por una consulta SQL.

Roles:
  admin  — acceso total (lectura + escritura + configuración)
  editor — lectura + escritura en catálogos y cuarentena
  viewer — solo lectura

Funciones públicas:
  login_gate()         — Renderiza login o retorna True si ya autenticado
  obtener_usuario()    — Dict con info del usuario actual
  tiene_permiso()      — Verifica si el rol tiene permiso para una acción
  cerrar_sesion()      — Limpia session_state de auth
"""

from __future__ import annotations

import hashlib
import time

import streamlit as st


# ── Usuarios hardcodeados (Fase 1) ───────────────────────────────────────────
# Contraseñas hasheadas con SHA-256 para no tenerlas en texto plano ni siquiera
# en código. Para generar un hash: hashlib.sha256("mi_pass".encode()).hexdigest()

def _hash(password: str) -> str:
    return hashlib.sha256(password.encode()).hexdigest()

_USUARIOS_HARDCODED: dict[str, dict] = {
    "admin": {
        "password_hash": _hash("admin123"),
        "nombre":        "Administrador",
        "rol":           "admin",
        "avatar":        "👑",
    },
    "chernandez": {
        "password_hash": _hash("acp2026"),
        "nombre":        "Carlos Hernández",
        "rol":           "admin",
        "avatar":        "🧑‍💼",
    },
    "editor": {
        "password_hash": _hash("editor123"),
        "nombre":        "Editor MDM",
        "rol":           "editor",
        "avatar":        "✏️",
    },
    "viewer": {
        "password_hash": _hash("viewer123"),
        "nombre":        "Consultor",
        "rol":           "viewer",
        "avatar":        "👁️",
    },
}

# ── Permisos por rol ─────────────────────────────────────────────────────────

_PERMISOS: dict[str, set[str]] = {
    "admin":  {"leer", "escribir", "configurar", "ejecutar_etl", "admin"},
    "editor": {"leer", "escribir"},
    "viewer": {"leer"},
}


def tiene_permiso(accion: str) -> bool:
    """Verifica si el usuario actual tiene permiso para `accion`."""
    usuario = obtener_usuario()
    if usuario is None:
        return False
    rol = usuario.get("rol", "viewer")
    return accion in _PERMISOS.get(rol, set())


def obtener_usuario() -> dict | None:
    """Retorna el dict del usuario autenticado o None."""
    if st.session_state.get("autenticado"):
        return {
            "username": st.session_state.get("username", ""),
            "nombre":   st.session_state.get("nombre_usuario", ""),
            "rol":      st.session_state.get("rol_usuario", "viewer"),
            "avatar":   st.session_state.get("avatar_usuario", "👤"),
        }
    return None


def cerrar_sesion() -> None:
    """Limpia el session_state de autenticación."""
    for key in ["autenticado", "username", "nombre_usuario", "rol_usuario",
                "avatar_usuario", "login_error", "login_time"]:
        st.session_state.pop(key, None)


# ── Validación de credenciales ───────────────────────────────────────────────

def _validar_credenciales(username: str, password: str) -> dict | None:
    """Valida contra el backend y retorna la info del usuario."""
    from utils.api_client import login_backend
    token = login_backend(username, password)
    if token:
        # Extraer rol desde local por ahora, o mapear desde el JWT (Fase rápida)
        user = _USUARIOS_HARDCODED.get(username.lower().strip())
        if user:
            user["token"] = token
            return user
    return None


# ── CSS del Login ────────────────────────────────────────────────────────────

_LOGIN_CSS = """
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap');

/* Ocultar elementos de UI no deseados */
section[data-testid="stSidebar"], header[data-testid="stHeader"], footer { display: none !important; }

/* Centrado perfecto vertical y horizontal sin scrollbars */
.stApp {
    background: linear-gradient(135deg, #F4F7F5 0%, #E9F0EC 100%) !important;
    background-attachment: fixed !important;
}

/* Ocultar padding por defecto y barras de desplazamiento extra */
.stMainBlockContainer {
    padding: 0 !important;
    margin: 0 !important;
    overflow: hidden !important; 
    max-width: 100% !important;
}

/* Animaciones */
@keyframes fadeInUp {
    from { opacity: 0; transform: translateY(20px); }
    to   { opacity: 1; transform: translateY(0); }
}
@keyframes float {
    0%, 100% { transform: translateY(0px); }
    50%      { transform: translateY(-6px); }
}

/* Decorative dots background */
.login-dots {
    position: fixed; top: 0; left: 0; right: 0; bottom: 0; pointer-events: none; z-index: 0;
}
.login-dots::before, .login-dots::after {
    content: ''; position: absolute; border-radius: 50%; opacity: 0.05;
}
.login-dots::before {
    width: 500px; height: 500px; background: radial-gradient(circle, #2D5A27, transparent 70%);
    top: -150px; right: 10%;
}
.login-dots::after {
    width: 400px; height: 400px; background: radial-gradient(circle, #C38D4F, transparent 70%);
    bottom: -100px; left: 10%;
}

/* Glassmorphism Form Container (Light version) */
div[data-testid="stForm"] {
    position: fixed !important;
    top: 50% !important;
    left: 50% !important;
    transform: translate(-50%, -50%) !important;
    background: rgba(255, 255, 255, 0.85) !important; /* Cristal un poco más opaco */
    backdrop-filter: blur(28px) !important;
    -webkit-backdrop-filter: blur(28px) !important;
    border: 1px solid rgba(255, 255, 255, 0.9) !important;
    border-radius: 20px !important;
    padding: 32px 32px 36px !important;
    width: 360px !important; /* Cuadro céntrico */
    height: fit-content !important; /* MUY IMPORTANTE: Evita que se estire a lo largo */
    max-height: 90vh !important;
    box-shadow:
        0 16px 40px rgba(45, 90, 39, 0.08),
        inset 0 1px 0 rgba(255, 255, 255, 1) !important;
    animation: fadeInUp 0.6s ease;
    z-index: 10;
}

/* Ajustes internos del formulario */
div[data-testid="stForm"] > div { gap: 14px !important; } /* Un poco más de espacio interno para que respiren los inputs */

/* Logo header interno */
.login-logo { text-align: center; margin-bottom: 24px; }
.login-logo-icon {
    font-size: 2.8rem; display: inline-block; animation: float 4s ease-in-out infinite;
    filter: drop-shadow(0 4px 10px rgba(45, 90, 39, 0.2));
    line-height: 1; margin-bottom: 8px;
}
.login-logo h2 {
    color: #2D5A27; font-size: 1.35rem; font-weight: 800; letter-spacing: 1px; margin: 0 0 2px 0;
}
.login-logo p {
    color: #667085; font-size: 0.75rem; margin: 0; font-weight: 500;
}

/* Inputs Streamlit override */
div[data-testid="stForm"] .stTextInput > div > div > input {
    background: rgba(255, 255, 255, 0.9) !important;
    border: 1px solid rgba(45, 90, 39, 0.15) !important;
    border-radius: 8px !important;
    color: #1A252F !important;
    font-size: 0.9rem !important;
    padding: 10px 14px !important;
    transition: all 0.3s ease !important;
}
div[data-testid="stForm"] .stTextInput > div > div > input::placeholder {
    color: #98A2B3 !important;
}
div[data-testid="stForm"] .stTextInput > div > div > input:focus {
    border-color: #43843C !important;
    box-shadow: 0 0 0 3px rgba(67, 132, 60, 0.15) !important;
}
div[data-testid="stForm"] .stTextInput label { display: none !important; }

.login-label {
    color: #344054; font-size: 0.75rem; font-weight: 700;
    text-transform: uppercase; letter-spacing: 0.5px; 
    margin-bottom: 2px; margin-top: 8px; /* Sin márgenes negativos para que no se corten las letras */
    display: inline-block;
}

/* Submit Button */
div[data-testid="stForm"] button[kind="primaryFormSubmit"] {
    background: linear-gradient(135deg, #2D5A27 0%, #43843C 100%) !important;
    color: white !important; border: none !important; border-radius: 8px !important;
    padding: 12px !important; font-size: 0.95rem !important; font-weight: 700 !important;
    width: 100% !important; margin-top: 14px !important;
    box-shadow: 0 4px 12px rgba(45, 90, 39, 0.3) !important;
    transition: all 0.3s ease !important;
}
div[data-testid="stForm"] button[kind="primaryFormSubmit"]:hover {
    transform: translateY(-2px) !important;
    box-shadow: 0 6px 16px rgba(45, 90, 39, 0.4) !important;
    background: linear-gradient(135deg, #43843C 0%, #2D5A27 100%) !important;
}

/* Error banner */
.login-error {
    background: #FEF3F2; border: 1px solid #F04438;
    border-radius: 6px; padding: 10px; color: #B42318; font-size: 0.8rem;
    font-weight: 600; text-align: center; margin-bottom: 12px;
}

/* Footer fijo al borde inferior de la pantalla */
.login-footer-out {
    position: fixed; bottom: 20px; width: 100%; text-align: center;
    color: #667085; font-size: 0.75rem; z-index: 10; font-family: 'Inter', sans-serif;
}
</style>
"""


# ── Pantalla de Login ────────────────────────────────────────────────────────

def _render_login() -> bool:
    """Renderiza la pantalla de login premium. Retorna True si se autentica."""
    st.markdown(_LOGIN_CSS, unsafe_allow_html=True)

    # Decorative background dots
    st.markdown('<div class="login-dots"></div>', unsafe_allow_html=True)

    # El formulario 'login_form' será el cuadro glassmorphism gracias al CSS
    with st.form("login_form", clear_on_submit=False):
        st.markdown("""
            <div class="login-logo">
                <div class="login-logo-icon">🌿</div>
                <h2>ACP MDM PORTAL</h2>
                <p>Data Quality · Enterprise</p>
            </div>
        """, unsafe_allow_html=True)

        # Mensaje de error (si hubo un intento fallido previo)
        if st.session_state.get("login_error"):
            st.markdown(
                '<div class="login-error">🔒 Credenciales inválidas.</div>',
                unsafe_allow_html=True,
            )

        st.markdown('<div class="login-label">Usuario</div>', unsafe_allow_html=True)
        username = st.text_input(
            "Usuario", key="login_username",
            placeholder="ej: admin",
            label_visibility="collapsed",
        )

        st.markdown('<div class="login-label">Contraseña</div>', unsafe_allow_html=True)
        password = st.text_input(
            "Contraseña", key="login_password",
            type="password",
            placeholder="••••••••",
            label_visibility="collapsed",
        )

        # Al usar st.form, el usuario puede presionar 'Enter' para enviar
        submit = st.form_submit_button("🔐 Iniciar Sesión", type="primary", use_container_width=True)
        
        if submit:
            user = _validar_credenciales(username, password)
            if user:
                st.session_state["autenticado"]      = True
                st.session_state["username"]          = username.lower().strip()
                st.session_state["nombre_usuario"]    = user["nombre"]
                st.session_state["rol_usuario"]       = user["rol"]
                st.session_state["avatar_usuario"]    = user["avatar"]
                st.session_state["jwt_token"]         = user.get("token")
                st.session_state["login_time"]        = time.time()
                st.session_state["login_error"]       = False
                st.rerun()
            else:
                st.session_state["login_error"] = True
                st.rerun()

    # Footer independiente del card, abajo de la pantalla
    st.markdown("""
        <div class="login-footer-out">
            ACP Equipo de Proyecciones · 2026
        </div>
    """, unsafe_allow_html=True)

    return False


# ── Gate principal ───────────────────────────────────────────────────────────

def login_gate() -> bool:
    """
    Punto de entrada de autenticación.
    Si el usuario NO está autenticado, renderiza login y retorna False.
    Si SÍ está autenticado, retorna True (el caller continúa con el portal).
    """
    if st.session_state.get("autenticado"):
        return True
    _render_login()
    return False
