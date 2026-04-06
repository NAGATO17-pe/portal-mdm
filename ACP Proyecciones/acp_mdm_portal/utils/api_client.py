import json
import requests
import streamlit as st

URL_BASE = "http://127.0.0.1:8000/api/v1"

def _get_headers():
    token = st.session_state.get("jwt_token")
    if token:
        return {"Authorization": f"Bearer {token}", "Content-Type": "application/json"}
    return {"Content-Type": "application/json"}

def login_backend(username, password):
    try:
        url_login = "http://127.0.0.1:8000/auth/login"
        payload = {"username": username, "password": password}
        headers = {"Content-Type": "application/x-www-form-urlencoded"}
        res = requests.post(url_login, data=payload, headers=headers)
        if res.status_code == 200:
            return res.json().get("access_token")
        return None
    except Exception as e:
        st.error(f"Error conectando al backend: {e}")
        return None

def get_api(endpoint: str, base_url: str = URL_BASE):
    try:
        return requests.get(f"{base_url}{endpoint}", headers=_get_headers())
    except Exception as e:
        return None

def post_api(endpoint: str, payload: dict, base_url: str = URL_BASE):
    try:
        return requests.post(f"{base_url}{endpoint}", json=payload, headers=_get_headers())
    except Exception as e:
        return None
        
def patch_api(endpoint: str, payload: dict, base_url: str = URL_BASE):
    try:
        return requests.patch(f"{base_url}{endpoint}", json=payload, headers=_get_headers())
    except Exception as e:
        return None
