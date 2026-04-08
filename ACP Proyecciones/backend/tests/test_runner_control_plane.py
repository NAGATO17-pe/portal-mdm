from __future__ import annotations

from types import SimpleNamespace

import repositorios.repo_control as repo_control
from runner import runner


class _ResultadoExecute:
    def __init__(self, fila):
        self._fila = fila

    def fetchone(self):
        return self._fila


class _ConexionFake:
    def __init__(self, fila):
        self._fila = fila
        self.sql_ejecutado = None

    def execute(self, sentencia):
        self.sql_ejecutado = getattr(sentencia, "text", str(sentencia))
        return _ResultadoExecute(self._fila)


class _ContextoBeginFake:
    def __init__(self, conexion):
        self._conexion = conexion

    def __enter__(self):
        return self._conexion

    def __exit__(self, exc_type, exc, tb):
        return False


class _EngineFake:
    def __init__(self, conexion):
        self._conexion = conexion

    def begin(self):
        return _ContextoBeginFake(self._conexion)


def test_tomar_comando_pendiente_consume_retry_en_fifo(monkeypatch):
    fila = SimpleNamespace(
        _mapping={
            "ID_Comando": 11,
            "ID_Corrida": "corrida-r2",
            "Tipo_Comando": "REINTENTAR",
            "Iniciado_Por": "operador01",
            "Comentario": "__ETL_OPTS__{}",
            "Max_Reintentos": 1,
            "Timeout_Seg": 900,
        }
    )
    conexion = _ConexionFake(fila)
    monkeypatch.setattr(repo_control, "obtener_engine", lambda: _EngineFake(conexion))

    comando = repo_control.tomar_comando_pendiente()

    assert comando["Tipo_Comando"] == "REINTENTAR"
    assert "Tipo_Comando IN ('INICIAR', 'REINTENTAR')" in conexion.sql_ejecutado
    assert "ORDER BY Fecha_Comando ASC, ID_Comando ASC" in conexion.sql_ejecutado


def test_runner_reintento_preserva_comentario_etl(monkeypatch):
    comentario_etl = '__ETL_OPTS__{"m":"facts","f":["Fact_Telemetria_Clima"],"d":1,"g":0,"b":1}'
    cmd = {
        "ID_Corrida": "corrida-001",
        "ID_Comando": 77,
        "Iniciado_Por": "operador01",
        "Comentario": comentario_etl,
        "Max_Reintentos": 2,
        "Timeout_Seg": 1200,
    }

    llamadas: dict[str, dict] = {}
    estados_comando: list[tuple[int, str, str | None]] = []

    monkeypatch.setattr(runner.rc, "adquirir_lock", lambda *_args, **_kwargs: True)
    monkeypatch.setattr(runner.rc, "liberar_lock", lambda *_args, **_kwargs: None)
    monkeypatch.setattr(
        runner.rc,
        "marcar_comando",
        lambda id_comando, estado, mensaje=None: estados_comando.append((id_comando, estado, mensaje)),
    )
    monkeypatch.setattr(runner, "ejecutar_corrida", lambda **_kwargs: "ERROR")
    monkeypatch.setattr(
        runner.rc,
        "obtener_corrida",
        lambda _id_corrida: {"intento_numero": 1, "comentario": comentario_etl},
    )
    monkeypatch.setattr(
        runner.rc,
        "insertar_corrida",
        lambda **kwargs: llamadas.setdefault("insertar_corrida", kwargs),
    )
    monkeypatch.setattr(
        runner.rc,
        "encolar_comando",
        lambda **kwargs: llamadas.setdefault("encolar_comando", kwargs),
    )

    runner._procesar_comando(cmd)

    assert estados_comando[-1][1] == "PROCESADO"
    assert llamadas["insertar_corrida"]["comentario"] == comentario_etl
    assert llamadas["insertar_corrida"]["timeout_segundos"] == 1200
    assert llamadas["encolar_comando"]["tipo_comando"] == "REINTENTAR"
    assert llamadas["encolar_comando"]["comentario"] == comentario_etl
