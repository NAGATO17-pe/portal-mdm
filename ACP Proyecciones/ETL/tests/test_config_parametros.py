from types import SimpleNamespace

from config import parametros


class _Resultado:
    def __init__(self, filas):
        self._filas = list(filas)

    def fetchall(self):
        return list(self._filas)


class _Conexion:
    def __init__(self, nombre_columna: str):
        self.nombre_columna = nombre_columna

    def execute(self, stmt):
        sql = str(stmt)
        if "INFORMATION_SCHEMA.COLUMNS" in sql:
            return _Resultado([(self.nombre_columna,)])
        return _Resultado([
            SimpleNamespace(Parametro="CAMA_MIN_PERMITIDA", Valor="0"),
            SimpleNamespace(Parametro="TABLAS_BRONCE_SP_CAMA", Valor="Bronce.Evaluacion_Pesos,Bronce.Evaluacion_Vegetativa"),
        ])

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        return False


class _Engine:
    def __init__(self, nombre_columna: str):
        self.nombre_columna = nombre_columna

    def connect(self):
        return _Conexion(self.nombre_columna)


def test_cargar_parametros_soporta_nombre_parametro(monkeypatch):
    parametros.limpiar_cache()
    monkeypatch.setattr(parametros, "obtener_engine", lambda: _Engine("Nombre_Parametro"))

    resultado = parametros.cargar_parametros()

    assert resultado["CAMA_MIN_PERMITIDA"] == "0"


def test_obtener_lista_acepta_json_y_csv(monkeypatch):
    parametros.limpiar_cache()
    monkeypatch.setattr(
        parametros,
        "cargar_parametros",
        lambda: {
            "LISTA_JSON": '["a", "b"]',
            "LISTA_CSV": "x, y;z|w",
        },
    )

    assert parametros.obtener_lista("LISTA_JSON") == ["a", "b"]
    assert parametros.obtener_lista("LISTA_CSV") == ["x", "y", "z", "w"]
