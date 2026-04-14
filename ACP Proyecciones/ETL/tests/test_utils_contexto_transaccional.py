from utils.contexto_transaccional import ContextoTransaccionalETL, administrar_recurso_db


def test_administrar_recurso_db_reusa_conexion_existente():
    class _Conexion:
        def execute(self, *_args, **_kwargs):
            return None

    conexion = _Conexion()

    with administrar_recurso_db(conexion) as conexion_resultado:
        assert conexion_resultado is conexion


def test_contexto_transaccional_etl_delega_en_misma_conexion(monkeypatch):
    class _Conexion:
        pass

    class _Gestor:
        def __init__(self, conexion):
            self._conexion = conexion

        def __enter__(self):
            return self._conexion

        def __exit__(self, exc_type, exc, tb):
            return False

    class _Engine:
        def __init__(self):
            self.conexion = _Conexion()

        def begin(self):
            return _Gestor(self.conexion)

    llamadas = []

    def _marcar_estado(conexion, tabla_origen, columna_id, ids, estado='PROCESADO', tam_lote=2000):
        llamadas.append((conexion, tabla_origen, columna_id, tuple(ids), estado, tam_lote))
        return len(ids)

    monkeypatch.setattr(
        'utils.sql_lotes.marcar_estado_carga_por_ids',
        _marcar_estado,
    )

    engine = _Engine()
    with ContextoTransaccionalETL(engine) as contexto:
        total = contexto.marcar_estado_carga('Bronce.Test', 'ID_Test', [1, 2])

    assert total == 2
    assert len(llamadas) == 1
    assert llamadas[0][0] is engine.conexion
    assert llamadas[0][1] == 'Bronce.Test'
