from unittest.mock import MagicMock

from dq.cuarentena import (
    _deduplicar_payload_pendiente,
    _normalizar_payload_cuarentena,
    enviar_a_cuarentena,
)


def test_deduplicar_payload_pendiente_colapsa_duplicados_del_mismo_lote():
    payload = [
        {
            'tabla_origen': 'Bronce.Clima',
            'campo_origen': 'Humedad_Raw',
            'valor_recibido': 'nan',
            'motivo': 'Humedad fuera de rango',
            'tipo_regla': 'DQ',
            'score': None,
            'id_registro_origen': 10,
            'fecha_ingreso': object(),
        },
        {
            'tabla_origen': 'Bronce.Clima',
            'campo_origen': 'Humedad_Raw',
            'valor_recibido': 'nan',
            'motivo': 'Humedad fuera de rango',
            'tipo_regla': 'DQ',
            'score': None,
            'id_registro_origen': 10,
            'fecha_ingreso': object(),
        },
    ]

    deduplicado = _deduplicar_payload_pendiente(payload)

    assert len(deduplicado) == 1


def test_normalizar_payload_cuarentena_usa_defaults_estables():
    payload = _normalizar_payload_cuarentena('Bronce.Clima', {}, fecha_ingreso=object())

    assert payload['tabla_origen'] == 'Bronce.Clima'
    assert payload['campo_origen'] == 'DESCONOCIDA'
    assert payload['motivo'] == 'Sin motivo'
    assert payload['tipo_regla'] == 'DQ'


def test_enviar_a_cuarentena_retorna_cero_si_no_hay_filas():
    engine = MagicMock()

    assert enviar_a_cuarentena(engine, 'Bronce.Clima', []) == 0


def test_enviar_a_cuarentena_deduplica_antes_de_insertar():
    class _Resultado:
        rowcount = 1

    class _Conexion:
        def __init__(self):
            self.ejecuciones = []

        def execute(self, sentencia, payload):
            self.ejecuciones.append(list(payload))
            return _Resultado()

    class _Contexto:
        def __init__(self, conexion):
            self.conexion = conexion

        def __enter__(self):
            return self.conexion

        def __exit__(self, exc_type, exc, tb):
            return False

    conexion = _Conexion()
    engine = MagicMock()
    engine.begin.return_value = _Contexto(conexion)

    filas = [
        {'columna': 'Humedad_Raw', 'valor': 'nan', 'motivo': 'Humedad fuera de rango', 'id_registro_origen': 11},
        {'columna': 'Humedad_Raw', 'valor': 'nan', 'motivo': 'Humedad fuera de rango', 'id_registro_origen': 11},
    ]

    insertadas = enviar_a_cuarentena(engine, 'Bronce.Clima', filas)

    assert insertadas == 1
    assert len(conexion.ejecuciones) == 1
    assert len(conexion.ejecuciones[0]) == 1
