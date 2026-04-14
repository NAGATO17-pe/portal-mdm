import json
from pathlib import Path

from bronce import cargador, rutas


def test_archivar_archivo_copia_y_marca_si_el_origen_esta_bloqueado(tmp_path, monkeypatch):
    origen = tmp_path / 'entrada' / 'peladas' / 'Peladas_V2.xlsx'
    origen.parent.mkdir(parents=True, exist_ok=True)
    origen.write_bytes(b'xlsx-falso')

    procesados = tmp_path / 'procesados'
    monkeypatch.setattr(cargador, 'CARPETA_PROCESADOS', procesados)

    movimiento_real = cargador.shutil.move

    def _move_bloqueado(_src, _dst):
        raise PermissionError('archivo bloqueado')

    monkeypatch.setattr(cargador.shutil, 'move', _move_bloqueado)

    destino, bloqueado = cargador.archivar_archivo(origen, 'peladas')

    assert bloqueado is True
    assert destino.exists()
    assert origen.exists()

    ruta_marca = origen.with_name(f'{origen.name}.procesado.json')
    assert ruta_marca.exists()

    payload = json.loads(ruta_marca.read_text(encoding='utf-8'))
    assert payload['estado'] == 'PROCESADO'
    assert payload['destino'] == str(destino)

    monkeypatch.setattr(cargador.shutil, 'move', movimiento_real)


def test_obtener_archivo_mas_reciente_omite_archivo_marcado(tmp_path):
    archivo = tmp_path / 'Peladas_V2.xlsx'
    archivo.write_bytes(b'xlsx-falso')

    ruta_marca = rutas._ruta_marca_archivo(archivo)
    ruta_marca.write_text(
        json.dumps({
            'estado': 'PROCESADO',
            'tamano_bytes': archivo.stat().st_size,
            'mtime_ns': archivo.stat().st_mtime_ns,
        }),
        encoding='utf-8',
    )

    assert rutas.obtener_archivo_mas_reciente(tmp_path) is None
