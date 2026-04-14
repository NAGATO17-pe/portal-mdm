import pandas as pd

from dq.validador import validar_dataframe


def test_validar_dataframe_sanidad_activo_procesa_total_plantas():
    df, errores = validar_dataframe(
        pd.DataFrame(
            [
                {
                    'Total_Plantas_Raw': '0',
                }
            ]
        ),
        'sanidad_activo',
    )

    assert 'Total_Plantas_Procesado' in df.columns
    assert df.loc[0, 'Total_Plantas_Procesado'] is None
    assert any(error['columna'] == 'Total_Plantas' for error in errores)
