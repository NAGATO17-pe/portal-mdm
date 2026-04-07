from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

# Funciones de prueba de Python
def print_hello():
    print("¡Hola desde el DAG de prueba de Airflow!")
    return "Hola"

# Configuración básica del DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'dag_de_prueba_inicial',
    default_args=default_args,
    description='Un DAG sencillo para probar el entorno de Airflow',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['prueba'],
) as dag:

    # Tarea 1: Ejecutar script en la terminal (Bash)
    tarea_bash = BashOperator(
        task_id='imprimir_fecha',
        bash_command='date',
    )

    # Tarea 2: Ejecutar la función de Python
    tarea_python = PythonOperator(
        task_id='decir_hola',
        python_callable=print_hello,
    )

    # Tarea 3: Otra tarea Bash de cierre
    tarea_cierre = BashOperator(
        task_id='tarea_finalizada',
        bash_command='echo "¡El DAG ha terminado con éxito!"',
    )

    # Definir el orden de ejecución (Dependencias)
    tarea_bash >> tarea_python >> tarea_cierre
