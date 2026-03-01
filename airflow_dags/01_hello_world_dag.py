"""
01_hello_world_dag.py

DAG de ejemplo para entender conceptos básicos de Airflow
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging

# ==========================================
# CONFIGURACIÓN DEL DAG
# ==========================================

# Default arguments aplicados a todas las tasks
default_args = {
    'owner': 'data-team',
    'depends_on_past': False,  # No depende de ejecuciones previas
    'start_date': datetime(2024, 1, 1),  # Fecha de inicio
    'email': ['alerts@example.com'],
    'email_on_failure': False,  # Enviar email si falla
    'email_on_retry': False,
    'retries': 2,  # Número de reintentos
    'retry_delay': timedelta(minutes=5),  # Tiempo entre reintentos
}

# Crear DAG
dag = DAG(
    'hello_world',  # ID único del DAG
    default_args=default_args,
    description='DAG simple de ejemplo',
    schedule_interval='@daily',  # Cron: ejecuta diario a medianoche
    # Otros valores: '@hourly', '@weekly', '0 0 * * *', None (manual)
    catchup=False,  # No ejecutar fechas pasadas
    tags=['example', 'tutorial'],  # Tags para organizar
)

# ==========================================
# DEFINIR TASKS
# ==========================================

def print_hello():
    """Función Python simple"""
    logging.info("=" * 60)
    logging.info("HELLO FROM AIRFLOW!")
    logging.info("=" * 60)
    logging.info("This is running inside a PythonOperator")
    logging.info(f"Execution date: {datetime.now()}")
    return "Hello task completed"

def print_context(**context):
    """
    Función que recibe context de Airflow
    Context contiene metadata de la ejecución
    """
    logging.info("Context information:")
    logging.info(f"  - DAG ID: {context['dag'].dag_id}")
    logging.info(f"  - Task ID: {context['task'].task_id}")
    logging.info(f"  - Execution date: {context['execution_date']}")
    logging.info(f"  - Run ID: {context['run_id']}")
    
    # Retornar valor (puede ser usado por siguientes tasks con XCom)
    return {
        'status': 'success',
        'processed_at': datetime.now().isoformat()
    }

# Task 1: Bash command
task_print_date = BashOperator(
    task_id='print_date',
    bash_command='date',
    dag=dag,
)

# Task 2: Python function simple
task_hello = PythonOperator(
    task_id='say_hello',
    python_callable=print_hello,
    dag=dag,
)

# Task 3: Python function con context
task_context = PythonOperator(
    task_id='print_context',
    python_callable=print_context,
    provide_context=True,  # Inyecta context como kwargs
    dag=dag,
)

# Task 4: Bash command con templating
task_check_files = BashOperator(
    task_id='check_files',
    bash_command="""
    echo "Checking files..."
    ls -la ~/airflow/dags/
    echo "Total DAGs: $(ls ~/airflow/dags/*.py | wc -l)"
    """,
    dag=dag,
)

# Task 5: Python con parametros
def process_data(data_source, **context):
    """
    Función con argumentos personalizados
    """
    logging.info(f"Processing data from: {data_source}")
    
    # Simular procesamiento
    import time
    time.sleep(2)
    
    logging.info("Processing completed!")
    return f"Processed {data_source}"

task_process = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    op_kwargs={'data_source': 'Olist'},  # Pasar argumentos
    dag=dag,
)

# ==========================================
# DEFINIR DEPENDENCIAS (ORDEN DE EJECUCIÓN)
# ==========================================

# Opción 1: Usando >> (recomendado)
task_print_date >> task_hello >> task_context >> task_check_files >> task_process

# Equivalente a:
# task_print_date.set_downstream(task_hello)
# task_hello.set_downstream(task_context)
# etc...

# También puedes hacer dependencias en paralelo:
# task1 >> [task2, task3] >> task4
# Esto ejecuta task2 y task3 en paralelo después de task1

# ==========================================
# DOCUMENTACIÓN
# ==========================================

# Esta documentación aparece en la UI de Airflow
dag.doc_md = """
# Hello World DAG

Este es un DAG de ejemplo para aprender conceptos básicos de Airflow.

## Tasks:
1. **print_date**: Imprime fecha actual
2. **say_hello**: Función Python simple
3. **print_context**: Muestra información del context
4. **check_files**: Lista archivos en dags/
5. **process_data**: Simula procesamiento de datos

## Schedule:
- Ejecuta diariamente a medianoche
- Retries: 2 veces con 5 minutos de delay
"""
