"""
02_xcom_example_dag.py

Ejemplo de comunicación entre tasks usando XCom
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import random

default_args = {
    'owner': 'data-team',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
}

dag = DAG(
    'xcom_example',
    default_args=default_args,
    description='Ejemplo de XCom para compartir datos entre tasks',
    schedule_interval=None,  # Manual
    catchup=False,
    tags=['example', 'xcom'],
)

def generate_data(**context):
    """
    Task 1: Genera datos y los pushea a XCom
    """
    data = {
        'total_orders': random.randint(100, 1000),
        'total_revenue': round(random.uniform(10000, 50000), 2),
        'generated_at': datetime.now().isoformat()
    }
    
    print(f"Generated data: {data}")
    
    # Push a XCom (retornando el valor)
    return data

def process_data(**context):
    """
    Task 2: Pull data from XCom y procesa
    """
    # Pull del XCom de la task anterior
    ti = context['ti']  # Task Instance
    data = ti.xcom_pull(task_ids='generate_data')
    
    print(f"Received data: {data}")
    
    # Procesar
    processed = {
        'orders': data['total_orders'],
        'revenue': data['total_revenue'],
        'avg_order_value': round(data['total_revenue'] / data['total_orders'], 2),
        'processed_at': datetime.now().isoformat()
    }
    
    print(f"Processed data: {processed}")
    
    return processed

def send_alert(**context):
    """
    Task 3: Usa datos procesados para enviar alerta
    """
    ti = context['ti']
    processed = ti.xcom_pull(task_ids='process_data')
    
    print("=" * 60)
    print("📊 DAILY REPORT")
    print("=" * 60)
    print(f"Total Orders: {processed['orders']}")
    print(f"Total Revenue: ${processed['revenue']:,.2f}")
    print(f"Avg Order Value: ${processed['avg_order_value']:.2f}")
    print("=" * 60)
    
    # Aquí podrías enviar a Slack, email, etc.

# Define tasks
task_generate = PythonOperator(
    task_id='generate_data',
    python_callable=generate_data,
    dag=dag,
)

task_process = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    dag=dag,
)

task_alert = PythonOperator(
    task_id='send_alert',
    python_callable=send_alert,
    dag=dag,
)

# Dependencies
task_generate >> task_process >> task_alert
