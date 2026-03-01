"""
06_olist_conditional_pipeline.py

DAG con branching - ejecuta diferentes paths según condiciones
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'data-engineering',
    'start_date': datetime(2024, 2, 1),
}

dag = DAG(
    'olist_conditional_pipeline',
    default_args=default_args,
    description='Pipeline con lógica condicional',
    schedule_interval='0 5 * * *',  # 5 AM
    catchup=False,
    tags=['olist', 'conditional', 'example'],
)

# ==========================================
# BRANCH: Decidir qué path ejecutar
# ==========================================

def decide_processing_strategy(**context):
    """
    Decide si usar procesamiento rápido o completo según volumen de datos
    """
    from google.cloud import bigquery
    
    PROJECT_ID = 'ecommerce-olist-150226'
    
    client = bigquery.Client(project=PROJECT_ID)
    
    # Contar órdenes de ayer
    query = f"""
    SELECT COUNT(*) as order_count
    FROM `{PROJECT_ID}.olist_raw.orders`
    WHERE DATE(order_purchase_timestamp) = CURRENT_DATE() - 1
    """
    
    result = list(client.query(query).result())[0]
    order_count = result['order_count']
    
    logging.info(f"Orders from yesterday: {order_count}")
    
    # Decidir estrategia
    if order_count > 1000:
        logging.info("High volume detected → Using FULL processing")
        return 'full_processing'  # Task ID que debe ejecutarse
    else:
        logging.info("Low volume detected → Using QUICK processing")
        return 'quick_processing'  # Task ID alternativo

task_branch = BranchPythonOperator(
    task_id='decide_strategy',
    python_callable=decide_processing_strategy,
    dag=dag,
)

# ==========================================
# PATH 1: Procesamiento Rápido
# ==========================================

def quick_process(**context):
    """Procesamiento ligero para bajo volumen"""
    
    logging.info("=" * 60)
    logging.info("QUICK PROCESSING")
    logging.info("=" * 60)
    logging.info("Skipping complex transformations...")
    logging.info("Using simplified aggregations...")
    logging.info("Estimated time: X minutes")
    logging.info("=" * 60)
    
    # Aquí iría procesamiento simplificado
    
    return "Quick processing completed"

task_quick = PythonOperator(
    task_id='quick_processing',
    python_callable=quick_process,
    dag=dag,
)

# ==========================================
# PATH 2: Procesamiento Completo
# ==========================================

def full_process(**context):
    """Procesamiento completo para alto volumen"""
    
    logging.info("=" * 60)
    logging.info("FULL PROCESSING")
    logging.info("=" * 60)
    logging.info("Running all transformations...")
    logging.info("Computing complex features...")
    logging.info("Estimated time: X minutes")
    logging.info("=" * 60)
    
    # Aquí iría procesamiento completo
    
    return "Full processing completed"

task_full = PythonOperator(
    task_id='full_processing',
    python_callable=full_process,
    dag=dag,
)

# ==========================================
# CONVERGENCIA: Ambos paths se unen aquí
# ==========================================

task_finalize = BashOperator(
    task_id='finalize',
    bash_command='echo "Processing complete. Generating final reports..."',
    trigger_rule='none_failed_min_one_success',  # Ejecuta si al menos uno de los paths tuvo éxito
    dag=dag,
)

# ==========================================
# DEPENDENCIAS
# ==========================================

task_branch >> [task_quick, task_full]
task_quick >> task_finalize
task_full >> task_finalize