"""
03_olist_daily_ingestion.py

DAG para ingesta diaria de datos de Olist
Lee archivos de GCS, valida y carga a BigQuery
"""

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.providers.google.cloud.operators.bigquery import (
    BigQueryCheckOperator,
    BigQueryInsertJobOperator,
)
from airflow.providers.google.cloud.sensors.gcs import GCSObjectExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import GCSToBigQueryOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta
import logging

# ==========================================
# CONFIGURACIÓN
# ==========================================

PROJECT_ID = 'ecommerce-olist-150226'  # ⚠️ Cambiar por tu project
GCS_BUCKET = f'{PROJECT_ID}-raw-data'
BQ_DATASET = 'olist_raw'

default_args = {
    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 1),
    'email': ['data@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'sla': timedelta(minutes=30),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'olist_daily_ingestion',
    default_args=default_args,
    description='Ingesta diaria de datos Olist desde GCS a BigQuery',
    schedule_interval='0 2 * * *',  # 2 AM DIARIO
    catchup=False,
    tags=['olist', 'ingestion', 'production'],
)

# ==========================================
# TASK 1: Verificar archivos en GCS
# ==========================================

def check_gcs_files(**context):
    """
    Verifica que los archivos necesarios existan en GCS
    """
    from google.cloud import storage
    
    execution_date = context['execution_date']
    date_str = execution_date.strftime('%Y-%m-%d')
    
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(GCS_BUCKET)
    
    required_files = [
        f'olist/{date_str}/olist_orders_dataset.csv',
        f'olist/{date_str}/olist_order_items_dataset.csv',
        f'olist/{date_str}/olist_order_payments_dataset.csv',
    ]
    
    missing_files = []
    
    for file_path in required_files:
        blob = bucket.blob(file_path)
        if not blob.exists():
            missing_files.append(file_path)
            logging.warning(f"Missing file: {file_path}")
        else:
            logging.info(f"Found file: {file_path}")
    
    if missing_files:
        raise FileNotFoundError(
            f"Missing {len(missing_files)} required files: {missing_files}"
        )
    
    logging.info("✅ All required files found")
    return True

task_check_files = PythonOperator(
    task_id='check_gcs_files',
    python_callable=check_gcs_files,
    dag=dag,
)

# ==========================================
# TASK 2: Validar calidad de datos
# ==========================================

def validate_data_quality(**context):
    """
    Ejecuta checks de calidad en los datos
    """
    from google.cloud import storage
    import pandas as pd
    import io
    
    execution_date = context['execution_date']
    date_str = execution_date.strftime('%Y-%m-%d')
    
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(GCS_BUCKET)
    
    # Leer archivo de órdenes
    blob = bucket.blob(f'olist/{date_str}/olist_orders_dataset.csv')
    content = blob.download_as_string()
    orders_df = pd.read_csv(io.StringIO(content.decode('utf-8')))
    
    logging.info(f"Loaded {len(orders_df)} orders")
    
    # Check 1: Verificar que no esté vacío
    assert len(orders_df) > 0, "Orders file is empty"
    
    # Check 2: Verificar columnas requeridas
    required_columns = ['order_id', 'customer_id', 'order_status', 'order_purchase_timestamp']
    missing_columns = set(required_columns) - set(orders_df.columns)
    assert len(missing_columns) == 0, f"Missing columns: {missing_columns}"
    
    # Check 3: Verificar order_id únicos
    duplicates = orders_df['order_id'].duplicated().sum()
    assert duplicates == 0, f"Found {duplicates} duplicate order_ids"
    
    # # Check 4: Verificar valores nulos en columnas críticas
    # null_counts = orders_df[required_columns].isnull().sum()
    # assert null_counts.sum() == 0, f"Found null values: {null_counts.to_dict()}"
    
    logging.info("✅ All quality checks passed")
    
    # Push metadata a XCom
    return {
        'total_orders': len(orders_df),
        'date': date_str,
        'status': 'validated'
    }

task_validate = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    dag=dag,
)

# ==========================================
# TASK 3: Cargar a BigQuery
# ==========================================

def load_to_bigquery(**context):
    """
    Carga archivos CSV desde GCS a BigQuery
    """
    from google.cloud import bigquery
    
    execution_date = context['execution_date']
    date_str = execution_date.strftime('%Y-%m-%d')
    
    client = bigquery.Client(project=PROJECT_ID)
    
    # Configurar tabla destino
    table_id = f"{PROJECT_ID}.{BQ_DATASET}.orders"
    
    # URI del archivo en GCS
    uri = f"gs://{GCS_BUCKET}/olist/{date_str}/olist_orders_dataset.csv"
    
    # Configuración del load job
    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV,
        skip_leading_rows=1,
        autodetect=False,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,  # Append, no overwrite
        schema=[
            bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("order_status", "STRING"),
            bigquery.SchemaField("order_purchase_timestamp", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("order_approved_at", "TIMESTAMP"),
            bigquery.SchemaField("order_delivered_carrier_date", "TIMESTAMP"),
            bigquery.SchemaField("order_delivered_customer_date", "TIMESTAMP"),
            bigquery.SchemaField("order_estimated_delivery_date", "TIMESTAMP"),
        ]
    )
    
    # Iniciar load job
    load_job = client.load_table_from_uri(uri, table_id, job_config=job_config)
    
    logging.info(f"Starting load job for {uri}")
    
    # Esperar a que termine
    load_job.result()
    
    # Ver resultado
    table = client.get_table(table_id)
    logging.info(f"✅ Loaded {load_job.output_rows} rows to {table_id}")
    logging.info(f"Total rows in table: {table.num_rows:,}")
    
    return {
        'rows_loaded': load_job.output_rows,
        'total_rows': table.num_rows,
        'table_id': table_id
    }

task_load_orders = PythonOperator(
    task_id='load_orders_to_bigquery',
    python_callable=load_to_bigquery,
    pool='bigquery_pool',  # LIMITA A 3 TASKS EN PARALELO SEGÚN CONFIGURACIÓN EN AIRFLOW
    dag=dag,
)

# ==========================================
# TASK 4: Verificar carga en BigQuery
# ==========================================

# Usar BigQueryCheckOperator para verificar datos
task_verify_load = BigQueryCheckOperator(
    task_id='verify_load_success',
    sql=f"""
    SELECT COUNT(*) > 0
    FROM `{PROJECT_ID}.{BQ_DATASET}.orders`
    WHERE DATE(order_purchase_timestamp) = '{{{{ ds }}}}'
    """,
    use_legacy_sql=False,
    dag=dag,
)

# ==========================================
# TASK 5: Limpiar archivos temporales
# ==========================================

def cleanup_temp_files(**context):
    """
    Limpia archivos temporales más antiguos de 7 días
    """
    from google.cloud import storage
    from datetime import datetime, timedelta
    
    client = storage.Client(project=PROJECT_ID)
    bucket = client.bucket(GCS_BUCKET)
    
    # Fecha límite (7 días atrás)
    cutoff_date = datetime.now() - timedelta(days=7)
    
    deleted_count = 0
    
    # Listar archivos en /temp/
    blobs = bucket.list_blobs(prefix='temp/')
    
    for blob in blobs:
        if blob.time_created < cutoff_date:
            blob.delete()
            deleted_count += 1
            logging.info(f"Deleted: {blob.name}")
    
    logging.info(f"✅ Cleaned up {deleted_count} temp files")
    return deleted_count

task_cleanup = PythonOperator(
    task_id='cleanup_temp_files',
    python_callable=cleanup_temp_files,
    trigger_rule=TriggerRule.ALL_DONE,  # Ejecuta aunque fallen tasks anteriores
    dag=dag,
)

# ==========================================
# TASK 6: Enviar notificación de éxito
# ==========================================

def send_success_notification(**context):
    """
    Envía notificación cuando el pipeline termina exitosamente
    """
    ti = context['ti']
    
    # Pull metadata de tasks anteriores
    validation_data = ti.xcom_pull(task_ids='validate_data_quality')
    load_data = ti.xcom_pull(task_ids='load_orders_to_bigquery')
    
    message = f"""
    DAILY INGESTION SUCCESSFUL
    
    Date: {context['ds']}
    Orders validated: {validation_data['total_orders']}
    Rows loaded: {load_data['rows_loaded']}
    Total rows in table: {load_data['total_rows']:,}
    
    Pipeline completed at: {datetime.now().isoformat()}
    """
    

    # Aqui se configura email, Slack, etc.
    # Por ahora solo un logging
    logging.info(message)
    
    # from airflow.providers.slack.operators.slack_webhook import SlackWebhookOperator
    
    return message

task_notify = PythonOperator(
    task_id='send_success_notification',
    python_callable=send_success_notification,
    trigger_rule=TriggerRule.ALL_SUCCESS,
    dag=dag,
)

# ==========================================
# TASK 7: Manejo de errores
# ==========================================

def handle_failure(**context):
    """
    Maneja fallos del pipeline
    """
    error_message = f"""
    DAILY INGESTION FAILED
    
    Date: {context['ds']}
    DAG: {context['dag'].dag_id}
    Task: {context['task'].task_id}
    
    Check logs at: http://localhost:8080
    """
    
    
    
    # Enviar alerta  a correo, slack, etc
    logging.error(error_message)
    # send_slack_alert(error_message)
    # send_pagerduty_alert(error_message)
    
    return error_message

task_handle_failure = PythonOperator(
    task_id='handle_failure',
    python_callable=handle_failure,
    trigger_rule=TriggerRule.ONE_FAILED,  # Ejecuta si alguna task falla
    dag=dag,
)

# ==========================================
# DEPENDENCIAS
# ==========================================

# Flujo normal
task_check_files >> task_validate >> task_load_orders >> task_verify_load >> [task_notify, task_cleanup]

# Flujo de error
[task_check_files, task_validate, task_load_orders, task_verify_load] >> task_handle_failure

# ==========================================
# DOCUMENTACIÓN
# ==========================================

dag.doc_md = """
# Daily Ingestion DAG

Pipeline de ingesta diaria de datos de Olist desde GCS a BigQuery.

## Flujo:
1. Verificar archivos en GCS
2. Validar calidad de datos
3. Cargar a BigQuery (APPEND mode)
4. Verificar carga exitosa
5. Limpiar archivos temporales
6. Enviar notificación

## Schedule:
- Ejecuta diariamente a las 2 AM
- Retries: 2 veces con 5 minutos de delay

## Alertas:
- Email si falla cualquier task
- Slack notification en éxito/fallo

## Monitoreo:
- Ver dashboard: http://localhost:8080
- Logs en: ~/airflow/logs/
"""
