"""
08_olist_streaming_dag.py

DAG para lanzar y monitorear pipeline streaming de Olist
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.dataflow import DataflowStartPythonOperator
from datetime import datetime, timedelta
import os

PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'ecommerce-olist-ben-260301')

default_args = {
    'owner': 'data-engineering',
    'start_date': datetime(2024, 2, 1),
}

dag = DAG(
    'olist_streaming_pipeline',
    default_args=default_args,
    description='Pipeline streaming con Pub/Sub y Dataflow',
    schedule_interval=None,  # Manual trigger
    catchup=False,
    tags=['olist', 'streaming', 'realtime'],
)

# Task: Start Dataflow streaming job
task_start_streaming = DataflowStartPythonOperator(
    task_id='start_dataflow_streaming',
    py_file='/home/youruser/olist-data-pipeline/src/pipelines/olist_streaming_pipeline.py',
    job_name='olist-streaming-{{ ts_nodash }}',
    options={
        'project': PROJECT_ID,
        'region': 'southamerica-west1',
        'runner': 'DataflowRunner',
        'streaming': True,
        'temp_location': f'gs://{PROJECT_ID}-dataflow-temp/temp',
        'staging_location': f'gs://{PROJECT_ID}-dataflow-temp/staging',
        'num_workers': 2,
        'max_num_workers': 5,
        'autoscaling_algorithm': 'THROUGHPUT_BASED',
    },
    dag=dag,
)