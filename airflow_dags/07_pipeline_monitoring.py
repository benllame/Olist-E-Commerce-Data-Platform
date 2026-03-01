"""
07_pipeline_monitoring.py

DAG para monitorear salud de otros DAGs
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import DagRun, TaskInstance
from datetime import datetime, timedelta
import logging

default_args = {
    'owner': 'platform',
    'start_date': datetime(2024, 2, 1),
}

dag = DAG(
    'pipeline_monitoring',
    default_args=default_args,
    description='Monitoreo de salud de pipelines',
    schedule_interval='0 */6 * * *',  # Cada 6 horas
    catchup=False,
    tags=['monitoring', 'ops'],
)

def check_dag_health(**context):
    """
    Revisa salud de DAGs críticos en las últimas 24 horas
    """
    from airflow.models import DagBag
    from sqlalchemy import func
    
    critical_dags = [
        'olist_daily_ingestion',
        'olist_transformations',
        'olist_daily_report'
    ]
    
    session = context['ti'].get_dagrun().get_session()
    
    report = []
    
    for dag_id in critical_dags:
        # Obtener últimas 10 ejecuciones
        dag_runs = session.query(DagRun).filter(
            DagRun.dag_id == dag_id,
            DagRun.execution_date >= datetime.now() - timedelta(days=1)
        ).order_by(DagRun.execution_date.desc()).limit(10).all()
        
        if not dag_runs:
            report.append({
                'dag_id': dag_id,
                'status': 'NO_RUNS',
                'message': 'No runs in last 24 hours'
            })
            continue
        
        # Calcular estadísticas
        success_count = sum(1 for run in dag_runs if run.state == 'success')
        failed_count = sum(1 for run in dag_runs if run.state == 'failed')
        
        # Tiempo promedio de ejecución
        durations = []
        for run in dag_runs:
            if run.end_date and run.start_date:
                duration = (run.end_date - run.start_date).total_seconds() / 60
                durations.append(duration)
        
        avg_duration = sum(durations) / len(durations) if durations else 0
        
        # Determinar salud
        if failed_count > 2:
            status = 'UNHEALTHY'
        elif success_count / len(dag_runs) < 0.8:
            status = 'DEGRADED'
        else:
            status = 'HEALTHY'
        
        report.append({
            'dag_id': dag_id,
            'status': status,
            'runs': len(dag_runs),
            'success': success_count,
            'failed': failed_count,
            'avg_duration_min': round(avg_duration, 2)
        })
    
    # Log reporte
    logging.info("=" * 80)
    logging.info("PIPELINE HEALTH REPORT")
    logging.info("=" * 80)
    
    for item in report:
        icon = {'HEALTHY': '✅', 'DEGRADED': '⚠️', 'UNHEALTHY': '❌', 'NO_RUNS': '⏸️'}
        logging.info(f"{icon.get(item['status'], '❓')} {item['dag_id']}: {item['status']}")
        if 'runs' in item:
            logging.info(f"   Runs: {item['runs']} | Success: {item['success']} | Failed: {item['failed']}")
            logging.info(f"   Avg Duration: {item['avg_duration_min']:.2f} min")
    
    logging.info("=" * 80)
    
    # Enviar alertas si hay problemas
    unhealthy = [r for r in report if r['status'] in ['UNHEALTHY', 'NO_RUNS']]
    
    if unhealthy:
        logging.warning(f"{len(unhealthy)} DAGs require attention!")
        # Aquí enviarías alerta a Slack/PagerDuty
    
    return report

task_health_check = PythonOperator(
    task_id='check_dag_health',
    python_callable=check_dag_health,
    dag=dag,
)

def check_slow_tasks(**context):
    """
    Identifica tasks que están tardando más de lo esperado
    """
    from airflow.models import TaskInstance as TI
    from sqlalchemy import func
    
    session = context['ti'].get_dagrun().get_session()
    
    # Encontrar tasks que tardaron más de 30 minutos en últimas 24 horas
    slow_tasks = session.query(
        TI.dag_id,
        TI.task_id,
        func.avg(
            func.extract('epoch', TI.end_date) - func.extract('epoch', TI.start_date)
        ).label('avg_duration_sec')
    ).filter(
        TI.start_date >= datetime.now() - timedelta(days=1),
        TI.end_date.isnot(None),
        TI.state == 'success'
    ).group_by(
        TI.dag_id,
        TI.task_id
    ).having(
        func.avg(
            func.extract('epoch', TI.end_date) - func.extract('epoch', TI.start_date)
        ) > 1800  # 30 minutos
    ).all()
    
    logging.info("=" * 80)
    logging.info("SLOW TASKS REPORT (> 30 min)")
    logging.info("=" * 80)
    
    if not slow_tasks:
        logging.info("✅ No slow tasks detected")
    else:
        for task in slow_tasks:
            duration_min = task.avg_duration_sec / 60
            logging.warning(f"⏱️  {task.dag_id}.{task.task_id}: {duration_min:.2f} min")
    
    logging.info("=" * 80)
    
    return slow_tasks

task_slow_tasks = PythonOperator(
    task_id='check_slow_tasks',
    python_callable=check_slow_tasks,
    dag=dag,
)

# Dependencias
task_health_check >> task_slow_tasks