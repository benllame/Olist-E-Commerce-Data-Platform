"""
05_olist_daily_report.py

DAG de reportes diarios que espera a que transformaciones termine
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from datetime import datetime, timedelta
import logging
import os

# ==========================================
# CONFIGURACIÓN
# ==========================================

PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'ecommerce-olist-ben-260301')

default_args = {
    'owner': 'analytics',
    'start_date': datetime(2024, 2, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'olist_daily_report',
    default_args=default_args,
    description='Genera reportes diarios de métricas clave',
    schedule_interval='0 4 * * *',  # 4 AM
    catchup=False,
    tags=['olist', 'reporting', 'production'],
)

# ==========================================
# SENSOR: Esperar a que transformaciones termine
# ==========================================

wait_for_transformations = ExternalTaskSensor(
    task_id='wait_for_transformations',
    external_dag_id='olist_transformations',  # El DAG que debe terminar
    external_task_id=None,  # None = espera a que todo el DAG termine
    allowed_states=['success'],  # Solo si fue exitoso
    failed_states=['failed'],  # Si falló, este task falla
    mode='reschedule',  # Libera el worker mientras espera
    timeout=7200,  # Espera máximo 2 horas
    poke_interval=60,  # Chequea cada 60 segundos
    dag=dag,
)

# ==========================================
# REPORTE 1: KPIs Diarios
# ==========================================

sql_daily_kpis = f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.olist_analytics.report_daily_kpis` AS
SELECT 
  CURRENT_DATE() as report_date,
  
  -- Métricas de ventas
  (SELECT COUNT(*) FROM `{PROJECT_ID}.olist_analytics.daily_metrics` 
   WHERE order_date = CURRENT_DATE() - 1) as orders_yesterday,
  
  (SELECT SUM(total_revenue) FROM `{PROJECT_ID}.olist_analytics.daily_metrics`
   WHERE order_date = CURRENT_DATE() - 1) as revenue_yesterday,
  
  (SELECT AVG(total_revenue) FROM `{PROJECT_ID}.olist_analytics.daily_metrics`
   WHERE order_date >= CURRENT_DATE() - 7) as avg_revenue_7d,
  
  -- Métricas de clientes
  (SELECT COUNT(DISTINCT customer_unique_id) 
   FROM `{PROJECT_ID}.olist_analytics.customer_segments`
   WHERE customer_segment = 'champions') as champions_count,
  
  (SELECT COUNT(DISTINCT customer_unique_id)
   FROM `{PROJECT_ID}.olist_analytics.customer_segments`
   WHERE churn_risk IN ('high', 'very_high')) as at_risk_customers,
  
  -- Métricas de productos
  (SELECT COUNT(*) FROM `{PROJECT_ID}.olist_analytics.product_analytics`
   WHERE avg_review_score >= 4.5) as top_rated_products,
  
  CURRENT_TIMESTAMP() as generated_at
"""

task_generate_daily_kpis = BigQueryInsertJobOperator(
    task_id='generate_daily_kpis',
    configuration={
        'query': {
            'query': sql_daily_kpis,
            'useLegacySql': False,
        }
    },
    location='southamerica-west1',
    dag=dag,
)

# ==========================================
# REPORTE 2: Top 10 Productos del Día
# ==========================================

sql_top_products = f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.olist_analytics.report_top_products_daily` AS

WITH yesterday_sales AS (
  SELECT 
    oi.product_id,
    p.product_category_name_english as category,
    COUNT(DISTINCT oi.order_id) as orders,
    SUM(oi.price) as revenue,
    AVG(r.review_score) as avg_rating
  FROM `{PROJECT_ID}.olist_raw.order_items` oi
  LEFT JOIN `{PROJECT_ID}.olist_raw.orders` o
    ON oi.order_id = o.order_id
  LEFT JOIN `{PROJECT_ID}.olist_raw.products` p
    ON oi.product_id = p.product_id
  LEFT JOIN `{PROJECT_ID}.olist_raw.product_category_translation` t
    ON p.product_category_name = t.product_category_name
  LEFT JOIN `{PROJECT_ID}.olist_raw.order_reviews` r
    ON o.order_id = r.order_id
  WHERE DATE(o.order_purchase_timestamp) = CURRENT_DATE() - 1
    AND o.order_status = 'delivered'
  GROUP BY oi.product_id, p.product_category_name_english
)

SELECT 
  ROW_NUMBER() OVER (ORDER BY revenue DESC) as rank,
  product_id,
  category,
  orders,
  revenue,
  ROUND(avg_rating, 2) as avg_rating,
  CURRENT_DATE() - 1 as report_date,
  CURRENT_TIMESTAMP() as generated_at
FROM yesterday_sales
ORDER BY revenue DESC
LIMIT 10
"""

task_generate_top_products = BigQueryInsertJobOperator(
    task_id='generate_top_products',
    configuration={
        'query': {
            'query': sql_top_products,
            'useLegacySql': False,
        }
    },
    location='southamerica-west1',
    dag=dag,
)

# ==========================================
# REPORTE 3: Alertas de Negocio
# ==========================================

def check_business_alerts(**context):
    """
    Revisa métricas críticas y genera alertas
    """
    from google.cloud import bigquery
    
    client = bigquery.Client(project=PROJECT_ID)
    
    alerts = []
    
    # Alerta 1: Caída súbita de revenue
    query_revenue_drop = f"""
    WITH revenue_comparison AS (
      SELECT 
        SUM(CASE WHEN order_date = CURRENT_DATE() - 1 THEN total_revenue ELSE 0 END) as yesterday,
        AVG(CASE WHEN order_date BETWEEN CURRENT_DATE() - 8 AND CURRENT_DATE() - 2 
            THEN total_revenue ELSE NULL END) as avg_7d_before
      FROM `{PROJECT_ID}.olist_analytics.daily_metrics`
    )
    SELECT 
      yesterday,
      avg_7d_before,
      ROUND((yesterday - avg_7d_before) / avg_7d_before * 100, 2) as pct_change
    FROM revenue_comparison
    """
    
    result = client.query(query_revenue_drop).result()
    row = list(result)[0]
    
    if row['pct_change'] < -20:
        alerts.append({
            'type': 'CRITICAL',
            'title': 'Revenue Drop Alert',
            'message': f"Revenue dropped {abs(row['pct_change']):.1f}% vs 7-day average"
        })
    
    # Alerta 2: Alto % de cancelaciones
    query_cancellation_rate = f"""
    SELECT 
      ROUND(COUNTIF(order_status = 'canceled') / COUNT(*) * 100, 2) as cancel_rate
    FROM `{PROJECT_ID}.olist_analytics.daily_metrics`
    WHERE order_date = CURRENT_DATE() - 1
    """
    
    result = client.query(query_cancellation_rate).result()
    cancel_rate = list(result)[0]['cancel_rate']
    
    if cancel_rate > 5:
        alerts.append({
            'type': 'WARNING',
            'title': 'High Cancellation Rate',
            'message': f"Cancellation rate at {cancel_rate}% (threshold: 5%)"
        })
    
    # Alerta 3: Churn risk increase
    query_churn_risk = f"""
    SELECT 
      COUNT(*) as at_risk_count
    FROM `{PROJECT_ID}.olist_analytics.customer_segments`
    WHERE churn_risk IN ('high', 'very_high')
      AND days_since_last_order BETWEEN 60 AND 90
    """
    
    result = client.query(query_churn_risk).result()
    at_risk = list(result)[0]['at_risk_count']
    
    if at_risk > 100:
        alerts.append({
            'type': 'INFO',
            'title': 'Churn Risk Alert',
            'message': f"{at_risk} customers at risk of churning (60-90 days inactive)"
        })
    
    # Log todas las alertas
    logging.info("=" * 80)
    logging.info("BUSINESS ALERTS")
    logging.info("=" * 80)
    
    if not alerts:
        logging.info("No alerts. All metrics are healthy.")
    else:
        for alert in alerts:
            icon = {'CRITICAL': '🔴', 'WARNING': '🟡', 'INFO': '🔵'}[alert['type']]
            logging.warning(f"{icon} {alert['type']}: {alert['title']}")
            logging.warning(f"   {alert['message']}")
    
    logging.info("=" * 80)
    
    # Push a XCom
    context['ti'].xcom_push(key='alerts', value=alerts)
    
    return alerts

task_business_alerts = PythonOperator(
    task_id='check_business_alerts',
    python_callable=check_business_alerts,
    dag=dag,
)

# ==========================================
# ENVIAR REPORTE POR EMAIL (simulado)
# ==========================================

def send_daily_report(**context):
    """
    Genera y envía reporte diario por email
    """
    from google.cloud import bigquery
    
    ti = context['ti']
    alerts = ti.xcom_pull(task_ids='check_business_alerts', key='alerts')
    
    client = bigquery.Client(project=PROJECT_ID)
    
    # Obtener KPIs del día
    query = f"""
    SELECT * FROM `{PROJECT_ID}.olist_analytics.report_daily_kpis`
    ORDER BY report_date DESC
    LIMIT 1
    """
    
    kpis = list(client.query(query).result())[0]
    
    # Construir email (simulado)
    email_body = f"""
    ╔══════════════════════════════════════════════════════════╗
    ║           OLIST DAILY REPORT - {context['ds']}           ║
    ╚══════════════════════════════════════════════════════════╝
    
    DAILY METRICS
    ─────────────────────────────────────────────────────────
    Orders Yesterday:       {kpis['orders_yesterday']:,}
    Revenue Yesterday:      ${kpis['revenue_yesterday']:,.2f}
    7-Day Avg Revenue:      ${kpis['avg_revenue_7d']:,.2f}
    
    CUSTOMER METRICS
    ─────────────────────────────────────────────────────────
    Champion Customers:     {kpis['champions_count']:,}
    At-Risk Customers:      {kpis['at_risk_customers']:,}
    
    PRODUCT METRICS
    ─────────────────────────────────────────────────────────
    Top-Rated Products:     {kpis['top_rated_products']:,}
    
    ALERTS
    ─────────────────────────────────────────────────────────
    """
    
    if not alerts:
        email_body += "    No alerts. All systems normal.\n"
    else:
        for alert in alerts:
            email_body += f"    {alert['type']}: {alert['title']}\n"
            email_body += f"      → {alert['message']}\n\n"
    
    email_body += """
    ═════════════════════════════════════════════════════════
    Generated by Airflow - Data Pipeline
    ═════════════════════════════════════════════════════════
    """
    
    logging.info(email_body)
    
    # Aquí enviarías el email real:
    # from airflow.providers.smtp.operators.smtp import EmailOperator
    # send_email(to='team@company.com', subject='Daily Report', body=email_body)
    
    logging.info("Daily report sent")
    
    return "Report sent successfully"

task_send_report = PythonOperator(
    task_id='send_daily_report',
    python_callable=send_daily_report,
    dag=dag,
)

# ==========================================
# DEPENDENCIAS
# ==========================================

# Flujo principal
wait_for_transformations >> [
    task_generate_daily_kpis,
    task_generate_top_products,
    task_business_alerts
] >> task_send_report

# ==========================================
# DOCUMENTACIÓN
# ==========================================

dag.doc_md = """
# Daily Report DAG

Genera reportes diarios consolidados después de que las transformaciones terminen.

## Workflow:
1. **Wait**: Espera a que olist_transformations complete exitosamente
2. **Daily KPIs**: Calcula métricas clave del día anterior
3. **Top Products**: Identifica productos más vendidos
4. **Business Alerts**: Revisa métricas críticas y genera alertas
5. **Send Report**: Envía email con resumen y alertas

## Schedule:
- Ejecuta a las 4 AM (después de transformaciones a las 3 AM)
- Usa ExternalTaskSensor para esperar dependencias

## Alertas:
- Revenue drop > 20%
- Cancellation rate > 5%
- High churn risk (100+ customers)
"""