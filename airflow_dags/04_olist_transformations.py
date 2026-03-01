"""
04_olist_transformations.py

DAG para transformaciones de datos de Olist
Ejecuta queries SQL en BigQuery para crear tablas analytics
"""

from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import os

# ==========================================
# CONFIGURACIÓN
# ==========================================

PROJECT_ID = os.environ.get('GCP_PROJECT_ID', 'ecommerce-olist-ben-260301')
BQ_RAW_DATASET = 'olist_raw'
BQ_ANALYTICS_DATASET = 'olist_analytics'

default_args = {
    'owner': 'data-engineering',
    'start_date': datetime(2024, 2, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'olist_transformations',
    default_args=default_args,
    description='Transformaciones diarias de datos Olist',
    schedule_interval='0 3 * * *',  # 3 AM (1 hora después de ingesta)
    catchup=False,
    tags=['olist', 'transformation', 'production'],
)

# ==========================================
# TRANSFORMATION 1: Order Enrichment
# ==========================================

sql_enrich_orders = f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.{BQ_ANALYTICS_DATASET}.orders_enriched` AS
SELECT 
  o.order_id,
  o.customer_id,
  c.customer_unique_id,
  c.customer_city,
  c.customer_state,
  o.order_status,
  o.order_purchase_timestamp,
  o.order_delivered_customer_date,
  o.order_estimated_delivery_date,
  
  -- Delivery metrics
  TIMESTAMP_DIFF(
    o.order_delivered_customer_date,
    o.order_purchase_timestamp,
    DAY
  ) as delivery_time_days,
  
  TIMESTAMP_DIFF(
    o.order_delivered_customer_date,
    o.order_estimated_delivery_date,
    DAY
  ) as delivery_delay_days,
  
  -- Payment info (aggregated)
  ARRAY_AGG(STRUCT(
    p.payment_type,
    p.payment_value,
    p.payment_installments
  )) as payments,
  
  SUM(p.payment_value) as total_payment,
  MAX(p.payment_installments) as max_installments,
  
  -- Review info
  MAX(r.review_score) as review_score,
  MAX(r.review_comment_message) as review_comment,
  
  -- Processing metadata
  CURRENT_TIMESTAMP() as processed_at,
  '{{{{ ds }}}}' as processing_date

FROM `{PROJECT_ID}.{BQ_RAW_DATASET}.orders` o
LEFT JOIN `{PROJECT_ID}.{BQ_RAW_DATASET}.customers` c 
  ON o.customer_id = c.customer_id
LEFT JOIN `{PROJECT_ID}.{BQ_RAW_DATASET}.order_payments` p 
  ON o.order_id = p.order_id
LEFT JOIN `{PROJECT_ID}.{BQ_RAW_DATASET}.order_reviews` r 
  ON o.order_id = r.order_id

WHERE DATE(o.order_purchase_timestamp) <= '{{{{ ds }}}}'

GROUP BY 
  o.order_id, o.customer_id, c.customer_unique_id,
  c.customer_city, c.customer_state, o.order_status,
  o.order_purchase_timestamp, o.order_delivered_customer_date,
  o.order_estimated_delivery_date
"""

task_enrich_orders = BigQueryInsertJobOperator(
    task_id='enrich_orders',
    configuration={
        'query': {
            'query': sql_enrich_orders,
            'useLegacySql': False,
        }
    },
    location='southamerica-west1',
    dag=dag,
)

# ==========================================
# TRANSFORMATION 2: Product Analytics
# ==========================================

sql_product_analytics = f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.{BQ_ANALYTICS_DATASET}.product_analytics` AS
SELECT 
  p.product_id,
  p.product_category_name,
  t.product_category_name_english as product_category,
  
  -- Sales metrics
  COUNT(DISTINCT oi.order_id) as total_orders,
  COUNT(*) as total_items_sold,
  SUM(oi.price) as total_revenue,
  AVG(oi.price) as avg_price,
  SUM(oi.freight_value) as total_freight,
  
  -- Product characteristics
  AVG(p.product_weight_g) as avg_weight_g,
  AVG(p.product_length_cm * p.product_width_cm * p.product_height_cm) as avg_volume_cm3,
  
  -- Seller metrics
  COUNT(DISTINCT oi.seller_id) as unique_sellers,
  
  -- Review metrics
  AVG(r.review_score) as avg_review_score,
  COUNT(DISTINCT r.review_id) as total_reviews,
  
  -- Date metrics
  MIN(o.order_purchase_timestamp) as first_sale_date,
  MAX(o.order_purchase_timestamp) as last_sale_date,
  
  -- Processing metadata
  CURRENT_TIMESTAMP() as processed_at

FROM `{PROJECT_ID}.{BQ_RAW_DATASET}.products` p
LEFT JOIN `{PROJECT_ID}.{BQ_RAW_DATASET}.product_category_translation` t
  ON p.product_category_name = t.product_category_name
LEFT JOIN `{PROJECT_ID}.{BQ_RAW_DATASET}.order_items` oi
  ON p.product_id = oi.product_id
LEFT JOIN `{PROJECT_ID}.{BQ_RAW_DATASET}.orders` o
  ON oi.order_id = o.order_id
LEFT JOIN `{PROJECT_ID}.{BQ_RAW_DATASET}.order_reviews` r
  ON o.order_id = r.order_id

WHERE o.order_status = 'delivered'

GROUP BY 
  p.product_id, p.product_category_name, 
  t.product_category_name_english
"""

task_product_analytics = BigQueryInsertJobOperator(
    task_id='compute_product_analytics',
    configuration={
        'query': {
            'query': sql_product_analytics,
            'useLegacySql': False,
        }
    },
    location='southamerica-west1',
    dag=dag,
)

# ==========================================
# TRANSFORMATION 3: Daily Metrics
# ==========================================

sql_daily_metrics = f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.{BQ_ANALYTICS_DATASET}.daily_metrics` AS
SELECT 
  DATE(o.order_purchase_timestamp) as order_date,
  
  -- Order metrics
  COUNT(DISTINCT o.order_id) as num_orders,
  COUNT(DISTINCT o.customer_id) as num_customers,
  
  -- Revenue metrics
  SUM(p.payment_value) as total_revenue,
  AVG(p.payment_value) as avg_order_value,
  
  -- Status distribution
  COUNTIF(o.order_status = 'delivered') as delivered_orders,
  COUNTIF(o.order_status = 'canceled') as canceled_orders,
  COUNTIF(o.order_status = 'shipped') as shipped_orders,
  
  -- Payment methods
  COUNTIF(p.payment_type = 'credit_card') as credit_card_orders,
  COUNTIF(p.payment_type = 'boleto') as boleto_orders,
  COUNTIF(p.payment_type = 'voucher') as voucher_orders,
  
  -- Geographic distribution
  COUNT(DISTINCT c.customer_state) as unique_states,
  
  -- Product metrics
  COUNT(DISTINCT oi.product_id) as unique_products,
  SUM(oi.price) as items_revenue,
  SUM(oi.freight_value) as freight_revenue

FROM `{PROJECT_ID}.{BQ_RAW_DATASET}.orders` o
LEFT JOIN `{PROJECT_ID}.{BQ_RAW_DATASET}.customers` c
  ON o.customer_id = c.customer_id
LEFT JOIN `{PROJECT_ID}.{BQ_RAW_DATASET}.order_payments` p 
  ON o.order_id = p.order_id
LEFT JOIN `{PROJECT_ID}.{BQ_RAW_DATASET}.order_items` oi
  ON o.order_id = oi.order_id

WHERE DATE(o.order_purchase_timestamp) <= '{{{{ ds }}}}'

GROUP BY order_date
ORDER BY order_date
"""

task_daily_metrics = BigQueryInsertJobOperator(
    task_id='compute_daily_metrics',
    configuration={
        'query': {
            'query': sql_daily_metrics,
            'useLegacySql': False,
        }
    },
    location='southamerica-west1',
    dag=dag,
)

# ==========================================
# TRANSFORMATION 4: Customer Segments
# ==========================================

sql_customer_segments = f"""
CREATE OR REPLACE TABLE `{PROJECT_ID}.{BQ_ANALYTICS_DATASET}.customer_segments` AS
WITH customer_metrics AS (
  SELECT 
    c.customer_unique_id,
    c.customer_state,
    c.customer_city,
    
    -- Order metrics
    COUNT(DISTINCT o.order_id) as total_orders,
    SUM(p.payment_value) as total_spent,
    AVG(p.payment_value) as avg_order_value,
    
    -- Temporal metrics
    MIN(o.order_purchase_timestamp) as first_order_date,
    MAX(o.order_purchase_timestamp) as last_order_date,
    DATE_DIFF(
      MAX(DATE(o.order_purchase_timestamp)), 
      MIN(DATE(o.order_purchase_timestamp)), 
      DAY
    ) as customer_lifetime_days,
    
    -- Product diversity
    COUNT(DISTINCT oi.product_id) as unique_products_bought,
    COUNT(DISTINCT t.product_category_name_english) as unique_categories,
    
    -- Review behavior
    AVG(r.review_score) as avg_review_score,
    COUNT(r.review_id) as total_reviews

  FROM `{PROJECT_ID}.{BQ_RAW_DATASET}.customers` c
  LEFT JOIN `{PROJECT_ID}.{BQ_RAW_DATASET}.orders` o
    ON c.customer_id = o.customer_id
  LEFT JOIN `{PROJECT_ID}.{BQ_RAW_DATASET}.order_payments` p
    ON o.order_id = p.order_id
  LEFT JOIN `{PROJECT_ID}.{BQ_RAW_DATASET}.order_items` oi
    ON o.order_id = oi.order_id
  LEFT JOIN `{PROJECT_ID}.{BQ_RAW_DATASET}.products` pr
    ON oi.product_id = pr.product_id
  LEFT JOIN `{PROJECT_ID}.{BQ_RAW_DATASET}.product_category_translation` t
    ON pr.product_category_name = t.product_category_name
  LEFT JOIN `{PROJECT_ID}.{BQ_RAW_DATASET}.order_reviews` r
    ON o.order_id = r.order_id
  
  WHERE o.order_status = 'delivered'
  
  GROUP BY 
    c.customer_unique_id, c.customer_state, c.customer_city
)

SELECT 
  *,
  
  -- RFM Segmentation
  CASE 
    WHEN DATE_DIFF(CURRENT_DATE(), DATE(last_order_date), DAY) <= 90 
         AND total_orders >= 3 
         AND total_spent >= 500
      THEN 'VIP'
    WHEN DATE_DIFF(CURRENT_DATE(), DATE(last_order_date), DAY) <= 180
         AND total_orders >= 2
      THEN 'Active'
    WHEN DATE_DIFF(CURRENT_DATE(), DATE(last_order_date), DAY) <= 365
      THEN 'At Risk'
    ELSE 'Churned'
  END as customer_segment,
  
  -- Value tier
  CASE
    WHEN total_spent >= 1000 THEN 'High Value'
    WHEN total_spent >= 500 THEN 'Medium Value'
    ELSE 'Low Value'
  END as value_tier,
  
  CURRENT_TIMESTAMP() as processed_at

FROM customer_metrics
"""

task_customer_segments = BigQueryInsertJobOperator(
    task_id='compute_customer_segments',
    configuration={
        'query': {
            'query': sql_customer_segments,
            'useLegacySql': False,
        }
    },
    location='southamerica-west1',
    dag=dag,
)

# ==========================================
# VERIFICATION: Data Quality Checks
# ==========================================

def verify_transformations(**context):
    """
    Verifica que las transformaciones se ejecutaron correctamente
    """
    from google.cloud import bigquery
    
    client = bigquery.Client(project=PROJECT_ID)
    
    checks = {
        'orders_enriched': f"SELECT COUNT(*) as count FROM `{PROJECT_ID}.{BQ_ANALYTICS_DATASET}.orders_enriched`",
        'product_analytics': f"SELECT COUNT(*) as count FROM `{PROJECT_ID}.{BQ_ANALYTICS_DATASET}.product_analytics`",
        'daily_metrics': f"SELECT COUNT(*) as count FROM `{PROJECT_ID}.{BQ_ANALYTICS_DATASET}.daily_metrics`",
        'customer_segments': f"SELECT COUNT(*) as count FROM `{PROJECT_ID}.{BQ_ANALYTICS_DATASET}.customer_segments`",
    }
    
    results = {}
    
    for table_name, query in checks.items():
        result = client.query(query).result()
        count = list(result)[0]['count']
        results[table_name] = count
        
        logging.info(f"✅ {table_name}: {count:,} rows")
        
        # Verificar que no esté vacío
        assert count > 0, f"Table {table_name} is empty!"
    
    logging.info("✅ All transformation tables verified")
    return results

task_verify = PythonOperator(
    task_id='verify_transformations',
    python_callable=verify_transformations,
    dag=dag,
)

# ==========================================
# DEPENDENCIAS
# ==========================================

# Ejecutar transformaciones en paralelo
[task_enrich_orders, task_product_analytics, task_daily_metrics, task_customer_segments] >> task_verify

# ==========================================
# DOCUMENTACIÓN
# ==========================================

dag.doc_md = """
# Transformations DAG

Pipeline de transformaciones SQL para crear tablas analytics.

## Transformaciones:
1. **orders_enriched**: Órdenes con info completa de customer, payment, review
2. **product_analytics**: Métricas por producto (ventas, reviews, sellers)
3. **daily_metrics**: KPIs diarios agregados
4. **customer_segments**: Segmentación RFM de clientes

## Schedule:
- Ejecuta a las 3 AM (1 hora después de ingesta)
- Procesa datos hasta la fecha de ejecución

## Dependencies:
- Requiere que `olist_daily_ingestion` haya completado
"""
