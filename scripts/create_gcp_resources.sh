#!/bin/bash
# create_gcp_resources.sh
# Crea recursos GCP mínimos sin Terraform (alternativa manual)
# Uso: bash scripts/create_gcp_resources.sh

set -e

echo "=========================================="
echo "🛠️  CREANDO RECURSOS GCP"
echo "=========================================="
echo ""

# Obtener project ID
PROJECT_ID=$(gcloud config get-value project)

if [ -z "$PROJECT_ID" ]; then
    echo "❌ No se pudo obtener el project ID"
    echo "Ejecuta: gcloud config set project YOUR_PROJECT_ID"
    exit 1
fi

echo "📋 Project ID: $PROJECT_ID"
echo ""

# ==========================================
# CREAR BUCKETS
# ==========================================

echo "📦 Creando buckets en Cloud Storage..."

gsutil mb -p $PROJECT_ID -l southamerica-west1 gs://${PROJECT_ID}-raw-data/ 2>/dev/null || echo "  ⚠️  Bucket raw-data ya existe"
gsutil mb -p $PROJECT_ID -l southamerica-west1 gs://${PROJECT_ID}-processed-data/ 2>/dev/null || echo "  ⚠️  Bucket processed-data ya existe"
gsutil mb -p $PROJECT_ID -l southamerica-west1 gs://${PROJECT_ID}-airflow-dags/ 2>/dev/null || echo "  ⚠️  Bucket airflow-dags ya existe"
gsutil mb -p $PROJECT_ID -l southamerica-west1 gs://${PROJECT_ID}-dataflow-temp/ 2>/dev/null || echo "  ⚠️  Bucket dataflow-temp ya existe"

echo "✅ Buckets creados"
echo ""

# ==========================================
# CREAR DATASETS BIGQUERY
# ==========================================

echo "📊 Creando datasets en BigQuery..."

bq mk --dataset --location=southamerica-west1 ${PROJECT_ID}:olist_raw 2>/dev/null || echo "  ⚠️  Dataset olist_raw ya existe"
bq mk --dataset --location=southamerica-west1 ${PROJECT_ID}:olist_staging 2>/dev/null || echo "  ⚠️  Dataset olist_staging ya existe"
bq mk --dataset --location=southamerica-west1 ${PROJECT_ID}:olist_analytics 2>/dev/null || echo "  ⚠️  Dataset olist_analytics ya existe"

echo "✅ Datasets creados"
echo ""

# ==========================================
# CREAR TABLAS EN olist_raw
# ==========================================

echo "📋 Creando tablas en BigQuery..."

bq mk --table ${PROJECT_ID}:olist_raw.customers \
  customer_id:STRING,customer_unique_id:STRING,customer_zip_code_prefix:STRING,customer_city:STRING,customer_state:STRING,ingestion_timestamp:TIMESTAMP \
  2>/dev/null || echo "  ⚠️  Tabla customers ya existe"

bq mk --table ${PROJECT_ID}:olist_raw.orders \
  order_id:STRING,customer_id:STRING,order_status:STRING,order_purchase_timestamp:TIMESTAMP,order_approved_at:TIMESTAMP,order_delivered_carrier_date:TIMESTAMP,order_delivered_customer_date:TIMESTAMP,order_estimated_delivery_date:TIMESTAMP,ingestion_timestamp:TIMESTAMP \
  2>/dev/null || echo "  ⚠️  Tabla orders ya existe"

bq mk --table ${PROJECT_ID}:olist_raw.order_items \
  order_id:STRING,order_item_id:INTEGER,product_id:STRING,seller_id:STRING,shipping_limit_date:TIMESTAMP,price:FLOAT,freight_value:FLOAT,ingestion_timestamp:TIMESTAMP \
  2>/dev/null || echo "  ⚠️  Tabla order_items ya existe"

bq mk --table ${PROJECT_ID}:olist_raw.order_payments \
  order_id:STRING,payment_sequential:INTEGER,payment_type:STRING,payment_installments:INTEGER,payment_value:FLOAT,ingestion_timestamp:TIMESTAMP \
  2>/dev/null || echo "  ⚠️  Tabla order_payments ya existe"

bq mk --table ${PROJECT_ID}:olist_raw.order_reviews \
  review_id:STRING,order_id:STRING,review_score:INTEGER,review_comment_title:STRING,review_comment_message:STRING,review_creation_date:TIMESTAMP,review_answer_timestamp:TIMESTAMP,ingestion_timestamp:TIMESTAMP \
  2>/dev/null || echo "  ⚠️  Tabla order_reviews ya existe"

bq mk --table ${PROJECT_ID}:olist_raw.products \
  product_id:STRING,product_category_name:STRING,product_name_length:INTEGER,product_description_length:INTEGER,product_photos_qty:INTEGER,product_weight_g:INTEGER,product_length_cm:INTEGER,product_height_cm:INTEGER,product_width_cm:INTEGER,ingestion_timestamp:TIMESTAMP \
  2>/dev/null || echo "  ⚠️  Tabla products ya existe"

bq mk --table ${PROJECT_ID}:olist_raw.sellers \
  seller_id:STRING,seller_zip_code_prefix:STRING,seller_city:STRING,seller_state:STRING,ingestion_timestamp:TIMESTAMP \
  2>/dev/null || echo "  ⚠️  Tabla sellers ya existe"

bq mk --table ${PROJECT_ID}:olist_raw.geolocation \
  geolocation_zip_code_prefix:STRING,geolocation_lat:FLOAT,geolocation_lng:FLOAT,geolocation_city:STRING,geolocation_state:STRING,ingestion_timestamp:TIMESTAMP \
  2>/dev/null || echo "  ⚠️  Tabla geolocation ya existe"

bq mk --table ${PROJECT_ID}:olist_raw.product_category_translation \
  product_category_name:STRING,product_category_name_english:STRING,ingestion_timestamp:TIMESTAMP \
  2>/dev/null || echo "  ⚠️  Tabla product_category_translation ya existe"

echo "✅ Tablas creadas"
echo ""

# ==========================================
# CREAR PUB/SUB
# ==========================================

echo "📨 Creando Pub/Sub topic y subscription..."

gcloud pubsub topics create olist-order-events 2>/dev/null || echo "  ⚠️  Topic ya existe"
gcloud pubsub subscriptions create olist-order-events-subscription \
  --topic=olist-order-events 2>/dev/null || echo "  ⚠️  Subscription ya existe"

echo "✅ Pub/Sub creado"
echo ""

# ==========================================
# CREAR SERVICE ACCOUNTS
# ==========================================

echo "👤 Creando Service Accounts..."

# SA para Dataflow
gcloud iam service-accounts create olist-dataflow-runner \
  --display-name="Olist Dataflow Runner" 2>/dev/null || echo "  ⚠️  SA dataflow-runner ya existe"

# SA para Airflow
gcloud iam service-accounts create olist-airflow-runner \
  --display-name="Olist Airflow Runner" 2>/dev/null || echo "  ⚠️  SA airflow-runner ya existe"

# Permisos para Dataflow SA
for role in roles/dataflow.worker roles/storage.objectAdmin roles/bigquery.dataEditor roles/bigquery.jobUser roles/pubsub.subscriber roles/pubsub.viewer; do
  gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:olist-dataflow-runner@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="$role" --quiet 2>/dev/null
done

# Permisos para Airflow SA
for role in roles/composer.worker roles/storage.objectAdmin roles/bigquery.dataEditor roles/bigquery.jobUser roles/dataflow.developer; do
  gcloud projects add-iam-policy-binding $PROJECT_ID \
    --member="serviceAccount:olist-airflow-runner@${PROJECT_ID}.iam.gserviceaccount.com" \
    --role="$role" --quiet 2>/dev/null
done

echo "✅ Service Accounts creadas"
echo ""

# ==========================================
# PERMISOS EN DATASETS
# ==========================================

echo "🔐 Configurando permisos en datasets..."

MY_EMAIL=$(gcloud config get-value account)

# Dar permisos al usuario actual sobre olist_analytics
bq update --dataset \
  --add_access_entry="role=WRITER,userByEmail=$MY_EMAIL" \
  ${PROJECT_ID}:olist_analytics 2>/dev/null || echo "  ⚠️  Permisos ya configurados"

# Dar permisos a la SA de dataflow
bq update --dataset \
  --add_access_entry="role=OWNER,userByEmail=olist-dataflow-runner@${PROJECT_ID}.iam.gserviceaccount.com" \
  ${PROJECT_ID}:olist_analytics 2>/dev/null || echo "  ⚠️  Permisos ya configurados"

echo "✅ Permisos configurados"
echo ""

# ==========================================
# VERIFICAR
# ==========================================

echo "=========================================="
echo "✅ RECURSOS CREADOS"
echo "=========================================="
echo ""

echo "📦 Buckets:"
gsutil ls | grep $PROJECT_ID

echo ""
echo "📊 Datasets:"
bq ls | grep olist

echo ""
echo "📋 Tablas en olist_raw:"
bq ls olist_raw

echo ""
echo "📨 Pub/Sub:"
gcloud pubsub topics list --format="value(name)" | grep olist

echo ""
echo "=========================================="
echo "🎉 ¡LISTO!"
echo "=========================================="
echo ""
echo "Próximos pasos:"
echo "  1. Descargar dataset: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce"
echo "  2. Cargar datos:      python src/data_ingestion/load_olist_to_gcp.py"
echo ""
