# main.tf
# Infraestructura completa del proyecto Olist

# ==========================================
# PROVIDER CONFIGURATION
# ==========================================
terraform {
  required_version = ">= 1.0"

  required_providers {
    google = {
      source  = "hashicorp/google"
      version = "~> 5.0"
    }
  }
}

provider "google" {
  project = var.project_id
  region  = var.region
}

# ==========================================
# GOOGLE CLOUD STORAGE BUCKETS
# ==========================================

# Bucket para datos RAW (Olist original)
resource "google_storage_bucket" "raw_data" {
  name          = "${var.project_id}-raw-data"
  location      = var.bucket_location
  force_destroy = true # ⚠️ Permite borrar bucket con contenido (solo dev)

  uniform_bucket_level_access = true

  # Lifecycle: Mover a Nearline después de 30 días (-50% costo storage)
  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  # Lifecycle: Mover a Coldline después de 60 días (-75% costo storage)
  lifecycle_rule {
    condition {
      age = 60
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }

  # Lifecycle: Borrar archivos después de 180 días
  lifecycle_rule {
    condition {
      age = 180
    }
    action {
      type = "Delete"
    }
  }

  labels = var.labels
}

# Bucket para datos PROCESADOS
resource "google_storage_bucket" "processed_data" {
  name          = "${var.project_id}-processed-data"
  location      = var.bucket_location
  force_destroy = true

  uniform_bucket_level_access = true

  versioning {
    enabled = true # Mantener versiones de archivos
  }

  # Lifecycle: Mover a Nearline después de 30 días (-50% costo storage)
  lifecycle_rule {
    condition {
      age = 30
    }
    action {
      type          = "SetStorageClass"
      storage_class = "NEARLINE"
    }
  }

  # Lifecycle: Mover a Coldline después de 90 días (-75% costo storage)
  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type          = "SetStorageClass"
      storage_class = "COLDLINE"
    }
  }

  # Lifecycle: Mover a Archive después de 180 días (-85% costo storage)
  lifecycle_rule {
    condition {
      age = 180
    }
    action {
      type          = "SetStorageClass"
      storage_class = "ARCHIVE"
    }
  }

  # Lifecycle: Borrar versiones antiguas no-current después de 30 días
  lifecycle_rule {
    condition {
      num_newer_versions = 3
      with_state         = "ARCHIVED" # Solo versiones no-current
    }
    action {
      type = "Delete"
    }
  }

  labels = var.labels
}

# Bucket para Airflow DAGs
resource "google_storage_bucket" "airflow_dags" {
  name          = "${var.project_id}-airflow-dags"
  location      = var.bucket_location
  force_destroy = true

  uniform_bucket_level_access = true

  # Lifecycle: Borrar DAGs/logs obsoletos después de 90 días
  lifecycle_rule {
    condition {
      age = 90
    }
    action {
      type = "Delete"
    }
  }

  labels = merge(var.labels, {
    purpose = "airflow-dags"
  })
}

# Bucket para Dataflow staging/temp
resource "google_storage_bucket" "dataflow_temp" {
  name          = "${var.project_id}-dataflow-temp"
  location      = var.bucket_location
  force_destroy = true

  uniform_bucket_level_access = true

  # Auto-delete archivos temporales después de 7 días
  lifecycle_rule {
    condition {
      age = 7
    }
    action {
      type = "Delete"
    }
  }

  labels = merge(var.labels, {
    purpose = "dataflow-temp"
  })
}

# ==========================================
# BIGQUERY DATASETS
# ==========================================

# Dataset: RAW (datos sin procesar)
resource "google_bigquery_dataset" "raw" {
  dataset_id    = "olist_raw"
  friendly_name = "Olist Raw Data"
  description   = "Raw data from Olist Brazilian E-Commerce"
  location      = var.bigquery_location

  # No expirar tablas en raw (son datos originales)
  # default_table_expiration_ms = null

  labels = merge(var.labels, {
    data_type = "raw"
  })
}

# Dataset: STAGING (datos en transformación)
resource "google_bigquery_dataset" "staging" {
  dataset_id    = "olist_staging"
  friendly_name = "Olist Staging Data"
  description   = "Staging area for data transformations"
  location      = var.bigquery_location

  # Tablas staging expiran en 30 días
  default_table_expiration_ms = 2592000000 # 30 días

  labels = merge(var.labels, {
    data_type = "staging"
  })
}

# Dataset: ANALYTICS (datos finales para análisis)
resource "google_bigquery_dataset" "analytics" {
  dataset_id    = "olist_analytics"
  friendly_name = "Olist Analytics Data"
  description   = "Final analytics tables for dashboards and ML"
  location      = var.bigquery_location
  # No expirar (son datos finales)

  # NOTA: Cuando se definen bloques access {} explícitos en Terraform,
  # estos REEMPLAZAN todos los permisos heredados del proyecto.
  # Sin el bloque de projectOwners, ni siquiera el Owner del proyecto
  # puede crear tablas aquí, causando:
  #   "Permission bigquery.tables.create denied on dataset olist_analytics"
  # Ver: https://registry.terraform.io/providers/hashicorp/google/latest/docs/resources/bigquery_dataset#access

  access {
    role          = "OWNER"
    user_by_email = google_service_account.dataflow_runner.email
  }

  # Acceso para la SA usada en ejecución local (olist-sa-key.json)
  access {
    role          = "WRITER"
    user_by_email = "olist-pipeline-sa@${var.project_id}.iam.gserviceaccount.com"
  }

  # OBLIGATORIO: sin esto, los owners del proyecto quedan bloqueados
  access {
    role          = "OWNER"
    special_group = "projectOwners"
  }

  labels = merge(var.labels, {
    data_type = "analytics"
  })
}

# ==========================================
# BIGQUERY TABLES - RAW
# ==========================================

# Schema para tabla customers (raw)
resource "google_bigquery_table" "raw_customers" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  table_id   = "customers"

  deletion_protection = false # Solo para dev

  schema = jsonencode([
    {
      name        = "customer_id"
      type        = "STRING"
      mode        = "REQUIRED"
      description = "Unique customer ID"
    },
    {
      name        = "customer_unique_id"
      type        = "STRING"
      mode        = "NULLABLE"
      description = "Unique ID across multiple orders"
    },
    {
      name = "customer_zip_code_prefix"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "customer_city"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "customer_state"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name        = "ingestion_timestamp"
      type        = "TIMESTAMP"
      mode        = "NULLABLE"
      description = "When this row was ingested"
    }
  ])

  labels = var.labels
}

# Schema para tabla orders (raw)
resource "google_bigquery_table" "raw_orders" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  table_id   = "orders"

  deletion_protection = false

  # Particionar por fecha de compra (mejor performance)
  time_partitioning {
    type  = "DAY"
    field = "order_purchase_timestamp"
  }

  schema = jsonencode([
    {
      name = "order_id"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "customer_id"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "order_status"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "order_purchase_timestamp"
      type = "TIMESTAMP"
      mode = "REQUIRED"
    },
    {
      name = "order_approved_at"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    },
    {
      name = "order_delivered_carrier_date"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    },
    {
      name = "order_delivered_customer_date"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    },
    {
      name = "order_estimated_delivery_date"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    },
    {
      name = "ingestion_timestamp"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    }
  ])

  labels = var.labels
}

# Schema para order_items
resource "google_bigquery_table" "raw_order_items" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  table_id   = "order_items"

  deletion_protection = false

  schema = jsonencode([
    {
      name = "order_id"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "order_item_id"
      type = "INTEGER"
      mode = "REQUIRED"
    },
    {
      name = "product_id"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "seller_id"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "shipping_limit_date"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    },
    {
      name = "price"
      type = "FLOAT64"
      mode = "REQUIRED"
    },
    {
      name = "freight_value"
      type = "FLOAT64"
      mode = "NULLABLE"
    },
    {
      name = "ingestion_timestamp"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    }
  ])

  labels = var.labels
}

# Schema para products
resource "google_bigquery_table" "raw_products" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  table_id   = "products"

  deletion_protection = false

  schema = jsonencode([
    {
      name = "product_id"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "product_category_name"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "product_name_length"
      type = "INTEGER"
      mode = "NULLABLE"
    },
    {
      name = "product_description_length"
      type = "INTEGER"
      mode = "NULLABLE"
    },
    {
      name = "product_photos_qty"
      type = "INTEGER"
      mode = "NULLABLE"
    },
    {
      name = "product_weight_g"
      type = "INTEGER"
      mode = "NULLABLE"
    },
    {
      name = "product_length_cm"
      type = "INTEGER"
      mode = "NULLABLE"
    },
    {
      name = "product_height_cm"
      type = "INTEGER"
      mode = "NULLABLE"
    },
    {
      name = "product_width_cm"
      type = "INTEGER"
      mode = "NULLABLE"
    },
    {
      name = "ingestion_timestamp"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    }
  ])

  labels = var.labels
}

# Schema para sellers
resource "google_bigquery_table" "raw_sellers" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  table_id   = "sellers"

  deletion_protection = false

  schema = jsonencode([
    {
      name = "seller_id"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "seller_zip_code_prefix"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "seller_city"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "seller_state"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "ingestion_timestamp"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    }
  ])

  labels = var.labels
}

# Schema para order_payments
resource "google_bigquery_table" "raw_order_payments" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  table_id   = "order_payments"

  deletion_protection = false

  schema = jsonencode([
    {
      name = "order_id"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "payment_sequential"
      type = "INTEGER"
      mode = "REQUIRED"
    },
    {
      name = "payment_type"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "payment_installments"
      type = "INTEGER"
      mode = "NULLABLE"
    },
    {
      name = "payment_value"
      type = "FLOAT64"
      mode = "REQUIRED"
    },
    {
      name = "ingestion_timestamp"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    }
  ])

  labels = var.labels
}

# Schema para order_reviews
resource "google_bigquery_table" "raw_order_reviews" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  table_id   = "order_reviews"

  deletion_protection = false

  schema = jsonencode([
    {
      name = "review_id"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "order_id"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "review_score"
      type = "INTEGER"
      mode = "NULLABLE"
    },
    {
      name = "review_comment_title"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "review_comment_message"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "review_creation_date"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    },
    {
      name = "review_answer_timestamp"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    },
    {
      name = "ingestion_timestamp"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    }
  ])

  labels = var.labels
}

# Schema para geolocation
resource "google_bigquery_table" "raw_geolocation" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  table_id   = "geolocation"

  deletion_protection = false

  schema = jsonencode([
    {
      name = "geolocation_zip_code_prefix"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "geolocation_lat"
      type = "FLOAT64"
      mode = "NULLABLE"
    },
    {
      name = "geolocation_lng"
      type = "FLOAT64"
      mode = "NULLABLE"
    },
    {
      name = "geolocation_city"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "geolocation_state"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "ingestion_timestamp"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    }
  ])

  labels = var.labels
}

# Schema para product_category_translation
resource "google_bigquery_table" "raw_product_categories" {
  dataset_id = google_bigquery_dataset.raw.dataset_id
  table_id   = "product_category_translation"

  deletion_protection = false

  schema = jsonencode([
    {
      name = "product_category_name"
      type = "STRING"
      mode = "REQUIRED"
    },
    {
      name = "product_category_name_english"
      type = "STRING"
      mode = "NULLABLE"
    },
    {
      name = "ingestion_timestamp"
      type = "TIMESTAMP"
      mode = "NULLABLE"
    }
  ])

  labels = var.labels
}

# ==========================================
# PUB/SUB - Para streaming
# ==========================================

# Topic para eventos de órdenes
resource "google_pubsub_topic" "order_events" {
  name = "olist-order-events"

  labels = merge(var.labels, {
    purpose = "streaming-events"
  })

  message_retention_duration = "86400s" # 24 horas
}

# Subscription al topic
resource "google_pubsub_subscription" "order_events_sub" {
  name  = "olist-order-events-subscription"
  topic = google_pubsub_topic.order_events.name

  ack_deadline_seconds = 20

  message_retention_duration = "86400s" # 24 horas

  retry_policy {
    minimum_backoff = "10s"
    maximum_backoff = "600s"
  }

  labels = var.labels
}

# ==========================================
# SERVICE ACCOUNTS
# ==========================================

# Service Account para Dataflow
resource "google_service_account" "dataflow_runner" {
  account_id   = "olist-dataflow-runner"
  display_name = "Olist Dataflow Pipeline Runner"
  description  = "Service account for running Dataflow pipelines"
}

# Permisos para Dataflow
resource "google_project_iam_member" "dataflow_worker" {
  project = var.project_id
  role    = "roles/dataflow.worker"
  member  = "serviceAccount:${google_service_account.dataflow_runner.email}"
}

resource "google_project_iam_member" "dataflow_storage_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.dataflow_runner.email}"
}

resource "google_project_iam_member" "dataflow_bigquery_admin" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor"
  member  = "serviceAccount:${google_service_account.dataflow_runner.email}"
}

resource "google_project_iam_member" "dataflow_pubsub_subscriber" {
  project = var.project_id
  role    = "roles/pubsub.subscriber"
  member  = "serviceAccount:${google_service_account.dataflow_runner.email}"
}

# Service Account para Airflow
resource "google_service_account" "airflow_runner" {
  account_id   = "olist-airflow-runner"
  display_name = "Olist Airflow DAG Runner"
  description  = "Service account for Airflow orchestration"
}

# Permisos para Airflow
resource "google_project_iam_member" "airflow_composer_worker" {
  project = var.project_id
  role    = "roles/composer.worker"
  member  = "serviceAccount:${google_service_account.airflow_runner.email}"
}

resource "google_project_iam_member" "airflow_storage_admin" {
  project = var.project_id
  role    = "roles/storage.objectAdmin"
  member  = "serviceAccount:${google_service_account.airflow_runner.email}"
}

resource "google_project_iam_member" "airflow_bigquery_editor" {
  project = var.project_id
  role    = "roles/bigquery.dataEditor" # Solo leer/escribir, NO crear/borrar datasets
  member  = "serviceAccount:${google_service_account.airflow_runner.email}"
}

resource "google_project_iam_member" "airflow_dataflow_developer" {
  project = var.project_id
  role    = "roles/dataflow.developer"
  member  = "serviceAccount:${google_service_account.airflow_runner.email}"
}
