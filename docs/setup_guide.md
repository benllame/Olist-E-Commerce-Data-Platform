# 🛠️ Guía Completa de Setup — Olist E-Commerce Data Platform

Tutorial paso a paso para clonar este repositorio y poner todo en funcionamiento desde cero.

---

## Índice

1. [Prerequisitos](#1-prerequisitos)
2. [Instalar Google Cloud SDK](#2-instalar-google-cloud-sdk)
3. [Configurar Proyecto GCP](#3-configurar-proyecto-gcp)
4. [Habilitar APIs de GCP](#4-habilitar-apis-de-gcp)
5. [Configurar Credenciales y Service Accounts](#5-configurar-credenciales-y-service-accounts)
6. [Instalar Terraform](#6-instalar-terraform)
7. [Clonar Repositorio y Configurar Ambientes](#7-clonar-repositorio-y-configurar-ambientes)
8. [Desplegar Infraestructura](#8-desplegar-infraestructura)
9. [Descargar y Cargar Datos](#9-descargar-y-cargar-datos)
10. [Configurar Apache Airflow](#10-configurar-apache-airflow)
11. [Ejecutar Pipelines](#11-ejecutar-pipelines)
12. [Verificación Final](#12-verificación-final)

---

## 1. Prerequisitos

Antes de empezar, necesitas:

| Requisito | Versión Mínima | Verificar |
|-----------|----------------|-----------|
| **Linux** (Ubuntu/Debian) | 20.04+ | `lsb_release -a` |
| **Miniconda/Anaconda** | — | `conda --version` |
| **Git** | 2.0+ | `git --version` |
| **Cuenta Google Cloud** | Free Tier | [console.cloud.google.com](https://console.cloud.google.com) |

> [!IMPORTANT]
> Se necesitan **2 ambientes conda** separados:
> - `airflow` — Para Airflow, DAGs, operaciones BigQuery y tests (324 paquetes)
> - `dev` — Para pipelines Beam, streaming, procesamiento de datos y pre-commit (509 paquetes)

---

## 2. Instalar Google Cloud SDK

### 2.1. Agregar repositorio e instalar

```bash
# 1. Agregar repositorio de Google Cloud
echo "deb [signed-by=/usr/share/keyrings/cloud.google.gpg] https://packages.cloud.google.com/apt cloud-sdk main" \
  | sudo tee -a /etc/apt/sources.list.d/google-cloud-sdk.list

# 2. Importar llave pública
curl https://packages.cloud.google.com/apt/doc/apt-key.gpg \
  | sudo apt-key --keyring /usr/share/keyrings/cloud.google.gpg add -

# 3. Actualizar e instalar
sudo apt-get update && sudo apt-get install google-cloud-sdk

# 4. Instalar componentes adicionales (opcionales pero recomendados)
sudo apt-get install google-cloud-sdk-pubsub-emulator
```

### 2.2. Verificar instalación

```bash
gcloud --version

# Deberías ver algo como:
# Google Cloud SDK 460.0.0
# bq 2.0.101
# core 2024.01.12
# gcloud-crc32c 1.0.0
# gsutil 5.27
```

---

## 3. Configurar Proyecto GCP

### 3.1. Iniciar autenticación

```bash
# Esto abre tu navegador para autenticar
gcloud init

# Sigue estos pasos:
# ✓ Selecciona tu cuenta Gmail
# ✓ Click "Allow" para dar permisos
# ✓ Vuelve a la terminal
```

### 3.2. Crear proyecto nuevo

Cuando `gcloud init` pregunte "Pick cloud project to use:", selecciona **"Create a new project"**.

```bash
# IMPORTANTE: El Project ID debe ser ÚNICO globalmente en GCP
# Formato recomendado: ecommerce-olist-TUNOMBRE-YYMMDD
# Ejemplo: ecommerce-olist-benjamin-150226

# Si prefieres crear el proyecto manualmente:
gcloud projects create ecommerce-olist-TUNOMBRE \
  --name="Olist E-Commerce Pipeline"

# Setear como proyecto activo
gcloud config set project ecommerce-olist-TUNOMBRE
```

### 3.3. Verificar proyecto actual

```bash
# Ver proyecto actual
echo $GCP_PROJECT_ID

# Si no aparece nada, configurarlo:
export GCP_PROJECT_ID="ecommerce-olist-TUNOMBRE"
gcloud config set project $GCP_PROJECT_ID

# Agregar a ~/.bashrc para que persista entre sesiones
echo "export GCP_PROJECT_ID=\"$GCP_PROJECT_ID\"" >> ~/.bashrc
```

### 3.4. Configurar región

```bash
# Usar southamerica-west1 (Santiago, Chile) como en el proyecto original
# Puedes cambiar la región si prefieres (ej: us-central1)
gcloud config set compute/region southamerica-west1
gcloud config set compute/zone southamerica-west1-a
```

### 3.5. Verificar configuración completa

```bash
gcloud config list

# Deberías ver:
# [compute]
# region = southamerica-west1
# zone = southamerica-west1-a
# [core]
# account = TU_EMAIL@gmail.com
# project = ecommerce-olist-TUNOMBRE
```

### 3.6. Vincular cuenta de facturación

```bash
# Listar cuentas de facturación disponibles
gcloud billing accounts list

# Vincular al proyecto (necesario para crear recursos)
gcloud billing projects link $GCP_PROJECT_ID \
  --billing-account=TU_BILLING_ACCOUNT_ID
```

> [!CAUTION]
> Sin cuenta de facturación vinculada, no podrás habilitar APIs ni crear recursos.
> El Free Tier de GCP incluye **$300 USD de crédito por 90 días** — suficiente para este proyecto.

---

## 4. Habilitar APIs de GCP

```bash
# Habilitar todas las APIs necesarias (toma ~5 min)
gcloud services enable compute.googleapis.com
gcloud services enable storage.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable dataflow.googleapis.com
gcloud services enable pubsub.googleapis.com
gcloud services enable cloudresourcemanager.googleapis.com
gcloud services enable iam.googleapis.com

# Verificar APIs habilitadas
gcloud services list --enabled

# Deberías ver una lista con:
# ✓ compute.googleapis.com
# ✓ storage.googleapis.com
# ✓ bigquery.googleapis.com
# ✓ dataflow.googleapis.com
# ✓ pubsub.googleapis.com
# ✓ cloudresourcemanager.googleapis.com
# ✓ iam.googleapis.com
```

---

## 5. Configurar Credenciales y Service Accounts

### 5.1. Crear credenciales de aplicación

```bash
# Esto permite que tu código local se autentique con GCP
gcloud auth application-default login

# Se abre el navegador → selecciona tu cuenta → "Allow"
# Las credenciales se guardan en:
#   ~/.config/gcloud/application_default_credentials.json
```

> [!IMPORTANT]
> **Los pasos 5.2 a 5.4** (crear Service Account y generar key JSON) deben ejecutarse
> **después del paso 8** (Terraform). Terraform necesita permisos de Owner para
> crear buckets, datasets, service accounts e IAM bindings. La SA creada aquí
> solo tiene permisos limitados (`storage.objectAdmin`, `bigquery.dataEditor`, etc.)
> y **no puede crear infraestructura**.
>
> 👉 **Salta directamente al [paso 6](#6-instalar-terraform)** y vuelve aquí después de completar el paso 8.

### 5.2. Crear Service Account para el pipeline

```bash
# Crear SA principal para ejecución local de pipelines
gcloud iam service-accounts create olist-pipeline-sa \
  --display-name="Olist Data Pipeline Service Account"
```

### 5.3. Asignar permisos al Service Account

```bash
# Permisos para BigQuery (leer/escribir datos + ejecutar queries)
gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
  --member="serviceAccount:olist-pipeline-sa@${GCP_PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
  --member="serviceAccount:olist-pipeline-sa@${GCP_PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

# Permisos para Cloud Storage (subir/descargar archivos)
gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
  --member="serviceAccount:olist-pipeline-sa@${GCP_PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

# Permisos para Pub/Sub (publicar/suscribir eventos)
gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
  --member="serviceAccount:olist-pipeline-sa@${GCP_PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/pubsub.editor"

# Permisos para Dataflow (ejecutar pipelines)
gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
  --member="serviceAccount:olist-pipeline-sa@${GCP_PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/dataflow.worker"
```

### 5.4. Generar key JSON

```bash
# Crear llave JSON del Service Account
gcloud iam service-accounts keys create ~/olist-sa-key.json \
  --iam-account=olist-pipeline-sa@${GCP_PROJECT_ID}.iam.gserviceaccount.com

# Setear variable de entorno
export GOOGLE_APPLICATION_CREDENTIALS="$HOME/olist-sa-key.json"

# Agregar a ~/.bashrc para que persista
echo 'export GOOGLE_APPLICATION_CREDENTIALS="$HOME/olist-sa-key.json"' >> ~/.bashrc

# Verificar que funciona
cat $GOOGLE_APPLICATION_CREDENTIALS | python3 -c \
  "import json,sys; print('✅ SA:', json.load(sys.stdin)['client_email'])"

# Esperado: ✅ SA: olist-pipeline-sa@TU_PROJECT_ID.iam.gserviceaccount.com
```

### 5.5. (Opcional) Service Account para Airflow

Si vas a usar Airflow con su propia SA separada:

```bash
# Crear SA para Airflow
gcloud iam service-accounts create olist-airflow-runner \
  --display-name="Olist Airflow Runner"

# Permisos para Airflow SA
gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
  --member="serviceAccount:olist-airflow-runner@${GCP_PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/composer.worker"

gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
  --member="serviceAccount:olist-airflow-runner@${GCP_PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/storage.objectAdmin"

gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
  --member="serviceAccount:olist-airflow-runner@${GCP_PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/bigquery.dataEditor"

gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
  --member="serviceAccount:olist-airflow-runner@${GCP_PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/bigquery.jobUser"

gcloud projects add-iam-policy-binding $GCP_PROJECT_ID \
  --member="serviceAccount:olist-airflow-runner@${GCP_PROJECT_ID}.iam.gserviceaccount.com" \
  --role="roles/dataflow.developer"

# Generar key para Airflow
gcloud iam service-accounts keys create ~/airflow-sa-key.json \
  --iam-account=olist-airflow-runner@${GCP_PROJECT_ID}.iam.gserviceaccount.com
```

---

## 6. Instalar Terraform

```bash
# 1. Agregar repositorio de HashiCorp
wget -O- https://apt.releases.hashicorp.com/gpg | \
  sudo gpg --dearmor -o /usr/share/keyrings/hashicorp-archive-keyring.gpg

echo "deb [signed-by=/usr/share/keyrings/hashicorp-archive-keyring.gpg] \
  https://apt.releases.hashicorp.com $(lsb_release -cs) main" | \
  sudo tee /etc/apt/sources.list.d/hashicorp.list

# 2. Instalar
sudo apt update && sudo apt install terraform

# 3. Verificar
terraform --version
# Esperado: Terraform v1.6.0+ (cualquier versión >= 1.0)
```

---

## 7. Clonar Repositorio y Configurar Ambientes

### 7.1. Clonar repositorio

```bash
git clone https://github.com/TU_USUARIO/brazilian_ecommerce.git
cd brazilian_ecommerce
```

### 7.2. Crear ambiente `airflow` (Python 3.10)

Este ambiente contiene Airflow, providers de GCP, y herramientas de testing.

```bash
# Crear ambiente conda
conda create -n airflow python=3.10 -y
conda activate airflow

# Instalar todas las dependencias
pip install -r requirements-airflow.txt
```

**Paquetes clave** (324 total):

| Paquete | Versión | Propósito |
|---------|---------|-----------|
| `apache-airflow` | 2.8.1 | Orquestación de workflows |
| `apache-airflow-providers-google` | 10.13.1 | Operators para GCP (BigQuery, GCS, Dataflow) |
| `apache-airflow-providers-apache-beam` | 5.5.0 | Operators para Apache Beam |
| `apache-beam` | 2.53.0 | Procesamiento batch/streaming |
| `google-cloud-bigquery` | 3.16.0 | Cliente BigQuery |
| `google-cloud-storage` | 2.14.0 | Cliente GCS |
| `google-cloud-pubsub` | 2.19.0 | Cliente Pub/Sub |
| `great-expectations` | 0.18.8 | Data quality framework |
| `pandas` | 2.1.4 | Manipulación de datos |
| `pytest` | 9.0.2 | Testing |
| `coverage` | 7.13.4 | Code coverage |

### 7.3. Crear ambiente `dev` (Python 3.10)

Este ambiente contiene PyTorch, Beam, herramientas de ML y pre-commit hooks.

```bash
# Crear ambiente conda
conda create -n dev python=3.10 -y
conda activate dev

# Instalar todas las dependencias
pip install -r requirements-dev.txt
```

**Paquetes clave** (509 total):

| Paquete | Versión | Propósito |
|---------|---------|-----------|
| `torch` | 2.7.1 | ML framework (GPU-ready) |
| `scikit-learn` | 1.7.1 | ML clásico |
| `xgboost` | 3.1.2 | Gradient boosting |
| `google-cloud-bigquery` | 3.40.1 | Cliente BigQuery |
| `google-cloud-storage` | 3.9.0 | Cliente GCS |
| `google-cloud-pubsub` | 2.35.0 | Cliente Pub/Sub |
| `google-cloud-dataflow-client` | 0.11.0 | Cliente Dataflow |
| `pandas` | 2.3.2 | Manipulación de datos |
| `matplotlib` / `seaborn` | última | Visualizaciones |
| `pre_commit` | 4.5.1 | Code quality hooks |
| `flake8` / `mypy` | última | Linting y type checking |

### 7.4. Configurar pre-commit hooks

```bash
conda activate dev

# Instalar hooks de pre-commit
pre-commit install

# Verificar que funcionan (ejecuta black, flake8, mypy, etc.)
pre-commit run --all-files
```

---

## 8. Desplegar Infraestructura

Tienes **2 opciones** para crear los recursos GCP:

### Opción A: Terraform (Recomendado)

#### 8.A.1. Configurar variables

**IMPORTANTE**: Antes de ejecutar Terraform, actualiza `terraform/variables.tf` con **tu** Project ID:

```hcl
# terraform/variables.tf — Línea 7
variable "project_id" {
  description = "GCP Project ID"
  type        = string
  default     = "TU_PROJECT_ID"   # ← CAMBIAR AQUÍ
}
```

> [!IMPORTANT]
> **Debes cambiar** `project_id` en `terraform/variables.tf` a tu propio Project ID.
> También puedes cambiar `region` y `bucket_location` si prefieres otra región.
> El default es `southamerica-west1`.

#### 8.A.2. Autenticar Terraform con GCP

```bash
# Usar Application Default Credentials (tus credenciales de usuario)
gcloud auth application-default login
```

> [!CAUTION]
> Terraform debe correr con **tus credenciales de usuario** (Owner del proyecto), **NO** con
> una Service Account key. Si la variable `GOOGLE_APPLICATION_CREDENTIALS` está seteada,
> Terraform la usará automáticamente y fallará con errores de permisos.
>
> ```bash
> # Si tienes GOOGLE_APPLICATION_CREDENTIALS seteada, quítala antes de correr Terraform:
> unset GOOGLE_APPLICATION_CREDENTIALS
> ```

#### 8.A.3. Inicializar y crear recursos

```bash
cd terraform

# Inicializar Terraform (descarga provider de Google)
terraform init

# Ver qué recursos se van a crear (dry-run)
terraform plan

# Deberías ver: "Plan: 28 to add, 0 to change, 0 to destroy."
#
# Los 28 recursos que se crean:
# ✓ 4 GCS Buckets       (raw-data, processed-data, airflow-dags, dataflow-temp)
# ✓ 3 BigQuery Datasets  (olist_raw, olist_staging, olist_analytics)
# ✓ 9 BigQuery Tables    (customers, orders, order_items, products, sellers,
#                          order_payments, order_reviews, geolocation,
#                          product_category_translation)
# ✓ 1 Pub/Sub Topic + 1 Subscription
# ✓ 2 Service Accounts   (olist-dataflow-runner, olist-airflow-runner)
# ✓ 8 IAM Bindings

# Aplicar cambios (crear infraestructura)
terraform apply
# Confirmar con "yes" cuando pregunte
```

> [!NOTE]
> **Sobre permisos en `olist_analytics`**: El Terraform define bloques `access {}` explícitos
> en el dataset `olist_analytics`. Esto es necesario porque los bloques `access {}` **reemplazan**
> todos los permisos heredados del proyecto. Sin incluir `projectOwners` como OWNER,
> ni siquiera el Owner del proyecto puede crear tablas. El `main.tf` ya incluye este fix.

#### 8.A.4. Verificar infraestructura

```bash
# Verificar buckets
gsutil ls
# Esperado:
# gs://TU_PROJECT_ID-raw-data/
# gs://TU_PROJECT_ID-processed-data/
# gs://TU_PROJECT_ID-airflow-dags/
# gs://TU_PROJECT_ID-dataflow-temp/

# Verificar datasets BigQuery
bq ls
# Esperado:
#    datasetId
#  ---------------
#  olist_analytics
#  olist_raw
#  olist_staging

# Verificar tablas en olist_raw
bq ls olist_raw
# Esperado: 9 tablas (customers, orders, order_items, etc.)

# Verificar Pub/Sub
gcloud pubsub topics list
# Esperado: projects/TU_PROJECT_ID/topics/olist-order-events

gcloud pubsub subscriptions list
# Esperado: projects/TU_PROJECT_ID/subscriptions/olist-order-events-subscription
```

### Opción B: Script Manual (sin Terraform)

Si prefieres crear los recursos sin Terraform:

```bash
# Ejecutar el script de creación manual
bash scripts/create_gcp_resources.sh
```

Este script crea los mismos recursos que Terraform: buckets, datasets, tablas, Pub/Sub y Service Accounts.

---

## 9. Descargar y Cargar Datos

### 9.1. Descargar dataset de Kaggle

```bash
# Opción 1: Descargar manualmente desde:
# https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
# Descomprimir en: data/olist/

# Opción 2: Usar Kaggle CLI
pip install kaggle
kaggle datasets download -d olistbr/brazilian-ecommerce -p data/olist/ --unzip
```

### 9.2. Explorar datos localmente (Opcional)

```bash
conda activate dev

python src/data_exploration/explore_olist_dataset.py

# Genera un reporte con:
# ✓ Estadísticas por tabla (rows, columns, nulls, duplicados)
# ✓ Verificaciones de integridad referencial
# ✓ Distribuciones de valores
```

### 9.3. Cargar datos a GCP

```bash
conda activate dev
export GOOGLE_APPLICATION_CREDENTIALS="$HOME/olist-sa-key.json"

# Subir CSVs a GCS y crear tablas en BigQuery
python src/data_ingestion/load_olist_to_gcp.py
```

> [!NOTE]
> La tabla `order_reviews` contiene texto con comillas y saltos de línea.
> El loader detecta esta tabla automáticamente y aplica configuración CSV especial
> (`allow_quoted_newlines`, `allow_jagged_rows`, `max_bad_records=500`).
> **No se necesita ningún script de fix adicional.**

### 9.4. Permisos adicionales en BigQuery (si hay errores)

Si recibes `Permission bigquery.tables.create denied on dataset olist_analytics`:

```bash
# Obtener tu email
MY_EMAIL=$(gcloud config get-value account)

# Dar permisos sobre el dataset olist_analytics
bq update --dataset \
  --add_access_entry="role=WRITER,userByEmail=$MY_EMAIL" \
  $GCP_PROJECT_ID:olist_analytics

# También para la SA del pipeline
bq update --dataset \
  --add_access_entry="role=WRITER,userByEmail=olist-pipeline-sa@${GCP_PROJECT_ID}.iam.gserviceaccount.com" \
  $GCP_PROJECT_ID:olist_analytics
```

### 9.5. Verificar carga

```bash
# Contar filas por tabla
bq query --use_legacy_sql=false '
  SELECT
    table_id,
    row_count,
    ROUND(size_bytes/1024/1024, 2) as size_mb
  FROM `olist_raw.__TABLES__`
  ORDER BY row_count DESC
'

# Esperado:
# +-----------------------------------+-----------+---------+
# |            table_id                | row_count | size_mb |
# +-----------------------------------+-----------+---------+
# | geolocation                        |  1000163  |   xx.xx |
# | order_items                        |   112650  |   xx.xx |
# | order_payments                     |   103886  |   xx.xx |
# | orders                             |    99441  |   xx.xx |
# | order_reviews                      |    99224  |   xx.xx |
# | customers                          |    99441  |   xx.xx |
# | products                           |    32951  |   xx.xx |
# | sellers                            |     3095  |   xx.xx |
# | product_category_translation       |       71  |   xx.xx |
# +-----------------------------------+-----------+---------+
```

---

## 10. Configurar Apache Airflow

### 10.1. Inicializar base de datos

```bash
conda activate airflow

# Configurar variables de entorno
export AIRFLOW_HOME=~/airflow
export GOOGLE_APPLICATION_CREDENTIALS="$HOME/olist-sa-key.json"

# Agregar a ~/.bashrc para persistencia
echo 'export AIRFLOW_HOME=~/airflow' >> ~/.bashrc

# Inicializar base de datos (SQLite por defecto)
airflow db init
```

### 10.2. Crear usuario admin

```bash
# Crear usuario para acceder a la UI
airflow users create \
  --username admin \
  --firstname Admin \
  --lastname User \
  --role Admin \
  --email admin@example.com \
  --password admin123
```

### 10.3. Copiar DAGs del proyecto

```bash
# Copiar los 6 DAGs de producción
cp ~/Portafolio/brazilian_ecommerce/airflow_dags/0[3-8]*.py ~/airflow/dags/

# Verificar que se copiaron
ls ~/airflow/dags/

# Esperado:
# 03_olist_daily_ingestion.py
# 04_olist_transformations.py
# 05_olist_daily_reports.py
# 06_olist_conditional_pipeline.py
# 07_pipeline_monitoring.py
# 08_olist_streaming_dag.py
```

### 10.4. (Opcional) Configurar SA separada para Airflow

Si creaste una SA específica para Airflow (paso 5.5):

```bash
# Configurar variable de entorno para la SA de Airflow
export GOOGLE_APPLICATION_CREDENTIALS=~/airflow-sa-key.json

# Agregar a ~/.bashrc para persistencia
echo 'export GOOGLE_APPLICATION_CREDENTIALS=~/airflow-sa-key.json' >> ~/.bashrc
```

### 10.5. Iniciar Airflow (2 terminales)

> [!IMPORTANT]
> Airflow requiere **2 procesos corriendo simultáneamente**: webserver y scheduler.
> Abre 2 terminales separadas.

**Terminal 1 — Webserver**:
```bash
conda activate airflow
export AIRFLOW_HOME=~/airflow
export GOOGLE_APPLICATION_CREDENTIALS="$HOME/olist-sa-key.json"

airflow webserver --port 8080
```

**Terminal 2 — Scheduler**:
```bash
conda activate airflow
export AIRFLOW_HOME=~/airflow
export GOOGLE_APPLICATION_CREDENTIALS="$HOME/olist-sa-key.json"

airflow scheduler
```

### 10.6. Acceder a la UI

Abre tu navegador en **`http://localhost:8080`** y loguéate con:
- **Usuario**: `admin`
- **Password**: `admin123`

Deberías ver los siguientes 6 DAGs:

| DAG | Schedule | Propósito |
|-----|----------|-----------|
| `olist_daily_ingestion` | `@daily` (2 AM) | Ingesta diaria GCS→BQ |
| `olist_transformations` | `@daily` (3 AM) | Transformaciones SQL |
| `olist_daily_report` | `@daily` (4 AM) | Reportes + alertas de negocio |
| `olist_conditional_pipeline` | `@daily` (5 AM) | Branching condicional (quick vs full) |
| `pipeline_monitoring` | `0 */6 * * *` | Health checks cada 6 horas |
| `olist_streaming_pipeline` | `None` (manual) | Lanza streaming en Dataflow |

> [!TIP]
> Para ejecutar un DAG manualmente, haz click en el botón **▶ (Trigger DAG)** desde la UI.
> Los DAGs arrancan en estado **Paused** por defecto — actívalos con el toggle.

---

## 11. Ejecutar Pipelines

> [!NOTE]
> Todos los scripts ahora leen `GCP_PROJECT_ID` desde la variable de entorno.
> Asegúrate de tener `export GCP_PROJECT_ID="tu-project-id"` seteado (ver paso 3.2).

### 11.1. Batch Pipeline (Apache Beam)

```bash
conda activate dev
export GOOGLE_APPLICATION_CREDENTIALS="$HOME/olist-sa-key.json"

# Ejecución local (DirectRunner) — gratis, rápido, para desarrollo
python src/pipelines/olist_basic_pipeline.py \
  --project $GCP_PROJECT_ID \
  --runner DirectRunner \
  --temp_location gs://${GCP_PROJECT_ID}-dataflow-temp/temp
```

Para producción con Dataflow:

```bash
# Ejecución en Dataflow (producción) — managed, escala automáticamente
python src/pipelines/olist_basic_pipeline.py \
  --project $GCP_PROJECT_ID \
  --runner DataflowRunner \
  --temp_location gs://${GCP_PROJECT_ID}-dataflow-temp/temp \
  --staging_location gs://${GCP_PROJECT_ID}-dataflow-temp/staging \
  --region southamerica-west1
```

### 11.2. Streaming Pipeline

Requiere **2 terminales** simultáneas:

> [!CAUTION]
> El streaming pipeline **NO funciona** con el ambiente `airflow` (`apache-beam 2.53.0`)
> debido a un bug de compatibilidad con `google-cloud-bigquery`. Usa el ambiente `dev`
> (o cualquier ambiente con `apache-beam >= 2.60.0`).

**Terminal 1 — Simulador de eventos** (genera datos fake en Pub/Sub):
```bash
conda activate dev
export GOOGLE_APPLICATION_CREDENTIALS="$HOME/olist-sa-key.json"

# 2 eventos/segundo durante 60 segundos
python src/streaming/event_simulator.py \
  --project $GCP_PROJECT_ID \
  --rate 2 \
  --duration 60
```

**Terminal 2 — Pipeline de streaming** (lee de Pub/Sub, escribe a BigQuery):
```bash
conda activate dev
export GOOGLE_APPLICATION_CREDENTIALS="$HOME/olist-sa-key.json"

python src/pipelines/olist_streaming_pipeline.py \
  --project $GCP_PROJECT_ID \
  --runner DirectRunner
```

> [!NOTE]
> Para detalles avanzados sobre el streaming pipeline (schemas BigQuery, recreación
> de tablas, errores comunes, etc.), ver [streaming_pipeline_setup.md](streaming_pipeline_setup.md).

### 11.3. Feature Engineering (SQL en BigQuery)

```bash
# Customer Features (RFM + segmentación + churn risk)
bq query --use_legacy_sql=false < src/ml_features/customer_features.sql

# Product Recommendation Features
bq query --use_legacy_sql=false < src/ml_features/product_recommendation_features.sql

# Particionamiento y clustering optimizado
bq query --use_legacy_sql=false < src/ml_features/partitioning.sql
```

---

## 12. Verificación Final

### 12.1. Tests

```bash
conda activate airflow

# Unit tests (8 tests)
pytest tests/unit/ -v

# Integration tests (4 tests — requiere SA key y datos cargados)
pytest tests/integration/ -v
```

### 12.2. Verificar datos en BigQuery

```bash
# Datos raw
bq query --use_legacy_sql=false '
  SELECT COUNT(*) as total_orders FROM `olist_raw.orders`
'
# Esperado: 99441

# Datos analytics (tras ejecutar DAG 04 o transformaciones SQL)
bq query --use_legacy_sql=false '
  SELECT * FROM `olist_analytics.daily_metrics`
  ORDER BY order_date DESC LIMIT 5
'

# Features ML (tras ejecutar SQL de features)
bq query --use_legacy_sql=false '
  SELECT customer_segment, COUNT(*) as n
  FROM `olist_analytics.ml_customer_features`
  GROUP BY 1 ORDER BY 2 DESC
'

# Streaming (tras ejecutar streaming pipeline)
bq query --use_legacy_sql=false '
  SELECT * FROM `olist_analytics.realtime_orders`
  ORDER BY processing_timestamp DESC LIMIT 10
'
```

### 12.3. Checklist final ✅

- [ ] `gcloud config list` → muestra tu proyecto correcto
- [ ] `gsutil ls` → muestra 4 buckets con tu PROJECT_ID
- [ ] `bq ls` → muestra 3 datasets (olist_raw, olist_staging, olist_analytics)
- [ ] `bq ls olist_raw` → muestra 9 tablas
- [ ] `bq query 'SELECT COUNT(*) FROM olist_raw.orders'` → retorna ~99k filas
- [ ] `pytest tests/unit/ -v` → todos los tests pasan ✓
- [ ] `terraform plan` → "No changes" (infraestructura en sync)
- [ ] Airflow UI accesible en `http://localhost:8080`
- [ ] 6 DAGs visibles en Airflow
- [ ] `pre-commit run --all-files` → todos los hooks pasan ✓

---

## Variables de Entorno — Resumen

Agregar todas a `~/.bashrc` para persistir entre sesiones:

```bash
# === GCP ===
export GCP_PROJECT_ID="ecommerce-olist-TUNOMBRE"
export GOOGLE_APPLICATION_CREDENTIALS="$HOME/olist-sa-key.json"

# === Airflow ===
export AIRFLOW_HOME=~/airflow
```

Luego recargar: `source ~/.bashrc`

---

## Troubleshooting

### ❌ Error: "Permission bigquery.tables.create denied on dataset olist_analytics"

**Causa**: El dataset `olist_analytics` tiene bloques `access {}` explícitos en Terraform que **reemplazan** los permisos heredados del proyecto.

**Solución**:
```bash
MY_EMAIL=$(gcloud config get-value account)
bq update --dataset \
  --add_access_entry="role=WRITER,userByEmail=$MY_EMAIL" \
  $GCP_PROJECT_ID:olist_analytics
```

### ❌ Error: "DefaultCredentialsError" o "Service account key not found"

**Causa**: La variable `GOOGLE_APPLICATION_CREDENTIALS` no apunta a un archivo válido.

**Solución**:
```bash
# Verificar que el archivo existe
ls -la $GOOGLE_APPLICATION_CREDENTIALS

# Si no existe, regenerar:
gcloud iam service-accounts keys create ~/olist-sa-key.json \
  --iam-account=olist-pipeline-sa@${GCP_PROJECT_ID}.iam.gserviceaccount.com

export GOOGLE_APPLICATION_CREDENTIALS="$HOME/olist-sa-key.json"
```

### ❌ Error: "Billing account not found"

**Causa**: El proyecto no tiene cuenta de facturación vinculada.

**Solución**:
```bash
gcloud billing accounts list
gcloud billing projects link $GCP_PROJECT_ID --billing-account=ACCOUNT_ID
```

### ❌ Error: "API not enabled"

**Causa**: Las APIs de GCP no están habilitadas.

**Solución**:
```bash
gcloud services enable bigquery.googleapis.com storage.googleapis.com \
  dataflow.googleapis.com pubsub.googleapis.com
```

### ❌ Error: "terraform init" falla

**Causa**: Cache de Terraform corrupto o versión incompatible.

**Solución**:
```bash
rm -rf terraform/.terraform terraform/.terraform.lock.hcl
terraform init
```

### ❌ Error: "Module not found" en Python

**Causa**: Ambiente conda incorrecto activado.

**Solución**:
```bash
# Verificar ambiente actual
conda info --envs

# Activar el correcto
conda activate airflow   # para Airflow, tests, BigQuery ops
conda activate dev  # para Beam pipelines, streaming, ML
```

### ❌ Error al cargar order_reviews (filas corruptas)

**Causa**: El CSV contiene texto con comillas y newlines que rompen el parser estándar.

**Solución**: El loader maneja esto automáticamente. Si aún falla, verificar que estás usando
la versión actual de `load_olist_to_gcp.py` que incluye `TABLES_WITH_SPECIAL_CSV`.
```bash
# Verificar que el loader tiene la config especial
grep -n "TABLES_WITH_SPECIAL_CSV" src/data_ingestion/load_olist_to_gcp.py
# Esperado: línea con order_reviews config
```

### ❌ Airflow DAGs no aparecen en la UI

**Causa**: Los DAGs no están en el directorio correcto o hay errores de sintaxis.

**Solución**:
```bash
# Verificar directorio de DAGs
ls ~/airflow/dags/

# Copiar DAGs si no están
cp ~/Portafolio/brazilian_ecommerce/airflow_dags/0[3-8]*.py ~/airflow/dags/

# Verificar errores de importación
airflow dags list

# Si hay errores, ver logs en:
cat ~/airflow/logs/scheduler/latest/*.log | tail -50
```
