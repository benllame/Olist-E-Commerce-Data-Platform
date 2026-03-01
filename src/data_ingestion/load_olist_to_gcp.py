"""
load_olist_to_gcp.py

Carga el dataset de Olist a Google Cloud Storage y BigQuery
"""

import pandas as pd
from google.cloud import storage, bigquery
from pathlib import Path
import sys
from datetime import datetime
from typing import Dict
import time

class OlistGCPLoader:
    """Carga datos de Olist a GCP"""
    
    # Tablas que requieren configuración CSV especial
    # order_reviews: contiene texto con comillas y newlines embebidos
    TABLES_WITH_SPECIAL_CSV = {
        'order_reviews': {
            'allow_quoted_newlines': True,   # Permite \n dentro de comillas
            'allow_jagged_rows': True,       # Permite filas con columnas variables
            'max_bad_records': 500,           # Tolera hasta 500 filas con errores
        }
    }
    
    def __init__(
        self,
        project_id: str,
        data_dir: str = "./data/raw"
    ):
        self.project_id = project_id
        self.data_dir = Path(data_dir).expanduser()
        
        # Clientes GCP
        self.storage_client = storage.Client(project=project_id)
        self.bq_client = bigquery.Client(project=project_id)
        
        # Configuración de buckets y datasets
        self.raw_bucket_name = f"{project_id}-raw-data"
        self.bq_dataset_id = "olist_raw"
        
        print(f"🔧 Configuración:")
        print(f"  - Project ID: {project_id}")
        print(f"  - Raw Bucket: {self.raw_bucket_name}")
        print(f"  - BQ Dataset: {self.bq_dataset_id}")
        print("")
        
    def upload_csv_to_gcs(self, local_file: Path, gcs_path: str):
        """Sube un archivo CSV a GCS"""
        
        try:
            bucket = self.storage_client.bucket(self.raw_bucket_name)
            blob = bucket.blob(gcs_path)
            
            # Upload
            blob.upload_from_filename(str(local_file))
            
            print(f"  ✅ {local_file.name} → gs://{self.raw_bucket_name}/{gcs_path}")
            return f"gs://{self.raw_bucket_name}/{gcs_path}"
            
        except Exception as e:
            print(f"  ❌ Error subiendo {local_file.name}: {e}")
            return None
    
    def upload_all_csvs(self):
        """Sube todos los CSVs a GCS"""
        
        print("=" * 80)
        print("📤 SUBIENDO CSVs A GOOGLE CLOUD STORAGE")
        print("=" * 80)
        print("")
        
        csv_files = list(self.data_dir.glob("*.csv"))
        
        if not csv_files:
            print(f"❌ No se encontraron archivos CSV en {self.data_dir}")
            return
        
        print(f"Archivos encontrados: {len(csv_files)}")
        print("")
        
        uploaded_files = {}
        
        for csv_file in csv_files:
            # Crear path en GCS: olist/YYYY-MM-DD/filename.csv
            date_str = datetime.now().strftime("%Y-%m-%d")
            gcs_path = f"olist/{date_str}/{csv_file.name}"
            
            gcs_uri = self.upload_csv_to_gcs(csv_file, gcs_path)
            if gcs_uri:
                uploaded_files[csv_file.stem] = gcs_uri
        
        print("")
        print(f"✅ {len(uploaded_files)}/{len(csv_files)} archivos subidos")
        print("")
        
        return uploaded_files
    
    def load_csv_to_bigquery(
        self,
        gcs_uri: str,
        table_id: str,
        schema: list = None
    ):
        """Carga un CSV desde GCS a BigQuery
        
        Detecta automáticamente tablas que requieren configuración CSV especial
        (ej: order_reviews con quoted newlines y jagged rows).
        """
        
        try:
            table_ref = f"{self.project_id}.{self.bq_dataset_id}.{table_id}"
            
            # Configuración base del job
            job_config = bigquery.LoadJobConfig(
                source_format=bigquery.SourceFormat.CSV,
                skip_leading_rows=1,  # Skip header
                autodetect=True if schema is None else False,
                write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
                schema=schema
            )
            
            # Aplicar configuración especial si la tabla lo requiere
            if table_id in self.TABLES_WITH_SPECIAL_CSV:
                special = self.TABLES_WITH_SPECIAL_CSV[table_id]
                job_config.allow_quoted_newlines = special.get('allow_quoted_newlines', False)
                job_config.allow_jagged_rows = special.get('allow_jagged_rows', False)
                job_config.max_bad_records = special.get('max_bad_records', 0)
                print(f"  ⚙️  {table_id}: usando config CSV especial "
                      f"(quoted_newlines={job_config.allow_quoted_newlines}, "
                      f"jagged_rows={job_config.allow_jagged_rows}, "
                      f"max_bad_records={job_config.max_bad_records})")
            
            # Iniciar load job
            load_job = self.bq_client.load_table_from_uri(
                gcs_uri,
                table_ref,
                job_config=job_config
            )
            
            # Esperar a que termine
            load_job.result()
            
            # Obtener info de la tabla
            table = self.bq_client.get_table(table_ref)
            
            print(f"  ✅ {table_id}: {table.num_rows:,} filas cargadas")
            
            # Reportar errores tolerados (si hubo)
            if load_job.errors:
                print(f"  ⚠️  {len(load_job.errors)} filas con errores fueron ignoradas")
            
            return True
            
        except Exception as e:
            print(f"  ❌ Error cargando {table_id}: {e}")
            return False
    
    def get_bigquery_schema(self, table_name: str) -> list:
        """Retorna el schema de BigQuery para cada tabla"""
        
        # Schemas optimizados (basados en terraform/main.tf)
        schemas = {
            'olist_customers_dataset': [
                bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("customer_unique_id", "STRING"),
                bigquery.SchemaField("customer_zip_code_prefix", "STRING"),
                bigquery.SchemaField("customer_city", "STRING"),
                bigquery.SchemaField("customer_state", "STRING"),
            ],
            
            'olist_orders_dataset': [
                bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("customer_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("order_status", "STRING"),
                bigquery.SchemaField("order_purchase_timestamp", "TIMESTAMP", mode="REQUIRED"),
                bigquery.SchemaField("order_approved_at", "TIMESTAMP"),
                bigquery.SchemaField("order_delivered_carrier_date", "TIMESTAMP"),
                bigquery.SchemaField("order_delivered_customer_date", "TIMESTAMP"),
                bigquery.SchemaField("order_estimated_delivery_date", "TIMESTAMP"),
            ],
            
            'olist_order_items_dataset': [
                bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("order_item_id", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("seller_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("shipping_limit_date", "TIMESTAMP"),
                bigquery.SchemaField("price", "FLOAT64", mode="REQUIRED"),
                bigquery.SchemaField("freight_value", "FLOAT64"),
            ],
            
            'olist_order_payments_dataset': [
                bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("payment_sequential", "INTEGER", mode="REQUIRED"),
                bigquery.SchemaField("payment_type", "STRING"),
                bigquery.SchemaField("payment_installments", "INTEGER"),
                bigquery.SchemaField("payment_value", "FLOAT64", mode="REQUIRED"),
            ],
            
            'olist_order_reviews_dataset': [
                bigquery.SchemaField("review_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("order_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("review_score", "INTEGER"),
                bigquery.SchemaField("review_comment_title", "STRING"),
                bigquery.SchemaField("review_comment_message", "STRING"),
                bigquery.SchemaField("review_creation_date", "TIMESTAMP"),
                bigquery.SchemaField("review_answer_timestamp", "TIMESTAMP"),
            ],
            
            'olist_products_dataset': [
                bigquery.SchemaField("product_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("product_category_name", "STRING"),
                bigquery.SchemaField("product_name_length", "INTEGER"),
                bigquery.SchemaField("product_description_length", "INTEGER"),
                bigquery.SchemaField("product_photos_qty", "INTEGER"),
                bigquery.SchemaField("product_weight_g", "INTEGER"),
                bigquery.SchemaField("product_length_cm", "INTEGER"),
                bigquery.SchemaField("product_height_cm", "INTEGER"),
                bigquery.SchemaField("product_width_cm", "INTEGER"),
            ],
            
            'olist_sellers_dataset': [
                bigquery.SchemaField("seller_id", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("seller_zip_code_prefix", "STRING"),
                bigquery.SchemaField("seller_city", "STRING"),
                bigquery.SchemaField("seller_state", "STRING"),
            ],
            
            'olist_geolocation_dataset': [
                bigquery.SchemaField("geolocation_zip_code_prefix", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("geolocation_lat", "FLOAT64"),
                bigquery.SchemaField("geolocation_lng", "FLOAT64"),
                bigquery.SchemaField("geolocation_city", "STRING"),
                bigquery.SchemaField("geolocation_state", "STRING"),
            ],
            
            'product_category_name_translation': [
                bigquery.SchemaField("product_category_name", "STRING", mode="REQUIRED"),
                bigquery.SchemaField("product_category_name_english", "STRING"),
            ],
        }
        
        return schemas.get(table_name, None)
    
    def load_all_to_bigquery(self, gcs_files: Dict[str, str]):
        """Carga todos los archivos desde GCS a BigQuery"""
        
        print("=" * 80)
        print("📥 CARGANDO DATOS A BIGQUERY")
        print("=" * 80)
        print("")
        
        # Mapeo de nombres de archivos a nombres de tablas
        table_mapping = {
            'olist_customers_dataset': 'customers',
            'olist_orders_dataset': 'orders',
            'olist_order_items_dataset': 'order_items',
            'olist_order_payments_dataset': 'order_payments',
            'olist_order_reviews_dataset': 'order_reviews',
            'olist_products_dataset': 'products',
            'olist_sellers_dataset': 'sellers',
            'olist_geolocation_dataset': 'geolocation',
            'product_category_name_translation': 'product_category_translation',
        }
        
        loaded_count = 0
        
        for file_stem, gcs_uri in gcs_files.items():
            table_id = table_mapping.get(file_stem, file_stem)
            schema = self.get_bigquery_schema(file_stem)
            
            success = self.load_csv_to_bigquery(gcs_uri, table_id, schema)
            if success:
                loaded_count += 1
            
            # Pequeña pausa entre loads
            time.sleep(1)
        
        print("")
        print(f"✅ {loaded_count}/{len(gcs_files)} tablas cargadas")
        print("")
    
    def verify_data(self):
        """Verifica que los datos estén en BigQuery"""
        
        print("=" * 80)
        print("✅ VERIFICANDO DATOS EN BIGQUERY")
        print("=" * 80)
        print("")
        
        tables = [
            'customers', 'orders', 'order_items', 'order_payments',
            'order_reviews', 'products', 'sellers', 'geolocation',
            'product_category_translation'
        ]
        
        for table_id in tables:
            table_ref = f"{self.project_id}.{self.bq_dataset_id}.{table_id}"
            
            try:
                table = self.bq_client.get_table(table_ref)
                
                # Query simple
                query = f"SELECT COUNT(*) as count FROM `{table_ref}`"
                result = self.bq_client.query(query).result()
                count = list(result)[0]['count']
                
                print(f"  ✅ {table_id:30} - {count:,} filas")
                
            except Exception as e:
                print(f"  ❌ {table_id:30} - Error: {e}")
        
        print("")
    
    def run_sample_queries(self):
        """Ejecuta queries de ejemplo"""
        
        print("=" * 80)
        print("🔍 SAMPLE QUERIES")
        print("=" * 80)
        print("")
        
        # Query 1: Top 10 ciudades con más clientes
        print("1️⃣  Top 10 ciudades con más clientes:")
        print("-" * 40)
        
        query1 = f"""
        SELECT 
            customer_city,
            customer_state,
            COUNT(*) as num_customers
        FROM `{self.project_id}.{self.bq_dataset_id}.customers`
        GROUP BY customer_city, customer_state
        ORDER BY num_customers DESC
        LIMIT 10
        """
        
        result = self.bq_client.query(query1).result()
        for row in result:
            print(f"  {row['customer_city']}, {row['customer_state']}: {row['num_customers']:,}")
        
        print("")
        
        # Query 2: Revenue por mes
        print("2️⃣  Revenue por mes (últimos 12 meses):")
        print("-" * 40)
        
        query2 = f"""
        SELECT 
            FORMAT_TIMESTAMP('%Y-%m', o.order_purchase_timestamp) as year_month,
            COUNT(DISTINCT o.order_id) as num_orders,
            SUM(p.payment_value) as total_revenue
        FROM `{self.project_id}.{self.bq_dataset_id}.orders` o
        JOIN `{self.project_id}.{self.bq_dataset_id}.order_payments` p
            ON o.order_id = p.order_id
        WHERE o.order_status = 'delivered'
        GROUP BY year_month
        ORDER BY year_month DESC
        LIMIT 12
        """
        
        result = self.bq_client.query(query2).result()
        for row in result:
            revenue_usd = row['total_revenue'] * 0.20  # Aprox conversion rate
            print(f"  {row['year_month']}: {row['num_orders']:,} órdenes - "
                  f"R$ {row['total_revenue']:,.2f} (≈ ${revenue_usd:,.2f} USD)")
        
        print("")
        
        # Query 3: Categorías más vendidas
        print("3️⃣  Top 10 categorías de productos:")
        print("-" * 40)
        
        query3 = f"""
        SELECT 
            p.product_category_name,
            t.product_category_name_english,
            COUNT(DISTINCT oi.order_id) as num_orders
        FROM `{self.project_id}.{self.bq_dataset_id}.order_items` oi
        JOIN `{self.project_id}.{self.bq_dataset_id}.products` p
            ON oi.product_id = p.product_id
        LEFT JOIN `{self.project_id}.{self.bq_dataset_id}.product_category_translation` t
            ON p.product_category_name = t.product_category_name
        WHERE p.product_category_name IS NOT NULL
        GROUP BY p.product_category_name, t.product_category_name_english
        ORDER BY num_orders DESC
        LIMIT 10
        """
        
        result = self.bq_client.query(query3).result()
        for row in result:
            category_en = row['product_category_name_english'] or row['product_category_name']
            print(f"  {category_en}: {row['num_orders']:,} órdenes")
        
        print("")
    
    def run_full_load(self):
        """Ejecuta carga completa"""
        
        print("\n")
        print("=" * 80)
        print("🚀 OLIST → GCP FULL LOAD")
        print("=" * 80)
        print(f"Timestamp: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("")
        
        # 1. Upload CSVs a GCS
        gcs_files = self.upload_all_csvs()
        
        if not gcs_files:
            print("❌ No se subieron archivos. Abortando.")
            return
        
        # 2. Cargar a BigQuery
        self.load_all_to_bigquery(gcs_files)
        
        # 3. Verificar
        self.verify_data()
        
        # 4. Sample queries
        self.run_sample_queries()


def main():
    """Función principal"""
    
    # Obtener project ID
    import subprocess
    
    try:
        result = subprocess.run(
            ["gcloud", "config", "get-value", "project"],
            capture_output=True,
            text=True,
            check=True
        )
        project_id = result.stdout.strip()
        
        if not project_id:
            print("❌ No se pudo obtener el project ID")
            print("Ejecuta: gcloud config set project YOUR_PROJECT_ID")
            sys.exit(1)
        
    except Exception as e:
        print(f"❌ Error obteniendo project ID: {e}")
        sys.exit(1)
    
    # Crear loader
    loader = OlistGCPLoader(project_id=project_id)
    
    # Ejecutar carga completa
    loader.run_full_load()

if __name__ == "__main__":
    main()
