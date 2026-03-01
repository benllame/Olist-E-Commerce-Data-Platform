
"""
explore_olist_dataset.py

Script para explorar y validar el dataset de Olist
Ejecutar ANTES de cargar a GCP para entender la estructura de datos
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys
from datetime import datetime

# Configuración de pandas
pd.set_option('display.max_columns', None)
pd.set_option('display.width', None)
pd.set_option('display.max_colwidth', 50)

class OlistDataExplorer:
    """Explora y valida el dataset de Olist"""
    
    def __init__(self, data_dir: str = "~/Portafolio/brazilian_ecommerce/data/raw"):
        self.data_dir = Path(data_dir).expanduser()        
        print(f"📂 Directorio de datos: {self.data_dir}")
        print("")
    
    def load_datasets(self):
        """Carga todos los CSV en memoria"""
        
        print("📥 Cargando datasets...")
        
        datasets = {
            'customers': 'olist_customers_dataset.csv',
            'orders': 'olist_orders_dataset.csv',
            'order_items': 'olist_order_items_dataset.csv',
            'payments': 'olist_order_payments_dataset.csv',
            'reviews': 'olist_order_reviews_dataset.csv',
            'products': 'olist_products_dataset.csv',
            'sellers': 'olist_sellers_dataset.csv',
            'geolocation': 'olist_geolocation_dataset.csv',
            'category_translation': 'product_category_name_translation.csv'
        }
        
        self.dfs = {}
        
        for name, filename in datasets.items():
            filepath = self.data_dir / filename
            
            try:
                df = pd.read_csv(filepath)
                self.dfs[name] = df
                print(f"  ✅ {name}: {len(df):,} filas")
            except Exception as e:
                print(f"  ❌ Error cargando {name}: {e}")
        
        print("")
        return self.dfs
    
    def explore_customers(self):
        """Explora tabla de clientes"""
        
        print("=" * 80)
        print("👥 CUSTOMERS (Clientes)")
        print("=" * 80)
        
        df = self.dfs['customers']
        
        print(f"\nDimensiones: {df.shape[0]:,} filas × {df.shape[1]} columnas")
        print(f"\nColumnas: {list(df.columns)}")
        
        print(f"\nPrimeras 3 filas:")
        print(df.head(3))
        
        print(f"\nEstadísticas:")
        print(f"  - Clientes únicos: {df['customer_unique_id'].nunique():,}")
        print(f"  - IDs de cliente: {df['customer_id'].nunique():,}")
        print(f"  - Diferencia: {df['customer_id'].nunique() - df['customer_unique_id'].nunique():,} "
              "(clientes con múltiples órdenes)")
        
        print(f"\nDistribución por estado:")
        print(df['customer_state'].value_counts().head(10))
        
        print(f"\nCiudades más populares:")
        print(df['customer_city'].value_counts().head(10))
        
        print(f"\nValores nulos:")
        print(df.isnull().sum())
        
        print("")
    
    def explore_orders(self):
        """Explora tabla de órdenes"""
        
        print("=" * 80)
        print("🛒 ORDERS (Órdenes)")
        print("=" * 80)
        
        df = self.dfs['orders']
        
        print(f"\nDimensiones: {df.shape[0]:,} filas × {df.shape[1]} columnas")
        print(f"\nColumnas: {list(df.columns)}")
        
        # Convertir fechas
        date_cols = [col for col in df.columns if 'timestamp' in col or 'date' in col]
        for col in date_cols:
            df[col] = pd.to_datetime(df[col], errors='coerce')
        
        print(f"\nPrimeras 3 filas:")
        print(df.head(3))
        
        print(f"\nDistribución de estados de orden:")
        print(df['order_status'].value_counts())
        
        print(f"\nRango de fechas:")
        print(f"  - Primera orden: {df['order_purchase_timestamp'].min()}")
        print(f"  - Última orden: {df['order_purchase_timestamp'].max()}")
        
        # Órdenes por mes
        df['year_month'] = df['order_purchase_timestamp'].dt.to_period('M')
        print(f"\nÓrdenes por mes:")
        print(df['year_month'].value_counts().sort_index().tail(12))
        
        # Tiempo de entrega
        df['delivery_time_days'] = (
            df['order_delivered_customer_date'] - df['order_purchase_timestamp']
        ).dt.days
        
        print(f"\nTiempo de entrega (días):")
        print(f"  - Media: {df['delivery_time_days'].mean():.1f}")
        print(f"  - Mediana: {df['delivery_time_days'].median():.1f}")
        print(f"  - Min: {df['delivery_time_days'].min():.1f}")
        print(f"  - Max: {df['delivery_time_days'].max():.1f}")
        
        print(f"\nValores nulos:")
        print(df.isnull().sum())
        
        print("")
    
    def explore_order_items(self):
        """Explora items de órdenes"""
        
        print("=" * 80)
        print("📦 ORDER ITEMS (Items de órdenes)")
        print("=" * 80)
        
        df = self.dfs['order_items']
        
        print(f"\nDimensiones: {df.shape[0]:,} filas × {df.shape[1]} columnas")
        
        print(f"\nEstadísticas de precio:")
        print(df['price'].describe())
        
        print(f"\nEstadísticas de flete:")
        print(df['freight_value'].describe())
        
        # Items por orden
        items_per_order = df.groupby('order_id').size()
        print(f"\nItems por orden:")
        print(f"  - Promedio: {items_per_order.mean():.2f}")
        print(f"  - Máximo: {items_per_order.max()}")
        
        print(f"\nÓrdenes con múltiples items:")
        print(items_per_order.value_counts().head(10))
        
        # Productos más vendidos
        print(f"\nTop 10 productos más vendidos:")
        print(df['product_id'].value_counts().head(10))
        
        # Sellers más activos
        print(f"\nTop 10 sellers más activos:")
        print(df['seller_id'].value_counts().head(10))
        
        print("")
    
    def explore_payments(self):
        """Explora pagos"""
        
        print("=" * 80)
        print("💳 PAYMENTS (Pagos)")
        print("=" * 80)
        
        df = self.dfs['payments']
        
        print(f"\nDimensiones: {df.shape[0]:,} filas × {df.shape[1]} columnas")
        
        print(f"\nTipos de pago:")
        print(df['payment_type'].value_counts())
        
        print(f"\nDistribución de cuotas:")
        print(df['payment_installments'].value_counts().head(15))
        
        print(f"\nEstadísticas de monto de pago:")
        print(df['payment_value'].describe())
        
        # Monto promedio por tipo de pago
        print(f"\nMonto promedio por tipo de pago:")
        print(df.groupby('payment_type')['payment_value'].agg(['mean', 'median', 'count']))
        
        # Total revenue
        total_revenue = df['payment_value'].sum()
        print(f"\nRevenue total: R$ {total_revenue:,.2f} (Reales brasileños)")
        print(f"Revenue total (USD aprox): $ {total_revenue * 0.20:,.2f}")
        
        print("")
    
    def explore_reviews(self):
        """Explora reviews"""
        
        print("=" * 80)
        print("⭐ REVIEWS (Reseñas)")
        print("=" * 80)
        
        df = self.dfs['reviews']
        
        print(f"\nDimensiones: {df.shape[0]:,} filas × {df.shape[1]} columnas")
        
        print(f"\nDistribución de scores (1-5):")
        print(df['review_score'].value_counts().sort_index())
        
        # Score promedio
        avg_score = df['review_score'].mean()
        print(f"\nScore promedio: {avg_score:.2f} / 5.0")
        
        # Reviews con comentarios
        has_title = df['review_comment_title'].notna().sum()
        has_message = df['review_comment_message'].notna().sum()
        
        print(f"\nReviews con contenido:")
        print(f"  - Con título: {has_title:,} ({has_title/len(df)*100:.1f}%)")
        print(f"  - Con mensaje: {has_message:,} ({has_message/len(df)*100:.1f}%)")
        
        # Ejemplos de reviews con 1 estrella
        print(f"\nEjemplos de reviews con 1 estrella:")
        bad_reviews = df[df['review_score'] == 1]['review_comment_message'].dropna().head(3)
        for i, review in enumerate(bad_reviews, 1):
            print(f"\n  {i}. {review[:200]}...")
        
        # Ejemplos de reviews con 5 estrellas
        print(f"\nEjemplos de reviews con 5 estrellas:")
        good_reviews = df[df['review_score'] == 5]['review_comment_message'].dropna().head(3)
        for i, review in enumerate(good_reviews, 1):
            print(f"\n  {i}. {review[:200]}...")
        
        print("")
    
    def explore_products(self):
        """Explora productos"""
        
        print("=" * 80)
        print("🏷️  PRODUCTS (Productos)")
        print("=" * 80)
        
        df = self.dfs['products']
        
        print(f"\nDimensiones: {df.shape[0]:,} filas × {df.shape[1]} columnas")
        
        print(f"\nTop 20 categorías:")
        print(df['product_category_name'].value_counts().head(20))
        
        print(f"\nEstadísticas de peso (gramos):")
        print(df['product_weight_g'].describe())
        
        print(f"\nEstadísticas de dimensiones (cm):")
        print(df[['product_length_cm', 'product_height_cm', 'product_width_cm']].describe())
        
        print(f"\nEstadísticas de fotos:")
        print(df['product_photos_qty'].value_counts().head(10))
        
        # Categorías sin traducción
        categories_no_translation = df[df['product_category_name'].isna()].shape[0]
        print(f"\nProductos sin categoría: {categories_no_translation:,}")
        
        print("")
    
    def explore_sellers(self):
        """Explora vendedores"""
        
        print("=" * 80)
        print("🏪 SELLERS (Vendedores)")
        print("=" * 80)
        
        df = self.dfs['sellers']
        
        print(f"\nDimensiones: {df.shape[0]:,} filas × {df.shape[1]} columnas")
        
        print(f"\nDistribución por estado:")
        print(df['seller_state'].value_counts())
        
        print(f"\nCiudades con más sellers:")
        print(df['seller_city'].value_counts().head(15))
        
        print("")
    
    def check_data_quality(self):
        """Verifica calidad de datos"""
        
        print("=" * 80)
        print("DATA QUALITY CHECKS")
        print("=" * 80)
        
        print("\n1. ")
        print("-" * 40)
        
        # Check 1: Todos los customer_id en orders existen en customers?
        orders_customers = set(self.dfs['orders']['customer_id'])
        customers_ids = set(self.dfs['customers']['customer_id'])
        orphan_customers = orders_customers - customers_ids
        
        if len(orphan_customers) == 0:
            print("✅ Todos los customers en orders existen en tabla customers")
        else:
            print(f"❌ {len(orphan_customers)} customers en orders NO existen en customers")
        
        # Check 2: Todos los order_id en order_items existen en orders?
        order_items_orders = set(self.dfs['order_items']['order_id'])
        orders_ids = set(self.dfs['orders']['order_id'])
        orphan_orders = order_items_orders - orders_ids
        
        if len(orphan_orders) == 0:
            print("✅ Todos los orders en order_items existen en tabla orders")
        else:
            print(f"❌ {len(orphan_orders)} orders en order_items NO existen en orders")
        
        # Check 3: Todos los product_id en order_items existen en products?
        order_items_products = set(self.dfs['order_items']['product_id'])
        products_ids = set(self.dfs['products']['product_id'])
        orphan_products = order_items_products - products_ids
        
        if len(orphan_products) == 0:
            print("✅ Todos los products en order_items existen en tabla products")
        else:
            print(f"❌ {len(orphan_products)} products en order_items NO existen en products")
        
        print("\n2. VALORES NULOS")
        print("-" * 40)
        
        for name, df in self.dfs.items():
            null_counts = df.isnull().sum().sum()
            total_cells = df.shape[0] * df.shape[1]
            null_pct = (null_counts / total_cells) * 100
            
            print(f"{name:20} - {null_counts:,} nulos ({null_pct:.2f}%)")
        
        print("\n3. DUPLICADOS")
        print("-" * 40)
        
        for name, df in self.dfs.items():
            duplicates = df.duplicated().sum()
            pct = (duplicates / len(df)) * 100
            
            print(f"{name:20} - {duplicates:,} filas duplicadas ({pct:.2f}%)")
        
        print("")
    
    def generate_report(self):
        """Genera reporte completo"""
        
        print("\n")
        print("=" * 80)
        print("OLIST DATASET EXPLORATION REPORT")
        print("=" * 80)
        print(f"Fecha: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        print("")
        
        # Cargar datos
        self.load_datasets()
        
        # Explorar cada tabla
        self.explore_customers()
        self.explore_orders()
        self.explore_order_items()
        self.explore_payments()
        self.explore_reviews()
        self.explore_products()
        self.explore_sellers()
        
        # Quality checks
        self.check_data_quality()
        

def main():
    """Función principal"""
    
    explorer = OlistDataExplorer()
    explorer.generate_report()

if __name__ == "__main__":
    main()
