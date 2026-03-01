"""
test_bigquery_integration.py

Tests de integración con BigQuery
"""

import os
import subprocess
import unittest
from unittest.mock import Mock, patch
from google.cloud import bigquery
import pandas as pd


def _get_project_id() -> str:
    """Obtiene project ID de env var o gcloud CLI"""
    project_id = os.environ.get("GCP_PROJECT_ID")
    if project_id:
        return project_id
    try:
        result = subprocess.run(
            ["gcloud", "config", "get-value", "project"],
            capture_output=True,
            text=True,
            check=True,
        )
        return result.stdout.strip()
    except Exception:
        return ""


PROJECT_ID = _get_project_id()
SA_KEY_PATH = os.path.expanduser("~/olist-sa-key.json")


@unittest.skipUnless(
    os.path.exists(SA_KEY_PATH), f"Service account key not found at {SA_KEY_PATH}"
)
class TestBigQueryIntegration(unittest.TestCase):
    """Tests de integración con BigQuery"""

    @classmethod
    def setUpClass(cls):
        """Setup una vez para toda la clase"""
        os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = SA_KEY_PATH
        cls.client = bigquery.Client(project=PROJECT_ID)

    def test_orders_table_exists(self):
        """Test que tabla orders existe"""

        table_id = f"{PROJECT_ID}.olist_raw.orders"

        try:
            table = self.client.get_table(table_id)
            self.assertIsNotNone(table)
            self.assertGreater(table.num_rows, 0)
        except Exception as e:
            self.fail(f"Table not found: {e}")

    def test_orders_schema_correct(self):
        """Test que schema de orders es correcto"""

        table_id = f"{PROJECT_ID}.olist_raw.orders"
        table = self.client.get_table(table_id)

        required_fields = ["order_id", "customer_id", "order_status"]

        schema_fields = [field.name for field in table.schema]

        for required_field in required_fields:
            self.assertIn(required_field, schema_fields)

    def test_referential_integrity_orders_customers(self):
        """Test integridad referencial entre orders y customers"""

        query = f"""
        SELECT COUNT(*) as orphan_count
        FROM `{PROJECT_ID}.olist_raw.orders` o
        LEFT JOIN `{PROJECT_ID}.olist_raw.customers` c
          ON o.customer_id = c.customer_id
        WHERE c.customer_id IS NULL
        """

        result = self.client.query(query).result()
        orphan_count = list(result)[0]["orphan_count"]

        self.assertEqual(orphan_count, 0, f"Found {orphan_count} orphan orders")

    def test_data_freshness(self):
        """Test que datos no son muy viejos"""

        query = f"""
        SELECT MAX(order_purchase_timestamp) as last_order
        FROM `{PROJECT_ID}.olist_raw.orders`
        """

        result = self.client.query(query).result()
        last_order = list(result)[0]["last_order"]

        # Por ahora solo verificamos que existe
        self.assertIsNotNone(last_order)


if __name__ == "__main__":
    unittest.main()
