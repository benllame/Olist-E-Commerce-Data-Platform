-- Recrear tabla orders con particionamiento

CREATE OR REPLACE TABLE `olist_raw.orders_partitioned`
PARTITION BY DATE(order_purchase_timestamp)
CLUSTER BY customer_id, order_status
AS
SELECT * FROM `olist_raw.orders`;

-- Verificar ahorro
SELECT 
  partition_id,
  total_rows,
  total_logical_bytes / POW(10, 9) as size_gb
FROM `olist_raw.INFORMATION_SCHEMA.PARTITIONS`
WHERE table_name = 'orders_partitioned'
ORDER BY partition_id DESC
LIMIT 10;