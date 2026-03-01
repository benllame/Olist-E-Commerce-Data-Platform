-- customer_features.sql
-- Features de clientes para ML

CREATE OR REPLACE TABLE `olist_analytics.ml_customer_features` AS

WITH customer_orders AS (
  SELECT 
    c.customer_unique_id,
    
    -- RFM Metrics
    MAX(DATE(o.order_purchase_timestamp)) as last_order_date,
    COUNT(DISTINCT o.order_id) as total_orders,
    SUM(p.payment_value) as total_spent,
    
    -- Behavioral patterns
    AVG(p.payment_value) as avg_order_value,
    STDDEV(p.payment_value) as stddev_order_value,
    MIN(p.payment_value) as min_order_value,
    MAX(p.payment_value) as max_order_value,
    
    -- Temporal patterns
    MIN(DATE(o.order_purchase_timestamp)) as first_order_date,
    AVG(p.payment_installments) as avg_installments,
    
    -- Product diversity
    COUNT(DISTINCT oi.product_id) as unique_products_bought,
    COUNT(DISTINCT t.product_category_name_english) as unique_categories,
    
    -- Review behavior
    AVG(r.review_score) as avg_review_score,
    COUNT(r.review_id) as total_reviews_given,
    
    -- Geographic
    ANY_VALUE(c.customer_state) as customer_state,
    ANY_VALUE(c.customer_city) as customer_city

  FROM `olist_raw.customers` c
  LEFT JOIN `olist_raw.orders` o 
    ON c.customer_id = o.customer_id
  LEFT JOIN `olist_raw.order_payments` p 
    ON o.order_id = p.order_id
  LEFT JOIN `olist_raw.order_items` oi 
    ON o.order_id = oi.order_id
  LEFT JOIN `olist_raw.products` pr 
    ON oi.product_id = pr.product_id
  LEFT JOIN `olist_raw.product_category_translation` t 
    ON pr.product_category_name = t.product_category_name
  LEFT JOIN `olist_raw.order_reviews` r 
    ON o.order_id = r.order_id
  
  WHERE o.order_status = 'delivered'
  
  GROUP BY c.customer_unique_id
),

recency_features AS (
  SELECT 
    customer_unique_id,
    
    -- Recency (days since last order)
    DATE_DIFF(CURRENT_DATE(), last_order_date, DAY) as days_since_last_order,
    
    -- Customer lifetime (days)
    DATE_DIFF(last_order_date, first_order_date, DAY) as customer_lifetime_days,
    
    -- Purchase frequency (orders per month)
    CASE 
      WHEN DATE_DIFF(last_order_date, first_order_date, DAY) > 0
      THEN (total_orders * 30.0) / DATE_DIFF(last_order_date, first_order_date, DAY)
      ELSE 0
    END as orders_per_month,
    
    -- Average days between orders
    CASE 
      WHEN total_orders > 1
      THEN DATE_DIFF(last_order_date, first_order_date, DAY) / (total_orders - 1.0)
      ELSE NULL
    END as avg_days_between_orders
    
  FROM customer_orders
)

SELECT 
  co.*,
  rf.days_since_last_order,
  rf.customer_lifetime_days,
  rf.orders_per_month,
  rf.avg_days_between_orders,
  
  -- RFM Scores (1-5)
  NTILE(5) OVER (ORDER BY rf.days_since_last_order DESC) as recency_score,
  NTILE(5) OVER (ORDER BY co.total_orders) as frequency_score,
  NTILE(5) OVER (ORDER BY co.total_spent) as monetary_score,
  
  -- Engagement score
  (
    NTILE(5) OVER (ORDER BY co.total_orders) + 
    NTILE(5) OVER (ORDER BY co.unique_products_bought) + 
    NTILE(5) OVER (ORDER BY co.unique_categories)
  ) / 3.0 as engagement_score,
  
  -- Customer value tier
  CASE 
    WHEN co.total_spent >= 2000 THEN 'platinum'
    WHEN co.total_spent >= 1000 THEN 'gold'
    WHEN co.total_spent >= 500 THEN 'silver'
    ELSE 'bronze'
  END as value_tier,
  
  -- Churn risk
  CASE 
    WHEN rf.days_since_last_order > 365 THEN 'very_high'
    WHEN rf.days_since_last_order > 180 THEN 'high'
    WHEN rf.days_since_last_order > 90 THEN 'medium'
    WHEN rf.days_since_last_order > 30 THEN 'low'
    ELSE 'very_low'
  END as churn_risk,
  
  -- Customer segment (RFM combined)
  CASE 
    WHEN NTILE(5) OVER (ORDER BY rf.days_since_last_order DESC) >= 4
         AND NTILE(5) OVER (ORDER BY co.total_orders) >= 4
         AND NTILE(5) OVER (ORDER BY co.total_spent) >= 4
      THEN 'champions'
    WHEN NTILE(5) OVER (ORDER BY rf.days_since_last_order DESC) >= 4
         AND NTILE(5) OVER (ORDER BY co.total_orders) >= 3
      THEN 'loyal_customers'
    WHEN NTILE(5) OVER (ORDER BY co.total_spent) >= 4
      THEN 'big_spenders'
    WHEN NTILE(5) OVER (ORDER BY rf.days_since_last_order DESC) <= 2
         AND NTILE(5) OVER (ORDER BY co.total_orders) >= 3
      THEN 'at_risk'
    WHEN NTILE(5) OVER (ORDER BY rf.days_since_last_order DESC) <= 2
      THEN 'lost'
    ELSE 'potential_loyalist'
  END as customer_segment,
  
  CURRENT_TIMESTAMP() as feature_timestamp

FROM customer_orders co
JOIN recency_features rf 
  ON co.customer_unique_id = rf.customer_unique_id
;