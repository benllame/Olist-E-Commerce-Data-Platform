-- product_recommendation_features.sql

CREATE OR REPLACE TABLE `olist_analytics.ml_product_features` AS

WITH product_stats AS (
  SELECT 
    p.product_id,
    p.product_category_name,
    t.product_category_name_english as category,
    
    -- Sales metrics
    COUNT(DISTINCT oi.order_id) as total_sales,
    COUNT(DISTINCT o.customer_id) as unique_buyers,
    SUM(oi.price) as total_revenue,
    AVG(oi.price) as avg_price,
    STDDEV(oi.price) as price_stddev,
    
    -- Quality metrics
    AVG(r.review_score) as avg_rating,
    STDDEV(r.review_score) as rating_stddev,
    COUNT(r.review_id) as review_count,
    
    -- Physical attributes
    AVG(p.product_weight_g) as avg_weight_g,
    AVG(p.product_length_cm * p.product_height_cm * p.product_width_cm) as avg_volume_cm3,
    
    -- Seller diversity
    COUNT(DISTINCT oi.seller_id) as unique_sellers,
    
    -- Delivery metrics
    AVG(TIMESTAMP_DIFF(o.order_delivered_customer_date, o.order_purchase_timestamp, DAY)) as avg_delivery_days

  FROM `olist_raw.products` p
  LEFT JOIN `olist_raw.product_category_translation` t
    ON p.product_category_name = t.product_category_name
  LEFT JOIN `olist_raw.order_items` oi 
    ON p.product_id = oi.product_id
  LEFT JOIN `olist_raw.orders` o 
    ON oi.order_id = o.order_id
  LEFT JOIN `olist_raw.order_reviews` r 
    ON o.order_id = r.order_id
  
  WHERE o.order_status = 'delivered'
  
  GROUP BY p.product_id, p.product_category_name, t.product_category_name_english
)

SELECT 
  *,
  
  -- Popularity score
  NTILE(10) OVER (ORDER BY total_sales) as popularity_decile,
  
  -- Quality score
  CASE 
    WHEN avg_rating >= 4.5 AND review_count >= 10 THEN 'excellent'
    WHEN avg_rating >= 4.0 AND review_count >= 5 THEN 'good'
    WHEN avg_rating >= 3.0 THEN 'average'
    ELSE 'poor'
  END as quality_tier,
  
  -- Price tier within category
  NTILE(5) OVER (PARTITION BY category ORDER BY avg_price) as price_tier_in_category,
  
  CURRENT_TIMESTAMP() as feature_timestamp

FROM product_stats
;