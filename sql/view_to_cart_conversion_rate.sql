-- view-to-cart conversion rate
WITH 
view_events AS (
  SELECT DISTINCT user_id, product_id, DATE(event_time) AS view_date
  FROM ecommerce_events 
  WHERE event_type = 'view'
),
cart_events AS (
  SELECT DISTINCT user_id, product_id
  FROM ecommerce_events 
  WHERE event_type = 'cart'
)
SELECT 
  v.view_date,
  COUNT(DISTINCT c.user_id || '-' || c.product_id) AS cart_adds,
  COUNT(DISTINCT v.user_id || '-' || v.product_id) AS viewers,
  ROUND(
    COUNT(DISTINCT c.user_id || '-' || c.product_id)::numeric / NULLIF(COUNT(DISTINCT v.user_id || '-' || v.product_id), 0),
    2
  ) AS view_to_cart_rate
FROM view_events v
LEFT JOIN cart_events c
  ON v.user_id = c.user_id AND v.product_id = c.product_id
GROUP BY v.view_date
ORDER BY v.view_date;