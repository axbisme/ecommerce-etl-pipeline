-- view-to-purchase conversion rate
WITH 
view_events AS (
  SELECT DISTINCT user_id, product_id, DATE(event_time) AS view_date
  FROM ecommerce_events 
  WHERE event_type = 'view'
),
purchase_events AS (
  SELECT DISTINCT user_id, product_id
  FROM ecommerce_events 
  WHERE event_type = 'purchase'
)
SELECT 
  v.view_date,
  COUNT(DISTINCT v.user_id || '-' || v.product_id) AS viewers,
  COUNT(DISTINCT p.user_id || '-' || p.product_id) AS purchases,
  ROUND(
    COUNT(DISTINCT p.user_id || '-' || p.product_id)::numeric / NULLIF(COUNT(DISTINCT v.user_id || '-' || v.product_id), 0),
    2
  ) AS view_to_purchase_rate
FROM view_events v
LEFT JOIN purchase_events p
  ON v.user_id = p.user_id AND v.product_id = p.product_id
GROUP BY v.view_date
ORDER BY v.view_date;