-- cart-to-purchase conversion rate
WITH 
cart_events AS (
  SELECT DISTINCT user_id, product_id, DATE(event_time) AS cart_date
  FROM ecommerce_events 
  WHERE event_type = 'cart'
),
purchase_events AS (
  SELECT DISTINCT user_id, product_id
  FROM ecommerce_events 
  WHERE event_type = 'purchase'
)
SELECT 
  c.cart_date,
  COUNT(DISTINCT c.user_id || '-' || c.product_id) AS cart_adds,
  COUNT(DISTINCT p.user_id || '-' || p.product_id) AS purchases,
  ROUND(
    COUNT(DISTINCT p.user_id || '-' || p.product_id)::numeric / NULLIF(COUNT(DISTINCT c.user_id || '-' || c.product_id), 0),
    2
  ) AS cart_to_purchase_rate
FROM cart_events c
LEFT JOIN purchase_events p 
  ON c.user_id = p.user_id AND c.product_id = p.product_id
GROUP BY c.cart_date
ORDER BY c.cart_date;