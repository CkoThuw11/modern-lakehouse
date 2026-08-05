SELECT
    o.order_date,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(oi.line_revenue) AS total_revenue
FROM {{ ref('silver_orders') }} o
JOIN {{ ref('silver_order_items') }} oi ON oi.order_id = o.order_id
GROUP BY o.order_date
ORDER BY o.order_date
