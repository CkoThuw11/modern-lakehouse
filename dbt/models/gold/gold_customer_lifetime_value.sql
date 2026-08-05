SELECT
    c.customer_id,
    c.first_name,
    c.last_name,
    c.email,
    COUNT(DISTINCT o.order_id) AS order_count,
    SUM(oi.line_revenue) AS lifetime_revenue
FROM {{ ref('silver_customers') }} c
JOIN {{ ref('silver_orders') }} o ON o.customer_id = c.customer_id
JOIN {{ ref('silver_order_items') }} oi ON oi.order_id = o.order_id
GROUP BY c.customer_id, c.first_name, c.last_name, c.email
ORDER BY lifetime_revenue DESC
