{{ config(materialized='table') }}

SELECT
    customer_id,
    COUNT(order_id) AS num_orders,
    SUM(amount) AS total_amount,
    MIN(order_date) AS first_order,
    MAX(order_date) AS last_order
FROM {{ ref('stg_sales') }}
GROUP BY customer_id
