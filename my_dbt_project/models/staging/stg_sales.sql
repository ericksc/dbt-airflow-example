{{ config(materialized='view') }}

SELECT
    order_id,
    customer_id,
    product_id,
    amount,
    order_date
FROM {{ source('raw', 'sales') }}
