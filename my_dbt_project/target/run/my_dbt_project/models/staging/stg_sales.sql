

  create or replace view `dbt-poc-469617`.`analytics`.`stg_sales`
  OPTIONS()
  as 

SELECT
    order_id,
    customer_id,
    product_id,
    amount,
    order_date
FROM `dbt-poc-469617`.`raw`.`sales`;

