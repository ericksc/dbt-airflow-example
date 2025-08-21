
    select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
  
    
    



select customer_id
from `dbt-poc-469617`.`analytics`.`int_sales_summary`
where customer_id is null



  
  
      
    ) dbt_internal_test