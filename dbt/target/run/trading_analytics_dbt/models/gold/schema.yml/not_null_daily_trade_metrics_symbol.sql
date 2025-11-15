select
      count(*) as failures,
      count(*) != 0 as should_warn,
      count(*) != 0 as should_error
    from (
      
    
    



select symbol
from "lakehouse"."gold"."daily_trade_metrics"
where symbol is null



      
    ) dbt_internal_test