{{ config(materialized='incremental', partition_by='trade_date') }}
select
  trade_date,
  symbol,
  sum(case when side='BUY' then -price*quantity else price*quantity end) as pnl
from {{ ref('silver_trades') }}
group by 1,2
