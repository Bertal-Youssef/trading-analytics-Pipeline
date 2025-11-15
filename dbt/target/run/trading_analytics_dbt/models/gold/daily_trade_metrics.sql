
  
    

    create table "lakehouse"."gold"."daily_trade_metrics__dbt_tmp"
      
      
    as (
      

with base as (
  select
    coalesce(date(source_ts), date(ingestion_time)) as trade_date,
    symbol,
    case when lower(side)='buy'  then quantity else 0 end as buy_qty_part,
    case when lower(side)='sell' then quantity else 0 end as sell_qty_part,
    case when lower(side)='buy'  then price*quantity else 0 end as buy_notional_part,
    case when lower(side)='sell' then price*quantity else 0 end as sell_notional_part
  from lakehouse.silver.trades_clean
  where coalesce(source_ts, ingestion_time) is not null
)
select
  trade_date,
  symbol,
  count(*) as trades,
  sum(buy_qty_part)  as buy_qty,
  sum(sell_qty_part) as sell_qty,
  sum(buy_qty_part) - sum(sell_qty_part) as net_qty,
  sum(buy_notional_part)  as buy_notional,
  sum(sell_notional_part) as sell_notional,
  sum(buy_notional_part) + sum(sell_notional_part) as gross_notional,
  case when sum(buy_qty_part)  > 0 then sum(buy_notional_part)/sum(buy_qty_part)  end as avg_buy_price,
  case when sum(sell_qty_part) > 0 then sum(sell_notional_part)/sum(sell_qty_part) end as avg_sell_price,
  current_timestamp as ingested_at
from base
group by 1,2
    );

  