select
  date_trunc('day', trade_ts) as day,
  symbol,
  approx_percentile(price, 0.05) as var_95
from {{ ref('silver_trades') }}
group by 1,2
