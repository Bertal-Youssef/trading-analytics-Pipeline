select
  symbol,
  sum(case when side='BUY' then quantity else -quantity end) as net_qty
from {{ ref('silver_trades') }}
group by 1
