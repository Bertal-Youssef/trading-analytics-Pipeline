#!/bin/bash
# scripts/init-clickhouse.sh

set -e

echo "🔧 Initializing ClickHouse database and tables..."

# Wait for ClickHouse to be ready
MAX_RETRIES=30
RETRY_COUNT=0

echo "⏳ Waiting for ClickHouse to be ready..."

while [ $RETRY_COUNT -lt $MAX_RETRIES ]; do
  echo "Attempt $((RETRY_COUNT + 1))/$MAX_RETRIES: Checking ClickHouse..."
  
  # IMPORTANT: Use --host clickhouse (not localhost!)
  if clickhouse-client --host clickhouse --query "SELECT 1" > /dev/null 2>&1; then
    echo "✅ ClickHouse is ready!"
    break
  fi
  
  RETRY_COUNT=$((RETRY_COUNT + 1))
  
  if [ $RETRY_COUNT -eq $MAX_RETRIES ]; then
    echo "❌ Failed to connect to ClickHouse after $MAX_RETRIES attempts"
    exit 1
  fi
  
  echo "ClickHouse not ready yet, waiting 2 seconds..."
  sleep 2
done

echo ""
echo "📋 Creating database..."

# Create database - USE --host clickhouse
clickhouse-client --host clickhouse --query "CREATE DATABASE IF NOT EXISTS realtime"

echo "✅ Database 'realtime' created"

echo ""
echo "📋 Creating tables..."

# Create trades_realtime table
clickhouse-client --host clickhouse --query "
CREATE TABLE IF NOT EXISTS realtime.trades_realtime (
    trade_id String,
    symbol String,
    side String,
    price Float64,
    quantity Float64,
    trade_ts DateTime
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(trade_ts)
ORDER BY (symbol, trade_ts)
SETTINGS index_granularity = 8192
"

echo "✅ Table 'trades_realtime' created"

# Create aggregated table for analytics
clickhouse-client --host clickhouse --query "
CREATE TABLE IF NOT EXISTS realtime.trades_aggregated (
    symbol String,
    minute DateTime,
    open Float64,
    high Float64,
    low Float64,
    close Float64,
    volume Float64,
    trade_count UInt64
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(minute)
ORDER BY (symbol, minute)
SETTINGS index_granularity = 8192
"

echo "✅ Table 'trades_aggregated' created"

# Create materialized view for real-time aggregation
clickhouse-client --host clickhouse --query "
CREATE MATERIALIZED VIEW IF NOT EXISTS realtime.trades_aggregated_mv
TO realtime.trades_aggregated
AS SELECT
    symbol,
    toStartOfMinute(trade_ts) as minute,
    argMin(price, trade_ts) as open,
    max(price) as high,
    min(price) as low,
    argMax(price, trade_ts) as close,
    sum(quantity) as volume,
    count() as trade_count
FROM realtime.trades_realtime
GROUP BY symbol, minute
"

echo "✅ Materialized view 'trades_aggregated_mv' created"

echo ""
echo "📊 Created tables:"
clickhouse-client --host clickhouse --query "SHOW TABLES FROM realtime"

echo ""
echo "✨ ClickHouse initialization complete!"