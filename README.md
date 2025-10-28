# Trading Analytics Platform — Hybrid Medallion (Real‑time + Batch)
!(image alt)(https://github.com/Bertal-Youssef/trading-analytics-Pipeline/blob/9718721787129757b50d2271aeeabbff7dad1172/DATALAKEHOUSE.jpg)
This repository scaffolds an enterprise‑grade data platform implementing a **Hybrid Medallion** architecture:

- **Hot path (≤5s):** Kafka → Spark Structured Streaming → Redis → Metabase
- **Cold path (T+1 @ 17:00):** Kafka → Spark Batch → Iceberg (Bronze/Silver/Gold) → Trino → Superset

> Goal: live trade monitoring + robust historical analytics with governance, quality and reproducibility.

---

## Repo layout

```
trading-analytics-platform/
├── docker/                 # Container configs & init scripts
├── airflow/                # DAGs, plugins, requirements
├── spark/                  # Streaming & batch jobs (Scala/PySpark)
├── dbt/                    # dbt project for Gold models
├── schemas/                # Avro + Iceberg table definitions
├── docs/                   # Architecture diagrams / ADRs
├── scripts/                # Utilities (producers, helpers)
├── superset/               # Superset bootstrap
├── metabase/               # Metabase notes
├── config/                 # Central configuration
├── .env.example
├── docker-compose.yml
└── Makefile
```

## SLAs

- **Real‑time dashboards:** end‑to‑end latency ≤ **5 seconds**
- **Batch refresh:** **17:00** daily (EOD) for Silver/Gold

## Architecture overview

### Realtime (Hot) path
1. Python producers publish **`trades_raw`** and **`market_data_raw`** to Kafka (Avro).
2. Spark Structured Streaming validates minimal schema and writes key metrics to **Redis** (TTL 1h).
3. **Metabase** queries Redis for live cards (PnL, prices, volumes, active positions).

### Batch (Cold) path
1. Continuous ingestion persists Kafka payloads to **Iceberg Bronze** (immutable) partitioned by date.
2. **Airflow** runs daily Spark jobs → **Silver** (clean, deduped, validated via Great Expectations).
3. **dbt** builds **Gold** marts (e.g., `daily_pnl`, `risk_metrics`, `portfolio_summary`) over Trino.
4. **Superset** delivers historical analytics with cost‑based optimizations enabled in Trino.

## Getting started

1. Copy `.env.example` to `.env` and adjust ports/passwords.
2. `make up` to launch the stack.
3. Open the UIs (see `Makefile` + comments in `docker-compose.yml`).
4. Run the sample producers to generate streaming data (see `scripts/`).

## Roadmap lock‑in

The detailed nine‑stage execution plan lives in `docs/ARCHITECTURE.md` and is mirrored in this README.
