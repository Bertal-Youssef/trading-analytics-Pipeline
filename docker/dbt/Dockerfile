# ---- Base Python image ----
FROM python:3.11-slim

# ---- System dependencies ----
RUN apt-get update \
 && apt-get install -y --no-install-recommends bash build-essential libpq-dev git \
 && rm -rf /var/lib/apt/lists/*

# ---- Install dbt core + Trino adapter ----
RUN pip install --no-cache-dir "dbt-core==1.8.4" "dbt-trino==1.8.4"

# ---- Set working directory ----
WORKDIR /usr/app

# ---- Keep container alive for debugging or Airflow execs ----
CMD ["tail", "-f", "/dev/null"]
