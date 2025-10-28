# -------------------------------------------------------------
# Base image: Apache Spark 3.5.1 (includes Hadoop & Java)
# -------------------------------------------------------------
FROM apache/spark:3.5.1

USER root

# -------------------------------------------------------------
# Install Python, pip, and required libraries
# -------------------------------------------------------------
RUN apt-get update && apt-get install -y python3 python3-pip wget curl libsnappy-dev \
 && pip3 install --no-cache-dir pyspark redis kafka-python clickhouse-driver clickhouse-connect python-snappy \
 && ln -s /usr/bin/python3 /usr/bin/python \
 && apt-get clean && rm -rf /var/lib/apt/lists/*

# -------------------------------------------------------------
# Download Kafka Connectors for Spark (3.5.1, Scala 2.12)
# -------------------------------------------------------------
RUN set -eux; \
    echo "=== Downloading Spark Kafka dependencies ==="; \
    for jar in \
      "https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.1/spark-sql-kafka-0-10_2.12-3.5.1.jar" \
      "https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.1/spark-token-provider-kafka-0-10_2.12-3.5.1.jar" \
      "https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar" \
      "https://repo1.maven.org/maven2/org/apache/kafka/kafka_2.12/3.5.1/kafka_2.12-3.5.1.jar" \
      "https://repo1.maven.org/maven2/org/lz4/lz4-java/1.8.0/lz4-java-1.8.0.jar" \
      "https://repo1.maven.org/maven2/org/xerial/snappy/snappy-java/1.1.10.5/snappy-java-1.1.10.5.jar" \
      "https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar"; \
    do \
      echo "Downloading $jar"; \
      curl -fsSL "$jar" -o "/opt/spark/jars/$(basename $jar)"; \
    done
# -------------------------------------------------------------
# Download Iceberg + Hadoop AWS dependencies
# -------------------------------------------------------------
RUN set -eux; \
    echo "=== Downloading Iceberg and Hadoop AWS dependencies ==="; \
    for jar in \
      "https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/1.5.2/iceberg-spark-runtime-3.5_2.12-1.5.2.jar" \
      "https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.6/hadoop-aws-3.3.6.jar" \
      "https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar"; \
    do \
      echo "Downloading $jar"; \
      curl -fsSL "$jar" -o "/opt/spark/jars/$(basename $jar)"; \
    done

# -------------------------------------------------------------
# Download ClickHouse JDBC Driver (Shaded version = includes dependencies)
# -------------------------------------------------------------
RUN wget -q \
  https://repo1.maven.org/maven2/com/clickhouse/clickhouse-jdbc/0.6.5/clickhouse-jdbc-0.6.5-shaded.jar \
  -O /opt/spark/jars/clickhouse-jdbc-0.6.5-shaded.jar


# -------------------------------------------------------------
# Download PostgreSQL JDBC driver (required for Iceberg catalog)
# -------------------------------------------------------------
RUN wget -q https://jdbc.postgresql.org/download/postgresql-42.7.3.jar \
    -O /opt/spark/jars/postgresql-42.7.3.jar

# -------------------------------------------------------------
# Verify all key JARs exist
# -------------------------------------------------------------
RUN echo "=== Verifying important JARs ===" && \
    ls -lh /opt/spark/jars | grep -E "(spark-sql-kafka|clickhouse-jdbc)" && \
    echo "✅ All required JARs downloaded successfully."

USER spark
WORKDIR /opt/spark/work-dir
