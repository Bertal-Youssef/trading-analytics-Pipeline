#!/usr/bin/env python3
import json, os, time, random, uuid
from datetime import datetime, timezone
from kafka import KafkaProducer

producer = KafkaProducer(
    bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP", "kafka:9092"),
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    compression_type="gzip",
    acks="all"
)


symbols = ["AAPL","MSFT","GOOG","AMZN"]
sides = ["BUY","SELL"]

try:
    while True:
        msg = {
            "trade_id": str(uuid.uuid4()),
            "symbol": random.choice(symbols),
            "side": random.choice(sides),
            "price": round(random.uniform(100, 300), 2),
            "quantity": round(random.uniform(1, 100), 2),
            "trade_ts": int(datetime.now(tz=timezone.utc).timestamp()*1000)
        }
        producer.send("trades_raw", msg) #publish messages to trades_raw Kafka Topic managed by the Kafka Brokers.
        producer.flush()
        time.sleep(0.25)
except KeyboardInterrupt:
    producer.close()
    print("Stopped trade producer.")
