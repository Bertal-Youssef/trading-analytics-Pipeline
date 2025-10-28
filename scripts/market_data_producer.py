#!/usr/bin/env python3
import json, os, time, random
from datetime import datetime, timezone
from kafka import KafkaProducer

producer = KafkaProducer(bootstrap_servers=os.getenv("KAFKA_BOOTSTRAP","kafka:9092"),
                         value_serializer=lambda v: json.dumps(v).encode())

symbols = ["AAPL","MSFT","GOOG","AMZN"]
try:
    while True:
        for s in symbols:
            bid = round(random.uniform(100, 300), 2)
            ask = round(bid + random.uniform(0.01, 0.5), 2)
            msg = {"symbol": s, "bid": bid, "ask": ask, "event_ts": int(datetime.now(tz=timezone.utc).timestamp()*1000)}
            producer.send("market_data_raw", msg)
        producer.flush()
        time.sleep(1.0)
except KeyboardInterrupt:
    producer.close()
    print("Stopped market producer.")