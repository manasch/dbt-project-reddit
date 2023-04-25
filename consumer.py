import json

from kafka import KafkaConsumer

consumer = KafkaConsumer("cricket_endpoint", bootstrap_servers=['localhost:9092'], value_deserializer=lambda x: json.loads(x.decode('utf8')))

for msg in consumer:
    print(json.dumps(msg, indent=2))
