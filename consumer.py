from kafka import KafkaConsumer
from kafka.errors import KafkaError

import json

consumer = KafkaConsumer(
    'test_topic',
    bootstrap_servers='localhost:9092',
    auto_offset_reset='earliest',
    enable_auto_commit=True,
    group_id='my-group',
    value_deserializer=lambda x: json.loads(x.decode('utf-8')) if x else None
)


try:
    for message in consumer:
        print(f"Reçu - Topic: {message.topic}, Partition: {message.partition}, Offset: {message.offset}, Value: {message.value}")
except KafkaError as e:
    print(f"Erreur lors de la consommation des messages: {e}")
except json.JSONDecodeError as e:
    print(f"Erreur de désérialisation JSON: {e}")
finally:
    consumer.close()