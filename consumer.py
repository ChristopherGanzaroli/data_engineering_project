from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json


topic = "odata_ve_paris"
kafka_bootstrap_servers = ['localhost:9092']
if __name__ == "__main__" :

    consumer = KafkaConsumer(
        topic,
        bootstrap_servers = kafka_bootstrap_servers,
        auto_offset_reset = 'earliest',
        group_id = "consumer-group-a"
    )

    print("Starting the consumer")
    for msg in consumer:
        print(json.loads(msg.value))