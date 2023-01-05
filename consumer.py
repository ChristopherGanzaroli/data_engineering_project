from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json
topic = "odata_ve_paris"
kafka_bootstrap_servers = ['localhost:9092']
from database.sqlite import SQL_con, delete_duplicate_rows

if __name__ == "__main__" :
    while 1==1 :

            consumer = KafkaConsumer(
                        topic,
                        bootstrap_servers = kafka_bootstrap_servers,
                        auto_offset_reset = 'earliest',
                        group_id = "consumer-group-a"

                    )

            print("Starting the consumer")

            for msg in consumer:
                data = json.loads(msg.value)
                print(data)
                val = (data['time'],data['time_for_duplicate_rows'],data['adresse_station'], data['arrondissement'], data['status'], data['cp'], data['lat'], data['long'], data['id_pdc'])
                SQL_con(val)



