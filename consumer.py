from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json
import sqlite3
topic = "odata_ve_paris"
kafka_bootstrap_servers = ['localhost:9092']

#con = sqlite3.connect("database/paris_ve.db",timeout=1)

#con.close()



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
                #print(json.loads(msg.value))
                data = json.loads(msg.value)
                print(data)

            con = sqlite3.connect("database/paris_ve.db",timeout=10)
            cur = con.cursor()
                #sql = "INSERT INTO paris_station_act (time, adress, district, status, post_code, lat_long, lat, long, id_pdc) VALUES (?,?,?,?,?,?,?,?)"
            val = (data['time'], data['adresse_station'], data['arrondissement'], data['status'], data['cp'], data['lat'], data['long'], data['id_pdc'])
            cur.execute("INSERT INTO paris_station_act (time, adress, district, status, post_code, lat, long, id_pdc) VALUES (?,?,?,?,?,?,?,?)",val).fetchall()

            con.commit()
            con.close()


