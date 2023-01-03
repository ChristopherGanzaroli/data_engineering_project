from kafka import KafkaConsumer
from kafka.errors import KafkaError
import json
import sqlite3
topic = "odata_ve_paris"
kafka_bootstrap_servers = ['localhost:9092']
from database.sqlite import SQL_con





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
                val = (data['time'], data['adresse_station'], data['arrondissement'], data['status'], data['cp'], data['lat'], data['long'], data['id_pdc'])
                SQL_con(val)

                #sql = "INSERT INTO paris_station_act (time, adress, district, status, post_code, lat_long, lat, long, id_pdc) VALUES (?,?,?,?,?,?,?,?)"
                # con = sqlite3.connect("database/paris_ve.db",timeout=1)
                # cur = con.cursor()
                # cur.execute("INSERT INTO paris_station_act (time, adress, district, status, post_code, lat, long, id_pdc) VALUES (?,?,?,?,?,?,?,?)",val)#.fetchall()
                # con.commit()
                # cur.execute(("SELECT * FROM paris_station_act"))
                # res = cur.fetchall()
                # print(res)


                #con.close()


