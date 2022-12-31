import time
import json
from time import sleep
from kafka import KafkaProducer
from kafka.errors import KafkaError
from datetime import datetime as dt
from data import get_weaher_detail



kafka_bootstrap_servers = ['localhost:9092']
topic = "odata_ve_paris"
def json_serialiser(data) :
    return json.dumps(data).encode("utf-8")
# def get_partition(key,all,available):
#     return 0

producer = KafkaProducer(bootstrap_servers= kafka_bootstrap_servers,
                         value_serializer=json_serialiser,
                         #partitioner=get_partition,
                         api_version= (0,10,1)
                         )
l = ["start"]
def sent_msg() :


    if get_weaher_detail()["id_pdc"] != l[-1] :
        producer.send(topic,get_weaher_detail())
        l.append(get_weaher_detail()["id_pdc"])

        print("sent")
        print(l)
        print(get_weaher_detail())

    else :
        print("not sent")
        print(l)


if __name__ == "__main__" :
    while True :
        #producer.send(topic,get_weaher_detail())
        sent_msg()
        time.sleep(1)