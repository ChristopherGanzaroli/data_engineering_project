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
l_id_pdc = ['start']
l_status = ['start']

def send_msg() :

        if (get_weaher_detail()["id_pdc"] not in l_id_pdc) and (get_weaher_detail()["status"] not in l_status) :
            producer.send(topic,get_weaher_detail())
            l_id_pdc.append(get_weaher_detail()["id_pdc"])
            l_status.append(get_weaher_detail()["status"])
            print("msg sent")
            print(get_weaher_detail())


        #elif (len(l_id_pdc) > 2) and (len(l_status) > 2) :
            l_id_pdc.remove(l_id_pdc[0])
            l_status.remove(l_status[0])

        else :
            print("msg not send")
            print(l_id_pdc,l_status)
def send_msg() :

    if get_weaher_detail()["id_pdc"] != l_id_pdc[-1] :
        producer.send(topic,get_weaher_detail())
        l_id_pdc.append(get_weaher_detail()["id_pdc"])
        print("msg sent")
        print(get_weaher_detail())
        l_id_pdc.remove(l_id_pdc[0])

    else :

        print("msg not send")





if __name__ == "__main__" :
    while True :
        send_msg()
        time.sleep(1)