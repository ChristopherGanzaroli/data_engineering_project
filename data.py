import requests
import json
from datetime import datetime

url = "https://opendata.paris.fr/api/records/1.0/search/?dataset=belib-points-de-recharge-pour-vehicules-electriques-disponibilite-temps-reel&q=&rows=999&facet=statut_pdc&facet=last_updated&facet=arrondissement"
odata_paris_api_endpoint = url

def get_weaher_detail() :
    api_response = requests.get(odata_paris_api_endpoint)
    json_data = api_response.json()
    while True :
        for elt in json_data["records"]:
            adress = elt["fields"]["adresse_station"]
            #print(adress)
            arrondissement = elt['fields']['arrondissement']
            # print(arrondissement)
            statut = elt['fields']['statut_pdc']
            # print(statut)
            cp = elt['fields']['code_insee_commune']
            # print(cp)
            #lat_long = elt['fields']['coordonneesxy']
            # print(lat_long)
            lat = elt['fields']['coordonneesxy'][0]
            # print(lat)
            long = elt['fields']['coordonneesxy'][1]
            # print(long)
            id_pdc = elt['fields']['id_pdc']
            # print(id_pdc)

            now = datetime.now()



        return {
                'time' : now.strftime("%Y-%m-%d %H:%M:%S"),
                'adresse_station' : adress,
                'arrondissement' :arrondissement,
                "status" : statut,
                "cp" :cp,
                #"lat_long" : ''.join(map(str,lat_long)),
                "lat": lat,
                "long":long,
                "id_pdc" : id_pdc
                }

if __name__ == "__main__":
    print(get_weaher_detail())
