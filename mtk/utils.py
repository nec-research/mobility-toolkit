from transport_co2 import estimate_co2
import enum
import requests
import datetime

class PredictedModeTypes(enum.IntEnum):
    UNKNOWN = 0
    WALKING = 1
    BICYCLING = 2
    BUS = 3
    TRAIN = 4
    CAR = 5
    AIR_OR_HSR = 6
    SUBWAY = 7
    TRAM = 8
    LIGHT_RAIL = 9


def translate_transport_mode(value):
    
    translation_dic = {
        "bicycle": "bicycle",
        "bike": "bicycle",
        "bicycling": "bicycle",
        PredictedModeTypes.BICYCLING: "bicycle",
        "bus": "bus",
        "minibus": "bus",
        PredictedModeTypes.BUS: "bus",
        "car": "car",
        "auto": "car",
        PredictedModeTypes.CAR: "car",
        "large_car": "large_car",
        "caravan": "large_car",
        "van": "large_car",
        "motorcycle": "motorcycle",
        "motorbike": "motorcycle",
        "moped":  "motorcycle",
        "motorcycleWithSideCar":  "motorcycle",
        "motorscooter":  "motorcycle",
        "tram": "tram",
        PredictedModeTypes.TRAM: "tram",
        "carWithCaravan": "large_car",
        "carWithTrailer": "large_car",
        "truck": "truck",
        "lorry": "truck",
        "trailer": "truck",
        "walk": "walk",
        "walking": "walk",
        PredictedModeTypes.WALKING: "walk",
        "subway": "subway",
        PredictedModeTypes.SUBWAY: "subway",
        "train": "train",
        PredictedModeTypes.TRAIN: "train",
        "light rail": "light_rail",
        "light_rail": "light_rail",
        PredictedModeTypes.LIGHT_RAIL: "light_rail",
        "unknown": "unknown",
        PredictedModeTypes.UNKNOWN: "unknown",
        "airplane": "airplane",
        "air": "airplane",
        PredictedModeTypes.AIR_OR_HSR: "airplane",
        "air_or_hsr": "airplane"
    }
    
    if hasattr(value, 'lower'):
        value = value.lower()
    if value in translation_dic:
        return translation_dic[value]
    else:
        return "unknown"

def compute_carbon_footprint(mode, distance):
    co2_mode_mapping = {
    "unknown": "walk",
    "walk": "walk",
    "bicycle": "bicycle",
    "bus": "bus",
    "car": "car",
    "large_car": "large_car",
    "airplane": "airplane",
    "light_rail":  "light_rail",
    "train":  "rail",
    "subway": "subway",
    "truck": "large_car",
    "tram" : "tram",
    "motorcycle": "scooter"      
    }
    
    mode = translate_transport_mode(mode)
    mode_co2 = co2_mode_mapping[mode]
    co2 = estimate_co2(mode=mode_co2, distance_in_km=distance/1000)

    return co2/1000 



# add context to fiware datamodel
def delete_entities(entity_type):
    url = "http://cema.nlehd.de:2042/ngsi-ld/v1/entities/"
    params = {}
    params['type'] = entity_type
    params["limit"] = 500
    headers={'Content-Type': 'application/ld+json'} 
    r = requests.get(url, params=params, headers=headers)
    if r.status_code != 200:
            print("failed")
            print(r)
            return
    entities = r.json()
    for e in entities:
        entity_id = e["id"]
        r = requests.delete("http://cema.nlehd.de:2042/ngsi-ld/v1/entities/"+entity_id, headers={'Content-Type': 'application/json'}) # without ld if context in header
        # &limit=500
        if r.status_code != 204:
            print("failed")
            print(r)
    print("deleted entities:", len(entities))
    
    url = "http://cema.nlehd.de:2042/ngsi-ld/v1/temporal/entities/"
    params = {'type': entity_type, 'timerel': 'after'}
    params['time'] = (datetime.datetime.now() - datetime.timedelta(weeks=52)).strftime('%Y-%m-%dT%H:%M:%SZ')
    r = requests.get(url, params=params, headers=None)
    if r.status_code != 200:
            print("failed")
            print(r)
            return
    entities_temp = r.json()
    for e in entities_temp:
        entity_id = e["id"]
        r = requests.delete("http://cema.nlehd.de:2042/ngsi-ld/v1/temporal/entities/"+entity_id, headers={'Content-Type': 'application/ld+json'})
        if r.status_code != 204:
            print("failed")
            print(r)
    print("deleted temporal entities:", len(entities_temp))
    return len(entities), len(entities_temp)


def delete_all_entities(entity_type):
    entities_len, entities_temp_len = 1, 1
    while entities_len+entities_temp_len>0:
        entities_len, entities_temp_len = delete_entities(entity_type)