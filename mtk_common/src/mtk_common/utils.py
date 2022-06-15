import datetime
import enum
import json
import logging
from datetime import timezone

import requests


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


def get_transport_mode(e: dict, logger: logging.Logger) -> str:
    """
    Get transport mode aka vehicleType from NGSI-LD object (either Vehicle or
    TrafficFlowObserved type)
    """

    logger.debug("get_transport_mode")
    if "https://uri.fiware.org/ns/data-models#vehicleType" in e:
        return e["https://uri.fiware.org/ns/data-models#vehicleType"]["value"]
    elif (
        "https://smart-data-models.github.io/data-models/terms.jsonld#/definitions/vehicleType"
        in e
    ):
        return e[
            "https://smart-data-models.github.io/data-models/terms.jsonld#/definitions/vehicleType"
        ]["value"]
    elif "vehicleType" in e:
        return e["vehicleType"]["value"]
    elif e["type"] == "TrafficFlowObserved":
        # e["type"] == "https://uri.fiware.org/ns/data-models#TrafficFlowObserved"
        logger.info("No VehicleType available, trying to infer for TrafficFlowObserved")
        for v in ["Velo", "ECO Counter"]:
            if v.lower() in e["description"]["value"].lower():
                return "bicycle"
            if v.lower() in e["source"]["value"].lower():
                return "bicycle"
        logger.info("Failed to infer VehicleType for %s", e)
    return ""


def translate_transport_mode(value: str) -> str:
    """
    Translate transport modes to cononical form.
    """

    translation_dic = {
        "bicycle": "bicycle",
        "bike": "bicycle",
        "bicycling": "bicycle",
        PredictedModeTypes.BICYCLING: "bicycle",
        "bus": "bus",
        "minibus": "bus",
        PredictedModeTypes.BUS: "bus",
        "qbus": "bus",
        "car": "car",
        "auto": "car",
        "qkfz": "car",
        "qpkw": "car",
        PredictedModeTypes.CAR: "car",
        "large_car": "large_car",
        "caravan": "large_car",
        "van": "large_car",
        "qlfw": "large_car",
        "motorcycle": "motorcycle",
        "motorbike": "motorcycle",
        "qkrad": "motorcycle",
        "moped": "motorcycle",
        "motorcycleWithSideCar": "motorcycle",
        "motorscooter": "motorcycle",
        "tram": "tram",
        PredictedModeTypes.TRAM: "tram",
        "carwithcaravan": "large_car",
        "carwithtrailer": "large_car",
        "qpkwa": "large_car",
        "truck": "truck",
        "lorry": "truck",
        "trailer": "truck",
        "qlkw": "truck",
        "qlkwa": "truck",
        "qsattel-kfz": "truck",
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
        "air_or_hsr": "airplane",
    }

    if hasattr(value, "lower"):
        value = value.lower()
    if value in translation_dic:
        return translation_dic[value]
    else:
        return "unknown"


def compute_carbon_footprint(mode: str, distance: float) -> float:
    from transport_co2 import estimate_co2

    co2_mode_mapping = {
        "unknown": "walk",
        "walk": "walk",
        "bicycle": "bicycle",
        "bus": "bus",
        "car": "car",
        "large_car": "large_car",
        "airplane": "airplane",
        "light_rail": "light_rail",
        "train": "rail",
        "subway": "subway",
        "truck": "large_car",
        "tram": "tram",
        "motorcycle": "scooter",
    }

    mode = translate_transport_mode(mode)
    mode_co2 = co2_mode_mapping[mode]
    co2 = estimate_co2(mode=mode_co2, distance_in_km=distance / 1000)

    return co2 / 1000


def get_request(
    url: str, params: dict, headers: dict, logger: logging.Logger, timeout=60
) -> list:
    try:
        r = requests.get(
            url,
            params=params,
            headers=headers,
            timeout=timeout,
        )
        logger.debug("Query URL %s", r.url)
        if r.status_code != 200:
            logger.warning(
                "Request to NGSI-LD Broker failed, status code: %s", r.status_code
            )
            return []
        entities = r.json()
        logger.debug("Received entities%s", len(entities))
        return entities
    except requests.exceptions.RequestException as e:
        logger.error(
            "Something went wrong connecting to the NGSI-LD broker. Maybe server is down."
        )
        return []

def chunks(lst, n):
    """Yield successive n-sized chunks from lst."""
    for i in range(0, len(lst), n):
        yield lst[i:i + n]

def post_payloads(payloads: list, broker_url: str, logger: logging.Logger) -> bool:
    success = True
    for chunk in chunks(payloads, 100):
        try:
            url = broker_url + "/ngsi-ld/v1/entityOperations/upsert"
            print(url)
            headers = {"Content-Type": "application/ld+json"}
            r = requests.post(url, data=json.dumps(chunk), headers=headers)
            if r.status_code not in [201, 204, 207]:
                logger.warning("request failed: %s", r.status_code)
                logger.warning(r.text)
                success = False
            else:
                logger.info("Pushed %s payloads", len(payloads))
        except requests.exceptions.RequestException as e:
            logger.error(
                "Something went wrong connecting to the NGSI-LD broker. Maybe server is down."
            )
            success = False
    return success


def get_temporal_entities(
    entity_type: str, intervall: int, url: str, logger: logging.Logger
) -> list:
    """
    get temporal entities during poll intervall
    """

    headers = {
        "Link": '<https://raw.githubusercontent.com/smart-data-models/data-models/master/context.jsonld>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'
    }
    params = {
        "type": entity_type,
        "timerel": "after",
        "timeAt": (
            datetime.datetime.now(timezone.utc) - datetime.timedelta(seconds=intervall)
        ).strftime("%Y-%m-%dT%H:%M:%SZ"),
    }
    entities_temporal = get_request(
        url=url + "/ngsi-ld/v1/temporal/entities/",
        params=params,
        headers=headers,
        logger=logger,
    )
    return entities_temporal


def get_entities(
    entity_type: str,
    observedAt_property: str,
    intervall: int,
    url: str,
    logger: logging.Logger,
) -> list:
    """
    get entities during poll intervall
    """
    headers = {
        "Link": '<https://raw.githubusercontent.com/smart-data-models/data-models/master/context.jsonld>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'
    }
    params = {
        "type": entity_type,
        "q": (
            observedAt_property
            + ".observedAt>="
            + (
                datetime.datetime.now(timezone.utc)
                - datetime.timedelta(seconds=intervall)
            ).strftime("%Y-%m-%dT%H:%M:%SZ")
        ),
    }
    entities = get_request(
        url=url + "/ngsi-ld/v1/entities/",
        params=params,
        headers=headers,
        logger=logger,
    )

    return entities


# add context to fiware datamodel
def delete_entities(entity_type: str) -> tuple:
    url = "http://cema.nlehd.de:2042/ngsi-ld/v1/entities/"
    params = {}
    params["type"] = entity_type
    params["limit"] = 500
    headers = {"Content-Type": "application/ld+json"}
    r = requests.get(url, params=params, headers=headers)
    if r.status_code != 200:
        print("failed")
        print(r)
        return 0, 0
    entities = r.json()
    for e in entities:
        entity_id = e["id"]
        r = requests.delete(
            "http://cema.nlehd.de:2042/ngsi-ld/v1/entities/" + entity_id,
            headers={"Content-Type": "application/json"},
        )  # without ld if context in header
        # &limit=500
        if r.status_code != 204:
            print("failed")
            print(r)
    print("deleted entities:", len(entities))

    url = "http://cema.nlehd.de:2042/ngsi-ld/v1/temporal/entities/"
    params = {"type": entity_type, "timerel": "after"}
    params["time"] = (datetime.datetime.now() - datetime.timedelta(weeks=52)).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )
    r = requests.get(url, params=params, headers=None)
    if r.status_code != 200:
        print("failed")
        print(r)
        return 0, 0
    entities_temp = r.json()
    for e in entities_temp:
        entity_id = e["id"]
        r = requests.delete(
            "http://cema.nlehd.de:2042/ngsi-ld/v1/temporal/entities/" + entity_id,
            headers={"Content-Type": "application/ld+json"},
        )
        if r.status_code != 204:
            print("failed")
            print(r)
    print("deleted temporal entities:", len(entities_temp))
    return len(entities), len(entities_temp)


def delete_all_entities(entity_type: str) -> None:
    entities_len, entities_temp_len = 1, 1
    while entities_len + entities_temp_len > 0:
        entities_len, entities_temp_len = delete_entities(entity_type)
