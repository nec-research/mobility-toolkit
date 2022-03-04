from requests_pkcs12 import get, post
from math import asin, cos, radians, sin, sqrt

import numpy as np

from pathlib import Path

import copy
from model import (
    ngsi_template_emissionobserved,
    ngsi_template_vehicle,
    traffic_sensor_locations,
    transport_modes_model,
    ngsi_template_trafficflow_observed,
)
from utils import compute_carbon_footprint, translate_transport_mode
import uuid
import datetime


import argparse
import enum
import json
import logging
import time
import uuid
import xmltodict
import json
import base64

import requests


ENTITY_TYPE = "TrafficFlowObserved"
# ENTITY_TYPE = "Vehicle"


def pandas_compute_carbon_footprint(x):
    return compute_carbon_footprint(x.sensed_mode, x.distance)


def create_payloads(parsed, logger):
    payloads = []
    for v in parsed:
        observedAt = v["time"] + "00"
        measurements = v["atg_tlsLveErgebnisMeldungVersion0Bis4"]
        for qmode in [
            "qBus",
            "qKfz",
            "qKrad",
            "qLkfw",
            "qLkwA",
            "qLkwvLkwdA",
            "qSattel-Kfz",
            "qPkw",
            "qPkwA",
        ]:
            if qmode in measurements:
                if measurements[qmode][0] != "0":
                    skey = qmode
                    skey = "v" + skey[1:]
                    speed = measurements[skey].split()[0]
                    description = v["description"]
                    entity_id = v["_id"]
                    coordinates = [v["longitude"], v["latitude"]]
                    vehicleType = qmode
                    try:
                        intensity = int(measurements[qmode].split()[0])
                    except:
                        logger.warning(
                            "could not convert to number: %s", measurements[qmode]
                        )
                        continue
                    laneDirection = v["direction1"]
                    streetAddress = v["Strasse"]
                    try:
                        occupancy = float(measurements["b"].split()[0]) / 100
                    except:
                        logger.warning(
                            "could not convert to number: %s", measurements[qmode]
                        )
                        continue
                    record_intervall = (
                        int(measurements["T"].split()[0]) * 60
                    )  # Note assuming we have minutes here
                    if ENTITY_TYPE == "TrafficFlowObserved":
                        payload = createTrafficFlowObserved(
                            description,
                            observedAt,
                            record_intervall,
                            coordinates,
                            speed,
                            vehicleType,
                            intensity,
                            laneDirection,
                            occupancy,
                            streetAddress=streetAddress,
                            entity_id="urn:ngsi-ld:TrafficFlowObserved:" + entity_id,
                        )
                        payloads.append(payload)
                    elif ENTITY_TYPE == "Vehicle":
                        for i in range(intensity):
                            payload = createVehicle(
                                description,
                                observedAt,
                                coordinates,
                                speed,
                                vehicleType,
                            )
                            payloads.append(payload)
    logger.info("Created %s payloads", len(payloads))
    return payloads


def post_payloads(payloads, broker_url, logger):
    url = broker_url + "/ngsi-ld/v1/entityOperations/upsert"
    print(url)
    headers = {"Content-Type": "application/ld+json"}
    r = requests.post(url, data=json.dumps(payloads), headers=headers)
    if r.status_code != 201:
        logger.warning("request failed: %s", r.status_code)
    logger.info("Pushed %s payloads", len(payloads))


def createTrafficFlowObserved(
    description,
    observedAt,
    record_intervall,
    coordinates,
    speed,
    vehicleType,
    intensity,
    laneDirection,
    occupancy,
    streetAddress="Unknown",
    entity_id=None,
):
    payload = copy.deepcopy(ngsi_template_trafficflow_observed)
    payload["description"]["value"] = description

    payload["dateObservedFrom"]["value"] = (
        datetime.datetime.fromisoformat(observedAt)
        - datetime.timedelta(seconds=record_intervall)
    ).strftime("%Y-%m-%dT%H:%M:%SZ")

    observedAt = datetime.datetime.fromisoformat(observedAt).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )

    payload["dateObserved"]["value"] = observedAt
    payload["dateObservedTo"]["value"] = observedAt

    payload["address"]["value"]["streetAddress"] = streetAddress

    payload["location"]["value"]["coordinates"] = coordinates
    payload["location"]["observedAt"] = observedAt

    payload["vehicleType"]["observedAt"] = observedAt
    payload["vehicleType"]["value"] = translate_transport_mode(vehicleType)

    payload["intensity"]["observedAt"] = observedAt
    payload["intensity"]["value"] = intensity

    payload["averageVehicleSpeed"]["observedAt"] = observedAt
    payload["averageVehicleSpeed"]["value"] = speed

    payload["laneDirection"]["observedAt"] = observedAt
    payload["laneDirection"]["value"] = laneDirection

    payload["occupancy"]["observedAt"] = observedAt
    payload["occupancy"]["value"] = occupancy

    if entity_id:
        payload["id"] = entity_id
    else:
        payload["id"] = "urn:ngsi-ld:TrafficFlowObserved:" + str(uuid.uuid4())
    payload["vehicleType"]["value"] = translate_transport_mode(vehicleType)
    return payload


def createVehicle(
    description, observedAt, coordinates, speed, vehicleType, entity_id=None
):
    payload = copy.deepcopy(ngsi_template_vehicle)

    observedAt = datetime.datetime.fromisoformat(observedAt).strftime(
        "%Y-%m-%dT%H:%M:%SZ"
    )

    payload["description"]["value"] = description
    payload["location"]["value"]["coordinates"] = coordinates
    payload["location"]["observedAt"] = observedAt
    payload["speed"]["observedAt"] = observedAt
    payload["heading"]["observedAt"] = observedAt
    if entity_id:
        payload["id"] = entity_id
    else:
        payload["id"] = "urn:ngsi-ld:Vehicle:" + str(uuid.uuid4())
    payload["speed"]["value"] = speed
    payload["vehicleType"]["value"] = translate_transport_mode(vehicleType)
    return payload


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start MDM NGSI-LD Adapter")
    parser.add_argument(
        "--url",
        dest="url",
        type=str,
        required=False,
        default="http://localhost:9090/",
        help="Url for NGSI-LD Context Broker",
    )
    parser.add_argument(
        "--intervall",
        dest="intervall",
        type=int,
        required=False,
        default=60,
        help="Poll intervall in seconds",
    )

    parser.add_argument(
        "--logs",
        dest="logs",
        type=str,
        required=False,
        default="./logs",
        help="Logs folder",
    )

    parser.add_argument(
        "--subscription_id",
        dest="sub_id",
        type=str,
        required=True,
        help="ID of MDM subscription",
    )

    parser.add_argument(
        "--p12_cert",
        dest="p12_cert",
        type=str,
        required=True,
        help="P12 certificate for MDM",
    )

    parser.add_argument(
        "--p12_pass", dest="p12_pass", type=str, required=True, help="Password for MDM",
    )

    args = parser.parse_args()
    Path(args.logs).mkdir(parents=True, exist_ok=True)
    LOGGER = logging.getLogger("mdm_ngsild_adapter")
    LOGGER.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    fh = logging.FileHandler(args.logs + "/mdm_ngsild_adapter.log")
    fh.setFormatter(formatter)
    fh.setLevel(logging.DEBUG)
    LOGGER.addHandler(fh)
    LOGGER.info("Logs Folder: %s", args.logs)
    url = (
        "https://broker.mdm-portal.de/BASt-MDM-Interface/srv/container/v1.0?subscriptionID="
        + args.sub_id
    )
    pkcs12_filename = args.p12_cert
    pkcs12_password = args.p12_pass
    prev_resp = ""
    sleep_time = 0
    while True:
        for i in range(int(sleep_time)):
            time.sleep(1)
        t0 = time.time()
        response = get(
            url,
            headers={"Content-Type": "application/json"},
            verify=False,
            pkcs12_filename=pkcs12_filename,
            pkcs12_password=pkcs12_password,
        )
        if response.status_code != 200:
            LOGGER.warning("request failed: %s", r.status_code)
            sleep_time = args.intervall - (time.time() - t0)
            continue
        dict_data = xmltodict.parse(response.content)
        base64_string = dict_data["container"]["body"]["binary"]["#text"]
        if base64_string == prev_resp:
            LOGGER.info("No new MDM data found")
            sleep_time = args.intervall - (time.time() - t0)
        prev_resp = base64_string
        b = base64.b64decode(base64_string)
        # Decode the bytes into str.
        s = b.decode("utf-8")
        s = s.replace("\n", ",")  # fix wrong json encoding in MDM
        s = "[" + s[:-1] + "]"
        parsed = json.loads(s)
        payloads = create_payloads(parsed, logger=LOGGER)
        post_payloads(payloads, broker_url=args.url, logger=LOGGER)
        LOGGER.info("Pushed new MDM data to NGSI-LD")

        t1 = time.time()
        delta = t1 - t0
        LOGGER.info("Processed data in %s seconds", delta)
        sleep_time = args.intervall - delta
        if sleep_time < 0:
            LOGGER.warning("Cannot process new data fast enough!")
            sleep_time = 0
        LOGGER.info("Sleeping time set to %s seconds", sleep_time)
