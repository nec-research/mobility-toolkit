import argparse
import json
import logging
import sys
import time
from pathlib import Path

import requests
from mtk_common.utils import post_payloads


def get_token(token_url, payload):
    while True:
        r = requests.post(token_url, data=payload)
        if r.status_code == 200:
            break
        else:
            LOGGER.error(
                "Cannot retrieve token, keep trying: %s %s", (r.status_code, r.text)
            )
        for _ in range(60):
            time.sleep(1)
    LOGGER.info("Succesfully retrieved new token")
    access_token = r.json()["access_token"]
    return access_token


def transform2ld(entity):
    observedAt = entity["dateObserved"]["value"]
    for k, v in entity.items():
        if isinstance(v, dict):
            if "type" in v and "value" in v:
                if v["type"] in ["Text", "Number", "DateTime", "StructuredValue"]:
                    vn = {
                        "type": "Property",
                        "value": v["value"],
                        "observedAt": observedAt,
                    }
                    entity[k] = vn
                elif v["type"] == "geo:json":
                    vn = {
                        "type": "GeoProperty",
                        "value": v["value"],
                        "observedAt": observedAt,
                    }
                    entity[k] = vn

    entity["@context"] = [
        "https://raw.githubusercontent.com/smart-data-models/data-models/master/context.jsonld",
        "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld",
    ]
    if "metadata" in entity:
        del entity["metadata"]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start NGSIv2 NGSI-LD Adapter")
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
        "--logging_folder",
        dest="logging_folder",
        type=str,
        required=False,
        default="",
        help="Logging folder",
    )
    parser.add_argument(
        "--logging_level",
        dest="logging_level",
        type=int,
        required=False,
        default=20,
        help="Logging level: 10|20|30|40|50",
    )

    parser.add_argument(
        "--ngsiv2_url",
        dest="ngsiv2_url",
        type=str,
        required=True,
        help="Query URL for NGSIv2 server",
    )

    parser.add_argument(
        "--oauth2_token_url",
        dest="oauth2_token_url",
        type=str,
        required=True,
        help="Query URL for oauth2 token server",
    )

    parser.add_argument(
        "--client_id",
        dest="client_id",
        type=str,
        required=True,
        help="Client ID of NGSIv2 subscription",
    )

    parser.add_argument(
        "--client_secret",
        dest="client_secret",
        type=str,
        required=True,
        help="Client Secret of NGSIv2 subscription",
    )
    parser.add_argument(
        "--username",
        dest="username",
        type=str,
        required=True,
        help="Username of NGSIv2 subscription",
    )
    parser.add_argument(
        "--password",
        dest="password",
        type=str,
        required=True,
        help="Password of NGSIv2 subscription",
    )
    args = parser.parse_args()

    LOGGER = logging.getLogger("ngsiv2_adapter")
    LOGGER.setLevel(args.logging_level)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    if args.logging_folder:
        Path(args.logging_folder).mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(args.logging_folder + "/ngsiv2_adapter.log")
        fh.setFormatter(formatter)
        fh.setLevel(args.logging_level)
        LOGGER.addHandler(fh)
        LOGGER.info("Logging Folder: %s", args.logging_folder)
    else:
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(formatter)
        sh.setLevel(args.logging_level)
        LOGGER.addHandler(sh)

    LOGGER.info("NGSIv2 Adapter starting...")

    payload = {
        "grant_type": "password",
        "client_id": args.client_id,
        "client_secret": args.client_secret,
        "username": args.username,
        "password": args.password,
    }

    access_token = get_token(token_url=args.oauth2_token_url, payload=payload)
    sleep_time = 0
    cache = {}
    while True:
        for i in range(int(sleep_time)):
            time.sleep(1)
        t0 = time.time()
        while True:
            headers = {
                "Authorization": "Bearer " + access_token,
                "fiware-service": "infoportal",
                "fiware-servicepath": "/",
            }
            r = requests.get(args.ngsiv2_url, headers=headers)
            if r.status_code == 403:
                LOGGER.info(
                    "Token not valid/expired, getting new token...: %s", args.logs
                )
                access_token = get_token(
                    token_url=args.oauth2_token_url, payload=payload
                )
                # try to get new valid token
            elif r.status_code == 200:
                break
            else:
                LOGGER.warning("request failed: %s", r.status_code)
                sleep_time = args.intervall - (time.time() - t0)
                continue
        results = r.json()
        payloads = []
        for entity in results:
            if entity["id"] in cache:
                # skip entities which have not been changed
                if entity["dateObserved"]["value"] == cache[entity["id"]]:
                    continue
            LOGGER.debug("NGSIv2 entity %s", entity)
            transform2ld(entity)
            LOGGER.debug("NGSI-LD entity %s", entity)
            payloads.append(entity)
            cache[entity["id"]] = entity["dateObserved"]["value"]
        if payloads:
            post_payloads(payloads=payloads, broker_url=args.url, logger=LOGGER)
            LOGGER.info("Pushed new v2 data to NGSI-LD")
        else:
            LOGGER.warning("No new data available, maybe increase polling intervall")

        t1 = time.time()
        delta = t1 - t0
        LOGGER.info("Processed data in %s seconds", delta)
        sleep_time = args.intervall - delta
        if sleep_time < 0:
            LOGGER.warning("Cannot process new data fast enough!")
            sleep_time = 0
        LOGGER.info("Sleeping time set to %s seconds", sleep_time)
