import argparse
import json
import logging
import time
from pathlib import Path

import requests


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
    for k, v in entity.items():
        if isinstance(v, dict):
            if "type" in v and "value" in v:
                if v["type"] in ["Text", "Number", "DateTime"]:
                    vn = {"type": "Property", "value": v["value"]}
                    entity[k] = vn
                elif v["type"] == "geo:json":
                    vn = {"type": "GeoProperty", "value": v["value"]}
                    entity[k] = vn

    entity["@context"] = [
        "https://raw.githubusercontent.com/smart-data-models/data-models/master/context.jsonld",
        "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld",
    ]


def post_payloads(payloads, broker_url):
    url = broker_url + "/ngsi-ld/v1/entityOperations/upsert"
    headers = {"Content-Type": "application/ld+json"}
    r = requests.post(url, data=json.dumps(payloads), headers=headers)
    if r.status_code != 201:
        LOGGER.warning("request failed: %s", r.status_code)
    LOGGER.info("Pushed %s payloads", len(payloads))


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
        "--logs",
        dest="logs",
        type=str,
        required=False,
        default="./logs",
        help="Logs folder",
    )

    parser.add_argument(
        "--ngsiv2_url",
        dest="ngsiv2_url",
        type=str,
        required=True,
        help="Query URL for NGSIv2 server",
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
    Path(args.logs).mkdir(parents=True, exist_ok=True)
    LOGGER = logging.getLogger("v2_ngsild_adapter")
    LOGGER.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    fh = logging.FileHandler(args.logs + "/v2_ngsild_adapter.log")
    fh.setFormatter(formatter)
    fh.setLevel(logging.DEBUG)
    LOGGER.addHandler(fh)
    LOGGER.info("Logs Folder: %s", args.logs)
    token_url = "https://accounts.fiware.kielregion.addix.io/oauth2/password"
    payload = {
        "grant_type": "password",
        "client_id": args.client_id,
        "client_secret": args.client_secret,
        "username": args.username,
        "password": args.password,
    }

    access_token = get_token(token_url=token_url, payload=payload)
    sleep_time = 0
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
                access_token = get_token(token_url=token_url, payload=payload)
                # try to get new valid token
            elif r.status_code == 200:
                break
            else:
                LOGGER.warning("request failed: %s", r.status_code)
                sleep_time = args.intervall - (time.time() - t0)
                continue
        results = r.json()
        cache = {}
        for entity in results:
            if entity["id"] in cache:
                if entity["dateObserved"] == cache[entity["id"]]:
                    continue
            transform2ld(entity)
            cache[entity["id"]] = entity["dateObserved"]
        post_payloads(results, broker_url=args.url)

        LOGGER.info("Pushed new v2 data to NGSI-LD")

        t1 = time.time()
        delta = t1 - t0
        LOGGER.info("Processed data in %s seconds", delta)
        sleep_time = args.intervall - delta
        if sleep_time < 0:
            LOGGER.warning("Cannot process new data fast enough!")
            sleep_time = 0
        LOGGER.info("Sleeping time set to %s seconds", sleep_time)
