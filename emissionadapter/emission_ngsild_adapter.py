import argparse
import copy
import enum
import json
import logging
import time
import uuid
from datetime import datetime
from pathlib import Path
from uuid import UUID

import arrow
#import emission.core.get_database as edb
#import emission.storage.timeseries.abstract_timeseries as esta
#import emission.storage.timeseries.timequery as estt
import pandas as pd
import requests
from transport_co2 import estimate_co2

from model import ngsi_template_section_observed
from utils import compute_carbon_footprint, translate_transport_mode


def pandas_compute_carbon_footprint(x):
    return compute_carbon_footprint(x.sensed_mode, x.distance)


def create_payloads(is_df, logger):
    payloads = []
    for index, row in is_df.iterrows():
        rnd_distance = np.random.uniform(200, 400)
        payload = copy.deepcopy(ngsi_template_section_observed)
        observedAt = datetime.utcfromtimestamp(row["end_ts"]).strftime(
            "%Y-%m-%dT%H:%M:%SZ"
        )
        payload["id"] = "urn:" + str(row["cleaned_section"])
        payload["transportMode"]["value"] = translate_transport_mode(row["sensed_mode"])
        payload["transportMode"]["observedAt"] = observedAt
        payload["distance"]["value"] = row["distance"]
        payload["distance"]["observedAt"] = observedAt
        payload["duration"]["value"] = row["duration"]
        payload["duration"]["observedAt"] = observedAt
        payload["speed"]["value"] = row["speed"]
        payload["speed"]["observedAt"] = observedAt
        payload["co2"]["value"] = row["co2"]
        payload["co2"]["observedAt"] = observedAt
        payload["location"]["value"]["coordinates"] = [
            add_randomness(row["start_loc"]["coordinates"], rnd_distance),
            add_randomness(row["end_loc"]["coordinates"], rnd_distance),
        ]
        payloads.append(payload)
    logger.info("Created %s payloads", len(payloads))
    return payloads


def post_payloads(payloads, broker_url, logger):
    url = broker_url + "/ngsi-ld/v1/entities"
    headers = {"Content-Type": "application/ld+json"}
    for e in payloads:
        r = requests.post(url, data=json.dumps(e), headers=headers)
        if r.status_code != 201:
            logger.warning("request failed: %s", r.status_code)
    logger.info("Pushed %s payloads", len(payloads))


from math import asin, cos, radians, sin, sqrt

import numpy as np


def haversine(lon1, lat1, lon2, lat2):
    """
    Calculate the great circle distance between two points
    on the earth (specified in decimal degrees)
    """
    # convert decimal degrees to radians
    lon1, lat1, lon2, lat2 = map(radians, [lon1, lat1, lon2, lat2])

    # haversine formula
    dlon = lon2 - lon1
    dlat = lat2 - lat1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371  # Radius of earth in kilometers. Use 3956 for miles
    return c * r


def add_randomness(coordinates, distance):
    """
    Utility method for simulation of the points
    """
    x0 = coordinates[0]
    y0 = coordinates[1]
    r = distance / 111300
    u = np.random.uniform(0, 1)
    v = np.random.uniform(0, 1)
    w = r * np.sqrt(u)
    t = 2 * np.pi * v
    x = w * np.cos(t)
    x1 = x / np.cos(y0)
    y = w * np.sin(t)
    return [x0 + x1, y0 + y]


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start Emission NGSI-LD Adapter")
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
        default=3600,
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
        "--dburl",
        dest="dburl",
        type=str,
        required=False,
        default="db",
        help="Hostname of the database",
    )
    args = parser.parse_args()
    print(str(args))
    print(str(args.dburl))
    conf = {'timeseries': {'url': args.dburl, 'result_limit': 250000}}
    with open('./conf/storage/db.conf.sample', 'w') as f:
#      print(str(f.readLines()))
      json.dump(conf, f)
    import emission.core.get_database as edb
    import emission.storage.timeseries.abstract_timeseries as esta
    import emission.storage.timeseries.timequery as estt
    Path(args.logs).mkdir(parents=True, exist_ok=True)
    LOGGER = logging.getLogger("emission_ngsild_adapter")
    LOGGER.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    fh = logging.FileHandler(args.logs + "/emission_ngsild_adapter.log")
    fh.setFormatter(formatter)
    fh.setLevel(logging.DEBUG)
    LOGGER.addHandler(fh)
    LOGGER.info("Logs Folder: %s", args.logs)
    LOGGER.info("Waiting for emission server to come up...")
    for i in range(60):
        time.sleep(1)
    while True:
        t0 = time.time()
        all_users = list(
            edb.get_uuid_db().find({}, {"user_email": 1, "uuid": 1, "_id": 0})
        )
        new_data = False
        for user in all_users:
            ts = esta.TimeSeries.get_time_series(user["uuid"])
            start = arrow.utcnow().float_timestamp - (args.intervall)
            end = arrow.utcnow().float_timestamp
            tq = estt.TimeQuery("data.start_ts", start, end)
            is_df = ts.get_data_df("analysis/inferred_section", time_query=tq)
            if is_df.empty:
                continue
            new_data = True
            is_df["speed"] = is_df.distance / is_df.duration
            is_df["co2"] = is_df.apply(pandas_compute_carbon_footprint, axis=1)
            payloads = create_payloads(is_df, logger=LOGGER)
            post_payloads(payloads, broker_url=args.url, logger=LOGGER)
        if not new_data:
            LOGGER.info("No new emission data found")
        else:
            LOGGER.info("Pushed new emission data to NGSI-LD")
        t1 = time.time()
        delta = t1 - t0
        LOGGER.info("Processed data in %s seconds", delta)
        sleep_time = args.intervall - delta
        sleep_time = (
            sleep_time * 0.95
        )  # sleeping a bit less so we make sure to process all data
        if sleep_time < 0:
            LOGGER.warning("Cannot process new data fast enough!")
            sleep_time = 0
        LOGGER.info("Sleeping %s seconds", sleep_time)
        for i in range(int(sleep_time)):
            time.sleep(1)
