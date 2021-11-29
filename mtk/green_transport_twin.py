import argparse
import copy
import datetime
import enum
import json
import logging
import os
import random
import signal
import sys
import time
import uuid
from datetime import timezone
from pathlib import Path
from random import randrange, uniform

import geog
import numpy as np
import requests
import shapely.geometry

from model import (ngsi_template_emissionobserved, ngsi_template_vehicle,
                   traffic_sensor_locations, transport_modes_model)
from utils import compute_carbon_footprint, translate_transport_mode


class GreenTransportTwin(object):
    def __init__(self, broker_url, poll_intervall, simulate_mode, logger):
        self.broker_url = broker_url
        self.poll_intervall = poll_intervall
        self.simulate_mode = simulate_mode
        self.logger = logger
        self.logger.info("Green Transport Twin Created")
        self.logger.info("Broker URL: %s", self.broker_url)
        self.logger.info("Poll Intervall: %s", self.poll_intervall)
        self.logger.info("Simulate vehicle data: %s", self.simulate_mode)

        self.overpass_cache = {}

        self.running = False
        signal.signal(signal.SIGINT, self.stop)
        signal.signal(signal.SIGTERM, self.stop)

    def start(self):
        self.running = True
        while self.running:
            if self.simulate_mode:
                self.simulate_vehicle_data()
                sleep_time = uniform(1, 10)
            else:
                t0 = time.time()
                vehicles = self.get_vehicles()
                if len(vehicles) > 0:
                    self.estimate_emission(vehicles, days=7)
                t1 = time.time()
                delta = t1 - t0
                self.logger.info(
                    "Processed %s vehicles in %s seconds", len(vehicles), delta
                )
                sleep_time = args.intervall - delta
                sleep_time = (
                    sleep_time * 0.95
                )  # sleeping a bit less so we make sure to process all data
                if sleep_time < 0:
                    self.logger.warning("Cannot process new data fast enough!")
                    sleep_time = 0
                self.logger.info("Sleeping %s seconds", sleep_time)
            for i in range(int(sleep_time)):
                time.sleep(1)
                if not self.running:
                    break

    def stop(self, *args):
        self.logger.info("Shutting down...")
        self.running = False
        sys.exit()

    def get_vehicles(self):
        params = {
            "type": "https://uri.fiware.org/ns/data-models#Vehicle"
        }  # , 'timerel': 'after'}
        params["q"] = "speed.observedAt>=" + (
            datetime.datetime.now(timezone.utc)
            - datetime.timedelta(seconds=self.poll_intervall)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
        headers = {
            "Link": '<https://raw.githubusercontent.com/smart-data-models/data-models/master/context.jsonld>; rel="http://www.w3.org/ns/json-ld#context"; type="application/ld+json"'
        }
        try:
            r = requests.get(
                self.broker_url + "/ngsi-ld/v1/entities/",
                params=params,
                headers=headers,
                timeout=60,
            )
            if r.status_code != 200:
                self.logger.warning(
                    "Request to NGSI-LD Broker failed, status code: %s", r.status_code
                )
                return []
            data = r.json()
            return data
        except requests.exceptions.RequestException as e:
            self.logger.error(
                "Something went wrong connecting to the NGSI-LD broker. Maybe server is down."
            )
            return []

    def simulate_vehicle_data(self):
        for traffic_sensor in traffic_sensor_locations:
            if traffic_sensor["traffic"] > randrange(100):
                observedAt = datetime.datetime.now(timezone.utc).strftime(
                    "%Y-%m-%dT%H:%M:%SZ"
                )
                payload = copy.deepcopy(ngsi_template_vehicle)
                payload["description"]["value"] = traffic_sensor["id"]
                payload["location"]["value"]["coordinates"] = traffic_sensor["loc"]
                payload["location"]["observedAt"] = observedAt
                payload["speed"]["observedAt"] = observedAt
                payload["heading"]["observedAt"] = observedAt
                for transport_mode, threshold in traffic_sensor[
                    "transport_modes"
                ].items():
                    if threshold > randrange(100):
                        payload = copy.deepcopy(payload)
                        payload["id"] = "urn:ngsi-ld:Vehicle:" + str(uuid.uuid4())
                        s = transport_modes_model[
                            translate_transport_mode(transport_mode)
                        ]["speed"]
                        payload["speed"]["value"] = s[randrange(len(s))]
                        payload["vehicleType"]["value"] = translate_transport_mode(
                            transport_mode
                        )
                        headers = {"Content-Type": "application/ld+json"}
                        try:
                            r = requests.post(
                                self.broker_url + "/ngsi-ld/v1/entities",
                                data=json.dumps(payload),
                                headers=headers,
                            )
                        except requests.exceptions.RequestException as e:
                            self.logger.error(
                                "Something went wrong connecting to the NGSI-LD broker. Maybe server is down."
                            )
                            continue

                        if r.status_code != 201:
                            self.logger.warning(
                                "Request failed %s %s", r.status_code, r.text
                            )
                        else:
                            self.logger.debug("Created simulated vehicle")
                            # create directly emission here
                            self.estimate_emission(vehicles=[payload], days=7)

    def get_simulated_point(self, transport_mode, lon, lat, radius):
        query_file = open("./overpass_template")
        query_string = "".join(query_file.readlines())
        osm_urls = [
            "https://overpass-api.de/api/interpreter",
            "http://overpass.openstreetmap.fr/api/interpreter",
        ]
        bbox_string = "around:%s,%s,%s" % (radius, lat, lon)
        if bbox_string in self.overpass_cache:
            res_json = self.overpass_cache[bbox_string]
        else:
            overpass_public_transit_query_template = query_string
            overpass_query = overpass_public_transit_query_template.replace(
                "bbox", bbox_string
            )
            status_code = 404
            while status_code != 200:
                osm_url = random.choice(osm_urls)
                try:
                    response = requests.post(osm_url, data=overpass_query, timeout=60.0)
                    status_code = response.status_code
                    if status_code != 200:
                        time.sleep(30)
                except:
                    self.logger.error(
                        "Something went wrong connecting to the OSM server, trying again."
                    )
                    time.sleep(30)

            res_json = response.json()
            self.overpass_cache[bbox_string] = res_json
        ways = {}
        nodes = {}
        for e in res_json["elements"]:
            if e["type"] == "way":
                if e["tags"]["highway"] not in ways:
                    ways[e["tags"]["highway"]] = []
                ways[e["tags"]["highway"]].append(random.choice(e["nodes"]))
            if e["type"] == "node":
                nodes[e["id"]] = [e["lon"], e["lat"]]

        common_keys = set(ways) & set(transport_modes_model[transport_mode]["highways"])
        aggregated = {}
        if not common_keys:
            self.logger.warning("No common paths between OSM and transport mode found")
            return None
        for key in common_keys:
            length = len(ways[key])
            weight = transport_modes_model[transport_mode]["highways"][key]
            aggregated[key] = length + weight
        choice = random.choices(
            population=list(aggregated.keys()), weights=list(aggregated.values()), k=1
        )[0]
        return nodes[random.choice(ways[choice])]

    def estimate_emission(self, vehicles, days=7):
        for vehicle in vehicles:
            if "location" not in vehicle:
                continue
                # FIXME, bug in scorpio temporal query?
            emission_obs_id = vehicle["id"].split(":")[-1]
            emission_obs_id = "EmissionObserved:" + emission_obs_id

            try:
                r = requests.get(
                    self.broker_url + "/ngsi-ld/v1/entities/" + emission_obs_id,
                    headers={"Content-Type": "application/ld+json"},
                    timeout=60,
                )
                if r.status_code == 200:
                    self.logger.debug("Entity exists")
                    continue
            except requests.exceptions.RequestException as e:
                self.logger.error(
                    "Something went wrong connecting to the NGSI-LD broker. Maybe server is down."
                )
                continue

            if "https://uri.fiware.org/ns/data-models#vehicleType" in vehicle:
                vehicle_transport_mode = vehicle[
                    "https://uri.fiware.org/ns/data-models#vehicleType"
                ]["value"]
            elif (
                "https://smart-data-models.github.io/data-models/terms.jsonld#/definitions/vehicleType"
                in vehicle
            ):
                vehicle_transport_mode = vehicle[
                    "https://smart-data-models.github.io/data-models/terms.jsonld#/definitions/vehicleType"
                ]["value"]
            elif "vehicleType" in vehicle:
                vehicle_transport_mode = vehicle["vehicleType"]["value"]
            else:
                continue
            vehicle_transport_mode = translate_transport_mode(vehicle_transport_mode)
            payload = {"type": "SectionObserved", "timerel": "after"}
            payload["time"] = (
                datetime.datetime.now() - datetime.timedelta(days=days)
            ).strftime("%Y-%m-%dT%H:%M:%SZ")
            payload["georel"] = "near;maxDistance==2000"
            payload["geometry"] = "Point"
            coordinates = vehicle["location"]["value"]["coordinates"]
            payload["coordinates"] = str(coordinates).replace(" ", "")
            broker_temp_url = self.broker_url + "/ngsi-ld/v1/temporal/entities/"
            try:
                r = requests.get(broker_temp_url, params=payload)
                sections = r.json()
            except requests.exceptions.RequestException as e:
                self.logger.error(
                    "Something went wrong connecting to the NGSI-LD broker. Maybe server is down."
                )
                sections = []
            matching_sections = []
            for s in sections:
                if "odala:transportMode" not in s:
                    continue
                if vehicle_transport_mode == translate_transport_mode(
                    s["odala:transportMode"]["value"]
                ):
                    distance = s["odala:distance"]["value"]
                    speed = s["odala:speed"]["value"]  # FIXME
                    matching_sections.append([distance, speed])
            sums = [sum(x) for x in zip(*matching_sections)]
            means = [x / len(matching_sections) for x in sums]
            if len(means) > 0:
                # use emission based values
                distance_avg = means[0]
                speed_avg = means[1]
            else:
                # use default values
                distance_dist = transport_modes_model[vehicle_transport_mode][
                    "distance"
                ]
                distance_avg = distance_dist[randrange(len(distance_dist))]

                speed_dist = transport_modes_model[vehicle_transport_mode]["speed"]
                speed_avg = speed_dist[randrange(len(speed_dist))]

            co2 = compute_carbon_footprint(vehicle_transport_mode, distance_avg)

            coordinates_sim = self.get_simulated_point(
                vehicle_transport_mode, coordinates[0], coordinates[1], 500
            )
            if not coordinates_sim:
                continue

            observedAt = vehicle["speed"]["observedAt"]
            emission_observed = copy.deepcopy(ngsi_template_emissionobserved)
            emission_observed["id"] = emission_obs_id
            emission_observed["location"]["observedAt"] = observedAt
            emission_observed["location"]["value"]["coordinates"] = coordinates_sim
            emission_observed["co2"]["observedAt"] = observedAt
            emission_observed["co2"]["value"] = co2
            emission_observed["abstractionLevel"] = {"type": "Property", "value": 17}
            headers = {"Content-Type": "application/ld+json"}
            try:
                r = requests.post(
                    self.broker_url + "/ngsi-ld/v1/entities",
                    data=json.dumps(emission_observed),
                    headers=headers,
                )
                if r.status_code == 201:
                    self.logger.debug("Created new EmissionObserved entity")
                else:
                    self.logger.warning(
                        "EmissionObserved Creation failed, status code: %s",
                        r.status_code,
                    )
            except requests.exceptions.RequestException as e:
                self.logger.error(
                    "Something went wrong connecting to the NGSI-LD broker. Maybe server is down."
                )


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start Green Transport Twin")
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
        default=600,
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
        "--simulate",
        dest="simulate",
        type=int,
        required=False,
        default=0,
        help="Simulate vehicle data, 0|1",
    )

    args = parser.parse_args()

    Path(args.logs).mkdir(parents=True, exist_ok=True)
    LOGGER = logging.getLogger("green_transport_twin")
    LOGGER.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    fh = logging.FileHandler(args.logs + "/green_transport_twin.log")
    fh.setFormatter(formatter)
    fh.setLevel(logging.DEBUG)
    LOGGER.addHandler(fh)
    LOGGER.info("Logs Folder: %s", args.logs)

    green_transport_twin = GreenTransportTwin(
        broker_url=args.url,
        poll_intervall=args.intervall,
        simulate_mode=bool(args.simulate),
        logger=LOGGER,
    )

    green_transport_twin.start()
