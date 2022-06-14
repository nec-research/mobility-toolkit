import argparse
import copy
import datetime
import json
import logging
import random
import signal
import sys
import time
import uuid
from datetime import timezone
from pathlib import Path
from random import randrange, uniform

import requests
from mtk_common.model import (ngsi_template_emissionobserved,
                              ngsi_template_vehicle, traffic_sensor_locations,
                              transport_modes_model)
from mtk_common.utils import (compute_carbon_footprint, get_entities,
                              get_request, get_temporal_entities,
                              get_transport_mode, post_payloads,
                              translate_transport_mode)


class GreenTransportTwin(object):
    """
    GreenTransportTwin combines NGSI-LD city transport data (Vehicle,
    TrafficFlowObserved) to predict related CO2 emissions. To calibrate
    emission values, it uses available SectionObserved entities that can e.g.,
    collected through a crowd-sourcing platform such as E-mission/OpenPath. If
    these are not available, the transport model in model.py is used. It can be
    adapted to the sepcific city, e.g., based on available survey data for
    typical trip lengths for different transport modes.
    """

    def __init__(
        self,
        broker_url: str,
        poll_intervall: int,
        simulate_mode: bool,
        logger: logging.Logger,
    ):
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
                results = []
                t0 = time.time()
                vehicles = get_entities(
                    entity_type="Vehicle",
                    observedAt_property="speed",
                    intervall=self.poll_intervall,
                    url=self.broker_url,
                    logger=self.logger,
                )
                if len(vehicles) > 0:
                    results = results + self.get_entity_data(vehicles)
                tf_obs = self.get_trafficflow_entities()
                if len(tf_obs) > 0:
                    results = results + self.get_entity_data(tf_obs)
                if len(results) > 0:
                    self.estimate_emission(results, days=7, section_observed=False)
                t1 = time.time()
                delta = t1 - t0
                self.logger.info(
                    "Processed %s Vehicles and %s TrafficFlowObserved in %s seconds",
                    len(vehicles),
                    len(tf_obs),
                    delta,
                )
                sleep_time = args.intervall - delta
                sleep_time = (
                    sleep_time * 0.95
                )  # sleeping a bit less so we make sure to process all data
                if sleep_time < 0:
                    self.logger.warning("Cannot process new data fast enough!")
                    sleep_time = 0
                self.logger.info("Sleeping %s seconds", sleep_time)
            for _ in range(int(sleep_time)):
                time.sleep(1)
                if not self.running:
                    break

    def stop(self, *args):
        self.logger.info("Shutting down...")
        self.running = False
        sys.exit()

    def get_trafficflow_entities(self):
        """
        get all versions of TrafficFlowObserved entities during the poll_intervall
        """

        entity_type = "TrafficFlowObserved"
        observedAt_property = "intensity"

        # 1. get changed entities with normal query for all metadata
        entities_normal = get_entities(
            entity_type=entity_type,
            observedAt_property=observedAt_property,
            url=self.broker_url,
            intervall=self.poll_intervall,
            logger=self.logger,
        )
        if not entities_normal:
            return []

        metadata = {}
        for e in entities_normal:
            metadata[e["id"]] = {
                "description": e.get("description", ""),
                "address": e.get("address", ""),
                "source": e.get("source", ""),
            }

        # 2. get temporal data
        entities_temporal = get_temporal_entities(
            entity_type=entity_type,
            intervall=self.poll_intervall,
            url=self.broker_url,
            logger=self.logger,
        )
        if not entities_temporal:
            return []

        # 3. merge things
        for e in entities_temporal:
            for k, v in e.items():
                if type(v) == dict:
                    e[k] = [v]

        results = []
        for e in entities_temporal:
            if e["id"] not in metadata:
                continue  # might have new entities in temporal query that have been added to broker in between reqeuests
            i = 0
            for v in e[observedAt_property]:
                r = {}
                r[observedAt_property] = v
                r["location"] = e["location"][i]
                r["id"] = e["id"]
                r["type"] = e["type"]
                r["description"] = metadata[e["id"]]["description"]
                r["address"] = metadata[e["id"]]["address"]
                r["source"] = metadata[e["id"]]["source"]
                if "vehicleType" in e:
                    r["vehicleType"] = e["vehicleType"][i]
                else:
                    self.logger.info("No vehicleType available, trying to infer")
                    for va in ["Velo", "ECO Counter"]:
                        if va.lower() in r["description"]["value"].lower():
                            r["vehicleType"] = "bicycle"
                        if va.lower() in e["source"]["value"].lower():
                            r["vehicleType"] = "bicycle"
                results.append(r)
                i = i +1
        return results

    def simulate_vehicle_data(self) -> None:
        """
        simulate vehicle data based on defined traffic sensors in model.py
        """

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
                                "Something went wrong connecting to the NGSI-LD broker. Maybe server is down. %s",
                                e,
                            )
                            continue

                        if r.status_code != 201:
                            self.logger.warning(
                                "Request failed %s %s", r.status_code, r.text
                            )
                        else:
                            self.logger.debug("Created simulated vehicle")
                            # create directly emission here
                            results = self.get_entity_data([payload])
                            self.estimate_emission(
                                results, days=7, section_observed=False
                            )

    def get_simulated_point(
        self, transport_mode: str, lon: float, lat: float, radius: int
    ) -> list:
        """
        gets a likely point where an emission is assigned to based on typical
        trip lengths for the specific transport mode and the existing road
        network (based on openstreetmap) in the surroundings of the traffic
        sensor (e.g., a car observation will more likely be placed on a main
        road, a bicycle observation more likely on a bicycle path or smaller
        road).
        """

        self.logger.debug("get_simulated_point")
        query_file = open("./overpass_template")
        query_string = "".join(query_file.readlines())
        osm_urls = [
            "https://overpass-api.de/api/interpreter",
            # "http://overpass.openstreetmap.fr/api/interpreter",
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
                        self.logger.debug(
                            "Connection to OSM failed, status code %s", status_code
                        )
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
            return []
        for key in common_keys:
            length = len(ways[key])
            weight = transport_modes_model[transport_mode]["highways"][key]
            aggregated[key] = length + weight
        choice = random.choices(
            population=list(aggregated.keys()), weights=list(aggregated.values()), k=1
        )[0]
        return nodes[random.choice(ways[choice])]

    def get_entity_data(self, entities: list) -> list:
        """
        get relevant data from Vehicle or TrafficFlowObserved entities that is
        needed to predict EmissionObserved entities
        """

        self.logger.debug("get_entity_data")
        results = []
        for e in entities:
            if "location" not in e:
                continue
                # FIXME, bug in scorpio temporal query?
            emission_obs_id = e["id"].split(":")[-1]
            emission_obs_id = "EmissionObserved:" + emission_obs_id
            vehicle_transport_mode = get_transport_mode(e, self.logger)
            if not vehicle_transport_mode:
                continue  # skip this entity as we do not know transport mode
            vehicle_transport_mode = translate_transport_mode(vehicle_transport_mode)
            coordinates = e["location"]["value"]["coordinates"]
            if e["type"] == "Vehicle":
                observedAt = e["speed"]["observedAt"]
                results.append(
                    (emission_obs_id, observedAt, vehicle_transport_mode, coordinates)
                )
            elif e["type"] == "TrafficFlowObserved":
                observedAt = e["intensity"]["observedAt"]
                intensity = e["intensity"]["value"]
                for i in range(intensity):
                    results.append(
                        (
                            emission_obs_id + "-" + str(i),
                            observedAt,
                            vehicle_transport_mode,
                            coordinates,
                        )
                    )
        return results

    def get_means(
        self, vehicle_transport_mode: str, coordinates: list, days: int
    ) -> list:
        """
        get distance and speed means based on available SectionObserved entities
        """

        payload = {"type": "SectionObserved", "timerel": "after"}
        payload["time"] = (
            datetime.datetime.now() - datetime.timedelta(days=days)
        ).strftime("%Y-%m-%dT%H:%M:%SZ")
        payload["georel"] = "near;maxDistance==2000"
        payload["geometry"] = "Point"
        payload["coordinates"] = str(coordinates).replace(" ", "")
        broker_temp_url = self.broker_url + "/ngsi-ld/v1/temporal/entities/"
        try:
            r = requests.get(broker_temp_url, params=payload)
            sections = r.json()
        except requests.exceptions.RequestException as e:
            self.logger.error(
                "Something went wrong connecting to the NGSI-LD broker. Maybe server is down."
            )
            return []
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
        return means

    def estimate_emission(self, results: list, days=7, section_observed=False) -> None:
        """
        estimate EmissionObserved based on available SectionObserved or
        transport_modes_model defined in model.py and push to scorpio
        """

        self.logger.debug("estimate_emission")
        payloads = []
        cache = {}
        for result in results:
            emission_obs_id = result[0]
            observedAt = result[1]
            vehicle_transport_mode = result[2]
            coordinates = result[3]
            means = []
            if section_observed:
                self.logger.info("Using existing SectionObserved entities")
                if str(coordinates) in cache:
                    self.logger.info("using cache")
                    means = cache[str(coordinates)]
                else:
                    means = self.get_means(vehicle_transport_mode, coordinates, days)
                    cache[str(coordinates)] = means
            if len(means) > 0:
                # use emission based values
                distance_avg = means[0]
                speed_avg = means[1]
            else:
                self.logger.debug("using default values")
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

            emission_observed = copy.deepcopy(ngsi_template_emissionobserved)
            emission_observed["id"] = emission_obs_id
            emission_observed["location"]["observedAt"] = observedAt
            emission_observed["location"]["value"]["coordinates"] = coordinates_sim
            emission_observed["co2"]["observedAt"] = observedAt
            emission_observed["co2"]["value"] = co2
            # emission_observed["abstractionLevel"] = {"type": "Property", "value": 17}
            payloads.append(emission_observed)
        post_payloads(payloads=payloads, broker_url=self.broker_url, logger=self.logger)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Start Green Transport Twin")
    parser.add_argument(
        "--url",
        dest="url",
        type=str,
        required=False,
        default="http://localhost:9090/",
        help="URL for NGSI-LD Context Broker",
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
        "--simulate",
        dest="simulate",
        type=int,
        required=False,
        default=0,
        help="Simulate vehicle data, 0|1",
    )

    args = parser.parse_args()

    LOGGER = logging.getLogger("green_transport_twin")
    LOGGER.setLevel(args.logging_level)
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    if args.logging_folder:
        Path(args.logging_folder).mkdir(parents=True, exist_ok=True)
        fh = logging.FileHandler(args.logging_folder + "/green_transport_twin.log")
        fh.setFormatter(formatter)
        fh.setLevel(args.logging_level)
        LOGGER.addHandler(fh)
        LOGGER.info("Logging Folder: %s", args.logging_folder)
    else:
        sh = logging.StreamHandler(sys.stdout)
        sh.setFormatter(formatter)
        sh.setLevel(args.logging_level)
        LOGGER.addHandler(sh)

    LOGGER.info("Green Transport Twin starting...")
    green_transport_twin = GreenTransportTwin(
        broker_url=args.url,
        poll_intervall=args.intervall,
        simulate_mode=bool(args.simulate),
        logger=LOGGER,
    )

    green_transport_twin.start()
