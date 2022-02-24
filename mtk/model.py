import numpy as np

transport_modes_model = {
    "car": {
        "speed": np.random.normal(50, 50 / 10, 10000),
        "distance": np.random.normal(8000, 8000 / 10, 10000),
        "highways": {
            "motorway": 10,
            "trunk": 6,
            "primary": 5,
            "secondary": 4,
            "tertiary": 3,
            "residential": 2,
            "living_street": 1,
        },
    },
    "large_car": {
        "speed": np.random.normal(40, 40 / 10, 10000),
        "distance": np.random.normal(16000, 16000 / 10, 10000),
        "highways": {
            "motorway": 10,
            "trunk": 6,
            "primary": 5,
            "secondary": 4,
            "tertiary": 3,
            "residential": 2,
            "living_street": 1,
        },
    },
    "bicycle": {
        "speed": np.random.normal(16, 16 / 10, 10000),
        "distance": np.random.normal(4000, 4000 / 10, 10000),
        "highways": {
            "cycleway": 10,
            "living_street": 6,
            "residential": 5,
            "tertiary": 4,
            "secondary": 3,
            "footway": 2,
            "trunk": 1,
        },
    },
    "walk": {
        "speed": np.random.normal(4, 4 / 10, 10000),
        "distance": np.random.normal(1000, 1000 / 10, 10000),
        "highways": {
            "footway": 10,
            "living_street": 6,
            "residential": 5,
            "tertiary": 4,
            "secondary": 3,
            "cycleway": 2,
            "trunk": 1,
        },
    },
    "truck": {
        "speed": np.random.normal(60, 60 / 10, 10000),
        "distance": np.random.normal(100000, 100000 / 10, 10000),
        "highways": {
            "motorway": 10,
            "trunk": 6,
            "primary": 5,
            "secondary": 4,
            "tertiary": 3,
            "residential": 2,
            "living_street": 1,
        },
    },
    "air": {
        "speed": np.random.normal(900, 900 / 10, 10000),
        "distance": np.random.normal(1000000, 1000000 / 10, 10000),
    },
    "light_rail": {
        "speed": np.random.normal(30, 30 / 10, 10000),
        "distance": np.random.normal(8000, 8000 / 10, 10000),
    },
    "tram": {
        "speed": np.random.normal(20, 20 / 10, 10000),
        "distance": np.random.normal(4000, 4000 / 10, 10000),
    },
    "subway": {
        "speed": np.random.normal(50, 50 / 10, 10000),
        "distance": np.random.normal(8000, 8000 / 10, 10000),
    },
    "train": {
        "speed": np.random.normal(70, 70 / 10, 10000),
        "distance": np.random.normal(40000, 40000 / 10, 10000),
    },
    "bus": {
        "speed": np.random.normal(30, 30 / 10, 10000),
        "distance": np.random.normal(5000, 5000 / 10, 10000),
        "highways": {
            "busway": 10,
            "trunk": 6,
            "primary": 5,
            "secondary": 4,
            "tertiary": 3,
            "residential": 2,
            "motorway": 1,
        },
    },
    "motorcycle": {
        "speed": np.random.normal(50, 50 / 10, 10000),
        "distance": np.random.normal(8000, 8000 / 10, 10000),
        "highways": {
            "motorway": 10,
            "trunk": 6,
            "primary": 5,
            "secondary": 4,
            "tertiary": 3,
            "residential": 2,
            "living_street": 1,
        },
    },
}

ngsi_template_section_observed = {
    "id": "urn:section1",
    "type": "SectionObserved",
    "transportMode": {
        "type": "Property",
        "observedAt": "2021-03-24T12:10:00Z",
        "value": "car",
    },
    "distance": {
        "type": "Property",
        "observedAt": "2021-03-24T12:10:00Z",
        "value": 3464,
    },
    "duration": {
        "type": "Property",
        "observedAt": "2021-03-24T12:10:00Z",
        "value": 123,
    },
    "speed": {"type": "Property", "observedAt": "2021-03-24T12:10:00Z", "value": 40},
    "co2": {"type": "Property", "observedAt": "2021-03-24T12:10:00Z", "value": 123},
    "location": {
        "type": "GeoProperty",
        "value": {
            "type": "LineString",
            "coordinates": [[139.815535, 35.772622999999996], [139.815535, 35.774623]],
        },
    },
    "@context": [
        {
            "transportMode": "odala:transportMode",
            "distance": "odala:distance",
            "duration": "odala:duration",
            "co2": "odala:co2",
            "speed": "odala:speed",
        },
        "http://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld",
    ],
}


ngsi_template_vehicle = {
    "id": "urn:ngsi-ld:Vehicle:vehicle1",
    "type": "Vehicle",
    "vehicleType": {"type": "Property", "value": "car"},
    "description": {"type": "Property", "value": "camera1"},
    "location": {
        "type": "GeoProperty",
        "value": {
            "type": "Point",
            "coordinates": [-3.164485591715449, 40.62785133667262],
        },
        "observedAt": "2018-09-27T12:00:00Z",
    },
    "speed": {"type": "Property", "value": 50, "observedAt": "2018-09-27T12:00:00Z"},
    "heading": {"type": "Property", "value": 180, "observedAt": "2018-09-27T12:00:00Z"},
    "@context": [
        "https://raw.githubusercontent.com/smart-data-models/data-models/master/context.jsonld",
        "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld",
    ],
}


ngsi_template_trafficflow_observed = {
    "id": "urn:ngsi-ld:TrafficFlowObserved:TrafficFlowObserved1",
    "type": "TrafficFlowObserved",
    "description": {"type": "Property", "value": "loop1"},
    "dateObserved": {  
        "type": "Property",  
        "value": "2016-12-07T11:10:00/2016-12-07T11:15:00"
    }, 
    "dateObservedFrom": {  
        "type": "Property",  
        "value": {  
          "@type": "DateTime",  
          "@value": "2016-12-07T11:10:00Z"  
        }  
    },
    "dateObservedTo": {  
        "type": "Property",  
        "value": {  
          "@type": "DateTime",  
          "@value": "2016-12-07T11:10:00Z"  
        }  
    },
    "address": {  
        "type": "Property",  
        "value": {  
          "streetAddress": "Avenida de Salamanca",  
          "type": "PostalAddress"  
        }  
    },  
    "location": {
        "type": "GeoProperty",
        "value": {
            "type": "Point",
            "coordinates": [-3.164485591715449, 40.62785133667262],
        },
        "observedAt": "2018-09-27T12:00:00Z",
    },
    "vehicleType": {"type": "Property", "value": "car", "observedAt": "2018-09-27T12:00:00Z"},
    "intensity": {"type": "Property", "value": 42, "observedAt": "2018-09-27T12:00:00Z"},
    "averageVehicleSpeed": {"type": "Property", "value": 42, "observedAt": "2018-09-27T12:00:00Z"},
    "laneDirection": {"type": "Property", "value": "forward", "observedAt": "2018-09-27T12:00:00Z"},
    "occupancy": {"type": "Property", "value": 0.1, "observedAt": "2018-09-27T12:00:00Z"},
    
    
    "@context": [
        "https://raw.githubusercontent.com/smart-data-models/data-models/master/context.jsonld",
        "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld",
    ],
}


ngsi_template_emissionobserved = {
    "id": "urn:ngsi-ld:EmissionObserved:emissionobserved1",
    "type": "EmissionObserved",
    "co2": {"type": "Property", "value": 42, "observedAt": "2018-09-27T12:00:00Z"},
    #     "pm2.5": {
    #         "type": "Property",
    #         "value": 42,
    #         "observedAt": "2018-09-27T12:00:00Z"
    #     },
    #     "pm10": {
    #         "type": "Property",
    #         "value": 42,
    #         "observedAt": "2018-09-27T12:00:00Z"
    #     },
    #     "noise": {
    #         "type": "Property",
    #         "value": 42,
    #         "observedAt": "2018-09-27T12:00:00Z"
    #     },
    "location": {
        "type": "GeoProperty",
        "value": {
            "type": "Point",
            "coordinates": [-3.164485591715449, 40.62785133667262],
        },
        #             "type": "Polygon",
        #             "coordinates": [[-3.164485591715449, 40.62785133667262], [-3.164485591715449, 40.62785133667262],
        #                             [-3.164485591715449, 40.62785133667262], [-3.164485591715449, 40.62785133667262],
        #                             [-3.164485591715449, 40.62785133667262]]
        #         },
        "observedAt": "2018-09-27T12:00:00Z",
    },
    "@context": [
        # "https://schema.lab.fiware.org/ld/context", --> wrong one
        "https://raw.githubusercontent.com/smart-data-models/data-models/master/context.jsonld",
        "https://uri.etsi.org/ngsi-ld/v1/ngsi-ld-core-context.jsonld",
    ],
}




traffic_sensor_locations = [
    {
        "id": "camera1",
        "traffic": 70,
        "transport_modes": {
            "car": 70,
            "bicycle": 75,
            "walk": 34,
            "truck": 30,
            "motorcycle": 10,
        },
        "loc": [8.69221335511113, 49.41405979387663],
    },
    {
        "id": "camera2",
        "traffic": 75,
        "transport_modes": {
            "car": 88,
            "bicycle": 45,
            "walk": 54,
            "truck": 30,
            "motorcycle": 10,
        },
        "loc": [8.676596051243703, 49.41427887301294],
    },
    {
        "id": "camera3",
        "traffic": 60,
        "transport_modes": {
            "car": 60,
            "bicycle": 55,
            "walk": 63,
            "truck": 30,
            "motorcycle": 10,
        },
        "loc": [8.669639967650916, 49.408038251124125],
    },
    {
        "id": "camera4",
        "traffic": 80,
        "transport_modes": {
            "car": 78,
            "bicycle": 50,
            "walk": 45,
            "truck": 30,
            "motorcycle": 10,
        },
        "loc": [8.678131939364917, 49.40025166324303],
    },
    {
        "id": "camera5",
        "traffic": 78,
        "transport_modes": {
            "car": 65,
            "bicycle": 40,
            "walk": 60,
            "truck": 30,
            "motorcycle": 10,
        },
        "loc": [8.678457492993171, 49.40302784531021],
    },
    {
        "id": "camera6",
        "traffic": 50,
        "transport_modes": {
            "car": 90,
            "bicycle": 40,
            "walk": 30,
            "truck": 30,
            "motorcycle": 10,
        },
        "loc": [8.69250469350741, 49.40514778717638],
    },
]
