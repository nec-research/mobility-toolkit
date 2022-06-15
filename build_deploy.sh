#!/bin/sh
docker-compose -f docker-compose-build.yml build
docker tag mobility-toolkit_ngsildmap:latest scorpiobroker/ngsildmap:latest
docker tag mobility-toolkit_emission_adapter:latest scorpiobroker/emission_adapter:latest
docker tag mobility-toolkit_green_transport_twin:latest scorpiobroker/green_transport_twin:latest
docker tag mobility-toolkit_mdm_adapter:latest scorpiobroker/mdm_adapter:latest
docker tag mobility-toolkit_ngsiv2_adapter:latest scorpiobroker/ngsiv2_adapter:latest

docker push scorpiobroker/ngsildmap:latest
docker push scorpiobroker/emission_adapter:latest
docker push scorpiobroker/green_transport_twin:latest
docker push scorpiobroker/mdm_adapter:latest
docker push scorpiobroker/ngsiv2_adapter:latest
