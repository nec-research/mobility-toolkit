# syntax=docker/dockerfile:1
FROM python:3
ENV PYTHONUNBUFFERED=1
WORKDIR /gtt
COPY requirements.txt /gtt/
RUN pip install -r requirements.txt
COPY . /gtt/
#COPY ./emission_ngsild_adapter.py /gtt/emission/src/e-mission-server/
CMD python mdm_ngsild_adapter.py --url=${URL} --intervall=${INTERVAL} --logs=./ --subscription_id=${SUBSCRIPTION_ID} --p12_cert=${P12_CERT} --p12_pass=${P12_PASS}
#CMD python green_transport_twin.py --url=${GTT_URL} --intervall=${GTT_INTERVALL} --logs=${GTT_LOGS_FOLDER} --simulate=${GTT_SIMULATE}
