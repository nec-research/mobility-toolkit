# syntax=docker/dockerfile:1
FROM python:3
ENV PYTHONUNBUFFERED=1
WORKDIR /gtt
COPY requirements.txt /gtt/
RUN pip install -r requirements.txt
COPY . /gtt/
COPY ./emission_ngsild_adapter.py /gtt/emission/src/e-mission-server/
