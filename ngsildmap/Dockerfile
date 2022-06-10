# syntax=docker/dockerfile:1
FROM python:3
ENV PYTHONUNBUFFERED=1
WORKDIR /map
COPY requirements.txt /map/
RUN pip install -r requirements.txt
COPY . /map/
CMD python ngsildmap.py

