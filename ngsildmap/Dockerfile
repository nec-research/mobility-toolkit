# syntax=docker/dockerfile:1
FROM python:3
ENV PYTHONUNBUFFERED=1
WORKDIR /map

COPY ngsildmap/* /map/
RUN pip install -r requirements.txt
CMD python ngsildmap.py
