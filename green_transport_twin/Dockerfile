# syntax=docker/dockerfile:1
FROM python:3
ENV PYTHONUNBUFFERED=1

WORKDIR /mobility_toolkit
COPY green_transport_twin/* /mobility_toolkit/
COPY mtk_common/src/mtk_common/* /mobility_toolkit/mtk_common/
RUN pip install -r requirements.txt
CMD python green_transport_twin.py --url=${URL} --intervall=${INTERVAL} --logging_folder=${LOGGING_FOLDER} --logging_level=${LOGGING_LEVEL} --simulate=${SIMULATE}
