# syntax=docker/dockerfile:1
FROM python:3
ENV PYTHONUNBUFFERED=1
WORKDIR /mobility_toolkit
COPY mtk_common/src/mtk_common/* /mobility_toolkit/mtk_common/

COPY mdm_adapter/* /mobility_toolkit/
RUN pip install -r requirements.txt
CMD python mdm_adapter.py --url=${URL} --intervall=${INTERVAL} --logging_folder=${LOGGING_FOLDER} --logging_level=${LOGGING_LEVEL} --subscription_id=${SUBSCRIPTION_ID} --p12_cert=${P12_CERT} --p12_pass=${P12_PASS}
