# syntax=docker/dockerfile:1
FROM python:3
ENV PYTHONUNBUFFERED=1
WORKDIR /mobility_toolkit
COPY mtk_common/src/mtk_common/* /mobility_toolkit/mtk_common/

COPY ngsiv2_adapter/* /mobility_toolkit/
RUN pip install -r requirements.txt
CMD python ngsiv2_adapter.py --client_id=${CLIENT_ID} --client_secret=${CLIENT_SECRET} --username=${USERNAME} --password${PASSWORD} --url=${URL}  --ngsiv2_url=${NGSIV2_URL} --intervall=${INTERVAL} --oauth2_token_url=${OAUTH2_TOKEN_URL}

