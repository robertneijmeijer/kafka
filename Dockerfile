FROM python:3.9-alpine

COPY requirements.txt /
COPY app.py /
COPY avro_schema.avsc /
COPY languages.yml /

# Add this depenentcy seperatly since it's not released yet
# librdkafka-dev is needed for confluent-kafka
RUN sed -i -e 's/v3\.4/edge/g' /etc/apk/repositories \
    && apk upgrade --update-cache --available \
    && apk --no-cache add librdkafka-dev

RUN pip install -r /requirements.txt 

ENTRYPOINT ["python3", "/app.py"]
