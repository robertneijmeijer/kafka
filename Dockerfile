FROM python:3.9-alpine

COPY requirements.txt /
COPY app.py /
COPY avro_schema.avsc /

# Add this depenentcy seperatly since it's not released yet
RUN apk add build-base
# librdkafka-dev is needed for confluent-kafka
RUN sed -i -e 's/v3\.4/edge/g' /etc/apk/repositories \
    && apk upgrade --update-cache --available \
    && apk --no-cache add librdkafka-dev

RUN apk update && apk add ruby

RUN pip install -r /requirements.txt 

RUN ls /home/

ENTRYPOINT ["python3", "/app.py"]
