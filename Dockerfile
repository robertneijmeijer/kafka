FROM python:3.10-alpine

COPY requirements.txt /
COPY app.py /
# Add this depenentcy seperatly since it's not released yet
RUN pip install -r /requirements.txt 

RUN apk update && apk add gcc \
                         libc-dev \
                         --no-cache librdkafka-dev

RUN sed -i -e 's/v3\.4/edge/g' /etc/apk/repositories \
    && apk upgrade --update-cache --available \
    && apk --no-cache add librdkafka

RUN pip install confluent-kafka

ENTRYPOINT ["python3", "/app.py"]
