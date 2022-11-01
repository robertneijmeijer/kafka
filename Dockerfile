FROM python:3.9-alpine

COPY requirements.txt /
COPY app.py /
# Add this depenentcy seperatly since it's not released yet
RUN pip install -r /requirements.txt 

RUN wget -qO - https://packages.confluent.io/deb/7.2/archive.key | sudo apt-key add -

RUN sudo add-apt-repository "deb [arch=amd64] https://packages.confluent.io/deb/7.2 stable main"
RUN sudo add-apt-repository "deb https://packages.confluent.io/clients/deb $(lsb_release -cs) main"

RUN sudo apt-get update && sudo apt-get install confluent-platform

RUN apk update && apk add gcc \
                         libc-dev \
                         --no-cache librdkafka-dev

RUN pip install confluent-kafka

ENTRYPOINT ["python3", "/app.py"]
