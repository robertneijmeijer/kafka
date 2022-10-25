FROM python:3.9-alpine

COPY requirements.txt /
COPY app.py /

RUN pip install -r /requirements.txt
RUN pip install kafka-schema-registry

ENTRYPOINT ["python3", "/app.py"]
