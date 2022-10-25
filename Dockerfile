FROM python:3.9-alpine

COPY requirements.txt /
COPY app.py /
# Add this depenentcy seperatly since it's not released yet
RUN pip install -r /requirements.txt RUN pip install kafka-schema-registry --pre

ENTRYPOINT ["python3", "/app.py"]
