FROM python:3.9-alpine

COPY requirements.txt /
COPY app.py /

RUN pip install -r /requirements.txt

ENTRYPOINT ["python3", "/app.py"]
