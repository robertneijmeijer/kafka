#/usr/bin/env python3

import argparse
import json
import os
import logging
import yaml
from kafka import KafkaProducer
from schema import Schema, SchemaError, Optional, Hook, Or
from confluent_kafka import Producer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
import re
from collections import defaultdict

DEFAULT_DATA_FILE = 'system.yml'
DEFAULT_CA_FILE = 'ca.crt'
DATA_FILE_ENV_VAR = 'DATA_FILE'

# Env variables
KAFKA_TOPIC_NAME_ENV_VAR = 'KAFKA_TOPIC_NAME'
KAFKA_BOOTSTRAP_ENV_VAR = 'KAFKA_BOOTSTRAP_SERVERS'
KAFKA_PASSWD_ENV_VAR = 'KAFKA_PASSWORD'
KAFKA_USERNAME_ENV_VAR = 'KAFKA_USERNAME'
KAFKA_CA_ENV_VAR = 'KAFKA_CA_CONTENT'

# Kafka settings
KAFKA_TOPIC_DEFAULT_KEY = 'topic2'
KAFKA_SECURITY_PROTOCOL = 'PLAINTEXT'
KAFKA_SASL_MECHANISM = 'SCRAM-SHA-512'

global YAML_DATA

log = logging.getLogger()
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

def parse_args() -> dict:
    parser = argparse.ArgumentParser(description='Send system properties to Kafka topic')
    parser.add_argument('--bootstrap-servers', dest='bootstrap_servers',
                        default=os.getenv(KAFKA_BOOTSTRAP_ENV_VAR), type=str,
                        help='kafka bootstrap server url (host:port)')
    parser.add_argument('--topic-name', dest='topic_name',
                        default=os.getenv(KAFKA_TOPIC_NAME_ENV_VAR), type=str,
                        help='kafka topic name')
    parser.add_argument('--data-file', dest='data_file',
                        default=os.getenv(DATA_FILE_ENV_VAR, DEFAULT_DATA_FILE), type=str,
                        help='file with properties data')
    parser.add_argument('--username', dest='username',
                        default=os.getenv(KAFKA_USERNAME_ENV_VAR), type=str,
                        help='kafka username')
    parser.add_argument('--password', dest='password',
                        default=os.getenv(KAFKA_PASSWD_ENV_VAR), type=str,
                        help='kafka password')
    args = parser.parse_args()
    return {
        'bootstrap_servers': args.bootstrap_servers,
        'topic_name': args.topic_name,
        'data_file': args.data_file,
        'username': args.username,
        'password': args.password
    }

def parse_yaml(yaml_file: str) -> dict:
    with open(yaml_file, mode='r', encoding='utf-8') as file:
        data = yaml.safe_load(file)
    return data

def write_ca_file(content: str, filename: str=DEFAULT_CA_FILE):
    with open(filename, mode='w', encoding='utf-8') as file:
        file.truncate()
        file.write(content)

def send_to_kafka(settings: dict, data: dict):
    global YAML_DATA

    topic = "topic10"

    with open('/avro_schema.avsc') as f:
      schema_str = f.read()

    schema_registry_client = SchemaRegistryClient({'url': 'http://10.152.183.242:8081'})

    avro_serializer = AvroSerializer(schema_registry_client, schema_str)

    string_serializer = StringSerializer('utf_8')

    producer = Producer({'bootstrap.servers': '10.152.183.181:9094'})

    # producer.produce(topic=topic, key=string_serializer(YAML_DATA['name'], None), value=avro_serializer(data, SerializationContext(topic, MessageField.VALUE)))

    producer.flush()

def add_value(key):
    global YAML_DATA
    if(key == 'technology'):
      YAML_DATA['containers'][key] = str(find_main_language())
      print('key')
      print(YAML_DATA['containers'][key])
    elif(key == 'sox'):
      YAML_DATA['containers'][key] = False
    elif(key == 'icfr'):
      YAML_DATA['containers'][key] = False
    elif(key == 'hostedAt'):
      YAML_DATA['containers'][key] = "Azure Cloud"

schema_val = {
    "name": str,
    "description": str,
    "status": str,

    "containers": {
        "name": str,
        "sysnonyms": str,
        "description": str,
        Optional("technology", default= lambda : add_value('technology')): str,
        "parentSystem": str,
        "ciDataOwner": str,
        "productOwner": str,
        "applicationType": Or("Business", "Customer Facing", "External Service", "Infrastructure", "Interface", "Office", "Tool", "Unknown"),
        Optional("hostedAt", default = lambda : add_value('hostedAt')): Or("Amazon Web Services (AWS Cloud)", "AT&T", "Azure CF1", "Azure CF2", "Azure Cloud", "DXC", "Equinix", "Google Cloud Platform", "Hybric", "Inlumi", "Local server", "Multi-Cloud", "Not Applicable", "Other", "Salesforce", "ServiceNow", "Solvinity", "Unit4", "Unknown", "User device", "Azure"),
        "deploymentModel": Or("BPO", "CaaS", "IaaS", "On-Premise", "PaaS", "SaaS"),
        "personalData": bool,
        "confidentiality": str,
        "mcv": Or("Highly business critical", "Business critical", "Not business critical", "Not applicable"),
        "maxSeverityLevel": Or(1,2,3,4, "Not applicable"),
        Optional("sox", default= lambda : add_value('sox')): bool,
        Optional("icfr", default= lambda : add_value('icfr')): bool,
        "assignementGroup": str,
        # operational = deployed to prod, pipelined = in development not yet released
        "operationalStatus": Or("Pipelined", "Operational", "Non-Operational", "Submitted for decommissioning", "Decommissioned", "In decommissioning process"),
        "environments": Or("nl", "be"),

        "components": {
            "name": str,
            "description": str,
            "exposedAPIs": [{
                "name": str,
                "description": str,
                "type": str,
                "status": str,
            }],
            "consumedAPIs": [{
                "name": str,
                "description": str,
                "status": str,
                "read": bool,
                "write": bool,
                "execute": bool,
            }]
        },
    }
}

def validate_yaml(yaml_data, verbose = False):
    validator = Schema(schema_val)
    try:
        validator.validate(yaml_data)
        if(verbose):
          print('YML valid')
        return True
    except SchemaError as se:
        if(verbose):
          print(se)
        return False

def find_main_language(full_output = False):
  languages = parse_yaml("/languages.yml")
  matches = defaultdict(int)
  for root, directory, filenames in os.walk(os.getcwd()):
      for filename in filenames:
        for key, value in languages.items():
            for type in value:
                if re.search(f".({type}$)", filename):
                    size = os.path.getsize(root + '/' + filename)
                    matches[key] += size
  if(full_output):
    return matches
  else:
    return max(matches, key=matches.get)

def delete_keys_from_dict(d, to_delete):
    if isinstance(to_delete, str):
        to_delete = [to_delete]
    if isinstance(d, dict):
        for single_to_delete in set(to_delete):
            if single_to_delete in d:
                del d[single_to_delete]
        for k, v in d.items():
            delete_keys_from_dict(v, to_delete)
    elif isinstance(d, list):
        for i in d:
            delete_keys_from_dict(i, to_delete)

def filter_none(): 
    global YAML_DATA
    stack = list(YAML_DATA.items()) 
    visited = set() 
    while stack: 
        k, v = stack.pop() 
        if isinstance(v, dict): 
            if k not in visited: 
                stack.extend(v.items()) 
        else: 
            if v == None or v == '':
                delete_keys_from_dict(YAML_DATA,k)
        visited.add(k)

def main():
    kafka_settings = parse_args()
    log.info('Configuration: %s', kafka_settings)
    data = parse_yaml(kafka_settings['data_file'])
    global YAML_DATA 
    YAML_DATA = data
    filter_none()
    log.info('Data: %s', data)
    validate_yaml(YAML_DATA)
    print('root')
    print(os.getcwd())

    
    #ca_content = os.getenv(KAFKA_CA_ENV_VAR)
    #write_ca_file(ca_content, DEFAULT_CA_FILE)
    
    try:
        if(validate_yaml(YAML_DATA, True)):
            send_to_kafka(settings=kafka_settings, data=YAML_DATA)
            log.info('Data successfully sent')
            log.info("Data: %s", YAML_DATA)
    except Exception as e:
        print('error')
        raise e
        #os.remove(DEFAULT_CA_FILE)


if __name__ == '__main__':
    main()
