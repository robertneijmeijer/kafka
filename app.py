#/usr/bin/env python3

import argparse
import json
import os
import logging
import yaml
from kafka import KafkaProducer
from schema import Schema, SchemaError, Optional, Hook, Or
from confluent_kafka import Producer, Consumer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer
from confluent_kafka import Producer, Consumer, DeserializingConsumer
from confluent_kafka.serialization import StringSerializer, SerializationContext, MessageField, StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.avro import AvroSerializer, AvroDeserializer
from confluent_kafka import Message
from confluent_kafka import TopicPartition
import re
from collections import defaultdict
from collections.abc import Iterable
from itertools import islice
from pathlib import Path
import time
import uuid

DEFAULT_DATA_FILE = 'system.yml'
DEFAULT_CA_FILE = 'ca.crt'
DATA_FILE_ENV_VAR = 'DATA_FILE'

# Env variables
KAFKA_TOPIC_NAME_ENV_VAR = 'KAFKA_TOPIC_NAME'
KAFKA_BOOTSTRAP_ENV_VAR = 'KAFKA_BOOTSTRAP_SERVERS'
KAFKA_PASSWD_ENV_VAR = 'KAFKA_PASSWORD'
KAFKA_USERNAME_ENV_VAR = 'KAFKA_USERNAME'
KAFKA_CA_ENV_VAR = 'KAFKA_CA_CONTENT'
KAFKA_VALIDATION_CHECK_ENV_VAR ='KAFKA_VALIDATION_CHECK' 

# Kafka settings
KAFKA_TOPIC_DEFAULT_KEY = 'topic2'
KAFKA_SECURITY_PROTOCOL = 'PLAINTEXT'
KAFKA_SASL_MECHANISM = 'SCRAM-SHA-512'

global YAML_DATA

log = logging.getLogger()
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))

def include_constructor(loader, node):
  selector = loader.construct_sequence(node)
  name = selector.pop(0)

  path = Path(os.getcwd() + name)
  if not path.is_file():
    log.error("Values could not be found at: " + name + " please add the correct path or fill the value in manually")
    exit(1)

  with open(os.getcwd() + name ) as f:
    content = yaml.safe_load(f)
  
  for item in selector:
    for key, value in content.items():
      if key == item:
        for name in selector:
          content = content[name] 
        return content

  return None

yaml.add_constructor('!include', include_constructor, Loader=yaml.SafeLoader)

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
    parser.add_argument('--validation-check', dest='validation_check',
                        default=os.getenv(KAFKA_VALIDATION_CHECK_ENV_VAR), type=bool,
                        help='validation check')
    args = parser.parse_args()
    return {
        'bootstrap_servers': args.bootstrap_servers,
        'topic_name': args.topic_name,
        'data_file': args.data_file,
        'username': args.username,
        'password': args.password,
        'validation_check': args.validation_check
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

    topic = "topic12"

    with open('/avro_schema.avsc') as f:
      schema_str = f.read()

    schema_registry_client = SchemaRegistryClient({'url': 'http://10.152.183.242:8081'})

    avro_serializer = AvroSerializer(schema_registry_client, schema_str)

    string_serializer = StringSerializer('utf_8')

    producer = Producer({'bootstrap.servers': '10.152.183.52:9094'})

    producer.produce(topic=topic, key=string_serializer(YAML_DATA['name'], None), value=avro_serializer(data, SerializationContext(topic, MessageField.VALUE)))

    producer.flush()

def add_value(key):
    global YAML_DATA
    if(key == 'technology'):
      YAML_DATA['containers'][key] = str(find_main_language())
    elif(key == 'icfr'):
      YAML_DATA['containers'][key] = False
    elif(key == 'hostedAt'):
      YAML_DATA['containers'][key] = "Azure Cloud"
    elif(key == 'team'):
      YAML_DATA['containers'][key] = str(find_team())

def find_team():
    log.info('finding team')
    path = Path(os.getcwd() + '/CODEOWNERS')
    if not path.is_file():
        log.error("CODEOWNERS file could not be found, please manually fill in team")
        exit(1)

    with open(os.getcwd() + '/CODEOWNERS') as f:
        code = f.read()

    matches = re.findall(r"(CODEOWNERS|\*)[ \t]+(@RoyalAholdDelhaize\/)(.*)", code)

    return matches[0][-1]

schema_val = {
    "name": str,
    "description": str,

    "containers": [{
        "name": str,
        "sysnonyms": str,
        "description": str,
        Optional("technology", default= lambda : add_value('technology')): str,
        # "ciDataOwner": str,
        # "productOwner": str,
        Optional("team", default= lambda : add_value('team')): str,
        "applicationType": Or("Business", "Customer Facing", "External Service", "Infrastructure", "Interface", "Office", "Tool", "Unknown"),
        Optional("hostedAt", default = lambda : add_value('hostedAt')): Or("Amazon Web Services (AWS Cloud)", "AT&T", "Azure CF1", "Azure CF2", "Azure Cloud", "DXC", "Equinix", "Google Cloud Platform", "Hybric", "Inlumi", "Local server", "Multi-Cloud", "Not Applicable", "Other", "Salesforce", "ServiceNow", "Solvinity", "Unit4", "Unknown", "User device", "Azure"),
        "deploymentModel": Or("BPO", "CaaS", "IaaS", "On-Premise", "PaaS", "SaaS"),
        "personalData": bool,
        "confidentiality": str,
        "mcv": Or("Highly business critical", "Business critical", "Not business critical", "Not applicable"),
        "maxSeverityLevel": Or(1,2,3,4, "Not applicable"),
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
    }]
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
                if re.search(f"\.({type}$)", filename):
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

def replace_key(data, keys, index = 0):
    temp_data = {}

    for i, old_key in enumerate(data):
        list_data = list(data.values())
        temp_data[keys[i + index]] = list_data[i]

    return temp_data


def translate_keys(data):
    with open('/avro_schema.avsc') as f:
      schema_str = f.read()

    schema_str = re.findall('(?<=\"name"\ : ")(.*?)(?=\")',schema_str)
    del schema_str[0]
    del schema_str[3]

    first_data = replace_key(dict(islice(data.items(), 2)), schema_str)
    second_data = replace_key(dict(islice(data.items(), 2, 3)), schema_str, 2)
    containers = list()
    for container in second_data["containers"]:

        third_data = replace_key(container, schema_str, 3)
        fourth_data = replace_key(third_data["components"], ["name", "description", "exposedAPIs", "consumedAPIs"])
        fifth_data = list()

        for value in fourth_data["exposedAPIs"]:
            fifth_data.append(replace_key(value, ["name", "description", "type", "status"]))
        fourth_data["exposedAPIs"] = fifth_data

        sixth_data = list()

        for value in fourth_data["consumedAPIs"]:
            sixth_data.append(replace_key(value, ["name", "description", "status", "read", "write", "execute"]))

        fourth_data["consumedAPIs"] = sixth_data

        data = first_data 
        containers.append(third_data)
        
    data["containers"] = containers

    return data

def validate_names():
    global YAML_DATA 
    with open('/avro_schema.avsc') as f:
      schema_str = f.read()

    schema_registry_client = SchemaRegistryClient({'url': 'http://10.152.183.242:8081'})

    avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

    string_deserializer = StringDeserializer('utf_8')


    config = {'bootstrap.servers': '10.152.183.52:9094',
    'group.id': str(uuid.uuid4()),
    'auto.offset.reset': 'earliest',
    'value.deserializer': avro_deserializer,
    'key.deserializer': string_deserializer}
    consumer = DeserializingConsumer(config)
    explosedAPIs = list()
    try:
        consumer.subscribe(["topic12"])

        topic_partition = TopicPartition("topic12", partition=0)
        low, high = consumer.get_watermark_offsets(topic_partition)
        current_offset = 0

        log.info('Consuming data to see if data is already present')
        while current_offset < high:
            message = consumer.poll(timeout=1.0)
            current_offset += 1
            if message is None: continue
            if message.error():
                print(message)
            else:
                # log.info('Consumed data: %s', message.value())
                if message.value() == YAML_DATA:
                    log.info('Data is already present and validated')
                    return 
                else:
                    for containers in message.value()["containers"]:
                        for exposed in containers["components"]["exposedAPIs"]:
                            explosedAPIs.append(exposed)

        for containers in YAML_DATA["containers"]:
            for consumedAPI in containers["components"]["consumedAPIs"]:
                found = False
                for exposedAPI in explosedAPIs:
                    if consumedAPI["name"] == exposedAPI["name"]:
                        found = True
                        continue 
                if not found:
                    log.error("consumed API: " + consumedAPI["name"] + " Not found in system")
    finally:
        consumer.close()
    return True

def main():
    kafka_settings = parse_args()
    log.info('Configuration: %s', kafka_settings)
    data = parse_yaml(kafka_settings['data_file'])
    global YAML_DATA 
    YAML_DATA = data
    filter_none()
    # log.info("Validationcheck " + str(os.getenv(KAFKA_VALIDATION_CHECK_ENV_VAR)))
    log.info('Data: %s', data)
    # Validate before translate 
    YAML_DATA = translate_keys(YAML_DATA)
    log.info('kk ' + str(YAML_DATA))
    validate_yaml(YAML_DATA)
    # validate_names()

    if os.getenv(KAFKA_VALIDATION_CHECK_ENV_VAR):
        validate_yaml(YAML_DATA)
        
        exit(0)
    
    #ca_content = os.getenv(KAFKA_CA_ENV_VAR)
    #write_ca_file(ca_content, DEFAULT_CA_FILE)
    try:
        if(validate_yaml(YAML_DATA, True)):
            send_to_kafka(settings=kafka_settings, data=YAML_DATA)
            log.info('Data successfully sent')
            log.info("Data: %s", YAML_DATA)
            exit(0)
        else:
            # Exit code 2 since the data is missing or invalid
            exit(2)
    except Exception as e:
        # Print error and generic exit code 1
        print(e)
        exit(1)
        #os.remove(DEFAULT_CA_FILE)


if __name__ == '__main__':
    main()
