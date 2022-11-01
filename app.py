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
    # producer = KafkaProducer(
    #                          value_serializer=lambda v: json.dumps(v).encode('utf-8'),
    #                          bootstrap_servers=settings['bootstrap_servers'])
    # producer.send('topic2', value=data)
    # producer = prepare_producer(bootstrap_servers=["10.152.183.181:9094"], avro_schema_registry=f'http://10.152.183.242:8081', topic_name="topic2",value_schema=schema, num_partitions=1, replication_factor=1)

    # producer.send("topic2",data)
    # print('send')

    topic = "topic5"

    with open("avro_schema.avsc") as f:
      schema_str = f.read()

    schema_registry_client = SchemaRegistryClient({'url': 'http://10.152.183.242:8081'})

    avro_serializer = AvroSerializer(schema_registry_client, schema_str)

    string_serializer = StringSerializer('utf_8')

    producer = Producer({'bootstrap.servers': '10.152.183.181:9094'})

    producer.produce(topic=topic, key=string_serializer('testkey', None), value=avro_serializer(data, SerializationContext(topic, MessageField.VALUE)))

    producer.flush()

def add_value(key):
    print(key)
    # match key:
    #     case 'technology':
    #         print(key)
            
    #     case 'hostedAt':
    #         print(key)

schema_val = {
    "name": str,
    "description": str,
    "status": str,

    "consumers": {
        "name": str,
        "description": str,
        "type" : str
    },

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
        "operationalStatus": Or("Pipelined", "Operational", "Non-Operational", "Submitted for decommissioning", "Decommissioned", "In decommissioning process"),
        "environments": Or("nl", "be"),
        "relationships": {
            "type": str,
            "container": {
                "name": str,
            },
        },
        "components": {
            "name": str,
            "description": str,
            "exposedAPIs": {
                "name": str,
                "description": str,
                "type": str,
                "status": str,
            },
            "consumedAPIs": {
                "name": str,
                "description": str,
                "status": str
            }
        },
    }
}

schema = {
  "type" : "record",
  "name" : "SystemModel",
  "namespace" : "org.example.models.SystemModel",
  "fields" : [ {
    "name" : "name",
    "type" : "string"
  }, {
    "name" : "description",
    "type" : "string"
  }, {
    "name" : "status",
    "type" : "string"
  }, {
    "name" : "consumers",
    "type" : {
      "type" : "record",
      "name" : "consumers",
      "fields" : [ {
        "name" : "name",
        "type" : "string"
      }, {
        "name" : "description",
        "type" : "string"
      }, {
        "name" : "type",
        "type" : "string"
      } ]
    }
  }, {
    "name" : "containers",
    "type" : {
      "type" : "record",
      "name" : "containers",
      "fields" : [ {
        "name" : "name",
        "type" : "string"
      }, {
        "name" : "sysnonyms",
        "type" : "string"
      }, {
        "name" : "description",
        "type" : "string"
      }, {
        "name" : "technology",
        "type" : "string"
      }, {
        "name" : "parentSystem",
        "type" : "string"
      }, {
        "name" : "ciDataOwner",
        "type" : "string"
      }, {
        "name" : "productOwner",
        "type" : "string"
      }, {
        "name" : "applicationType",
        "type" : "string"
      }, {
        "name" : "hostedAt",
        "type" : "string"
      }, {
        "name" : "deploymentModel",
        "type" : "string"
      }, {
        "name" : "personalData",
        "type" : "boolean"
      }, {
        "name" : "confidentiality",
        "type" : "string"
      }, {
        "name" : "mcv",
        "type" : "string"
      }, {
        "name" : "maxSeverityLevel",
        "type" : "long"
      }, {
        "name" : "sox",
        "type" : "boolean"
      }, {
        "name" : "icfr",
        "type" : "boolean"
      }, {
        "name" : "assignementGroup",
        "type" : "string"
      }, {
        "name" : "operationalStatus",
        "type" : "string"
      }, {
        "name" : "environments",
        "type" : "string"
      }, {
        "name" : "relationships",
        "type" : {
          "type" : "record",
          "name" : "relationships",
          "fields" : [ {
            "name" : "type",
            "type" : "string"
          }, {
            "name" : "container",
            "type" : {
              "type" : "record",
              "name" : "container",
              "fields" : [ {
                "name" : "name",
                "type" : "string"
              } ]
            }
          } ]
        }
      }, {
        "name" : "components",
        "type" : {
          "type" : "record",
          "name" : "components",
          "fields" : [ {
            "name" : "name",
            "type" : "string"
          }, {
            "name" : "description",
            "type" : "string"
          }, {
            "name" : "exposedAPIs",
            "type" : {
              "type" : "record",
              "name" : "exposedAPIs",
              "fields" : [ {
                "name" : "name",
                "type" : "string"
              }, {
                "name" : "description",
                "type" : "string"
              }, {
                "name" : "type",
                "type" : "string"
              }, {
                "name" : "status",
                "type" : "string"
              } ]
            }
          }, {
            "name" : "consumedAPIs",
            "type" : {
              "type" : "record",
              "name" : "consumedAPIs",
              "fields" : [ {
                "name" : "name",
                "type" : "string"
              }, {
                "name" : "description",
                "type" : "string"
              }, {
                "name" : "status",
                "type" : "string"
              } ]
            }
          } ]
        }
      } ]
    }
  } ]
}

def validate_yaml(yaml_data):
    #schema = eval(open('./schema.yml', 'r').read())
    validator = Schema(schema_val)
    try:
        validator.validate(yaml_data)
        print('YML valid')
        return True
    except SchemaError as se:
        print(se)
        return False

def main():
    kafka_settings = parse_args()
    log.info('Configuration: %s', kafka_settings)
    data = parse_yaml(kafka_settings['data_file'])
    log.info('Data: %s', data)
    
    #ca_content = os.getenv(KAFKA_CA_ENV_VAR)
    #write_ca_file(ca_content, DEFAULT_CA_FILE)
    
    try:
        if(validate_yaml(data)):
            send_to_kafka(settings=kafka_settings, data=data)
            log.info('Data successfully sent')
    except Exception as e:
        print('error')
        print(e)
        #os.remove(DEFAULT_CA_FILE)


if __name__ == '__main__':
    main()
