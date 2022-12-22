# /usr/bin/env python3

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
from github import Github
from kafka import KafkaProducer
import requests

DEFAULT_DATA_FILE = 'system.yml'
DEFAULT_CA_FILE = 'ca.crt'
DATA_FILE_ENV_VAR = 'DATA_FILE'

# Env variables
KAFKA_TOPIC_NAME_ENV_VAR = 'KAFKA_TOPIC_NAME'
KAFKA_BOOTSTRAP_ENV_VAR = 'KAFKA_BOOTSTRAP_SERVERS'
KAFKA_PASSWD_ENV_VAR = 'KAFKA_PASSWORD'
KAFKA_USERNAME_ENV_VAR = 'KAFKA_USERNAME'
KAFKA_CA_ENV_VAR = 'KAFKA_CA_CONTENT'
KAFKA_VALIDATION_CHECK_ENV_VAR = 'KAFKA_VALIDATION_CHECK'
KAFKA_BYPASS_MODE_ENV_VAR = 'KAFKA_BYPASS_MODE_ENV_VAR'
TOKEN_GITHUB = 'TOKEN_GITHUB'
REPOSITORY_NAME = "REPOSITORY_NAME"

# Kafka settings
KAFKA_TOPIC_DEFAULT_KEY = 'topic2'
KAFKA_SECURITY_PROTOCOL = 'PLAINTEXT'
KAFKA_SASL_MECHANISM = 'SCRAM-SHA-512'

TOPIC_NAME = 'topic27'
BOOTSTRAP_SERVERS_URL = '10.152.183.52:9094'
SCHEMA_REGISTRY_URL = 'http://10.152.183.242:8081'
ORGANIZATION_NAME = 'RoyalAholdDelhaize'
TEAMS_AS_CODE_REPO_NAME = 'sre-teams-configuration'
ACTION_REPO_NAME = 'ah-ctp-kafka-action'

EXIT_OKAY = 0
EXIT_ERORR = 1
EXIT_MISSING = 2

global YAML_DATA

log = logging.getLogger()
logging.basicConfig(level=os.environ.get("LOGLEVEL", "INFO"))


def include_constructor(loader, node):
    selector = loader.construct_sequence(node)
    name = selector.pop(0)

    path = Path(os.getcwd() + name)
    if not path.is_file():
        log.error("Values could not be found at: " + name +
                  " please add the correct path or fill the value in manually")
        exit(EXIT_ERORR)

    with open(os.getcwd() + name) as f:
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
    parser = argparse.ArgumentParser(
        description='Send system properties to Kafka topic')
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


def write_ca_file(content: str, filename: str = DEFAULT_CA_FILE):
    with open(filename, mode='w', encoding='utf-8') as file:
        file.truncate()
        file.write(content)


def send_to_kafka(settings: dict, data: dict):
    global YAML_DATA

    with open('/avro_schema.avsc') as f:
        schema_str = f.read()

    schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})

    avro_serializer = AvroSerializer(schema_registry_client, schema_str)

    string_serializer = StringSerializer('utf_8')

    producer = Producer({'bootstrap.servers': BOOTSTRAP_SERVERS_URL})
    if "name" in YAML_DATA.keys():
        key_value = YAML_DATA["name"]
    else:
        key_value = YAML_DATA["containers"][0]["parentSystemName"]

    producer.produce(topic=TOPIC_NAME, key=string_serializer(key_value, None), value=avro_serializer(
        data, SerializationContext(TOPIC_NAME, MessageField.VALUE)))
    # producer.produce(topic=TOPIC_NAME, key=string_serializer(YAML_DATA['name'], None), value=data)

    producer.flush()


def add_value(key, container_index=0):
    log.info('add value ' + str(key) +
             ' for container: ' + str(container_index))
    global YAML_DATA
    if (key == 'technology'):
        YAML_DATA['containers'][container_index][key] = str(
            find_main_language())
    elif (key == 'icfr'):
        YAML_DATA['containers'][container_index][key] = False
    elif (key == 'hostedAt'):
        YAML_DATA['containers'][container_index][key] = "Azure Cloud"
    elif (key == 'team'):
        YAML_DATA['containers'][container_index][key] = find_team()
    elif (key == 'productOwner'):
        YAML_DATA['containers'][container_index][key] = find_product_owner(
            'product-owner')
    elif (key == 'maxSeverityLevel'):
        mcv = YAML_DATA['containers'][container_index]["missionCriticality"]
        if mcv == "High":
            YAML_DATA['containers'][container_index][key] = 1
        elif mcv == "Medium":
            YAML_DATA['containers'][container_index][key] = 2
        elif mcv == "Low":
            YAML_DATA['containers'][container_index][key] = 3
        else:
            YAML_DATA['containers'][container_index][key] = 4
    elif (key == 'githubURL'):
        YAML_DATA['containers'][container_index][key] = 'giturl'
    elif (key == 'deploymentModel'):
        YAML_DATA['containers'][container_index][key] = 'Custom'


def update_product_owners():
    github_client = Github(os.getenv(TOKEN_GITHUB))
    teams_as_code = github_client.get_organization(
        ORGANIZATION_NAME).get_repo(TEAMS_AS_CODE_REPO_NAME)

    persons = teams_as_code.get_contents(path='persons')

    person_dict = {}

    for person in persons:
        content = person.decoded_content.decode("utf-8")
        yaml_content = yaml.safe_load(content)
        name = yaml_content['person']['name']
        info = {'teams': yaml_content['person']['teams'],
                'roles': yaml_content['person']['roles']}
        person_dict[name] = info

    with open('/persons.yml', 'w') as outfile:
        yaml.safe_dump(person_dict, outfile)


def find_product_owner(role):
    global YAML_DATA

    # Fetch the persons file from the action repository releases
    github_client = Github(os.getenv(TOKEN_GITHUB))
    person_repository = github_client.get_organization(
        ORGANIZATION_NAME).get_repo(ACTION_REPO_NAME)

    url = person_repository.get_latest_release().get_assets().get_page(0)[
        0].url
    header = {'Authorization': 'Bearer ' +
              os.getenv(TOKEN_GITHUB), 'Accept': 'application/octet-stream'}

    response = requests.get(url, headers=header,
                            allow_redirects=False, stream=True)
    # If found get the location url from the Location header
    if response.status_code == 302:
        response = requests.get(
            response.headers['Location'], allow_redirects=False, stream=True)
    data = yaml.safe_load(response.content.decode())

    for k, v in data.items():
        if v['teams'] is None:
            continue
        if find_team() in v['teams']:
            if role in v['roles']:
                return k
    return None


def validate_keys_values(object):
    if isinstance(object, list):
        for item in object:
            validate_keys_values(item)

    return


def find_team():
    path = Path(os.getcwd() + '/CODEOWNERS')
    if not path.is_file():
        log.error(
            "CODEOWNERS file could not be found, please manually fill in team")
        exit(EXIT_ERORR)

    with open(os.getcwd() + '/CODEOWNERS') as f:
        code = f.read()

    matches = re.findall(
        r"(CODEOWNERS|\*)[ \t]+(@RoyalAholdDelhaize\/)(.*)", code)

    return matches[0][-1]


def check_value(key, container_index=0, container=False):
    log.info("Checking for key: " + str(key) +
             ' with container: ' + str(container))
    global YAML_DATA
    if (not container):
        found = False
        # Check if the keys are under the parent
        for k, v in YAML_DATA.items():
            if (k == key):
                found = True
                return
        # If not under the parent check each container
        if not found:
            for container in YAML_DATA['containers']:
                for k, v in container.items():
                    if k == key:
                        return
        # Chech if keys are under the parent objects
        for k, v in YAML_DATA['targetConsumers'].items():
            if (k == key):
                return
        for k, v in YAML_DATA['dataClassification'].items():
            if (k == key):
                return

        for container in YAML_DATA['containers']:
            found = False
            for k, v in container.items():
                if k == key:
                    found = True
            if not found:
                log_error('Please add the ' + str(key) +
                          ' object/key', EXIT_MISSING)
    # TODO: FIX THIS, LOOP THROUGH CONTAINER ITEMS
    elif (key == 'targetConsumers' and container):
        log.info("YAML KEYS")
        log.info(YAML_DATA.items())
        found = False
        for k, v in YAML_DATA.items():
            log.info("INDIVIDUAL KEYS AND VALUES")
            log.info("KEY: " + str(k) + " VALUE " + str(v))
            if (k == key):
                found = True
        if not found:
            log_error(
                'Please fill in the targetConsumers object on the parent level or override it in the container', EXIT_MISSING)
    elif (key == 'dataClassification' and container):
        found = False
        for k, v in YAML_DATA.items():
            if (k == key):
                found = True
        if not found:
            log_error(
                'Please fill in the dataClassification object on the parent level or override it in the container', EXIT_MISSING)
    elif (container):
        found = False
        for k, v in YAML_DATA['targetConsumers'].items():
            if (k == key):
                if YAML_DATA['targetConsumers'][key] is None:
                    log_error('Please provide a value for ' +
                              str(key) + ' or define it at parent level')
                found = True
        for k, v in YAML_DATA['dataClassification'].items():
            if (k == key):
                if YAML_DATA['dataClassification'][key] is None:
                    log_error('Please provide a value for ' +
                              str(key) + ' or define it at parent level')
                found = True
        if not found:
            log_error('Please provide a value for ' +
                      str(key) + ' or define it at parent level')


def validate_yaml(yaml_data, verbose=False):
    parent_schema_val = {
        Optional("name"): str,
        Optional("description"): str
    }

    first_validator = Schema(parent_schema_val)

    try:
        # if 'targetConsumers' and 'dataClassification' in YAML_DATA.items():
        #     first_validator.validate(dict(islice(yaml_data.items(), 0, 4)))
        # else:
        if "name" and "description" not in yaml_data.keys():
            for container in yaml_data["containers"]:
                if "parentSystemName" not in container.keys():
                    log_error(
                        "Please define the parentSystemName key with value or fill in the parent name and description", EXIT_MISSING)
        else:
            first_validator.validate(dict(islice(yaml_data.items(), 0, 2)))

        # Validate each container seperatly for replacing the values
        for index, container in enumerate(yaml_data["containers"]):
            container_schema_val = {
                "name": str,
                "synonyms": str,
                "description": str,
                "repositoryName": str,
                Optional("technology", default=lambda: add_value('technology', index)): str,
                Optional("team", default=lambda: add_value('team', index)): str,
                Optional("productOwner", default=lambda: add_value('productOwner', index)): str,
                Optional("parentSystemName", default=lambda: add_value('parentSystemName', index)): str,
                Optional("targetConsumers", default=lambda: check_value('targetConsumers', index, True)): {
                    Optional("customer", default=lambda: check_value('customer', index, True)): bool,
                    Optional("softwareSystem", default=lambda: check_value('softwareSystem', index, True)): bool,
                    Optional("thirdParty", default=lambda: check_value('thirdParty', index, True)): bool,
                    Optional("business", default=lambda: check_value('business', index, True)): bool,
                    Optional("developer", default=lambda: check_value('developer', index, True)): bool,
                },
                Optional("hostedAt", default=lambda: add_value('hostedAt', index)): Or("Amazon Web Services (AWS Cloud)", "AT&T", "Azure CF1", "Azure CF2", "Azure Cloud", "DXC", "Equinix", "Google Cloud Platform", "Hybric", "Inlumi", "Local server", "Multi-Cloud", "Not Applicable", "Other", "Salesforce", "ServiceNow", "Solvinity", "Unit4", "Unknown", "User device", "Azure"),
                Optional("deploymentModel", default=lambda: add_value('deploymentModel', index)): Or("BPO", "CaaS", "IaaS", "Custom", "PaaS", "SaaS"),
                Optional("dataClassification", default=lambda: check_value('dataClassification', index, True)): {
                    Optional("containsPersonalData", default=lambda: check_value('containsPersonalData', index, True)): bool,
                    Optional("containsFinancialData", default=lambda: check_value('containsFinancialData', index, True)): bool,
                    Optional("publiclyExposed", default=lambda: check_value('publiclyExposed', index, True)): bool,
                    Optional("restrictedAccess", default=lambda: check_value('restrictedAccess', index, True)): bool,
                },
                "missionCriticality": Or("High", "Medium", "Low", "None"),
                Optional("maxSeverityLevel", default=lambda: add_value('maxSeverityLevel', index)): Or(1, 2, 3, 4, "None"),
                Optional("icfr", default=lambda: add_value('icfr', index)): bool,
                "assignementGroup": str,
                # operational = deployed to prod, pipelined = in development not yet released
                "operationalStatus": Or("Pipelined", "Operational", "Non-Operational", "Submitted for decommissioning", "Decommissioned", "In decommissioning process"),
                "components": [{
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
                        Optional("read", default=False): bool,
                        Optional("write", default=False): bool,
                        Optional("execute", default=False): bool,
                    }]
                }],
            }
            container_validator = Schema(container_schema_val)
            container_validator.validate(container)

        if (verbose):
            log.info('YML valid')
        return True
    except SchemaError as se:
        if (verbose):
            log.error(se)
        return False


def find_main_language(full_output=False):
    languages = parse_yaml("/languages.yml")
    matches = defaultdict(int)
    for root, directory, filenames in os.walk(os.getcwd()):
        for filename in filenames:
            for key, value in languages.items():
                for type in value:
                    if re.search(f"\.({type}$)", filename):
                        size = os.path.getsize(root + '/' + filename)
                        matches[key] += size
    if (full_output):
        return matches
    else:
        return max(matches, key=matches.get)


def remove_none(obj):
    if isinstance(obj, (list, tuple, set)):
        return type(obj)(remove_none(x) for x in obj if x is not None or '')
    elif isinstance(obj, dict):
        return type(obj)((remove_none(k), remove_none(v))
                         for k, v in obj.items() if k is not None and v is not None or '')
    else:
        return obj


def replace_key(data, keys, index=0):
    temp_data = {}

    for i, old_key in enumerate(data):
        list_data = list(data.values())
        temp_data[keys[i + index]] = list_data[i]

    return temp_data


def translate_keys(data):
    first_data = replace_key(dict(islice(data.items(), 2)), [
                             'name', 'description'])
    containers = list()

    for container in data["containers"]:
        first_container = replace_key(dict(islice(container.items(), 0, 10)), [
                                      'name', 'synonyms', 'description', 'technology', 'team', 'productOwner', 'applicationType', 'hostedAt', 'deploymentModel', 'dataClassification'])
        first_container['dataClassification'] = replace_key(first_container['dataClassification'], [
                                                            'containsPersonalData', 'containsFinancialData', 'publiclyExposed', 'restrictedAccess'])
        second_container = replace_key(dict(islice(container.items(), 10, 14)), [
                                       'missionCriticality', 'assignementGroup', 'operationalStatus', 'components'])

        component_list = list()
        for component in container['components']:

            component = replace_key(
                component, ["name", "description", "exposedAPIs", "consumedAPIs"])

            exposedAPI_list = list()

            for value in component["exposedAPIs"]:
                exposedAPI_list.append(replace_key(
                    value, ["name", "description", "type", "status"]))

            component["exposedAPIs"] = exposedAPI_list

            consumedAPI_list = list()
            for value in component["consumedAPIs"]:
                consumedAPI_list.append(replace_key(
                    value, ["name", "description", "status", "read", "write", "execute"]))

            component["consumedAPIs"] = consumedAPI_list
            component_list.append(component)

        second_container['components'] = component_list

        first_container.update(second_container)

        containers.append(first_container)

    first_data["containers"] = containers

    return first_data


def validate_url_name(value):
    global YAML_DATA
    if os.getenv(KAFKA_BYPASS_MODE_ENV_VAR):
        return

    for containers in YAML_DATA['containers']:
        for container in value['containers']:
            if (container['name'] == containers['name'] or container['githubURL'] == containers['githubURL']):
                return True
    return False


def validate_names():
    global YAML_DATA
    with open('/avro_schema.avsc') as f:
        schema_str = f.read()

    schema_registry_client = SchemaRegistryClient({'url': SCHEMA_REGISTRY_URL})

    avro_deserializer = AvroDeserializer(schema_registry_client, schema_str)

    string_deserializer = StringDeserializer('utf_8')

    config = {'bootstrap.servers': BOOTSTRAP_SERVERS_URL,
              'group.id': str(uuid.uuid4()),
              'auto.offset.reset': 'earliest',
              'value.deserializer': avro_deserializer,
              'key.deserializer': string_deserializer}
    consumer = DeserializingConsumer(config)
    explosedAPIs = list()
    complete_containers = list()
    try:
        consumer.subscribe([TOPIC_NAME])

        topic_partition = TopicPartition(TOPIC_NAME, partition=0)
        low, high = consumer.get_watermark_offsets(topic_partition)
        current_offset = 0

        if high == 0:
            return

        log.info('Consuming data to see if data is already present')
        while current_offset < high:
            message = consumer.poll(timeout=1.0)
            message_content = remove_none(message.value())
            current_offset += 1
            if message_content is None:
                continue
            if message.error():
                log.error('Error when handling message: ' +
                          str(message_content))
            else:
                # Check if the message is the same as the current data
                if message_content == YAML_DATA:
                    log.info('Data is already present and validated')
                    return
                elif "name" in YAML_DATA.keys():
                    # Check if container is already stored, if not add it
                    if "name" in message_content.keys():
                        if message_content['name'] == YAML_DATA['name']:
                            containers = list()
                            # Loop through the current data's containers
                            for container in YAML_DATA['containers']:
                                # Loop through the containers in the message
                                for stored_container in message_content['containers']:
                                    # If container is not the same, add the new container to the list
                                    if container != stored_container:
                                        if not check_object_in_list(containers, container):
                                            containers.append(container)
                                    elif not check_object_in_list(containers, container):
                                        containers.append(stored_container)
                            # Loop through message containers to make sure their all in the list
                            for container in message_content['containers']:
                                # found = False
                                if not check_object_in_list(containers, container):
                                    containers.append(container)
                            # Set the new containers
                            YAML_DATA['containers'] = containers
                # Get the parent system name, find if the system exists, if so add the containers to the system object if they don't contain them already
                for container in message_content["containers"]:
                    # Check if the message has a system name
                    if "name" in message_content.keys():
                        for new_container in YAML_DATA["containers"]:
                            if "parentSystemName" in new_container.keys():
                                if new_container["parentSystemName"] == message_content["name"]:

                                    # check if container already is in message
                                    # if not add to message
                                    if container == new_container:
                                        if not check_object_in_list(complete_containers, container):
                                            complete_containers.append(
                                                container)
                                    elif container["name"] == new_container["name"]:
                                        if not check_object_in_list(complete_containers, new_container):
                                            new_container.pop(
                                                "parentSystemName")
                                            complete_containers.append(
                                                new_container)

                if "name" in message_content.keys() and len(complete_containers) > 0:
                    YAML_DATA = message_content
                    YAML_DATA["containers"] = complete_containers

                for containers in message_content["containers"]:
                    for component in containers["components"]:
                        for exposed in component["exposedAPIs"]:
                            explosedAPIs.append(exposed)

        for containers in YAML_DATA["containers"]:
            for component in containers['components']:
                for consumedAPI in component["consumedAPIs"]:
                    found = False
                    for exposedAPI in explosedAPIs:
                        if consumedAPI["name"] == exposedAPI["name"]:
                            found = True
                            continue
                    if not found:
                        log.error("consumed API: " +
                                  consumedAPI["name"] + " Not found in system")
    except Exception as e:
        log.error(e)
    finally:
        consumer.close()
    return True


def log_error(message, exit_code):
    log.error(str(message))
    exit(exit_code)


def check_object_in_list(object_list, object):
    for item in object_list:
        if item == object:
            return True
    return False


def move_objects_to_container(data, keys):
    temp_data = {}

    for key in keys:
        if key not in data.keys():
            continue
        temp_data[key] = data[key]

        for container in data["containers"]:
            if key not in container.keys():
                container[key] = temp_data[key]

            for k, v in container[key].items():
                if v is None:
                    if k in temp_data[key].keys():
                        container[key][k] = temp_data[key][k]
                    else:
                        log_error("Please fill in " + str(k) +
                                  " key on the container level or parent level", EXIT_MISSING)
            for k, v in temp_data[key].items():
                if k not in container[key].keys():
                    if v is not None:
                        container[key][k] = v
                    else:
                        log_error("Please fill in " + str(k) +
                                  " key on the container level or parent level", EXIT_MISSING)
        data.pop(key)
    return data


def move_values_to_container(data, keys):
    for key in keys:
        # Check if key is on the parent level of the data
        if key in data.keys():
            # If the key doesn't have a value remove it
            if data[key] == None:
                data.pop(key)
                continue
            # Check if the container contains the key
            for container in data["containers"]:
                if key in container.keys():
                    # If the key is in the container object but does not have a value set the value from the parent
                    if container[key] == None:
                        container[key] = data[key]
                        continue
                else:
                    # If the container does not contain the key, set the value from the parent
                    container[key] = data[key]
            # Finally remove the key from the parent object
            data.pop(key)
    return data

def set_repository_name(data):

    for container in data["containers"]:
        container["repositoryName"] = str(re.findall('\/(.*)', os.getenv(REPOSITORY_NAME))[0])

    return data

def main():
    kafka_settings = parse_args()
    log.info('Configuration: %s', kafka_settings)
    data = parse_yaml(kafka_settings['data_file'])
    global YAML_DATA
    YAML_DATA = data

    # log.info("Validationcheck " + str(os.getenv(KAFKA_VALIDATION_CHECK_ENV_VAR)))
    log.info('Data: %s', YAML_DATA)
    # Validate before translate
    # YAML_DATA = translate_keys(YAML_DATA)
    YAML_DATA = move_objects_to_container(
        YAML_DATA, ["targetConsumers", "dataClassification"])
    YAML_DATA = move_values_to_container(
        YAML_DATA, ["team", "technology", "productOwner"])
    YAML_DATA = remove_none(YAML_DATA)
    YAML_DATA = set_repository_name(YAML_DATA)

    validate_yaml(YAML_DATA)

    YAML_DATA = remove_none(YAML_DATA)

    validate_names()

    if os.getenv(KAFKA_VALIDATION_CHECK_ENV_VAR):
        validate_yaml(YAML_DATA)

        exit(EXIT_OKAY)

    # ca_content = os.getenv(KAFKA_CA_ENV_VAR)
    # write_ca_file(ca_content, DEFAULT_CA_FILE)
    try:
        if (validate_yaml(YAML_DATA, True) or os.getenv(KAFKA_BYPASS_MODE_ENV_VAR)):
            send_to_kafka(settings=kafka_settings, data=YAML_DATA)
            log.info('Data successfully sent, data: %s', YAML_DATA)
            exit(EXIT_OKAY)
        else:
            # Exit code 2 since the data is missing or invalid
            exit(2)
    except Exception as e:
        # Print error and generic exit code 1
        raise e
        exit(EXIT_ERORR)
        # os.remove(DEFAULT_CA_FILE)


if __name__ == '__main__':
    main()
