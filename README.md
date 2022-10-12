# ah-ctp-cmdb-action

## Overview

Github action that reads metadata from local repo and pushes in to Kafka topic.

Normally metadata is stored in `system.yml` in yaml format.

## Usage

```yaml
      - name: Kafka sync
        uses: RoyalAholdDelhaize/ah-ctp-cmdb-action@main
        id: git-to-kafka-cmdb
        with:
          bootstrap-servers: bootstrap-octopus-t8.messaging-tst.k8s.digitaldev.nl:443
        env:
          KAFKA_TOPIC_NAME: ${{ secrets.KAFKA_TOPIC_NAME }}
          KAFKA_USERNAME: ${{ secrets.KAFKA_USERNAME }}
          KAFKA_PASSWORD: ${{ secrets.KAFKA_PASSWORD }}
          KAFKA_CA_CONTENT: ${{ secrets.KAFKA_CA_CONTENT }}
```

## Configuration options

There are two possible ways to send configuration data to teh action.
- CLI parameters, this is passed to the action via `with` section in the workflow file
- Environment variables

CLI parameters have higher priority than environment vars.

### Possible CLI parameters

| Parameter | Description |
|-----------|-------------|
|bootstrap-servers| kafka bootstrap server(s) url (host:port) |
|topic-name| kafka topic name |
|data-file| file with application metadata (by default `system.yml`) |
|username| kafka username |
|password| kafka password |

### Environment variables

| Variable | Description |
|-----------|-------------|
|KAFKA_BOOTSTRAP_SERVERS| kafka bootstrap server(s) url (host:port) |
|KAFKA_TOPIC_NAME| kafka topic name |
|DATA_FILE| file with application metadata (by default `system.yml`) |
|KAFKA_USERNAME| kafka username |
|KAFKA_PASSWORD| kafka password |
|KAFKA_CA_CONTENT| the content of CA certificate used for auth in Kafka|

## Meta file data model

<TBD>