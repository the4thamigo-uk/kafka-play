#!/bin/bash
source ./env.sh

topic="$1"
key_schema="$(cat ./schema/key.avsc)"
value_schema="$(cat ./schema/value.avsc)"

docker-compose exec -T connect kafka-avro-console-producer --broker-list "$broker_addr" --topic "$topic" --property schema.registry.url="$schema_registry_url" --property parse.key=true --property key.schema="$key_schema" --property value.schema="$value_schema"
